// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Partial aggregate stream for ordered group input.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{OrderedAggregateTable, PartialMarker};
use super::hash_stream::AggregateMemoryMetrics;
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, MetricBuilder, SpillMetrics};
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream, metrics};

/// Partial aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// If the input is ordered by `k`, the aggregate can use ordered partial and
/// final stages:
///
/// ## Plan
/// AggregateExec(stage=final, ordered)
/// -- RepartitionExec(hash(k), preserves_order=true)
/// ---- AggregateExec(stage=partial, ordered)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (for example, `AVG(x)` emits `SUM(x)`
/// and `COUNT(x)`)
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (for example, `AVG(x)` calculated from the
/// state)
///
/// # Order-based Optimization
///
/// For the aggregation work, the hash aggregation implementation is reused.
///
/// After each input batch, check whether any groups can be emitted eagerly to
/// improve memory efficiency. For example, if the last group key seen is
/// `k = 100`, it is safe to emit all groups with keys less than 100 because the
/// input is ordered.
///
/// # Memory Pressure and Spilling
///
/// ## Fully ordered case
///
/// If the input is ordered by every group key, for example:
///
/// - Input order: `a, b`
/// - `GROUP BY`: `a, b`
///
/// Completed groups can be emitted as soon as the next group is observed. Thus,
/// only the current group remains active after completed groups are emitted, and
/// memory usage does not grow with the total number of groups.
///
/// If a reservation is refused, partial states, including the current incomplete
/// group, may be emitted early. The final aggregate merges repeated keys, so this
/// preserves the same ordered state semantics without requiring a disk spill.
///
/// ## Partially ordered case
///
/// If the input is ordered by only a subset of the group keys, for example:
///
/// - Input order: `a`
/// - `GROUP BY`: `a, b`
///
/// If one `a` value contains many distinct `b` values, the table may accumulate
/// enough groups to exceed the memory limit.
///
/// - `OrderedPartialAggregateStream`: On reservation failure, it emits all current
///   intermediate states downstream and resets the table. The final stage can
///   merge repeated `(a, b)` state rows, so no disk spill is required.
/// - `OrderedFinalAggregateStream`: It cannot emit incomplete final results. On
///   reservation failure, it sorts the current intermediate states by the complete
///   group key and spills them as one run. After the input ends, it spills any
///   remaining states, performs a sort-preserving merge of all runs, and feeds the
///   merged input into a fully ordered final aggregate stream.
///
/// ## Implementation Note
///
/// This is intentionally kept simple and closely maps to
/// `GroupedHashAggregateStream` to finish the refactor sooner.
///
/// See issue for details: <https://github.com/apache/datafusion/issues/22710>
///
pub(crate) struct OrderedPartialAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    /// Records only this aggregate consumer, including siblings used by spill replay.
    memory_metrics: AggregateMemoryMetrics,
    baseline_metrics: BaselineMetrics,
    reduction_factor: metrics::RatioMetrics,
    table: Option<OrderedAggregateTable<PartialMarker>>,
}

impl OrderedPartialAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, AggregateMode::Partial);
        debug_assert_ne!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let memory_metrics =
            AggregateMemoryMetrics::new(&agg.metrics, partition, context.memory_pool());

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let table = OrderedAggregateTable::<PartialMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;
        let reservation =
            MemoryConsumer::new(format!("OrderedPartialAggregateStream[{partition}]"))
                // Partial states may be emitted early for any input ordering.
                .with_can_spill(true)
                .register(&memory_metrics.pool());

        Ok(Self {
            schema,
            input,
            reservation,
            memory_metrics,
            baseline_metrics,
            reduction_factor,
            table: Some(table),
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema_clone = Arc::clone(&self.schema);

        let cloned_metrics = self.baseline_metrics.clone();
        let memory_metrics = self.memory_metrics.clone();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            self.create_stream(),
        ));

        memory_metrics.observe_stream(Box::pin(ObservedStream::new(
            stream,
            cloned_metrics,
            None,
        )))
    }

    /// Entry point for the ordered partial aggregate state machine.
    ///
    /// See comments in [`OrderedPartialAggregateStream`] for high-level ideas.
    ///
    /// State transitions are implemented using the generator pattern; see the comments in [`async_try_stream`].
    ///
    /// Conceptual state-transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling ordered input and aggregating batches
    ///      into the ordered partial aggregate table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one input batch. If the ordering proves some groups are
    ///      complete, yield one partial-state batch immediately, then continue
    ///      reading input. Otherwise continue directly with the next input batch.
    ///   -> DrainingFinal
    ///      Input was exhausted. Mark the table input as done so every remaining
    ///      group is safe to emit.
    ///
    /// DrainingFinal
    ///   -> DrainingFinal
    ///      One remaining partial-state batch was yielded; repeat to continue
    ///      draining the table.
    ///   -> Done
    ///      All remaining groups were emitted.
    ///
    /// Done
    ///   -> (end)
    /// ```
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            let mut table = self
                .table
                .take()
                .expect("OrderedPartialAggregateStream state should not be None");

            self.handle_reading_input(&mut table, &mut emitter).await?;

            // Input has exhausted, move to the final draining stage.
            self.close_input();
            table.input_done();

            self.handle_draining_final(table, &mut emitter).await?;

            Ok(())
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// Consumes one ordered input batch, then immediately emits completed groups
    /// if the ordering proves any group is ready.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn handle_reading_input(
        &mut self,
        table: &mut OrderedAggregateTable<PartialMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            let input_rows = batch.num_rows();
            self.reduction_factor.add_total(input_rows);

            let timer = elapsed_compute.timer();
            let taken = table.aggregate_batch(&batch)?;
            drop(timer);
            if let Some(taken) = taken {
                self.reservation.resize(
                    self.reservation.size().max(
                        table
                            .memory_size()
                            .saturating_add(taken.get_array_memory_size()),
                    ),
                );
                self.reduction_factor.add_part(taken.num_rows());
                emitter.emit(taken).await;
                // The current input batch was aggregated after taking the old keys.
                // Check its future emit budget too, including when this is the last input.
            }

            if let Some(batch) = self.resize_or_take_state_batch(table)? {
                self.reduction_factor.add_part(batch.num_rows());
                emitter.emit(batch).await;
                continue;
            }

            while let Some(batch) = table.next_output_batch()? {
                self.reduction_factor.add_part(batch.num_rows());
                self.reservation.resize(
                    self.reservation.size().max(
                        table
                            .memory_size()
                            .saturating_add(batch.get_array_memory_size()),
                    ),
                );
                emitter.emit(batch).await;
            }
        }

        Ok(())
    }

    /// Reserve retained state and its emit workspace. On a refused reservation,
    /// materialize every partial state and reset the table, for either full or
    /// prefix ordering. Partial output may repeat an active key: its downstream
    /// final stage merges those states in input order.
    ///
    /// The previous successful grant covered the old table and its emit workspace.
    /// After one new input batch, refusal can therefore add at most that input
    /// quantum's state/materialization before resetting. Account the resulting
    /// allocated output until handover; do not retain an ever-growing active group.
    fn resize_or_take_state_batch(
        &mut self,
        table: &mut OrderedAggregateTable<PartialMarker>,
    ) -> Result<Option<RecordBatch>> {
        let oom = match self
            .reservation
            .try_resize(table.memory_size().saturating_add(table.emit_size()))
        {
            Ok(()) => return Ok(None),
            Err(e @ DataFusionError::ResourcesExhausted(_)) => e,
            Err(e) => return Err(e),
        };

        if table.is_empty() {
            // An empty input batch may leave only constructor buffers. They already
            // exist and need no future materialization, so a refused first charge
            // must not turn an empty partial aggregate into a task failure.
            self.reservation.resize(table.memory_size());
            return Ok(None);
        }

        let Some(batch) = table.take_state_batch()? else {
            return Err(oom);
        };
        self.reservation.resize(
            table
                .memory_size()
                .saturating_add(batch.get_array_memory_size()),
        );
        Ok(Some(batch))
    }

    /// Emits one batch after input is exhausted.
    ///
    /// `table.input_done()` has already made every remaining group safe to emit,
    /// so this state keeps draining until the table is empty.
    ///
    /// See comments at [`Self::create_stream`] for details.
    ///
    async fn handle_draining_final(
        &mut self,
        mut table: OrderedAggregateTable<PartialMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut timer = elapsed_compute.timer();

        while let Some(batch) = table.next_output_batch()? {
            self.reduction_factor.add_part(batch.num_rows());

            if table.is_empty() {
                // Clear memory before emitting last batch so we don't have to wait for next poll to clear
                drop(table);
                let _ = self.reservation.try_resize(0);
                drop(timer);

                emitter.emit(batch).await;

                return Ok(());
            }

            self.reservation.resize(
                self.reservation.size().max(
                    table
                        .memory_size()
                        .saturating_add(batch.get_array_memory_size()),
                ),
            );

            timer.done();
            emitter.emit(batch).await;
            timer = elapsed_compute.timer();
        }

        // was empty
        Ok(())
    }
}
