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

//! Common utilities for aggregate tables used in aggregations that inputs are ordered
//! by the groups.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Fields, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::assert_or_internal_err;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;

use crate::InputOrderMode;
use crate::PhysicalExpr;
use crate::aggregates::group_values::{GroupByMetrics, GroupValues, new_group_values};
use crate::aggregates::grouped_hash_stream::create_group_accumulator;
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy, aggregate_expressions,
    evaluate_group_by,
};

use super::common::{
    AggregateAccumulator, EMIT_BYTES_PER_GROUP, EvaluatedAggregateBatch,
    MaterializedAggregateOutput, column_group_key,
};

/// Aggregate table shared by the ordered partial and final paths.
///
/// # Ordering optimization
///
/// The table consumes input batches while `GroupOrdering` tracks which groups
/// are proven complete. Completed groups can be emitted before the input stream
/// ends, which keeps memory bounded by the active ordered key range.
///
/// # Partial and final variant difference
///
/// The partial and final aggregate tables implement the two stages of grouped
/// aggregation. See
/// [`OrderedPartialAggregateStream`](crate::aggregates::ordered_partial_stream::OrderedPartialAggregateStream)
/// for the high-level plan shape.
///
/// Example: `AVG(v) FILTER (WHERE v>0) GROUP BY k`
///
/// Partial table ([`AggregateMode::Partial`], with optional filter from query):
/// - Input rows: `k, v`
/// - Table stores: `k, sum(v), count(v)`
/// - Output schema: `k, sum(v), count(v)`
///
/// Final table ([`AggregateMode::Final`], no filters):
/// - Input rows: `k, sum(v), count(v)`
/// - Table stores: `k, sum(v), count(v)`
/// - Output schema: `k, avg(v)`
///
/// # Marker Type
///
/// `OrderedAggrMode` selects the aggregate semantics. For example,
/// `OrderedAggregateTable::<PartialMarker>::new(...)` consumes raw rows
/// and emits partial states, while
/// `OrderedAggregateTable::<FinalMarker>::new_with_input_order(...)`
/// consumes partial states and emits final values.
///
/// Shared methods live on `impl<T>`; partial/final behavior lives on
/// marker-specific impls.
pub(in crate::aggregates) struct OrderedAggregateTable<OrderedAggrMode> {
    /// Output schema: group columns followed by aggregate state or final values.
    pub(super) output_schema: SchemaRef,

    /// Intermediate-state schema used when memory pressure requires the table
    /// to pass through or spill its current state.
    pub(super) state_schema: SchemaRef,

    /// Maximum rows per emitted output batch, from config `batch_size`.
    pub(super) batch_size: usize,
    group_key_offset_limit: usize,

    /// Grouping and accumulator-specific timing metrics.
    pub(super) group_by_metrics: GroupByMetrics,

    /// Group keys, ordering state, and accumulator states.
    pub(super) buffer: OrderedAggregateTableBuffer,

    pending: Option<MaterializedAggregateOutput>,

    /// State field groups for a final wrapper whose value packs its exact state.
    /// Pending batches keep the raw state schema, making pressure-driven spilling safe.
    struct_state_fields: Option<Vec<Fields>>,
    _mode: PhantomData<OrderedAggrMode>,
}

/// Buffer for the ordered aggregate table's group keys and accumulator states.
///
/// It accumulates input during aggregation and emits output rows as soon as the
/// input ordering proves those groups are complete.
///
/// [`GroupOrdering`] tracks when and how to do early emit.
/// [`GroupValues`] stores the physical group-key layout, while
/// [`datafusion_expr::GroupsAccumulator`] stores per-group aggregate state.
pub(super) struct OrderedAggregateTableBuffer {
    /// GROUP BY expressions evaluated against input batches.
    pub(super) group_by: Arc<PhysicalGroupBy>,

    /// Tracks how far ordered input allows this table to drain safely.
    pub(super) group_ordering: GroupOrdering,

    /// Interned group keys, in the same group-id order used by accumulators.
    pub(super) group_values: Box<dyn GroupValues>,

    /// Scratch group id vector for the current input batch.
    pub(super) group_indices: Vec<usize>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    pub(super) accumulators: Vec<AggregateAccumulator>,
}

/// Methods shared by all aggregate modes
impl<AggrMode> OrderedAggregateTable<AggrMode> {
    #[expect(
        clippy::too_many_arguments,
        reason = "keeps ordered partial and final table construction explicit"
    )]
    pub(super) fn new_for_mode(
        agg: &AggregateExec,
        input_schema: &SchemaRef,
        output_schema: SchemaRef,
        state_schema: SchemaRef,
        batch_size: usize,
        input_order_mode: &InputOrderMode,
        aggregate_mode: &AggregateMode,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
        group_by_metrics: GroupByMetrics,
    ) -> Result<Self> {
        assert_or_internal_err!(
            batch_size > 0,
            "OrderedAggregateTable requires config batch_size >= 1"
        );

        let group_ordering = GroupOrdering::try_new(input_order_mode)?;
        let group_schema = agg.group_by.group_schema(input_schema)?;
        let group_values = new_group_values(group_schema, &group_ordering)?;
        let aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            aggregate_mode,
            agg.group_by.num_group_exprs(),
        )?;
        let accumulators = agg
            .aggr_expr
            .iter()
            .zip(aggregate_arguments)
            .zip(filters)
            .map(|((agg_expr, arguments), filter)| {
                let accumulator = create_group_accumulator(agg_expr)?;
                Ok(AggregateAccumulator::new(
                    Arc::clone(agg_expr),
                    arguments,
                    filter,
                    accumulator,
                    agg.struct_state_output,
                ))
            })
            .collect::<Result<_>>()?;

        let struct_state_fields = if agg.struct_state_output {
            let fields = agg.aggr_expr.iter().map(|expr| {
                let fields = Fields::from(expr.state_fields()?);
                assert_or_internal_err!(
                    expr.field().data_type() == &DataType::Struct(fields.clone())
                        && !expr.field().is_nullable(),
                    "Struct-state output requires a non-null struct of the exact state fields"
                );
                Ok(fields)
            }).collect::<Result<Vec<_>>>()?;
            Some(fields)
        } else {
            None
        };

        Ok(Self {
            output_schema,
            state_schema,
            batch_size,
            group_key_offset_limit: agg.group_key_offset_limit(),
            group_by_metrics,
            buffer: OrderedAggregateTableBuffer {
                group_by: Arc::clone(&agg.group_by),
                group_ordering,
                group_values,
                group_indices: vec![],
                accumulators,
            },
            pending: None,
            struct_state_fields,
            _mode: PhantomData,
        })
    }

    /// Evaluates all group by keys and accumulator args.
    ///
    /// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function
    /// evaluates `k+1`, `v*v`.
    pub(super) fn evaluate_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluatedAggregateBatch> {
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        let grouping_set_args = evaluate_group_by(&self.buffer.group_by, batch)?;
        drop(timer);

        let timer = self.group_by_metrics.aggregate_arguments_time.timer();
        let accumulator_args = self
            .buffer
            .accumulators
            .iter()
            .map(|acc| acc.evaluate_acc_args(batch))
            .collect::<Result<Vec<_>>>()?;
        drop(timer);

        Ok(EvaluatedAggregateBatch {
            grouping_set_args,
            accumulator_args,
        })
    }

    /// Called after the input stream is exhausted and the last batch has been
    /// aggregated.
    ///
    /// Updates the internal `GroupOrdering` so it can continue emitting until
    /// the buffer is empty.
    pub(in crate::aggregates) fn input_done(&mut self) {
        self.buffer.group_ordering.input_done();
    }

    /// Number of groups currently buffered.
    pub(in crate::aggregates) fn num_groups(&self) -> usize {
        self.buffer.group_values.len()
    }

    /// Completed materialized output must be drained before pulling more input,
    /// preventing retained ranges from being combined/copied during later pressure.
    #[cfg(test)]
    pub(in crate::aggregates) fn has_pending_output(&self) -> bool {
        self.pending.is_some()
    }

    /// Check if there is zero groups accumulated so far.
    pub(in crate::aggregates) fn is_empty(&self) -> bool {
        self.num_groups() == 0
            && self
                .pending
                .as_ref()
                .is_none_or(MaterializedAggregateOutput::is_exhausted)
    }

    /// All internal buffer's memory size.
    pub(in crate::aggregates) fn memory_size(&self) -> usize {
        self.buffer
            .accumulators
            .iter()
            .map(|acc| acc.size())
            .sum::<usize>()
            + self.buffer.group_values.size()
            + self.buffer.group_ordering.size()
            + self.buffer.group_indices.allocated_size()
            + self
                .pending
                .as_ref()
                .map_or(0, MaterializedAggregateOutput::memory_size)
    }

    /// Extra materialization headroom, separate from retained table/pending buffers.
    /// Packed state structs are conservatively treated as copying, preserving all
    /// variable/nested inner states without depending on a specific wrapper UDAF.
    pub(in crate::aggregates) fn emit_size(&self) -> usize {
        let groups = self.buffer.group_values.len();
        if groups == 0 {
            return 0;
        }
        let accumulators = self
            .buffer
            .accumulators
            .iter()
            .map(AggregateAccumulator::emit_size)
            .sum::<usize>();
        let keys = if self
            .state_schema
            .fields()
            .iter()
            .take(self.buffer.group_by.num_group_exprs())
            .any(|field| !column_group_key(field.data_type()))
        {
            self.buffer.group_values.size()
        } else {
            0
        };
        if accumulators == 0 && keys == 0 {
            return 0;
        }
        accumulators
            .saturating_add(keys)
            .saturating_add(groups.saturating_mul(EMIT_BYTES_PER_GROUP))
    }

    pub(in crate::aggregates) fn group_by_metrics(&self) -> GroupByMetrics {
        self.group_by_metrics.clone()
    }

    /// Takes every intermediate aggregate state and resets the table so it can
    /// continue with a new ordered input segment.
    ///
    /// Unlike normal ordered emission, this operation is allowed to take the
    /// active (incomplete) groups. Partial aggregation can pass those states to
    /// its final stage, while final aggregation sorts and spills them before
    /// replay.
    pub(in crate::aggregates) fn take_state_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        let rest = match self.pending.take().and_then(|mut p| p.take_rest()) {
            Some(rest) => Some(RecordBatch::try_new(
                Arc::clone(&self.state_schema),
                rest.columns().to_vec(),
            )?),
            None => None,
        };
        if self.buffer.group_values.is_empty() {
            return Ok(rest);
        }

        let mut output = self.buffer.group_values.emit(EmitTo::All)?;
        for acc in &mut self.buffer.accumulators {
            output.extend(acc.state(EmitTo::All)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.state_schema), output)?;
        debug_assert!(batch.num_rows() > 0);
        let batch = match rest {
            Some(rest) => concat_batches(&self.state_schema, &[rest, batch])?,
            None => batch,
        };

        // `emit(EmitTo::All)` resets accumulator state. Explicitly shrink the
        // key/index buffers too so the memory reservation can be released
        // before the batch is passed downstream or sorted for spilling.
        self.buffer.group_values.clear_shrink(0);
        self.buffer.group_indices.clear();
        self.buffer.group_indices.shrink_to_fit();
        self.buffer.group_ordering.reset();

        Ok(Some(batch))
    }

    /// The pending range always stores raw state. Packing a returned slice is zero-copy,
    /// while taking it for spill can continue to use the exact intermediate schema.
    fn pack_state_output(
        &self,
        batch: RecordBatch,
        is_final: bool,
    ) -> Result<RecordBatch> {
        let Some(fields) = self.struct_state_fields.as_ref().filter(|_| is_final) else {
            return Ok(batch);
        };
        let num_keys = self.buffer.group_by.num_group_exprs();
        let mut columns = batch.columns()[..num_keys].to_vec();
        let mut start = num_keys;
        for fields in fields {
            let end = start + fields.len();
            let state: Vec<ArrayRef> = batch.columns()[start..end].to_vec();
            columns.push(Arc::new(StructArray::try_new(fields.clone(), state, None)?));
            start = end;
        }
        assert_or_internal_err!(
            start == batch.num_columns(),
            "Unexpected state column count"
        );
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            columns,
        )?)
    }

    /// Returns the [`EmitTo`], clamped to the specified batch size
    ///
    /// Returns `(emit_to, should_remove_groups)`, where `emit_to` is the number
    /// of groups to emit from `GroupValues` / accumulators, and
    /// `should_remove_groups` indicates whether `GroupOrdering` must also shift
    /// its tracked indexes.
    pub(super) fn clamp_emit_to(
        &self,
        group_count: usize,
        emit_to: EmitTo,
    ) -> (EmitTo, bool) {
        match emit_to {
            EmitTo::First(n) => (EmitTo::First(n.min(self.batch_size)), true),
            EmitTo::All if group_count <= self.batch_size => (EmitTo::All, false),
            EmitTo::All => (EmitTo::First(self.batch_size), false),
        }
    }
    /// Aggregates one evaluated input batch.
    ///
    /// This common utility is used by ordered partial and ordered final aggregation.
    ///
    /// # Argument: `is_final`
    ///
    /// - `true`: merge partial aggregate states for final aggregation.
    /// - `false`: update aggregate states from raw input for partial aggregation.
    pub(super) fn aggregate_evaluated_batch(
        &mut self,
        evaluated_batch: &EvaluatedAggregateBatch,
        is_final: bool,
    ) -> Result<Option<RecordBatch>> {
        let taken = if !self.buffer.group_values.is_empty()
            && !evaluated_batch.grouping_set_args.iter().all(|keys| {
                self.buffer
                    .group_values
                    .has_offset_room(keys, self.group_key_offset_limit)
            }) {
            self.take_state_batch()?
        } else {
            None
        };
        for group_values in &evaluated_batch.grouping_set_args {
            let starting_num_groups = self.buffer.group_values.len();
            self.buffer
                .group_values
                .intern(group_values, &mut self.buffer.group_indices)?;
            let total_num_groups = self.buffer.group_values.len();
            if total_num_groups > starting_num_groups {
                self.buffer.group_ordering.new_groups(
                    group_values,
                    &self.buffer.group_indices,
                    total_num_groups,
                )?;
            }

            let timer = self.group_by_metrics.aggregation_time.timer();
            for (acc, values) in self
                .buffer
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                if is_final {
                    acc.merge_batch(
                        values,
                        &self.buffer.group_indices,
                        total_num_groups,
                    )?;
                } else {
                    acc.update_batch(
                        values,
                        &self.buffer.group_indices,
                        total_num_groups,
                    )?;
                }
            }
            drop(timer);
        }

        Ok(taken)
    }

    /// Emits groups allowed by `GroupOrdering`, leaving only the current
    /// unfinished ordered-key range buffered.
    ///
    /// This common utility is used by ordered partial and ordered final aggregation.
    ///
    /// # Argument: `is_final`
    ///
    /// - `true`: output final aggregate values.
    /// - `false`: output partial accumulator states.
    pub(super) fn next_output_batch_for_mode(
        &mut self,
        is_final: bool,
    ) -> Result<Option<RecordBatch>> {
        if let Some(pending) = &mut self.pending {
            if let Some(batch) = pending.next_batch(self.batch_size) {
                return self.pack_state_output(batch, is_final).map(Some);
            }
            self.pending = None;
        }

        if self.buffer.group_values.is_empty() {
            return Ok(None);
        }

        let Some(emit_to) = self.buffer.group_ordering.emit_to() else {
            return Ok(None);
        };

        let ready = match emit_to {
            EmitTo::First(n) => n,
            EmitTo::All => self.buffer.group_values.len(),
        };
        if (!is_final || self.struct_state_fields.is_some()) && ready > self.batch_size {
            let timer = self.group_by_metrics.emitting_time.timer();
            let mut output = self.buffer.group_values.emit(emit_to)?;
            if let EmitTo::First(n) = emit_to {
                self.buffer.group_ordering.remove_groups(n);
            }
            for acc in &mut self.buffer.accumulators {
                output.extend(acc.state(emit_to)?);
            }
            drop(timer);
            let schema = if is_final {
                &self.state_schema
            } else {
                &self.output_schema
            };
            let batch = RecordBatch::try_new(Arc::clone(schema), output)?;
            let mut pending = MaterializedAggregateOutput::new(batch);
            let first = pending.next_batch(self.batch_size);
            self.pending = Some(pending);
            return first
                .map(|batch| self.pack_state_output(batch, is_final))
                .transpose();
        }

        let (emit_to, should_remove_groups) =
            self.clamp_emit_to(self.buffer.group_values.len(), emit_to);

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.buffer.group_values.emit(emit_to)?;
        if should_remove_groups {
            match emit_to {
                EmitTo::First(n) => self.buffer.group_ordering.remove_groups(n),
                // `EmitTo::All` is only used after `input_done`, when all
                // buffered groups are known complete and the ordering state is
                // no longer needed.
                EmitTo::All => {}
            }
        }

        for acc in &mut self.buffer.accumulators {
            if is_final {
                output.push(acc.evaluate(emit_to)?);
            } else {
                output.extend(acc.state(emit_to)?);
            }
        }
        drop(timer);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);

        Ok(Some(batch))
    }
}

#[cfg(test)]
mod comet_tests {
    use super::*;
    use crate::ExecutionPlan;
    use crate::aggregates::aggregate_hash_table::PartialMarker;
    use crate::test::TestMemoryExec;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::{
        LexOrdering, PhysicalSortExpr, aggregate::AggregateExprBuilder, expressions::col,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingKeys {
        inner: Box<dyn GroupValues>,
        emits: Arc<AtomicUsize>,
    }
    impl GroupValues for CountingKeys {
        fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
            self.inner.intern(cols, groups)
        }
        fn size(&self) -> usize {
            self.inner.size()
        }
        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
            self.emits.fetch_add(1, Ordering::SeqCst);
            self.inner.emit(emit_to)
        }
        fn clear_shrink(&mut self, num_rows: usize) {
            self.inner.clear_shrink(num_rows)
        }
        fn has_offset_room(&self, cols: &[ArrayRef], limit: usize) -> bool {
            self.inner.has_offset_room(cols, limit)
        }
    }

    #[test]
    fn completed_ordered_range_is_materialized_once_and_pending_is_accounted()
    -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("prefix", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let rows = 8192;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![0; rows])),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("key-{i}")),
                )),
                Arc::new(Int64Array::from(vec![1; rows])),
            ],
        )?;
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "prefix", &schema,
        )?)])
        .unwrap();
        let input =
            TestMemoryExec::try_new(&[vec![batch.clone()]], Arc::clone(&schema), None)?
                .try_with_sort_information(vec![order])?;
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
        let group_by = PhysicalGroupBy::new_single(vec![
            (col("prefix", &schema)?, "prefix".into()),
            (col("key", &schema)?, "key".into()),
        ]);
        let count = Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        );
        let agg = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![count],
            vec![None],
            input,
            Arc::clone(&schema),
        )?;
        let mut table =
            OrderedAggregateTable::<PartialMarker>::new(&agg, 0, agg.schema(), 256)?;
        assert!(table.aggregate_batch(&batch)?.is_none());
        let emits = Arc::new(AtomicUsize::new(0));
        let inner = std::mem::replace(
            &mut table.buffer.group_values,
            new_group_values(
                Arc::new(Schema::new(schema.fields()[..2].to_vec())),
                &GroupOrdering::None,
            )?,
        );
        table.buffer.group_values = Box::new(CountingKeys {
            inner,
            emits: Arc::clone(&emits),
        });
        table.input_done();
        let first = table.next_output_batch()?.unwrap();
        assert_eq!(first.num_rows(), 256);
        assert_eq!(emits.load(Ordering::SeqCst), 1);
        assert!(table.has_pending_output());
        assert!(!table.is_empty());
        assert!(table.memory_size() >= first.get_array_memory_size());
        let mut actual = first.num_rows();
        while let Some(batch) = table.next_output_batch()? {
            actual += batch.num_rows();
        }
        assert_eq!(actual, rows);
        assert_eq!(
            emits.load(Ordering::SeqCst),
            1,
            "one key materialization for the entire range"
        );
        assert!(table.is_empty());
        assert!(!table.has_pending_output());
        Ok(())
    }
}
