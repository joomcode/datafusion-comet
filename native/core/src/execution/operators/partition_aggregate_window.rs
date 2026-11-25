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

use std::collections::VecDeque;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::evaluate_partition_ranges;
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SpillFile, TaskContext};
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::PlainAggregateWindowExpr;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, SpillMetrics,
};
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::WindowAggExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements, PlanProperties,
    SendableRecordBatchStream, WindowExpr,
};
use futures::{stream, StreamExt};

/// Whole-partition aggregates need the final aggregate before they can emit the first row.
/// Keep the accumulator, but spill the rows instead of buffering the entire partition in RAM.
/// Existing Spark-compatible accumulators retain their null and overflow semantics.
#[derive(Debug)]
pub struct PartitionAggregateWindowExec {
    window: WindowAggExec,
    metrics: ExecutionPlanMetricsSet,
}

impl PartitionAggregateWindowExec {
    pub fn supports(exprs: &[Arc<dyn WindowExpr>]) -> bool {
        !exprs.is_empty()
            && exprs.iter().all(|expr| {
                let frame = expr.get_window_frame();
                frame.start_bound.is_unbounded()
                    && frame.end_bound.is_unbounded()
                    && expr
                        .as_any()
                        .downcast_ref::<PlainAggregateWindowExpr>()
                        .is_some_and(|agg| {
                            // These accumulators have bounded state; collection aggregates need
                            // a separate strategy for spilling their accumulator, not just rows.
                            matches!(
                                agg.get_aggregate_expr().fun().name(),
                                "sum" | "avg" | "count" | "min" | "max"
                            )
                        })
            })
    }

    pub fn new(window: WindowAggExec) -> Self {
        Self {
            window,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for PartitionAggregateWindowExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionAggregateWindowExec")
    }
}

impl ExecutionPlan for PartitionAggregateWindowExec {
    fn name(&self) -> &str {
        "PartitionAggregateWindowExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.window.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.window.children()
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.window.apply_expressions(f)
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.window.input_distribution_requirements()
    }
    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        self.window.required_input_ordering()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(WindowAggExec::try_new(
            self.window.window_expr().to_vec(),
            Arc::clone(&children[0]),
            !self.window.window_expr()[0].partition_by().is_empty(),
        )?)))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let runtime = context.runtime_env();
        let input = self
            .window
            .input()
            .execute(partition, Arc::clone(&context))?;
        let schema = self.schema();
        let state = WindowState {
            spill: SpillManager::new(
                Arc::clone(&runtime),
                SpillMetrics::new(&self.metrics, partition),
                input.schema(),
            ),
            rows_reservation: MemoryConsumer::new("WindowRows")
                .with_can_spill(true)
                .register(&runtime.memory_pool),
            state_reservation: MemoryConsumer::new("WindowAccumulator")
                .register(&runtime.memory_pool),
            baseline: BaselineMetrics::new(&self.metrics, partition),
            input,
            input_done: false,
            schema: Arc::clone(&schema),
            exprs: self.window.window_expr().to_vec(),
            keys: self.window.partition_by_sort_keys()?,
            pending: VecDeque::new(),
            current_key: None,
            accumulators: vec![],
            rows: vec![],
            files: VecDeque::new(),
            replay: None,
            result: vec![],
            emitting: false,
        };
        let stream = stream::try_unfold(state, |mut state| async move {
            Ok(state.next_batch().await?.map(|batch| (batch, state)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct WindowState {
    baseline: BaselineMetrics,
    input: SendableRecordBatchStream,
    input_done: bool,
    schema: SchemaRef,
    exprs: Vec<Arc<dyn WindowExpr>>,
    keys: Vec<PhysicalSortExpr>,
    pending: VecDeque<(Vec<ScalarValue>, RecordBatch)>,
    current_key: Option<Vec<ScalarValue>>,
    accumulators: Vec<Box<dyn Accumulator>>,
    rows: Vec<RecordBatch>,
    files: VecDeque<Arc<dyn SpillFile>>,
    spill: SpillManager,
    rows_reservation: MemoryReservation,
    state_reservation: MemoryReservation,
    replay: Option<SendableRecordBatchStream>,
    result: Vec<ScalarValue>,
    emitting: bool,
}

impl WindowState {
    fn spill_rows(&mut self) -> Result<()> {
        if let Some(file) = self
            .spill
            .spill_record_batch_and_finish(&self.rows, "window rows")?
        {
            self.files.push_back(file);
        }
        self.rows.clear();
        self.rows_reservation.free();
        Ok(())
    }

    fn append(&mut self, batch: RecordBatch) -> Result<()> {
        for (expr, accumulator) in self.exprs.iter().zip(&mut self.accumulators) {
            let args = expr
                .expressions()
                .iter()
                .map(|e| e.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            accumulator.update_batch(&args)?;
        }
        let state_size = self.accumulators.iter().map(|a| a.size()).sum();
        if self.state_reservation.try_resize(state_size).is_err() {
            self.spill_rows()?;
            self.state_reservation.try_resize(state_size)?;
        }
        let size = batch.get_array_memory_size();
        if self.rows_reservation.try_grow(size).is_err() {
            self.spill_rows()?;
            // A single input batch may itself exceed the share. Write it directly, without
            // retaining it or claiming an unbounded memory reservation.
            if self.rows_reservation.try_grow(size).is_err() {
                self.rows.push(batch);
                return self.spill_rows();
            }
        }
        self.rows.push(batch);
        Ok(())
    }

    fn finish_partition(&mut self) -> Result<()> {
        self.result = self
            .accumulators
            .iter_mut()
            .map(|a| a.evaluate())
            .collect::<Result<_>>()?;
        self.accumulators.clear();
        self.state_reservation.free();
        if !self.files.is_empty() {
            self.spill_rows()?;
        } else {
            let batches = std::mem::take(&mut self.rows);
            self.replay = Some(Box::pin(RecordBatchStreamAdapter::new(
                self.input.schema(),
                stream::iter(batches.into_iter().map(Ok)),
            )));
        }
        self.emitting = true;
        Ok(())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.emitting {
                if let Some(replay) = &mut self.replay {
                    if let Some(batch) = replay.next().await {
                        let batch = batch?;
                        let mut columns = batch.columns().to_vec();
                        for value in &self.result {
                            columns.push(value.to_array_of_size(batch.num_rows())?);
                        }
                        self.baseline.record_output(batch.num_rows());
                        return Ok(Some(RecordBatch::try_new(
                            Arc::clone(&self.schema),
                            columns,
                        )?));
                    }
                    self.replay = None;
                }
                if let Some(file) = self.files.pop_front() {
                    // Open one file at a time, without prefetching the rest of the partition.
                    self.replay = Some(self.spill.read_spill_as_stream_unbuffered(file, None)?);
                    continue;
                }
                self.rows_reservation.free();
                self.current_key = None;
                self.result.clear();
                self.emitting = false;
            }
            if let Some((key, batch)) = self.pending.pop_front() {
                if self
                    .current_key
                    .as_ref()
                    .is_some_and(|current| *current != key)
                {
                    self.pending.push_front((key, batch));
                    self.finish_partition()?;
                    continue;
                }
                if self.current_key.is_none() {
                    self.current_key = Some(key);
                    self.accumulators = self
                        .exprs
                        .iter()
                        .map(|expr| {
                            expr.as_any()
                                .downcast_ref::<PlainAggregateWindowExpr>()
                                .expect("supports checked the expression")
                                .get_aggregate_expr()
                                .create_accumulator()
                        })
                        .collect::<Result<_>>()?;
                }
                self.append(batch)?;
                continue;
            }
            match if self.input_done {
                None
            } else {
                self.input.next().await
            } {
                Some(batch) => {
                    let batch = batch?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let keys = self
                        .keys
                        .iter()
                        .map(|k| k.evaluate_to_sort_column(&batch))
                        .collect::<Result<Vec<_>>>()?;
                    for range in evaluate_partition_ranges(batch.num_rows(), &keys)? {
                        let key = keys
                            .iter()
                            .map(|k| ScalarValue::try_from_array(&k.values, range.start))
                            .collect::<Result<Vec<_>>>()?;
                        self.pending
                            .push_back((key, batch.slice(range.start, range.end - range.start)));
                    }
                }
                None if self.current_key.is_some() => {
                    self.input_done = true;
                    self.finish_partition()?;
                }
                None => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray, UInt32Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::WindowFrame;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::LexOrdering;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[tokio::test]
    async fn whole_partition_aggregates_spill_and_preserve_rows() -> Result<()> {
        for partitioned in [false, true] {
            // Exercise no spill, buffered spill, and a batch larger than the entire budget.
            for budget in [1_000_000, 16_000, 1024] {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Int64, true),
                    Field::new("value", DataType::Int64, true),
                    Field::new("payload", DataType::Utf8, false),
                ]));
                let keys: Vec<_> = (0..80)
                    .map(|i| if i < 9 { None } else { Some(i / 25) })
                    .collect();
                let values: Vec<_> = (0..80)
                    .map(|i| if i % 3 == 0 { None } else { Some(i) })
                    .collect();
                let payload = "x".repeat(1024);
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(keys.clone())),
                        Arc::new(Int64Array::from(values.clone())),
                        Arc::new(StringArray::from(vec![payload.as_str(); 80])),
                    ],
                )?;
                let mut batches = vec![batch.slice(0, 0)];
                for start in (0..80).step_by(7) {
                    let indices = UInt32Array::from(
                        (start as u32..(start + 7).min(80) as u32).collect::<Vec<_>>(),
                    );
                    batches.push(arrow::compute::take_record_batch(&batch, &indices)?);
                }
                let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
                let order = PhysicalSortExpr::new(
                    Arc::clone(&key),
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                );
                let config = MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)?
                    .try_with_sort_information(vec![LexOrdering::new(vec![order]).unwrap()])?;
                let input = Arc::new(DataSourceExec::new(Arc::new(config)));
                let aggregate =
                    AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("value", 1))])
                        .schema(schema)
                        .alias("total")
                        .build()?;
                let partition_keys = if partitioned { vec![key] } else { vec![] };
                let expr: Arc<dyn WindowExpr> = Arc::new(PlainAggregateWindowExpr::new(
                    Arc::new(aggregate),
                    &partition_keys,
                    &[],
                    Arc::new(WindowFrame::new(None)),
                    None,
                ));
                assert!(PartitionAggregateWindowExec::supports(&[Arc::clone(&expr)]));
                let plan = PartitionAggregateWindowExec::new(WindowAggExec::try_new(
                    vec![expr],
                    input,
                    partitioned,
                )?);
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
                let runtime = Arc::new(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(Arc::clone(&pool))
                        .build()?,
                );
                let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
                let mut output = plan.execute(0, ctx.task_ctx())?;
                let mut row = 0;
                while let Some(batch) = output.next().await {
                    let batch = batch?;
                    let sums = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let payloads = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        let expected: i64 = values
                            .iter()
                            .zip(&keys)
                            .filter(|(_, k)| !partitioned || **k == keys[row])
                            .filter_map(|(v, _)| *v)
                            .sum();
                        assert!(!sums.is_null(i));
                        assert_eq!(sums.value(i), expected);
                        assert_eq!(payloads.value(i), payload);
                        assert_eq!(
                            ScalarValue::try_from_array(batch.column(0), i)?,
                            ScalarValue::Int64(keys[row])
                        );
                        assert_eq!(
                            ScalarValue::try_from_array(batch.column(1), i)?,
                            ScalarValue::Int64(values[row])
                        );
                        row += 1;
                    }
                }
                assert_eq!(row, 80);
                drop(output);
                assert_eq!(pool.reserved(), 0);
                let spills = plan.metrics().unwrap().spill_count().unwrap_or(0);
                assert_eq!(spills > 0, budget < 1_000_000);
                // Dropping a partially consumed replay releases reservations and spill owners.
                let mut cancelled = plan.execute(0, ctx.task_ctx())?;
                assert!(cancelled.next().await.transpose()?.is_some());
                drop(cancelled);
                assert_eq!(pool.reserved(), 0);
            }
        }
        Ok(())
    }
}
