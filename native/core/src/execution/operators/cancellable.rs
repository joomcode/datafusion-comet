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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements, PlanProperties,
    RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_comet_common::cancellation::{cancelled_error, PlanCancellation};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Makes the stream of the operator it wraps end with an error once the plan is cancelled. It is
/// otherwise invisible: it reports the wrapped operator's name, children, properties, metrics and
/// downcast identity, so a plan reads and rewrites as if it were not there.
///
/// Every operator a Comet plan is built from is wrapped, so a cancelled plan stops at the next
/// batch any of its operators produces, even inside a parent that keeps polling its input without
/// ever yielding, such as an aggregate fed by a join that multiplies its probe rows.
/// Comet serializes its input plans with the Spark protobuf, not DataFusion's feature-gated
/// physical-plan protobuf codec, so that codec's `try_to_proto` hook is not used here.
#[derive(Debug)]
pub(crate) struct CancellableExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl CancellableExec {
    pub(crate) fn wrap(inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if is_cancellable(inner.as_ref()) {
            inner
        } else {
            Arc::new(Self { inner })
        }
    }
}

fn is_cancellable(plan: &dyn ExecutionPlan) -> bool {
    (plan as &dyn Any).is::<CancellableExec>()
}

impl DisplayAs for CancellableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(deprecated)]
impl ExecutionPlan for CancellableExec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        self.inner.check_invariants(check)
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.inner.dynamic_expressions_produced()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.inner.input_distribution_requirements()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.inner.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::wrap(
            Arc::clone(&self.inner).replace_children(children, options)?,
        ))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::wrap(
            Arc::clone(&self.inner).with_new_children(children)?,
        ))
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::wrap(
            Arc::clone(&self.inner).with_new_children_and_same_properties(children)?,
        ))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Self::wrap(Arc::clone(&self.inner).reset_state()?))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .inner
            .repartitioned(target_partitions, config)?
            .map(Self::wrap))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cancellation = PlanCancellation::of(&context);
        let stream = self.inner.execute(partition, context)?;
        Ok(match cancellation {
            Some(cancellation) => Box::pin(CancellableStream {
                inner: stream,
                cancellation,
            }),
            None => stream,
        })
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit).map(Self::wrap)
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(self
            .inner
            .try_swapping_with_projection(projection)?
            .map(Self::wrap))
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        self.inner
            .gather_filters_for_pushdown(phase, parent_filters, config)
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let mut result =
            self.inner
                .handle_child_pushdown_result(phase, child_pushdown_result, config)?;
        result.updated_node = result.updated_node.map(Self::wrap);
        Ok(result)
    }

    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_new_state(state).map(Self::wrap)
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .with_preserve_order(preserve_order)
            .map(Self::wrap)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(self.inner.try_pushdown_sort(order)?.map(Self::wrap))
    }
}

struct CancellableStream {
    inner: SendableRecordBatchStream,
    cancellation: Arc<PlanCancellation>,
}

impl Stream for CancellableStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(Some(Err(cancelled_error())));
        }
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for CancellableStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::lit;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::filter_pushdown::PushedDown;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::statistics::StatisticsContext;
    use datafusion::physical_plan::{collect, expressions::col};
    use datafusion::physical_plan::{displayable, ChildrenPropertiesMode};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::time::{Duration, Instant};

    fn constant_keys(rows: usize, batches: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1; rows]))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch; batches]], schema, None).unwrap()
    }

    fn wrapped_join_count(rows: usize) -> Arc<dyn ExecutionPlan> {
        let build = CancellableExec::wrap(constant_keys(rows, 1));
        let probe = CancellableExec::wrap(constant_keys(rows, 1));
        let on = vec![(
            col("k", &build.schema()).unwrap(),
            col("k", &probe.schema()).unwrap(),
        )];
        let join = CancellableExec::wrap(Arc::new(
            HashJoinExec::try_new(
                build,
                probe,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        ));
        let schema = join.schema();
        let count = AggregateExprBuilder::new(count_udaf(), vec![lit(1)])
            .schema(Arc::clone(&schema))
            .alias("count")
            .build()
            .unwrap();
        CancellableExec::wrap(Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(vec![]),
                vec![Arc::new(count)],
                vec![None],
                join,
                schema,
            )
            .unwrap(),
        ))
    }

    #[test]
    fn a_wrapped_plan_displays_and_downcasts_as_the_plan_it_wraps() {
        let source = constant_keys(3, 1);
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), Arc::clone(&source)).unwrap());
        let wrapped = CancellableExec::wrap(Arc::clone(&filter));
        assert!(wrapped.downcast_ref::<FilterExec>().is_some());
        assert_eq!(wrapped.name(), filter.name());
        assert_eq!(wrapped.children().len(), 1);
        assert_eq!(
            displayable(wrapped.as_ref()).indent(true).to_string(),
            displayable(filter.as_ref()).indent(true).to_string()
        );
        let rewritten = Arc::clone(&wrapped)
            .replace_children(
                vec![source],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        assert!(is_cancellable(rewritten.as_ref()));
        assert!(rewritten.downcast_ref::<FilterExec>().is_some());
        let rewrapped = CancellableExec::wrap(Arc::clone(&wrapped));
        assert!(Arc::ptr_eq(&rewrapped, &wrapped));
    }

    #[test]
    fn wrapped_filter_preserves_statistics_pushdown_and_projection_rewrites() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("payload", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let source: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(lit(true), Arc::clone(&source)).unwrap());
        let wrapped = CancellableExec::wrap(Arc::clone(&filter));
        let config = ConfigOptions::default();

        assert_eq!(
            wrapped.child_stats_requests(None),
            vec![ChildStats::At(None)]
        );
        let expected = StatisticsContext::new()
            .compute(filter.as_ref(), &StatisticsArgs::new())
            .unwrap();
        let actual = StatisticsContext::new()
            .compute(wrapped.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(actual.num_rows, expected.num_rows);
        assert_eq!(actual.column_statistics, expected.column_statistics);

        // A default ExecutionPlan would advertise no self filter and prevent scan pushdown.
        let description = wrapped
            .gather_filters_for_pushdown(FilterPushdownPhase::Pre, vec![], &config)
            .unwrap();
        assert_eq!(description.self_filters().len(), 1);
        assert_eq!(description.self_filters()[0].len(), 1);
        let pushed = wrapped
            .handle_child_pushdown_result(
                FilterPushdownPhase::Pre,
                ChildPushdownResult {
                    parent_filters: vec![],
                    self_filters: vec![vec![PushedDown::Yes.wrap_expression(lit(true))]],
                },
                &config,
            )
            .unwrap()
            .updated_node
            .expect("the filter absorbed by its input is removed");
        assert!(is_cancellable(pushed.as_ref()));
        assert!(pushed.downcast_ref::<FilterExec>().is_none());

        let projection = ProjectionExec::try_new(
            vec![(col("k", &filter.schema()).unwrap(), "k".to_string())],
            Arc::clone(&wrapped),
        )
        .unwrap();
        let swapped = wrapped
            .try_swapping_with_projection(&projection)
            .unwrap()
            .expect("the narrowing projection must still be pushed through the filter");
        assert!(is_cancellable(swapped.as_ref()));
        assert!(swapped.downcast_ref::<FilterExec>().is_some());
        assert_eq!(swapped.schema().fields().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_plan_stops_inside_an_operator_that_never_yields() {
        let cancellation = PlanCancellation::new();
        let config = SessionConfig::new().with_extension(Arc::clone(&cancellation));
        let ctx = SessionContext::new_with_config(config).task_ctx();
        let plan = wrapped_join_count(40_000);
        let canceller = Arc::clone(&cancellation);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            canceller.cancel();
        });
        let start = Instant::now();
        let err = collect(plan, ctx).await.unwrap_err();
        assert!(err.to_string().contains("cancelled"), "{err}");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn a_plan_without_cancellation_runs_unchanged() {
        let ctx = SessionContext::new().task_ctx();
        let batches = collect(wrapped_join_count(100), ctx).await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 10_000);
    }
}
