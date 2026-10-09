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
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute::{
    cast, concat_batches, interleave, take, take_record_batch, SortColumn, SortOptions,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::{compare_rows, evaluate_partition_ranges, get_row_at_idx};
use datafusion::common::{internal_datafusion_err, Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SpillFile, TaskContext};
use datafusion::logical_expr::{Accumulator, WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::window::{
    PlainAggregateWindowExpr, SlidingAggregateWindowExpr, StandardWindowExpr,
};
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, SpillMetrics,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec, WindowUDFExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputDistributionRequirements, InputOrderMode,
    PlanProperties, SendableRecordBatchStream, WindowExpr,
};
use datafusion_comet_common::offset_extents::OffsetBudget;
use futures::{stream, StreamExt};

/// Window operator for expressions that need the whole partition, or every row after the
/// current one, before they can emit a row. DataFusion's `WindowAggExec` buffers the entire
/// partition in memory for these; this operator reserves the buffered rows and spills them
/// through DataFusion's spill manager instead. Only the current window partition is retained.
///
/// Per expression:
/// * whole-partition `sum`/`avg`/`count`/`min`/`max` keep the existing native accumulators
///   (and their Spark null and overflow semantics); rows are replayed with the final value;
/// * whole-partition `first_value`/`last_value`/`nth_value`, with or without `IGNORE NULLS`,
///   track the selected value while rows are ingested;
/// * `ntile` and `percent_rank` are computed while replaying, from the partition size counted
///   during ingestion (percent_rank tracks the start of the current peer group);
/// * `cume_dist` and frames ending at `UNBOUNDED FOLLOWING` that start at `CURRENT ROW`,
///   `N PRECEDING` or `N FOLLOWING` (`ROWS` or `RANGE`) use a reverse pass: a narrow
///   reverse-order copy of the rows is visited from the end of the partition, producing the
///   value of the frame starting at every row. Those values are buffered as a spillable
///   stream in row order and read during the replay at each row's frame start.
#[derive(Debug)]
pub struct PartitionAggregateWindowEnabled;

#[derive(Debug)]
pub struct PartitionAggregateWindowExec {
    window: WindowAggExec,
    ignore_nulls: Vec<bool>,
    specs: Vec<Spec>,
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ValueKind {
    First,
    Last,
    Nth(usize),
}

#[derive(Debug, Clone, PartialEq)]
enum FrameStart {
    /// Signed row offset from the current row.
    Rows(i64),
    /// `delta` is `None` for `CURRENT ROW`.
    Range {
        delta: Option<ScalarValue>,
        preceding: bool,
    },
}

#[derive(Debug, Clone)]
enum SuffixFn {
    Aggregate(Arc<AggregateFunctionExpr>),
    Value { kind: ValueKind, ignore_nulls: bool },
}

#[derive(Debug, Clone)]
enum Kind {
    Aggregate(Arc<AggregateFunctionExpr>),
    Value { kind: ValueKind, ignore_nulls: bool },
    Ntile(u64),
    PercentRank,
    CumeDist,
    Suffix { func: SuffixFn, start: FrameStart },
}

#[derive(Debug, Clone)]
struct Spec {
    kind: Kind,
    args: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
}

impl Spec {
    fn reverse(&self) -> bool {
        matches!(self.kind, Kind::CumeDist | Kind::Suffix { .. })
    }

    /// Whether whole partitions within one batch can be evaluated together: everything but
    /// RANGE frames starting at an offset from the current row.
    fn batched(&self) -> bool {
        !matches!(
            self.kind,
            Kind::Suffix {
                start: FrameStart::Range { delta: Some(_), .. },
                ..
            }
        )
    }
}

/// Input rows waiting to be processed, in input order.
#[derive(Debug)]
enum Pending {
    /// Rows of one partition that may extend over other batches, processed row by row.
    Rows(Vec<ScalarValue>, RecordBatch),
    /// Whole partitions, all within this batch, at `ranges` of it. Evaluated together when
    /// every expression supports it.
    Partitions(RecordBatch, Vec<Range<usize>>),
}

fn aggregate_of(expr: &Arc<dyn WindowExpr>) -> Option<Arc<AggregateFunctionExpr>> {
    let any = expr.as_any();
    // Comet does not plan window aggregates with a FILTER clause.
    let aggregate = match any.downcast_ref::<PlainAggregateWindowExpr>() {
        Some(plain) => plain.get_aggregate_expr(),
        None => any
            .downcast_ref::<SlidingAggregateWindowExpr>()?
            .get_aggregate_expr(),
    };
    // These accumulators have bounded, order-insensitive state; collection aggregates need a
    // separate strategy for spilling their accumulator, not just rows.
    matches!(
        aggregate.fun().name(),
        "sum" | "avg" | "count" | "min" | "max"
    )
    .then(|| Arc::new(aggregate.clone()))
}

fn literal_of(expr: Option<&Arc<dyn PhysicalExpr>>) -> Option<&ScalarValue> {
    expr?
        .as_ref()
        .downcast_ref::<Literal>()
        .map(|l| l.value())
        .filter(|v| !v.is_null())
}

fn frame_start(expr: &Arc<dyn WindowExpr>) -> Option<FrameStart> {
    let frame = expr.get_window_frame();
    let rows = |n: &u64| i64::try_from(*n).unwrap_or(i64::MAX);
    let range = |delta: &ScalarValue, preceding| {
        (!delta.is_null() && expr.order_by().len() == 1).then(|| FrameStart::Range {
            delta: Some(delta.clone()),
            preceding,
        })
    };
    match (&frame.units, &frame.start_bound) {
        (WindowFrameUnits::Rows, WindowFrameBound::CurrentRow) => Some(FrameStart::Rows(0)),
        (WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n)))) => {
            Some(FrameStart::Rows(-rows(n)))
        }
        (WindowFrameUnits::Rows, WindowFrameBound::Following(ScalarValue::UInt64(Some(n)))) => {
            Some(FrameStart::Rows(rows(n)))
        }
        (WindowFrameUnits::Range, WindowFrameBound::CurrentRow) => Some(FrameStart::Range {
            delta: None,
            preceding: true,
        }),
        (WindowFrameUnits::Range, WindowFrameBound::Preceding(delta)) => range(delta, true),
        (WindowFrameUnits::Range, WindowFrameBound::Following(delta)) => range(delta, false),
        _ => None,
    }
}

fn classify(expr: &Arc<dyn WindowExpr>, ignore_nulls: bool) -> Option<Spec> {
    let frame = expr.get_window_frame();
    if frame.units == WindowFrameUnits::Groups {
        return None;
    }
    // Mirrors DataFusion's `is_window_constant_in_partition`.
    let constant = |bound: &WindowFrameBound| match bound {
        WindowFrameBound::CurrentRow => {
            frame.units == WindowFrameUnits::Range && expr.order_by().is_empty()
        }
        _ => bound.is_unbounded(),
    };
    let whole = constant(&frame.start_bound) && constant(&frame.end_bound);
    let suffix = || match &frame.end_bound {
        WindowFrameBound::Following(end) if end.is_null() => frame_start(expr),
        _ => None,
    };
    let data_type = expr.field().ok()?.data_type().clone();
    if let Some(aggregate) = aggregate_of(expr) {
        let kind = if whole {
            Kind::Aggregate(aggregate)
        } else {
            Kind::Suffix {
                func: SuffixFn::Aggregate(aggregate),
                start: suffix()?,
            }
        };
        return Some(Spec {
            kind,
            args: expr.expressions(),
            data_type,
        });
    }
    let udf = expr
        .as_any()
        .downcast_ref::<StandardWindowExpr>()?
        .get_standard_func_expr()
        .as_any()
        .downcast_ref::<WindowUDFExpr>()?;
    let args = udf.args();
    let value = match udf.fun().name() {
        "first_value" => Some(ValueKind::First),
        "last_value" => Some(ValueKind::Last),
        "nth_value" => match literal_of(args.get(1))?.cast_to(&DataType::Int64).ok()? {
            ScalarValue::Int64(Some(n)) if n > 0 => Some(ValueKind::Nth(n as usize)),
            _ => return None,
        },
        _ => None,
    };
    let kind = match (value, udf.fun().name()) {
        (Some(kind), _) if whole => Kind::Value { kind, ignore_nulls },
        (Some(kind), _) => Kind::Suffix {
            func: SuffixFn::Value { kind, ignore_nulls },
            start: suffix()?,
        },
        (None, "ntile") => match literal_of(args.first())?.cast_to(&DataType::UInt64).ok()? {
            ScalarValue::UInt64(Some(n)) if n > 0 => Kind::Ntile(n),
            _ => return None,
        },
        (None, "percent_rank") => Kind::PercentRank,
        (None, "cume_dist") => Kind::CumeDist,
        _ => return None,
    };
    let args = match kind {
        Kind::Value { .. } | Kind::Suffix { .. } => vec![Arc::clone(args.first()?)],
        _ => vec![],
    };
    Some(Spec {
        kind,
        args,
        data_type,
    })
}

impl PartitionAggregateWindowExec {
    /// Wraps `window` when every expression has a spilling implementation. `ignore_nulls`
    /// carries each expression's `IGNORE NULLS` flag, which the built DataFusion window
    /// function expression does not expose.
    pub fn try_new(window: WindowAggExec, ignore_nulls: Vec<bool>) -> Option<Self> {
        let exprs = window.window_expr();
        if exprs.is_empty() || exprs.len() != ignore_nulls.len() {
            return None;
        }
        let specs = exprs
            .iter()
            .zip(&ignore_nulls)
            .map(|(e, ignore)| classify(e, *ignore))
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            window,
            ignore_nulls,
            specs,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Plans a window node that has at least one expression which cannot run with bounded
    /// memory. Bounded expressions of a mixed node are evaluated by a streaming
    /// `BoundedWindowAggExec` below this operator, and a projection restores the original
    /// column order. Returns `None` when an unbounded expression has no spilling
    /// implementation.
    pub fn try_plan(
        exprs: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        can_repartition: bool,
        ignore_nulls: Vec<bool>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if exprs.len() != ignore_nulls.len() {
            return Ok(None);
        }
        type Indexed = (usize, (Arc<dyn WindowExpr>, bool));
        let (bounded, unbounded): (Vec<Indexed>, Vec<Indexed>) = exprs
            .iter()
            .cloned()
            .zip(ignore_nulls)
            .enumerate()
            .partition(|(_, (e, _))| e.uses_bounded_memory());
        if unbounded.is_empty()
            || unbounded
                .iter()
                .any(|(_, (e, ignore))| classify(e, *ignore).is_none())
        {
            return Ok(None);
        }
        let input_fields = input.schema().fields().len();
        let child = if bounded.is_empty() {
            input
        } else {
            Arc::new(BoundedWindowAggExec::try_new(
                bounded.iter().map(|(_, (e, _))| Arc::clone(e)).collect(),
                input,
                InputOrderMode::Sorted,
                can_repartition,
            )?) as Arc<dyn ExecutionPlan>
        };
        let (unbounded_exprs, unbounded_nulls) = unbounded
            .iter()
            .map(|(_, (e, ignore))| (Arc::clone(e), *ignore))
            .unzip();
        let window = WindowAggExec::try_new(unbounded_exprs, child, can_repartition)?;
        let Some(plan) = Self::try_new(window, unbounded_nulls) else {
            return Ok(None);
        };
        let plan: Arc<dyn ExecutionPlan> = Arc::new(plan);
        if bounded.is_empty() {
            return Ok(Some(plan));
        }
        let schema = plan.schema();
        let mut positions = vec![0; exprs.len()];
        for (column, (original, _)) in bounded.iter().chain(&unbounded).enumerate() {
            positions[*original] = input_fields + column;
        }
        let projection = (0..input_fields)
            .chain(positions)
            .map(|i| {
                let name = schema.field(i).name().to_string();
                (
                    Arc::new(Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect::<Vec<_>>();
        Ok(Some(Arc::new(ProjectionExec::try_new(projection, plan)?)))
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
        let window = WindowAggExec::try_new(
            self.window.window_expr().to_vec(),
            Arc::clone(&children[0]),
            !self.window.window_expr()[0].partition_by().is_empty(),
        )?;
        Self::try_new(window, self.ignore_nulls.clone())
            .map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>)
            .ok_or_else(|| internal_datafusion_err!("unsupported window expressions"))
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
        let order_by = self.window.window_expr()[0].order_by().to_vec();
        let spill_metrics = SpillMetrics::new(&self.metrics, partition);
        let reverse =
            ReverseLayout::try_new(&self.specs, &order_by, &input.schema())?.map(|layout| {
                ReverseState {
                    rows_spill: SpillManager::new(
                        Arc::clone(&runtime),
                        spill_metrics.clone(),
                        Arc::clone(&layout.narrow_schema),
                    ),
                    suffix_spill: SpillManager::new(
                        Arc::clone(&runtime),
                        spill_metrics.clone(),
                        Arc::clone(&layout.suffix_schema),
                    ),
                    layout,
                    files: VecDeque::new(),
                    pending: vec![],
                    suffix_files: vec![],
                    cursors: vec![],
                    empty: None,
                    reservation: ChunkedReservation::new(
                        MemoryConsumer::new("WindowSuffix")
                            .with_can_spill(true)
                            .register(&runtime.memory_pool),
                    ),
                }
            });
        let state = WindowState {
            spill: SpillManager::new(Arc::clone(&runtime), spill_metrics, input.schema()),
            rows_reservation: ChunkedReservation::new(
                MemoryConsumer::new("WindowRows")
                    .with_can_spill(true)
                    .register(&runtime.memory_pool),
            ),
            state_reservation: ChunkedReservation::new(
                MemoryConsumer::new("WindowAccumulator").register(&runtime.memory_pool),
            ),
            baseline: BaselineMetrics::new(&self.metrics, partition),
            input,
            input_done: false,
            schema: Arc::clone(&schema),
            specs: self.specs.clone(),
            keys: self.window.partition_by_sort_keys()?,
            order_by,
            pending: VecDeque::new(),
            batched: self.specs.iter().all(Spec::batched),
            target_rows: context.session_config().batch_size().max(1),
            buffered: vec![],
            buffered_rows: 0,
            buffered_offsets: OffsetBudget::default(),
            ready: VecDeque::new(),
            current_key: None,
            num_rows: 0,
            accumulators: vec![],
            values: vec![],
            rows: vec![],
            files: VecDeque::new(),
            replay: None,
            result: vec![],
            emitting: false,
            offset: 0,
            rank: None,
            reverse,
        };
        let stream = stream::try_unfold(state, |mut state| async move {
            Ok(state.next_coalesced().await?.map(|batch| (batch, state)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Column layout of the reverse pass. The narrow stream holds the ORDER BY keys (when needed)
/// followed by the arguments of every reverse expression, in reverse row order. The suffix
/// stream holds the keys needed to locate RANGE frame starts followed by one value column per
/// reverse expression, in row order.
#[derive(Debug)]
struct ReverseLayout {
    narrow_exprs: Vec<Arc<dyn PhysicalExpr>>,
    narrow_schema: SchemaRef,
    narrow_keys: usize,
    suffix_schema: SchemaRef,
    suffix_keys: usize,
    outputs: Vec<ReverseOutput>,
    starts: Vec<FrameStart>,
}

#[derive(Debug)]
struct ReverseOutput {
    expr: usize,
    args: Range<usize>,
    cursor: usize,
}

impl ReverseLayout {
    fn try_new(
        specs: &[Spec],
        order_by: &[PhysicalSortExpr],
        input: &SchemaRef,
    ) -> Result<Option<Self>> {
        if !specs.iter().any(Spec::reverse) {
            return Ok(None);
        }
        let range = specs.iter().any(|s| {
            matches!(
                s.kind,
                Kind::Suffix {
                    start: FrameStart::Range { .. },
                    ..
                }
            )
        });
        let cume_dist = specs.iter().any(|s| matches!(s.kind, Kind::CumeDist));
        let mut narrow_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![];
        let mut narrow_fields = vec![];
        if range || cume_dist {
            for (i, key) in order_by.iter().enumerate() {
                narrow_exprs.push(Arc::clone(&key.expr));
                narrow_fields.push(Field::new(
                    format!("key_{i}"),
                    key.expr.data_type(input)?,
                    true,
                ));
            }
        }
        let narrow_keys = narrow_exprs.len();
        let suffix_keys = if range { narrow_keys } else { 0 };
        let mut suffix_fields = narrow_fields[..suffix_keys].to_vec();
        let mut outputs = vec![];
        let mut starts: Vec<FrameStart> = vec![];
        for (expr, spec) in specs.iter().enumerate() {
            let start = match &spec.kind {
                Kind::CumeDist => FrameStart::Rows(0),
                Kind::Suffix { start, .. } => start.clone(),
                _ => continue,
            };
            let first = narrow_exprs.len();
            for arg in &spec.args {
                narrow_fields.push(Field::new(
                    format!("arg_{}", narrow_exprs.len()),
                    arg.data_type(input)?,
                    true,
                ));
                narrow_exprs.push(Arc::clone(arg));
            }
            suffix_fields.push(Field::new(
                format!("value_{expr}"),
                spec.data_type.clone(),
                true,
            ));
            let cursor = match starts.iter().position(|s| *s == start) {
                Some(cursor) => cursor,
                None => {
                    starts.push(start);
                    starts.len() - 1
                }
            };
            outputs.push(ReverseOutput {
                expr,
                args: first..narrow_exprs.len(),
                cursor,
            });
        }
        if narrow_exprs.is_empty() {
            // Spill files need a column to carry the row count.
            narrow_exprs.push(Arc::new(Literal::new(ScalarValue::Boolean(None))));
            narrow_fields.push(Field::new("rows", DataType::Boolean, true));
        }
        Ok(Some(Self {
            narrow_exprs,
            narrow_schema: Arc::new(Schema::new(narrow_fields)),
            narrow_keys,
            suffix_schema: Arc::new(Schema::new(suffix_fields)),
            suffix_keys,
            outputs,
            starts,
        }))
    }

    fn narrow(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .narrow_exprs
            .iter()
            .zip(self.narrow_schema.fields())
            .map(|(e, field)| {
                let array = e.evaluate(batch)?.into_array(batch.num_rows())?;
                cast_to(array, field.data_type())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(
            Arc::clone(&self.narrow_schema),
            columns,
        )?)
    }
}

fn cast_to(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        Ok(array)
    } else {
        Ok(cast(&array, data_type)?)
    }
}

fn reverse_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let indices = UInt32Array::from_iter_values((0..batch.num_rows() as u32).rev());
    Ok(take_record_batch(batch, &indices)?)
}

/// Converts batches between row order and reverse row order.
fn reverse_batches(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    batches.iter().rev().map(reverse_batch).collect()
}

/// Index of the `n`-th (0-based) non-null row.
fn nth_valid(array: &ArrayRef, n: usize) -> Option<usize> {
    match array.logical_nulls() {
        Some(nulls) => nulls.valid_indices().nth(n),
        None => (n < array.len()).then_some(n),
    }
}

fn last_valid(array: &ArrayRef) -> Option<usize> {
    match array.logical_nulls() {
        Some(nulls) => (0..array.len()).rev().find(|&i| nulls.is_valid(i)),
        None => array.len().checked_sub(1),
    }
}

fn is_valid(array: &ArrayRef) -> impl Fn(usize) -> bool {
    let nulls = array.logical_nulls();
    move |row| nulls.as_ref().is_none_or(|n| n.is_valid(row))
}

/// Selected row of a whole-partition `first_value`/`last_value`/`nth_value`.
#[derive(Debug)]
struct ValueState {
    kind: ValueKind,
    ignore_nulls: bool,
    seen: usize,
    value: Option<ScalarValue>,
}

impl ValueState {
    fn update(&mut self, array: &ArrayRef) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }
        let index = match self.kind {
            ValueKind::First | ValueKind::Nth(_) if self.value.is_some() => None,
            ValueKind::First if self.ignore_nulls => nth_valid(array, 0),
            ValueKind::First => Some(0),
            ValueKind::Last if self.ignore_nulls => last_valid(array),
            ValueKind::Last => Some(array.len() - 1),
            ValueKind::Nth(n) => {
                let count = if self.ignore_nulls {
                    array.len() - array.logical_null_count()
                } else {
                    array.len()
                };
                let before = self.seen;
                self.seen += count;
                if before + count >= n {
                    let k = n - before - 1;
                    if self.ignore_nulls {
                        nth_valid(array, k)
                    } else {
                        Some(k)
                    }
                } else {
                    None
                }
            }
        };
        if let Some(index) = index {
            self.value = Some(ScalarValue::try_from_array(array, index)?);
        }
        Ok(())
    }
}

/// Value of a frame that starts past the last row of the partition.
fn empty_value(spec: &Spec) -> Result<ScalarValue> {
    match &spec.kind {
        Kind::Suffix {
            func: SuffixFn::Aggregate(aggregate),
            ..
        } => aggregate.create_accumulator()?.evaluate(),
        _ => ScalarValue::try_from(&spec.data_type),
    }
}

/// Incremental state of one reverse expression while rows are visited from the end of the
/// partition. After visiting row `j` it describes the frame `[j, partition end)`.
#[derive(Debug)]
enum SuffixState {
    Aggregate(Box<dyn Accumulator>),
    First {
        ignore_nulls: bool,
        next: ScalarValue,
    },
    Last {
        ignore_nulls: bool,
        last: ScalarValue,
    },
    Nth {
        ignore_nulls: bool,
        n: usize,
        window: VecDeque<ScalarValue>,
        null: ScalarValue,
    },
    CumeDist {
        key: Option<Vec<ScalarValue>>,
        end: usize,
    },
}

impl SuffixState {
    fn try_new(spec: &Spec) -> Result<Self> {
        let null = ScalarValue::try_from(&spec.data_type)?;
        Ok(match &spec.kind {
            Kind::CumeDist => Self::CumeDist { key: None, end: 0 },
            Kind::Suffix {
                func: SuffixFn::Aggregate(aggregate),
                ..
            } => Self::Aggregate(aggregate.create_accumulator()?),
            Kind::Suffix {
                func: SuffixFn::Value { kind, ignore_nulls },
                ..
            } => match *kind {
                ValueKind::First => Self::First {
                    ignore_nulls: *ignore_nulls,
                    next: null,
                },
                ValueKind::Last => Self::Last {
                    ignore_nulls: *ignore_nulls,
                    last: null,
                },
                ValueKind::Nth(n) => Self::Nth {
                    ignore_nulls: *ignore_nulls,
                    n,
                    window: VecDeque::new(),
                    null,
                },
            },
            _ => return Err(internal_datafusion_err!("not a reverse window expression")),
        })
    }

    fn size(&self) -> usize {
        match self {
            Self::Aggregate(accumulator) => accumulator.size(),
            Self::Nth { window, .. } => window.iter().map(|v| v.size()).sum(),
            _ => 0,
        }
    }

    /// `args` and `keys` hold `rows` rows in reverse order, the first of which is at
    /// partition index `end - 1`.
    fn evaluate(
        &mut self,
        args: &[ArrayRef],
        keys: &[ArrayRef],
        rows: usize,
        end: usize,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let values = match self {
            Self::Aggregate(accumulator) => {
                let mut values = Vec::with_capacity(rows);
                for row in 0..rows {
                    let slice = args.iter().map(|a| a.slice(row, 1)).collect::<Vec<_>>();
                    accumulator.update_batch(&slice)?;
                    values.push(accumulator.evaluate()?);
                }
                values
            }
            Self::First {
                ignore_nulls: false,
                ..
            } => return Ok(Arc::clone(&args[0])),
            Self::First { next, .. } => {
                let valid = is_valid(&args[0]);
                let mut values = Vec::with_capacity(rows);
                for row in 0..rows {
                    if valid(row) {
                        *next = ScalarValue::try_from_array(&args[0], row)?;
                    }
                    values.push(next.clone());
                }
                values
            }
            Self::Last {
                ignore_nulls: false,
                last,
            } => {
                if end == num_rows {
                    *last = ScalarValue::try_from_array(&args[0], 0)?;
                }
                return last.to_array_of_size(rows);
            }
            Self::Last { last, .. } => {
                if last.is_null() {
                    if let Some(row) = nth_valid(&args[0], 0) {
                        *last = ScalarValue::try_from_array(&args[0], row)?;
                        let null = ScalarValue::try_from(args[0].data_type())?;
                        let mut values = vec![null; row];
                        values.extend(std::iter::repeat_n(last.clone(), rows - row));
                        return ScalarValue::iter_to_array(values);
                    }
                }
                return last.to_array_of_size(rows);
            }
            Self::Nth {
                ignore_nulls,
                n,
                window,
                null,
            } => {
                let valid = is_valid(&args[0]);
                let mut values = Vec::with_capacity(rows);
                for row in 0..rows {
                    if !*ignore_nulls || valid(row) {
                        window.push_front(ScalarValue::try_from_array(&args[0], row)?);
                        window.truncate(*n);
                    }
                    values.push(match window.back() {
                        Some(value) if window.len() == *n => value.clone(),
                        _ => null.clone(),
                    });
                }
                values
            }
            Self::CumeDist { key, end: peer_end } => {
                let columns = keys
                    .iter()
                    .map(|values| SortColumn {
                        values: Arc::clone(values),
                        options: None,
                    })
                    .collect::<Vec<_>>();
                let mut result = Vec::with_capacity(rows);
                for range in evaluate_partition_ranges(rows, &columns)? {
                    let current = get_row_at_idx(keys, range.start)?;
                    if key.as_ref() != Some(&current) {
                        // The first row of a peer group seen in reverse is its last row.
                        *peer_end = end - range.start;
                        *key = Some(current);
                    }
                    let value = *peer_end as f64 / num_rows as f64;
                    result.extend(std::iter::repeat_n(value, range.len()));
                }
                return Ok(Arc::new(Float64Array::from(result)));
            }
        };
        ScalarValue::iter_to_array(values)
    }
}

/// Memory reservations are taken from the pool in chunks and kept, up to one chunk, from
/// one window partition to the next: with Spark's memory manager behind the pool, a call
/// per small partition contends on its lock.
const RESERVATION_CHUNK: usize = 1 << 20;

#[derive(Debug)]
struct ChunkedReservation {
    reservation: MemoryReservation,
    used: usize,
}

impl ChunkedReservation {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            reservation,
            used: 0,
        }
    }

    fn try_resize(&mut self, used: usize) -> Result<()> {
        if used > self.reservation.size()
            && self
                .reservation
                .try_resize(used.next_multiple_of(RESERVATION_CHUNK))
                .is_err()
        {
            self.reservation.try_resize(used)?;
        }
        self.used = used;
        Ok(())
    }

    fn try_grow(&mut self, additional: usize) -> Result<()> {
        self.try_resize(self.used + additional)
    }

    /// The tracked memory has been released; keeps up to a chunk for the next partition.
    fn release(&mut self) {
        self.used = 0;
        if self.reservation.size() > RESERVATION_CHUNK {
            self.reservation.resize(RESERVATION_CHUNK);
        }
    }

    fn trim(&mut self) {
        if self.reservation.size() > self.used {
            self.reservation.resize(self.used);
        }
    }

    fn free(&mut self) {
        self.used = 0;
        self.reservation.free();
    }
}

#[derive(Clone)]
enum SuffixSource {
    Memory(RecordBatch),
    File(Arc<dyn SpillFile>),
}

/// Reads the suffix stream in row order at a monotonically advancing frame start.
struct Cursor {
    start: FrameStart,
    options: Vec<SortOptions>,
    sources: VecDeque<SuffixSource>,
    stream: Option<SendableRecordBatchStream>,
    batch: Option<RecordBatch>,
    batch_start: usize,
    batch_id: usize,
    position: usize,
}

impl Cursor {
    async fn load(&mut self, spill: &SpillManager) -> Result<()> {
        loop {
            if let Some(stream) = &mut self.stream {
                match stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        if batch.num_rows() > 0 {
                            self.set_batch(batch);
                            return Ok(());
                        }
                        continue;
                    }
                    None => self.stream = None,
                }
            }
            match self.sources.pop_front() {
                Some(SuffixSource::Memory(batch)) => {
                    if batch.num_rows() > 0 {
                        self.set_batch(batch);
                        return Ok(());
                    }
                }
                Some(SuffixSource::File(file)) => {
                    // Open one file at a time, without prefetching the rest of the partition.
                    self.stream = Some(spill.read_spill_as_stream_unbuffered(file, None)?);
                }
                None => return Err(internal_datafusion_err!("window suffix stream ended early")),
            }
        }
    }

    fn set_batch(&mut self, batch: RecordBatch) {
        if let Some(previous) = &self.batch {
            self.batch_start += previous.num_rows();
        }
        self.batch = Some(batch);
        self.batch_id += 1;
    }

    async fn seek(&mut self, index: usize, spill: &SpillManager) -> Result<()> {
        while self
            .batch
            .as_ref()
            .is_none_or(|b| index >= self.batch_start + b.num_rows())
        {
            self.load(spill).await?;
        }
        Ok(())
    }

    /// Start of a RANGE frame, following DataFusion's `WindowFrameStateRange`.
    async fn range_start(
        &mut self,
        current: Vec<ScalarValue>,
        delta: Option<&ScalarValue>,
        preceding: bool,
        keys: usize,
        num_rows: usize,
        spill: &SpillManager,
    ) -> Result<usize> {
        let target = match delta {
            None => current,
            Some(delta) => {
                let descending = self.options[0].descending;
                // An overflowing boundary is unbounded within the partition.
                let edge = if preceding { self.position } else { num_rows };
                let mut targets = Vec::with_capacity(current.len());
                for value in current {
                    if value.is_null() {
                        targets.push(value);
                        continue;
                    }
                    let target = if preceding == descending {
                        value.add_checked(delta)
                    } else if value.is_unsigned() && &value < delta {
                        value.sub(&value)
                    } else {
                        value.sub_checked(delta)
                    };
                    match target {
                        Ok(target) => targets.push(target),
                        Err(_) => {
                            self.position = edge;
                            return Ok(edge);
                        }
                    }
                }
                targets
            }
        };
        while self.position < num_rows {
            self.seek(self.position, spill).await?;
            let batch = self.batch.as_ref().expect("seek loaded a batch");
            let row = get_row_at_idx(&batch.columns()[..keys], self.position - self.batch_start)?;
            if compare_rows(&row, &target, &self.options)?.is_lt() {
                self.position += 1;
            } else {
                break;
            }
        }
        Ok(self.position)
    }

    /// Returns the suffix batches referenced by the `rows` output rows starting at partition
    /// index `offset` (the first one is `empty`, for frames starting past the partition end)
    /// and the `(batch, row)` of each output row.
    #[allow(clippy::too_many_arguments)]
    async fn gather(
        &mut self,
        offset: usize,
        order: &[ArrayRef],
        rows: usize,
        num_rows: usize,
        keys: usize,
        empty: &RecordBatch,
        spill: &SpillManager,
    ) -> Result<(Vec<RecordBatch>, Vec<(usize, usize)>)> {
        let mut batches = vec![empty.clone()];
        let mut indices = Vec::with_capacity(rows);
        let mut last_id = None;
        let frame_start = self.start.clone();
        for row in 0..rows {
            let current = offset + row;
            let start = match &frame_start {
                FrameStart::Rows(delta) => (current as i64)
                    .saturating_add(*delta)
                    .clamp(0, num_rows as i64) as usize,
                FrameStart::Range { delta, preceding } => {
                    let values = get_row_at_idx(order, row)?;
                    self.range_start(values, delta.as_ref(), *preceding, keys, num_rows, spill)
                        .await?
                }
            };
            if start >= num_rows {
                indices.push((0, 0));
                continue;
            }
            self.seek(start, spill).await?;
            if last_id != Some(self.batch_id) {
                batches.push(self.batch.clone().expect("seek loaded a batch"));
                last_id = Some(self.batch_id);
            }
            indices.push((batches.len() - 1, start - self.batch_start));
        }
        Ok((batches, indices))
    }
}

struct ReverseState {
    layout: ReverseLayout,
    rows_spill: SpillManager,
    suffix_spill: SpillManager,
    /// Narrow reverse-order copies of the spilled row files.
    files: VecDeque<Arc<dyn SpillFile>>,
    /// Suffix batches in reverse row order that have not been spilled yet.
    pending: Vec<RecordBatch>,
    /// Suffix files in creation order. Each is in row order and covers the rows before the
    /// previous one.
    suffix_files: Vec<Arc<dyn SpillFile>>,
    cursors: Vec<Cursor>,
    /// One-row suffix batch for frames starting past the partition end.
    empty: Option<RecordBatch>,
    reservation: ChunkedReservation,
}

impl ReverseState {
    fn spill_rows(&mut self, rows: &[RecordBatch]) -> Result<()> {
        let narrow = rows
            .iter()
            .map(|b| self.layout.narrow(b))
            .collect::<Result<Vec<_>>>()?;
        if let Some(file) = self
            .rows_spill
            .spill_record_batch_and_finish(&reverse_batches(&narrow)?, "window reverse rows")?
        {
            self.files.push_back(file);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let batches = reverse_batches(&std::mem::take(&mut self.pending))?;
        if let Some(file) = self
            .suffix_spill
            .spill_record_batch_and_finish(&batches, "window suffix values")?
        {
            self.suffix_files.push(file);
        }
        self.reservation.free();
        Ok(())
    }

    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        if self.reservation.try_grow(size).is_err() {
            self.flush()?;
            if self.reservation.try_grow(size).is_err() {
                self.pending.push(batch);
                return self.flush();
            }
        }
        self.pending.push(batch);
        Ok(())
    }

    fn clear(&mut self) {
        self.files.clear();
        self.pending.clear();
        self.suffix_files.clear();
        self.cursors.clear();
        self.empty = None;
        self.reservation.release();
    }
}

struct WindowState {
    baseline: BaselineMetrics,
    input: SendableRecordBatchStream,
    input_done: bool,
    schema: SchemaRef,
    specs: Vec<Spec>,
    keys: Vec<PhysicalSortExpr>,
    order_by: Vec<PhysicalSortExpr>,
    pending: VecDeque<Pending>,
    current_key: Option<Vec<ScalarValue>>,
    num_rows: usize,
    accumulators: Vec<Option<Box<dyn Accumulator>>>,
    values: Vec<Option<ValueState>>,
    rows: Vec<RecordBatch>,
    files: VecDeque<Arc<dyn SpillFile>>,
    spill: SpillManager,
    rows_reservation: ChunkedReservation,
    state_reservation: ChunkedReservation,
    replay: Option<SendableRecordBatchStream>,
    result: Vec<Option<ScalarValue>>,
    emitting: bool,
    /// Partition index of the next replayed row.
    offset: usize,
    /// ORDER BY key and start index of the current percent_rank peer group.
    rank: Option<(Vec<ScalarValue>, usize)>,
    reverse: Option<ReverseState>,
    /// Partitions inside one input batch are evaluated together.
    batched: bool,
    /// Output batches smaller than half of this are concatenated up to it.
    target_rows: usize,
    buffered: Vec<RecordBatch>,
    buffered_rows: usize,
    buffered_offsets: OffsetBudget,
    ready: VecDeque<RecordBatch>,
}

impl WindowState {
    /// Returns reserved but unused memory to the pool before spilling.
    fn trim(&mut self, reverse: Option<&mut ReverseState>) {
        self.rows_reservation.trim();
        self.state_reservation.trim();
        if let Some(reverse) = reverse.or(self.reverse.as_mut()) {
            reverse.reservation.trim();
        }
    }

    fn spill_rows(&mut self) -> Result<()> {
        if let Some(reverse) = &mut self.reverse {
            reverse.spill_rows(&self.rows)?;
        }
        self.spill_replay_rows()
    }

    /// Spills the buffered rows for the replay only, once the reverse pass no longer needs
    /// them.
    fn spill_replay_rows(&mut self) -> Result<()> {
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

    fn state_size(&self) -> usize {
        let accumulators: usize = self.accumulators.iter().flatten().map(|a| a.size()).sum();
        let values: usize = self
            .values
            .iter()
            .flatten()
            .filter_map(|v| v.value.as_ref())
            .map(|v| v.size())
            .sum();
        accumulators + values
    }

    fn start_partition(&mut self, key: Vec<ScalarValue>) -> Result<()> {
        self.current_key = Some(key);
        self.num_rows = 0;
        self.accumulators = self
            .specs
            .iter()
            .map(|spec| match &spec.kind {
                Kind::Aggregate(aggregate) => aggregate.create_accumulator().map(Some),
                _ => Ok(None),
            })
            .collect::<Result<_>>()?;
        self.values = self
            .specs
            .iter()
            .map(|spec| match spec.kind {
                Kind::Value { kind, ignore_nulls } => Some(ValueState {
                    kind,
                    ignore_nulls,
                    seen: 0,
                    value: None,
                }),
                _ => None,
            })
            .collect();
        Ok(())
    }

    fn append(&mut self, batch: RecordBatch) -> Result<()> {
        self.num_rows += batch.num_rows();
        for ((spec, accumulator), value) in self
            .specs
            .iter()
            .zip(&mut self.accumulators)
            .zip(&mut self.values)
        {
            if accumulator.is_none() && value.is_none() {
                continue;
            }
            let args = spec
                .args
                .iter()
                .map(|e| e.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            if let Some(accumulator) = accumulator {
                accumulator.update_batch(&args)?;
            }
            if let Some(value) = value {
                value.update(&args[0])?;
            }
        }
        let state_size = self.state_size();
        if self.state_reservation.try_resize(state_size).is_err() {
            self.trim(None);
            if self.state_reservation.try_resize(state_size).is_err() {
                self.spill_rows()?;
                self.state_reservation.try_resize(state_size)?;
            }
        }
        let size = batch.get_array_memory_size();
        if self.rows_reservation.try_grow(size).is_err() {
            self.trim(None);
            if self.rows_reservation.try_grow(size).is_ok() {
                self.rows.push(batch);
                return Ok(());
            }
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

    /// Visits the partition from its last row to its first and stores, for every reverse
    /// expression, the value of the frame starting at each row.
    async fn reverse_pass(&mut self) -> Result<()> {
        let Some(mut reverse) = self.reverse.take() else {
            return Ok(());
        };
        let result = self.run_reverse(&mut reverse).await;
        self.reverse = Some(reverse);
        result
    }

    fn reverse_batch(
        &mut self,
        reverse: &mut ReverseState,
        states: &mut [SuffixState],
        end: &mut usize,
        narrow: RecordBatch,
    ) -> Result<()> {
        let rows = narrow.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let layout = &reverse.layout;
        let schema = Arc::clone(&layout.suffix_schema);
        let keys = &narrow.columns()[..layout.narrow_keys];
        let mut columns = keys[..layout.suffix_keys].to_vec();
        for (output, state) in layout.outputs.iter().zip(states.iter_mut()) {
            let values = state.evaluate(
                &narrow.columns()[output.args.clone()],
                keys,
                rows,
                *end,
                self.num_rows,
            )?;
            columns.push(cast_to(values, &self.specs[output.expr].data_type)?);
        }
        *end -= rows;
        let size = self.state_size() + states.iter().map(|s| s.size()).sum::<usize>();
        if self.state_reservation.try_resize(size).is_err() {
            self.trim(Some(reverse));
            if self.state_reservation.try_resize(size).is_ok() {
                return reverse.push(RecordBatch::try_new(schema, columns)?);
            }
            reverse.flush()?;
            if self.state_reservation.try_resize(size).is_err() {
                // The reverse pass works on its own copy of in-memory rows.
                self.spill_replay_rows()?;
                self.state_reservation.try_resize(size)?;
            }
        }
        reverse.push(RecordBatch::try_new(schema, columns)?)
    }

    async fn run_reverse(&mut self, reverse: &mut ReverseState) -> Result<()> {
        let mut states = reverse
            .layout
            .outputs
            .iter()
            .map(|o| SuffixState::try_new(&self.specs[o.expr]))
            .collect::<Result<Vec<_>>>()?;
        let fields = reverse.layout.suffix_schema.fields();
        let mut empty = Vec::with_capacity(fields.len());
        for field in &fields[..reverse.layout.suffix_keys] {
            empty.push(ScalarValue::try_from(field.data_type())?.to_array_of_size(1)?);
        }
        for output in &reverse.layout.outputs {
            let spec = &self.specs[output.expr];
            let value = empty_value(spec)?.to_array_of_size(1)?;
            empty.push(cast_to(value, &spec.data_type)?);
        }
        reverse.empty = Some(RecordBatch::try_new(
            Arc::clone(&reverse.layout.suffix_schema),
            empty,
        )?);
        let mut end = self.num_rows;
        if reverse.files.is_empty() {
            for batch in self.rows.clone().iter().rev() {
                let narrow = reverse_batch(&reverse.layout.narrow(batch)?)?;
                self.reverse_batch(reverse, &mut states, &mut end, narrow)?;
            }
        } else {
            while let Some(file) = reverse.files.pop_back() {
                let mut stream = reverse
                    .rows_spill
                    .read_spill_as_stream_unbuffered(file, None)?;
                while let Some(narrow) = stream.next().await {
                    self.reverse_batch(reverse, &mut states, &mut end, narrow?)?;
                }
            }
        }
        // The unspilled suffix batches cover the start of the partition and stay reserved
        // until the partition has been emitted.
        let memory = reverse_batches(&std::mem::take(&mut reverse.pending))?;
        let sources = memory
            .into_iter()
            .map(SuffixSource::Memory)
            .chain(
                reverse
                    .suffix_files
                    .iter()
                    .rev()
                    .map(|f| SuffixSource::File(Arc::clone(f))),
            )
            .collect::<VecDeque<_>>();
        let options = self.order_by.iter().map(|o| o.options).collect::<Vec<_>>();
        reverse.cursors = reverse
            .layout
            .starts
            .iter()
            .map(|start| Cursor {
                start: start.clone(),
                options: options.clone(),
                sources: sources.clone(),
                stream: None,
                batch: None,
                batch_start: 0,
                batch_id: 0,
                position: 0,
            })
            .collect();
        Ok(())
    }

    async fn finish_partition(&mut self) -> Result<()> {
        self.result = self
            .accumulators
            .iter_mut()
            .zip(&mut self.values)
            .zip(&self.specs)
            .map(|((accumulator, value), spec)| {
                Ok(match (accumulator, value) {
                    (Some(accumulator), _) => Some(accumulator.evaluate()?),
                    (_, Some(value)) => Some(match value.value.take() {
                        Some(v) => v,
                        None => ScalarValue::try_from(&spec.data_type)?,
                    }),
                    _ => None,
                })
            })
            .collect::<Result<_>>()?;
        self.accumulators.clear();
        self.values.clear();
        if !self.files.is_empty() {
            self.spill_rows()?;
        }
        self.reverse_pass().await?;
        self.state_reservation.release();
        if self.files.is_empty() {
            let batches = std::mem::take(&mut self.rows);
            self.replay = Some(Box::pin(RecordBatchStreamAdapter::new(
                self.input.schema(),
                stream::iter(batches.into_iter().map(Ok)),
            )));
        }
        self.offset = 0;
        self.rank = None;
        self.emitting = true;
        Ok(())
    }

    async fn window_columns(&mut self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let rows = batch.num_rows();
        let order = self
            .order_by
            .iter()
            .map(|o| o.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<_>>>()?;
        let order_values = order
            .iter()
            .map(|c| Arc::clone(&c.values))
            .collect::<Vec<_>>();
        let mut gathered = vec![];
        if let Some(reverse) = &mut self.reverse {
            let empty = reverse.empty.clone().expect("reverse pass ran");
            for cursor in &mut reverse.cursors {
                gathered.push(
                    cursor
                        .gather(
                            self.offset,
                            &order_values,
                            rows,
                            self.num_rows,
                            reverse.layout.suffix_keys,
                            &empty,
                            &reverse.suffix_spill,
                        )
                        .await?,
                );
            }
        }
        let mut columns = Vec::with_capacity(self.specs.len());
        let mut output = 0;
        for (i, spec) in self.specs.iter().enumerate() {
            let column: ArrayRef = match &spec.kind {
                Kind::Aggregate(_) | Kind::Value { .. } => self.result[i]
                    .as_ref()
                    .ok_or_else(|| internal_datafusion_err!("missing partition result"))?
                    .to_array_of_size(rows)?,
                Kind::Ntile(n) => Arc::new(UInt64Array::from_iter_values(
                    (self.offset..self.offset + rows).map(|row| ntile(row, *n, self.num_rows)),
                )),
                Kind::PercentRank => {
                    let denominator = (self.num_rows as f64 - 1.0).max(1.0);
                    let mut values = Vec::with_capacity(rows);
                    for range in evaluate_partition_ranges(rows, &order)? {
                        let key = get_row_at_idx(&order_values, range.start)?;
                        let start = match &self.rank {
                            Some((last, start)) if *last == key => *start,
                            _ => self.offset + range.start,
                        };
                        values.extend(std::iter::repeat_n(start as f64 / denominator, range.len()));
                        self.rank = Some((key, start));
                    }
                    Arc::new(Float64Array::from(values))
                }
                Kind::CumeDist | Kind::Suffix { .. } => {
                    let reverse = self
                        .reverse
                        .as_ref()
                        .ok_or_else(|| internal_datafusion_err!("missing reverse pass"))?;
                    let column = reverse.layout.suffix_keys + output;
                    let (batches, indices) = &gathered[reverse.layout.outputs[output].cursor];
                    output += 1;
                    let arrays = batches
                        .iter()
                        .map(|b| b.column(column).as_ref())
                        .collect::<Vec<_>>();
                    interleave(&arrays, indices)?
                }
            };
            columns.push(cast_to(column, &spec.data_type)?);
        }
        self.offset += rows;
        Ok(columns)
    }

    /// Queues `batch`, split at its partition `ranges`. When every expression supports it,
    /// the partitions that begin and end inside the batch are queued
    /// together; only the first, which may continue the partition in progress, and the last,
    /// which may continue into the next batch, are processed row by row.
    fn split(
        &mut self,
        batch: &RecordBatch,
        keys: &[SortColumn],
        ranges: Vec<Range<usize>>,
    ) -> Result<()> {
        let key_at = |row: usize| {
            keys.iter()
                .map(|k| ScalarValue::try_from_array(&k.values, row))
                .collect::<Result<Vec<_>>>()
        };
        let rows = |range: &Range<usize>| -> Result<Pending> {
            Ok(Pending::Rows(
                key_at(range.start)?,
                batch.slice(range.start, range.end - range.start),
            ))
        };
        if !self.batched || ranges.len() < 2 {
            for range in &ranges {
                let pending = rows(range)?;
                self.pending.push_back(pending);
            }
            return Ok(());
        }
        let continues = match &self.current_key {
            Some(current) => *current == key_at(ranges[0].start)?,
            None => false,
        };
        let first = usize::from(continues);
        let last = ranges.len() - 1;
        if continues {
            let pending = rows(&ranges[0])?;
            self.pending.push_back(pending);
        }
        if first < last {
            let start = ranges[first].start;
            let end = ranges[last - 1].end;
            let relative = ranges[first..last]
                .iter()
                .map(|r| r.start - start..r.end - start)
                .collect();
            self.pending.push_back(Pending::Partitions(
                batch.slice(start, end - start),
                relative,
            ));
        }
        let pending = rows(&ranges[last])?;
        self.pending.push_back(pending);
        Ok(())
    }

    /// Output rows of whole partitions at `ranges` of `batch`, each evaluated with the rows of
    /// the batch, without buffering or reserving them.
    fn evaluate_partitions(
        &self,
        batch: &RecordBatch,
        ranges: &[Range<usize>],
    ) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let mut partition = Vec::with_capacity(rows);
        for range in ranges {
            partition.extend(std::iter::repeat_n(range.clone(), range.len()));
        }
        let peers = if self.specs.iter().any(|s| {
            matches!(
                s.kind,
                Kind::PercentRank
                    | Kind::CumeDist
                    | Kind::Suffix {
                        start: FrameStart::Range { .. },
                        ..
                    }
            )
        }) {
            let order = self
                .order_by
                .iter()
                .map(|o| o.evaluate_to_sort_column(batch))
                .collect::<Result<Vec<_>>>()?;
            let mut peers = Vec::with_capacity(rows);
            for range in evaluate_partition_ranges(rows, &order)? {
                let mut start = range.start;
                while start < range.end {
                    let end = partition[start].end.min(range.end);
                    peers.extend(std::iter::repeat_n(start..end, end - start));
                    start = end;
                }
            }
            peers
        } else {
            vec![]
        };
        let mut columns = batch.columns().to_vec();
        for spec in &self.specs {
            let args = spec
                .args
                .iter()
                .map(|e| e.evaluate(batch)?.into_array(rows))
                .collect::<Result<Vec<_>>>()?;
            let column: ArrayRef = match &spec.kind {
                Kind::Aggregate(_) | Kind::Value { .. } => {
                    let mut values = Vec::with_capacity(ranges.len());
                    let mut indices = Vec::with_capacity(rows);
                    for (i, range) in ranges.iter().enumerate() {
                        values.push(constant_value(spec, &args, range)?);
                        indices.extend(std::iter::repeat_n(i as u32, range.len()));
                    }
                    let values = ScalarValue::iter_to_array(values)?;
                    take(values.as_ref(), &UInt32Array::from(indices), None)?
                }
                Kind::Ntile(n) => Arc::new(UInt64Array::from_iter_values(
                    partition
                        .iter()
                        .enumerate()
                        .map(|(row, p)| ntile(row - p.start, *n, p.len())),
                )),
                Kind::PercentRank => Arc::new(Float64Array::from_iter_values(
                    partition.iter().zip(&peers).map(|(p, peer)| {
                        (peer.start - p.start) as f64 / (p.len() as f64 - 1.0).max(1.0)
                    }),
                )),
                Kind::CumeDist => Arc::new(Float64Array::from_iter_values(
                    partition
                        .iter()
                        .zip(&peers)
                        .map(|(p, peer)| (peer.end - p.start) as f64 / p.len() as f64),
                )),
                Kind::Suffix { func, start } => {
                    let starts = (0..rows).map(|row| {
                        let p = &partition[row];
                        match start {
                            FrameStart::Rows(delta) => (row as i64)
                                .saturating_add(*delta)
                                .clamp(p.start as i64, p.end as i64)
                                as usize,
                            FrameStart::Range { .. } => peers[row].start,
                        }
                    });
                    match func {
                        SuffixFn::Value { kind, ignore_nulls } => {
                            let indices = suffix_value_indices(
                                &args[0],
                                &partition,
                                starts,
                                *kind,
                                *ignore_nulls,
                            );
                            take(args[0].as_ref(), &UInt32Array::from(indices), None)?
                        }
                        SuffixFn::Aggregate(aggregate) => {
                            let mut suffix = vec![ScalarValue::Null; rows];
                            for range in ranges {
                                let mut accumulator = aggregate.create_accumulator()?;
                                for row in range.clone().rev() {
                                    let slice =
                                        args.iter().map(|a| a.slice(row, 1)).collect::<Vec<_>>();
                                    accumulator.update_batch(&slice)?;
                                    suffix[row] = accumulator.evaluate()?;
                                }
                            }
                            let empty = empty_value(spec)?;
                            let values = starts
                                .zip(&partition)
                                .map(|(start, p)| {
                                    if start < p.end {
                                        suffix[start].clone()
                                    } else {
                                        empty.clone()
                                    }
                                })
                                .collect::<Vec<_>>();
                            ScalarValue::iter_to_array(values)?
                        }
                    }
                }
            };
            columns.push(cast_to(column, &spec.data_type)?);
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    /// Output batches, with small ones concatenated up to `target_rows`. A partition whose
    /// rows are replayed one input slice at a time would otherwise produce a batch per
    /// partition, each paying the fixed cost of every operator downstream.
    async fn next_coalesced(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.ready.pop_front() {
                return Ok(Some(batch));
            }
            match self.next_batch().await? {
                Some(batch) if batch.num_rows() * 2 >= self.target_rows => {
                    return match self.flush()? {
                        Some(buffered) => {
                            self.ready.push_back(batch);
                            Ok(Some(buffered))
                        }
                        None => Ok(Some(batch)),
                    };
                }
                Some(batch) => {
                    if !self.buffered_offsets.try_add(batch.columns()) {
                        let buffered = self.flush()?;
                        self.buffered_offsets.try_add(batch.columns());
                        self.buffered_rows = batch.num_rows();
                        self.buffered.push(batch);
                        return Ok(buffered);
                    }
                    self.buffered_rows += batch.num_rows();
                    self.buffered.push(batch);
                    if self.buffered_rows >= self.target_rows {
                        return self.flush();
                    }
                }
                None => return self.flush(),
            }
        }
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.buffered.is_empty() {
            return Ok(None);
        }
        let batches = std::mem::take(&mut self.buffered);
        self.buffered_rows = 0;
        self.buffered_offsets.clear();
        Ok(Some(concat_batches(&self.schema, &batches)?))
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.emitting {
                if let Some(replay) = &mut self.replay {
                    if let Some(batch) = replay.next().await {
                        let batch = batch?;
                        let mut columns = batch.columns().to_vec();
                        columns.extend(self.window_columns(&batch).await?);
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
                self.rows_reservation.release();
                if let Some(reverse) = &mut self.reverse {
                    reverse.clear();
                }
                self.current_key = None;
                self.result.clear();
                self.emitting = false;
            }
            match self.pending.pop_front() {
                Some(Pending::Rows(key, batch)) => {
                    if self
                        .current_key
                        .as_ref()
                        .is_some_and(|current| *current != key)
                    {
                        self.pending.push_front(Pending::Rows(key, batch));
                        self.finish_partition().await?;
                        continue;
                    }
                    if self.current_key.is_none() {
                        self.start_partition(key)?;
                    }
                    self.append(batch)?;
                    continue;
                }
                Some(Pending::Partitions(batch, ranges)) => {
                    if self.current_key.is_some() {
                        // The partition in progress ends where these begin.
                        self.pending.push_front(Pending::Partitions(batch, ranges));
                        self.finish_partition().await?;
                        continue;
                    }
                    let output = self.evaluate_partitions(&batch, &ranges)?;
                    self.baseline.record_output(output.num_rows());
                    return Ok(Some(output));
                }
                None => {}
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
                    let ranges = evaluate_partition_ranges(batch.num_rows(), &keys)?;
                    self.split(&batch, &keys, ranges)?;
                }
                None if self.current_key.is_some() => {
                    self.input_done = true;
                    self.finish_partition().await?;
                }
                None => return Ok(None),
            }
        }
    }
}

/// Value of a whole-partition aggregate or `first_value`/`last_value`/`nth_value` over the
/// rows of `args` at `range`.
fn constant_value(spec: &Spec, args: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
    let slice = args
        .iter()
        .map(|a| a.slice(range.start, range.len()))
        .collect::<Vec<_>>();
    match &spec.kind {
        Kind::Aggregate(aggregate) => {
            let mut accumulator = aggregate.create_accumulator()?;
            accumulator.update_batch(&slice)?;
            accumulator.evaluate()
        }
        Kind::Value { kind, ignore_nulls } => {
            let mut value = ValueState {
                kind: *kind,
                ignore_nulls: *ignore_nulls,
                seen: 0,
                value: None,
            };
            value.update(&slice[0])?;
            match value.value {
                Some(v) => Ok(v),
                None => ScalarValue::try_from(&spec.data_type),
            }
        }
        _ => Err(internal_datafusion_err!("not a constant window expression")),
    }
}

/// Row of `values` selected by `first_value`/`last_value`/`nth_value` over the frame from
/// each row's frame start in `starts` to the end of its partition, or `None` for NULL.
fn suffix_value_indices(
    values: &ArrayRef,
    partition: &[Range<usize>],
    starts: impl Iterator<Item = usize>,
    kind: ValueKind,
    ignore_nulls: bool,
) -> Vec<Option<u32>> {
    let rows = values.len();
    let valid = is_valid(values);
    let found = |index: usize, end: usize| (index < end).then_some(index as u32);
    if !ignore_nulls {
        return starts
            .zip(partition)
            .map(|(start, p)| match kind {
                _ if start >= p.end => None,
                ValueKind::First => found(start, p.end),
                ValueKind::Last => found(p.end - 1, p.end),
                ValueKind::Nth(n) => found(start.saturating_add(n - 1), p.end),
            })
            .collect();
    }
    match kind {
        ValueKind::First => {
            let mut next = vec![rows; rows + 1];
            for row in (0..rows).rev() {
                next[row] = if valid(row) {
                    row
                } else if row + 1 < partition[row].end {
                    next[row + 1]
                } else {
                    rows
                };
            }
            starts
                .zip(partition)
                .map(|(start, p)| found(next[start], p.end))
                .collect()
        }
        ValueKind::Last => {
            let mut last = vec![None; rows];
            let mut row = 0;
            while row < rows {
                let p = &partition[row];
                let value = p.clone().rev().find(|&i| valid(i));
                last[p.clone()].fill(value);
                row = p.end;
            }
            starts
                .zip(&last)
                .map(|(start, last)| last.filter(|&l| l >= start).map(|l| l as u32))
                .collect()
        }
        ValueKind::Nth(n) => {
            let mut before = Vec::with_capacity(rows + 1);
            let mut positions = vec![];
            for row in 0..rows {
                before.push(positions.len());
                if valid(row) {
                    positions.push(row);
                }
            }
            before.push(positions.len());
            starts
                .zip(partition)
                .map(|(start, p)| {
                    positions
                        .get(before[start].saturating_add(n - 1))
                        .and_then(|&index| found(index, p.end))
                })
                .collect()
        }
    }
}

/// SQL NTILE: with `base = num_rows / n`, the first `num_rows % n` buckets hold `base + 1`
/// rows and the rest hold `base` rows (matches DataFusion's and Spark's bucket sizes).
fn ntile(row: usize, n: u64, num_rows: usize) -> u64 {
    let (row, num_rows) = (row as u64, num_rows as u64);
    let base = num_rows / n;
    let remainder = num_rows % n;
    let large_rows = remainder * (base + 1);
    if row < large_rows {
        row / (base + 1) + 1
    } else {
        remainder + (row - large_rows) / base + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow::compute::{concat_batches, SortOptions};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::FunctionRegistry;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::{WindowFrame, WindowFunctionDefinition};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::CastExpr;
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_plan::windows::create_window_expr;
    use datafusion::prelude::{SessionConfig, SessionContext};

    const LARGE: usize = 10_000_000;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("ord", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, false),
        ]))
    }

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        let index = schema().index_of(name).unwrap();
        Arc::new(Column::new(name, index))
    }

    fn lit(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(value))
    }

    fn sort(name: &str, descending: bool) -> PhysicalSortExpr {
        PhysicalSortExpr::new(
            col(name),
            SortOptions {
                descending,
                nulls_first: !descending,
            },
        )
    }

    /// Rows sorted by `key` (nulls first) and `ord` in the requested direction, split into
    /// irregular batches (including an empty one) that cross partition boundaries.
    fn input(
        rows: &[(Option<i64>, Option<i64>, Option<i64>)],
        descending: bool,
        payload: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut rows = rows.to_vec();
        let options = [
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending,
                nulls_first: !descending,
            },
        ];
        rows.sort_by(|a, b| {
            compare_rows(
                &[ScalarValue::Int64(a.0), ScalarValue::Int64(a.1)],
                &[ScalarValue::Int64(b.0), ScalarValue::Int64(b.1)],
                &options,
            )
            .unwrap()
        });
        let payload = "x".repeat(payload);
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from(vec![payload.as_str(); rows.len()])),
            ],
        )?;
        let mut batches = vec![batch.slice(0, 0)];
        for start in (0..rows.len()).step_by(7) {
            let indices =
                UInt32Array::from_iter_values(start as u32..(start + 7).min(rows.len()) as u32);
            batches.push(take_record_batch(&batch, &indices)?);
        }
        let ordering = LexOrdering::new(vec![sort("key", false), sort("ord", descending)]);
        let config = MemorySourceConfig::try_new(&[batches], schema(), None)?
            .try_with_sort_information(vec![ordering.unwrap()])?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(config))))
    }

    /// Partitions of sizes 1, 2, 5, 37, 3, 90 and 12 (the first with a NULL key), with ORDER
    /// BY ties and NULLs, and NULL values.
    fn rows() -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
        let mut rows = vec![];
        let mut i = 0i64;
        for (p, size) in [1, 2, 5, 37, 3, 90, 12].into_iter().enumerate() {
            for j in 0..size {
                let key = (p > 0).then_some(p as i64);
                let ord = (j % 9 != 4).then_some(j * 7 % 11);
                let value = ((i * 5) % 7 != 0).then_some(i * 13 % 17 - 8);
                rows.push((key, ord, value));
                i += 1;
            }
        }
        rows
    }

    struct Expr {
        name: &'static str,
        args: Vec<Arc<dyn PhysicalExpr>>,
        frame: WindowFrame,
        ignore_nulls: bool,
    }

    fn expr(name: &'static str, args: Vec<Arc<dyn PhysicalExpr>>, frame: WindowFrame) -> Expr {
        Expr {
            name,
            args,
            frame,
            ignore_nulls: false,
        }
    }

    fn ignoring_nulls(mut expr: Expr) -> Expr {
        expr.ignore_nulls = true;
        expr
    }

    fn frame(units: WindowFrameUnits, start: WindowFrameBound) -> WindowFrame {
        let unbounded = match units {
            WindowFrameUnits::Rows => ScalarValue::UInt64(None),
            _ => ScalarValue::Int64(None),
        };
        WindowFrame::new_bounds(units, start, WindowFrameBound::Following(unbounded))
    }

    fn whole() -> WindowFrame {
        frame(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        )
    }

    fn running() -> WindowFrame {
        WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        )
    }

    fn rows_from(start: i64) -> WindowFrame {
        let bound = match start {
            0 => WindowFrameBound::CurrentRow,
            n if n < 0 => WindowFrameBound::Preceding(ScalarValue::UInt64(Some(-n as u64))),
            n => WindowFrameBound::Following(ScalarValue::UInt64(Some(n as u64))),
        };
        frame(WindowFrameUnits::Rows, bound)
    }

    fn range_from(preceding: Option<i64>) -> WindowFrame {
        let bound = match preceding {
            None => WindowFrameBound::CurrentRow,
            Some(n) => WindowFrameBound::Preceding(ScalarValue::Int64(Some(n))),
        };
        frame(WindowFrameUnits::Range, bound)
    }

    fn build(
        exprs: &[Expr],
        partitioned: bool,
        descending: bool,
    ) -> Result<Vec<Arc<dyn WindowExpr>>> {
        let state = SessionContext::new().state();
        let partition_by = if partitioned {
            vec![col("key")]
        } else {
            vec![]
        };
        exprs
            .iter()
            .map(|e| {
                let fun = state
                    .udwf(e.name)
                    .map(WindowFunctionDefinition::WindowUDF)
                    .or_else(|_| {
                        state
                            .udaf(e.name)
                            .map(WindowFunctionDefinition::AggregateUDF)
                    })?;
                create_window_expr(
                    &fun,
                    e.name.to_string(),
                    &e.args,
                    &partition_by,
                    &[sort("ord", descending)],
                    Arc::new(e.frame.clone()),
                    schema(),
                    e.ignore_nulls,
                    false,
                    None,
                )
            })
            .collect()
    }

    fn context(budget: usize) -> Result<(SessionContext, Arc<dyn MemoryPool>)> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        );
        Ok((
            SessionContext::new_with_config_rt(SessionConfig::new(), runtime),
            pool,
        ))
    }

    fn spill_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = plan
            .metrics()
            .and_then(|m| m.spill_count())
            .unwrap_or_default();
        own + plan.children().into_iter().map(spill_count).sum::<usize>()
    }

    /// Runs `plan` and checks that reservations are released, both after a complete run and
    /// when the output stream is dropped part way.
    async fn run(plan: &Arc<dyn ExecutionPlan>, budget: usize) -> Result<(RecordBatch, usize)> {
        let (ctx, pool) = context(budget)?;
        let mut output = plan.execute(0, ctx.task_ctx())?;
        let mut batches = vec![];
        while let Some(batch) = output.next().await {
            batches.push(batch?);
        }
        drop(output);
        assert_eq!(pool.reserved(), 0);
        let spills = spill_count(plan);
        let mut cancelled = plan.execute(0, ctx.task_ctx())?;
        assert!(cancelled.next().await.transpose()?.is_some());
        drop(cancelled);
        assert_eq!(pool.reserved(), 0);
        Ok((concat_batches(&plan.schema(), &batches)?, spills))
    }

    /// Compares the spilling plan with DataFusion's in-memory `WindowAggExec` (the previous
    /// behaviour) without spilling, with spilling and with batches larger than the budget.
    /// `tiny` is below a single input batch; it must still fit the accumulator state, which
    /// is not spillable.
    async fn check(exprs: &[Expr], descending: bool, tiny: usize) -> Result<()> {
        for partitioned in [false, true] {
            let window = build(exprs, partitioned, descending)?;
            let ignore_nulls = exprs.iter().map(|e| e.ignore_nulls).collect::<Vec<_>>();
            let input = input(&rows(), descending, 1024)?;
            let reference: Arc<dyn ExecutionPlan> = Arc::new(WindowAggExec::try_new(
                window.clone(),
                Arc::clone(&input),
                partitioned,
            )?);
            let (ctx, _) = context(LARGE)?;
            let expected = concat_batches(
                &reference.schema(),
                &datafusion::physical_plan::collect(reference, ctx.task_ctx()).await?,
            )?;
            let plan =
                PartitionAggregateWindowExec::try_plan(window, input, partitioned, ignore_nulls)?
                    .expect("spilling window plan");
            assert_eq!(plan.schema(), expected.schema());
            for budget in [LARGE, 16_000, tiny] {
                let (actual, spills) = run(&plan, budget).await?;
                assert_eq!(actual.num_rows(), rows().len());
                for (i, field) in expected.schema().fields().iter().enumerate() {
                    assert_eq!(
                        actual.column(i).as_ref(),
                        expected.column(i).as_ref(),
                        "column {} partitioned={partitioned} budget={budget}",
                        field.name()
                    );
                }
                assert_eq!(spills > 0, budget < LARGE, "budget={budget}");
            }
        }
        Ok(())
    }

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
                    batches.push(take_record_batch(&batch, &indices)?);
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
                let plan = PartitionAggregateWindowExec::try_new(
                    WindowAggExec::try_new(vec![expr], input, partitioned)?,
                    vec![false],
                )
                .expect("supported");
                let (ctx, pool) = context(budget)?;
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

    #[tokio::test]
    async fn whole_partition_values_spill() -> Result<()> {
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        let mut exprs = vec![];
        for ignore in [false, true] {
            let with = |e: Expr| if ignore { ignoring_nulls(e) } else { e };
            exprs.push(with(expr("first_value", vec![col("value")], whole())));
            exprs.push(with(expr("last_value", vec![col("value")], whole())));
            exprs.push(with(expr("nth_value", vec![col("value"), n(2)], whole())));
            exprs.push(with(expr("nth_value", vec![col("value"), n(40)], whole())));
        }
        check(&exprs, false, 1024).await
    }

    #[tokio::test]
    async fn partition_size_functions_spill() -> Result<()> {
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        for descending in [false, true] {
            let exprs = vec![
                expr("ntile", vec![n(3)], running()),
                expr("ntile", vec![n(4)], running()),
                expr("ntile", vec![n(100)], running()),
                expr("percent_rank", vec![], running()),
                expr("cume_dist", vec![], running()),
            ];
            check(&exprs, descending, 1024).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn suffix_frames_spill() -> Result<()> {
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        for descending in [false, true] {
            let mut exprs = vec![];
            for frame in [
                rows_from(0),
                rows_from(-2),
                rows_from(3),
                range_from(None),
                range_from(Some(2)),
            ] {
                for name in ["sum", "count", "min", "max"] {
                    exprs.push(expr(name, vec![col("value")], frame.clone()));
                }
                // Comet plans AVG over a Float64 cast of integral inputs.
                let double = Arc::new(CastExpr::new(col("value"), DataType::Float64, None));
                exprs.push(expr("avg", vec![double], frame.clone()));
                for ignore in [false, true] {
                    let with = |e: Expr| if ignore { ignoring_nulls(e) } else { e };
                    exprs.push(with(expr("first_value", vec![col("value")], frame.clone())));
                    exprs.push(with(expr("last_value", vec![col("value")], frame.clone())));
                    exprs.push(with(expr(
                        "nth_value",
                        vec![col("value"), n(3)],
                        frame.clone(),
                    )));
                }
            }
            check(&exprs, descending, 6000).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn mixed_node_spills() -> Result<()> {
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        let exprs = vec![
            expr("sum", vec![col("value")], whole()),
            expr("row_number", vec![], running()),
            ignoring_nulls(expr("first_value", vec![col("value")], whole())),
            expr("ntile", vec![n(4)], running()),
            expr("sum", vec![col("value")], running()),
            expr("max", vec![col("value")], rows_from(-1)),
            expr("cume_dist", vec![], running()),
            expr("lag", vec![col("value")], running()),
        ];
        check(&exprs, false, 1024).await?;
        let window = build(&exprs, true, false)?;
        let plan = PartitionAggregateWindowExec::try_plan(
            window,
            input(&rows(), false, 8)?,
            true,
            vec![false; exprs.len()],
        )?
        .unwrap();
        // Bounded expressions stream below the spilling operator.
        let spilling = plan.children()[0];
        assert_eq!(spilling.name(), "PartitionAggregateWindowExec");
        assert_eq!(spilling.children()[0].name(), "BoundedWindowAggExec");
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_expressions_keep_window_agg_exec() -> Result<()> {
        let window = build(
            &[expr("array_agg", vec![col("value")], whole())],
            true,
            false,
        )?;
        assert!(PartitionAggregateWindowExec::try_plan(
            window,
            input(&rows(), false, 8)?,
            true,
            vec![false],
        )?
        .is_none());
        Ok(())
    }

    /// Hand-checked Spark semantics on one partition: values [NULL, 1, NULL, 3] ordered by
    /// [1, 1, 2, 3], in memory and spilled.
    #[tokio::test]
    async fn spark_semantics() -> Result<()> {
        let rows = vec![
            (Some(0), Some(1), None),
            (Some(0), Some(1), Some(1)),
            (Some(0), Some(2), None),
            (Some(0), Some(3), Some(3)),
        ];
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        let exprs = vec![
            ignoring_nulls(expr("first_value", vec![col("value")], whole())),
            ignoring_nulls(expr("last_value", vec![col("value")], whole())),
            ignoring_nulls(expr("nth_value", vec![col("value"), n(2)], whole())),
            expr("nth_value", vec![col("value"), n(2)], whole()),
            expr("ntile", vec![n(3)], running()),
            expr("ntile", vec![n(10)], running()),
            expr("percent_rank", vec![], running()),
            expr("cume_dist", vec![], running()),
            ignoring_nulls(expr("first_value", vec![col("value")], rows_from(0))),
            expr("sum", vec![col("value")], rows_from(1)),
            expr("sum", vec![col("value")], range_from(None)),
        ];
        let window = build(&exprs, true, false)?;
        let ignore = exprs.iter().map(|e| e.ignore_nulls).collect();
        let plan = PartitionAggregateWindowExec::try_plan(
            window,
            input(&rows, false, 1024)?,
            true,
            ignore,
        )?
        .unwrap();
        for budget in [LARGE, 1024] {
            let (batch, _) = run(&plan, budget).await?;
            let int = |i: usize| {
                let a = batch
                    .column(4 + i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                a.iter().collect::<Vec<_>>()
            };
            let uint = |i: usize| {
                let a = batch
                    .column(4 + i)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                a.values().to_vec()
            };
            let float = |i: usize| {
                let a = batch
                    .column(4 + i)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                a.values().to_vec()
            };
            assert_eq!(int(0), vec![Some(1); 4]);
            assert_eq!(int(1), vec![Some(3); 4]);
            assert_eq!(int(2), vec![Some(3); 4]);
            assert_eq!(int(3), vec![Some(1); 4]);
            assert_eq!(uint(4), vec![1, 1, 2, 3]);
            assert_eq!(uint(5), vec![1, 2, 3, 4]);
            assert_eq!(float(6), vec![0.0, 0.0, 2.0 / 3.0, 1.0]);
            assert_eq!(float(7), vec![0.5, 0.5, 0.75, 1.0]);
            assert_eq!(int(8), vec![Some(1), Some(1), Some(3), Some(3)]);
            assert_eq!(int(9), vec![Some(4), Some(3), Some(3), None]);
            assert_eq!(int(10), vec![Some(4), Some(4), Some(3), Some(3)]);
        }
        Ok(())
    }

    /// Many small partitions (the first with a NULL key), sorted by key and `ord`, in batches
    /// of `chunk` rows.
    fn small_partitions(chunk: usize) -> Result<(Arc<dyn ExecutionPlan>, usize)> {
        let mut rows = vec![];
        let mut i = 0i64;
        for (p, size) in [1, 1, 2, 1, 3, 1, 8, 1, 1, 20, 1, 2, 1, 1, 5, 1]
            .into_iter()
            .cycle()
            .take(160)
            .enumerate()
        {
            for j in 0..size {
                let key = (p > 0).then_some(p as i64);
                let value = ((i * 5) % 7 != 0).then_some(i * 13 % 17 - 8);
                rows.push((key, Some(j), value));
                i += 1;
            }
        }
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from(vec!["p"; rows.len()])),
            ],
        )?;
        let batches = (0..rows.len())
            .step_by(chunk)
            .map(|start| batch.slice(start, chunk.min(rows.len() - start)))
            .collect::<Vec<_>>();
        let ordering = LexOrdering::new(vec![sort("key", false), sort("ord", false)]);
        let config = MemorySourceConfig::try_new(&[batches], schema(), None)?
            .try_with_sort_information(vec![ordering.unwrap()])?;
        Ok((Arc::new(DataSourceExec::new(Arc::new(config))), rows.len()))
    }

    /// Partitions within an input batch are evaluated together, and partitions crossing batch
    /// boundaries row by row; both must match `WindowAggExec`, with and without spilling, and
    /// the output must be concatenated instead of a batch per partition.
    #[tokio::test]
    async fn small_partitions_within_and_across_batches() -> Result<()> {
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        let exprs = [
            expr("sum", vec![col("value")], whole()),
            expr("count", vec![col("value")], whole()),
            expr("min", vec![col("value")], whole()),
            expr("max", vec![col("value")], whole()),
            expr("first_value", vec![col("value")], whole()),
            ignoring_nulls(expr("last_value", vec![col("value")], whole())),
            expr("nth_value", vec![col("value"), n(2)], whole()),
            ignoring_nulls(expr("nth_value", vec![col("value"), n(3)], whole())),
        ];
        let window = build(&exprs, true, false)?;
        let ignore_nulls = exprs.iter().map(|e| e.ignore_nulls).collect::<Vec<_>>();
        for chunk in [1, 2, 3, 7, 64, 4096] {
            let (input, num_rows) = small_partitions(chunk)?;
            let reference: Arc<dyn ExecutionPlan> = Arc::new(WindowAggExec::try_new(
                window.clone(),
                Arc::clone(&input),
                true,
            )?);
            let (ctx, _) = context(LARGE)?;
            let expected = concat_batches(
                &reference.schema(),
                &datafusion::physical_plan::collect(reference, ctx.task_ctx()).await?,
            )?;
            let plan = PartitionAggregateWindowExec::try_plan(
                window.clone(),
                input,
                true,
                ignore_nulls.clone(),
            )?
            .expect("spilling window plan");
            for budget in [LARGE, 16_000] {
                let (actual, _) = run(&plan, budget).await?;
                assert_eq!(actual.num_rows(), num_rows);
                for (i, field) in expected.schema().fields().iter().enumerate() {
                    assert_eq!(
                        actual.column(i).as_ref(),
                        expected.column(i).as_ref(),
                        "column {} chunk={chunk} budget={budget}",
                        field.name()
                    );
                }
            }
            let (ctx, _) = context(LARGE)?;
            let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
            // Fewer rows than one output batch: concatenated, not a batch per partition.
            assert!(num_rows < ctx.task_ctx().session_config().batch_size());
            assert!(
                batches.len() <= 2,
                "chunk={chunk}: {} output batches for {num_rows} rows",
                batches.len()
            );
        }
        Ok(())
    }

    /// Measures a whole-partition `sum`/`count` over a wide input with many small window
    /// partitions: time and output batch count, against DataFusion's `WindowAggExec`.
    #[tokio::test]
    #[ignore]
    async fn bench_many_small_partitions() -> Result<()> {
        use datafusion::functions_aggregate::count::count_udaf;
        use datafusion::physical_plan::windows::WindowAggExec;
        const ROWS: usize = 193_536;
        const WIDE: usize = 125;
        for rows_per_key in [1usize, 10, 1000] {
            let mut fields = vec![Field::new("key", DataType::Int64, false)];
            for i in 0..WIDE {
                fields.push(Field::new(format!("c{i}"), DataType::Int64, true));
            }
            let schema = Arc::new(Schema::new(fields));
            let mut batches = vec![];
            for start in (0..ROWS).step_by(8192) {
                let end = (start + 8192).min(ROWS);
                let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|r| (r / rows_per_key) as i64),
                ))];
                for i in 0..WIDE {
                    columns.push(Arc::new(Int64Array::from_iter_values(
                        (start..end).map(|r| (r * 31 + i) as i64),
                    )));
                }
                batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
            }
            let window = |schema: &SchemaRef| -> Result<Vec<Arc<dyn WindowExpr>>> {
                let frame = Arc::new(whole());
                let partition_by = vec![col_in("key", schema)];
                Ok(vec![
                    create_window_expr(
                        &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                        "sum".to_string(),
                        &[col_in("c0", schema)],
                        &partition_by,
                        &[],
                        Arc::clone(&frame),
                        Arc::clone(schema),
                        false,
                        false,
                        None,
                    )?,
                    create_window_expr(
                        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
                        "count".to_string(),
                        &[col_in("c1", schema)],
                        &partition_by,
                        &[],
                        frame,
                        Arc::clone(schema),
                        false,
                        false,
                        None,
                    )?,
                ])
            };
            let source = || -> Result<Arc<dyn ExecutionPlan>> {
                let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
                    col_in("key", &schema),
                    SortOptions::default(),
                )])
                .unwrap();
                let config =
                    MemorySourceConfig::try_new(&[batches.clone()], Arc::clone(&schema), None)?
                        .try_with_sort_information(vec![ordering])?;
                Ok(Arc::new(DataSourceExec::new(Arc::new(config))))
            };
            let plans: Vec<(&str, Arc<dyn ExecutionPlan>)> = vec![
                (
                    "PartitionAggregateWindowExec",
                    PartitionAggregateWindowExec::try_plan(
                        window(&schema)?,
                        source()?,
                        true,
                        vec![false, false],
                    )?
                    .expect("planned"),
                ),
                (
                    "WindowAggExec",
                    Arc::new(WindowAggExec::try_new(window(&schema)?, source()?, true)?),
                ),
            ];
            for (name, plan) in plans {
                let (ctx, _pool) = context(usize::MAX / 2)?;
                let started = std::time::Instant::now();
                let mut output = plan.execute(0, ctx.task_ctx())?;
                let (mut out_batches, mut out_rows) = (0usize, 0usize);
                while let Some(batch) = output.next().await {
                    out_batches += 1;
                    out_rows += batch?.num_rows();
                }
                println!(
                    "BENCH rows_per_key={rows_per_key} {name}: {:?}, {out_rows} rows in {out_batches} batches",
                    started.elapsed()
                );
            }
        }
        Ok(())
    }

    /// Memory pool that counts the calls reaching it, as each reaches Spark's memory manager
    /// through JNI in Comet.
    #[derive(Debug)]
    struct CountingPool {
        inner: GreedyMemoryPool,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for CountingPool {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "CountingPool")
        }
    }

    impl MemoryPool for CountingPool {
        fn name(&self) -> &str {
            "counting"
        }
        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.grow(reservation, additional)
        }
        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.shrink(reservation, shrink)
        }
        fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.try_grow(reservation, additional)
        }
        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    /// Rows of partitions with `sizes`, sorted by key and `ord`, with NULL values, NULL and
    /// tied ORDER BY values, and a NULL key for the first partition.
    fn layout_rows(sizes: &[usize], seed: u64) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
        let mut state = seed;
        let mut next = move |n: u64| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % n
        };
        let mut rows = vec![];
        for (p, &size) in sizes.iter().enumerate() {
            let mut ord = 0i64;
            for _ in 0..size {
                let key = (p > 0).then_some(p as i64);
                let null_ord = next(10) == 0;
                ord += next(3) as i64;
                let value = (next(3) != 0).then(|| next(41) as i64 - 20);
                rows.push((key, (!null_ord).then_some(ord), value));
            }
        }
        rows
    }

    fn batched(
        rows: &[(Option<i64>, Option<i64>, Option<i64>)],
        chunk: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut rows = rows.to_vec();
        let options = [SortOptions {
            descending: false,
            nulls_first: true,
        }; 2];
        rows.sort_by(|a, b| {
            compare_rows(
                &[ScalarValue::Int64(a.0), ScalarValue::Int64(a.1)],
                &[ScalarValue::Int64(b.0), ScalarValue::Int64(b.1)],
                &options,
            )
            .unwrap()
        });
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from(vec!["p"; rows.len()])),
            ],
        )?;
        let batches = (0..rows.len())
            .step_by(chunk)
            .map(|start| batch.slice(start, chunk.min(rows.len() - start)))
            .collect::<Vec<_>>();
        let ordering = LexOrdering::new(vec![sort("key", false), sort("ord", false)]);
        let config = MemorySourceConfig::try_new(&[batches], schema(), None)?
            .try_with_sort_information(vec![ordering.unwrap()])?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(config))))
    }

    fn every_expression(range_offset: bool) -> Vec<Expr> {
        let n = |n: i64| lit(ScalarValue::Int64(Some(n)));
        let mut frames = vec![rows_from(0), rows_from(-2), rows_from(3), range_from(None)];
        if range_offset {
            frames.push(range_from(Some(2)));
        }
        let mut exprs = vec![
            expr("sum", vec![col("value")], whole()),
            expr("count", vec![col("value")], whole()),
            ignoring_nulls(expr("last_value", vec![col("value")], whole())),
            expr("nth_value", vec![col("value"), n(2)], whole()),
            expr("ntile", vec![n(3)], running()),
            expr("percent_rank", vec![], running()),
            expr("cume_dist", vec![], running()),
        ];
        for frame in frames {
            exprs.push(expr("sum", vec![col("value")], frame.clone()));
            exprs.push(expr("count", vec![col("value")], frame.clone()));
            exprs.push(expr("max", vec![col("value")], frame.clone()));
            for ignore in [false, true] {
                let with = |e: Expr| if ignore { ignoring_nulls(e) } else { e };
                exprs.push(with(expr("first_value", vec![col("value")], frame.clone())));
                exprs.push(with(expr("last_value", vec![col("value")], frame.clone())));
                exprs.push(with(expr(
                    "nth_value",
                    vec![col("value"), n(2)],
                    frame.clone(),
                )));
                exprs.push(with(expr(
                    "nth_value",
                    vec![col("value"), n(4)],
                    frame.clone(),
                )));
            }
        }
        exprs
    }

    /// Compares with `WindowAggExec` over partition size distributions and batch sizes that
    /// put partitions within batches, across batch boundaries and spanning several batches,
    /// without and with spilling.
    #[tokio::test]
    async fn partition_layouts_match_window_agg_exec() -> Result<()> {
        let layouts: Vec<Vec<usize>> = vec![
            vec![1; 300],
            vec![2; 150],
            [1, 2, 3, 1, 4, 2, 1, 1, 6].repeat(25),
            [1, 1, 2, 90, 1, 3, 1, 250, 2, 1, 7].repeat(4),
            vec![700],
            [3, 1, 1, 2].repeat(60).into_iter().chain([400]).collect(),
        ];
        for range_offset in [false, true] {
            let exprs = every_expression(range_offset);
            let window = build(&exprs, true, false)?;
            let ignore_nulls = exprs.iter().map(|e| e.ignore_nulls).collect::<Vec<_>>();
            for (l, sizes) in layouts.iter().enumerate() {
                let rows = layout_rows(sizes, l as u64 + 1);
                for chunk in [1, 3, 7, 64, 1000] {
                    let input = batched(&rows, chunk)?;
                    let reference: Arc<dyn ExecutionPlan> = Arc::new(WindowAggExec::try_new(
                        window.clone(),
                        Arc::clone(&input),
                        true,
                    )?);
                    let (ctx, _) = context(LARGE)?;
                    let expected = concat_batches(
                        &reference.schema(),
                        &datafusion::physical_plan::collect(reference, ctx.task_ctx()).await?,
                    )?;
                    let plan = PartitionAggregateWindowExec::try_plan(
                        window.clone(),
                        input,
                        true,
                        ignore_nulls.clone(),
                    )?
                    .expect("spilling window plan");
                    for budget in [LARGE, 16_000] {
                        let (actual, _) = run(&plan, budget).await?;
                        assert_eq!(actual.num_rows(), rows.len());
                        for (i, field) in expected.schema().fields().iter().enumerate() {
                            assert_eq!(
                                actual.column(i).as_ref(),
                                expected.column(i).as_ref(),
                                "column {i} {} layout={l} chunk={chunk} budget={budget} \
                                 range_offset={range_offset}",
                                field.name()
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// A suffix frame over many small partitions: the memory pool sees a bounded number of
    /// calls instead of several per partition, and a partition larger than the budget still
    /// spills.
    #[tokio::test]
    async fn small_partitions_do_not_call_the_pool_per_partition() -> Result<()> {
        let exprs = [ignoring_nulls(expr(
            "first_value",
            vec![col("value")],
            rows_from(0),
        ))];
        let window = build(&exprs, true, false)?;
        let sizes = [1, 2, 3, 2, 4, 1, 3].repeat(1500);
        let rows = layout_rows(&sizes, 7);
        let plan = PartitionAggregateWindowExec::try_plan(
            window.clone(),
            batched(&rows, 1000)?,
            true,
            vec![true],
        )?
        .expect("spilling window plan");
        let pool = Arc::new(CountingPool {
            inner: GreedyMemoryPool::new(LARGE),
            calls: Default::default(),
        });
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                .build()?,
        );
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let output = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(
            output.iter().map(|b| b.num_rows()).sum::<usize>(),
            rows.len()
        );
        let calls = pool.calls.load(std::sync::atomic::Ordering::Relaxed);
        let batches = rows.len().div_ceil(1000);
        assert!(
            calls <= 8 * batches,
            "{calls} pool calls for {} partitions in {batches} batches",
            sizes.len()
        );
        assert_eq!(pool.reserved(), 0);

        let large = layout_rows(&[1, 2, 3000, 1, 2], 3);
        let plan = PartitionAggregateWindowExec::try_plan(
            window,
            batched(&large, 100)?,
            true,
            vec![true],
        )?
        .expect("spilling window plan");
        let (_, spills) = run(&plan, 16_000).await?;
        assert!(spills > 0);
        Ok(())
    }

    /// Measures `FIRST_VALUE(x) IGNORE NULLS` over `ROWS BETWEEN CURRENT ROW AND UNBOUNDED
    /// FOLLOWING` with about 2.3 rows per partition, against DataFusion's `WindowAggExec`.
    #[tokio::test]
    #[ignore]
    async fn bench_small_partitions_suffix_frame() -> Result<()> {
        let exprs = [ignoring_nulls(expr(
            "first_value",
            vec![col("value")],
            rows_from(0),
        ))];
        let window = build(&exprs, true, false)?;
        let sizes = [1, 2, 3, 2, 4, 1, 3, 2].repeat(250_000);
        let rows = layout_rows(&sizes, 11);
        let input = batched(&rows, 8192)?;
        let plans: Vec<(&str, Arc<dyn ExecutionPlan>)> = vec![
            (
                "PartitionAggregateWindowExec",
                PartitionAggregateWindowExec::try_plan(
                    window.clone(),
                    Arc::clone(&input),
                    true,
                    vec![true],
                )?
                .expect("planned"),
            ),
            (
                "WindowAggExec",
                Arc::new(WindowAggExec::try_new(window, input, true)?),
            ),
        ];
        for (name, plan) in plans {
            let pool = Arc::new(CountingPool {
                inner: GreedyMemoryPool::new(usize::MAX / 2),
                calls: Default::default(),
            });
            let runtime = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                    .build()?,
            );
            let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
            let started = std::time::Instant::now();
            let output = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
            println!(
                "BENCH {name}: {:?}, {} rows, {} partitions, {} pool calls",
                started.elapsed(),
                output.iter().map(|b| b.num_rows()).sum::<usize>(),
                sizes.len(),
                pool.calls.load(std::sync::atomic::Ordering::Relaxed)
            );
        }
        Ok(())
    }

    fn col_in(name: &str, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        datafusion::physical_expr::expressions::col(name, schema).unwrap()
    }
}
