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

//! Streaming window operator for input sorted by `[partition_by..., order_by...]`, limited to
//! `ROW_NUMBER`, `RANK`, `DENSE_RANK` and `LEAD` / `LAG` with a constant offset and default
//! (without `IGNORE NULLS`).
//!
//! Every input batch is processed as a whole: partition and peer boundaries come from
//! vectorized comparisons of adjacent rows (the first row against the last row of the previous
//! batch), the window columns are computed for the whole batch, and the input columns pass
//! through untouched. Only running counters, the last keys and, for `LAG`, the last values
//! cross batch boundaries. A batch with a `LEAD` waits until enough following rows have
//! arrived to resolve its last rows.

use std::collections::VecDeque;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    make_comparator, new_empty_array, Array, ArrayRef, BooleanBufferBuilder, Int32Array,
    Int64Array, RecordBatch, RecordBatchOptions, UInt64Array,
};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::cmp::distinct;
use arrow::compute::{concat, interleave, SortOptions};
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

pub const SORTED_WINDOW_MAX_OFFSET: i64 = 1024;

#[derive(Debug)]
pub struct SortedWindowEnabled;

#[derive(Debug, Clone)]
pub enum SortedWindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Shift {
        value: Arc<dyn PhysicalExpr>,
        offset: i64,
        default: ScalarValue,
    },
}

impl SortedWindowFunction {
    fn offset(&self) -> i64 {
        match self {
            SortedWindowFunction::Shift { offset, .. } => *offset,
            _ => 0,
        }
    }

    fn name(&self) -> String {
        match self {
            SortedWindowFunction::RowNumber => "row_number".to_string(),
            SortedWindowFunction::Rank => "rank".to_string(),
            SortedWindowFunction::DenseRank => "dense_rank".to_string(),
            SortedWindowFunction::Shift { value, offset, .. } if *offset >= 0 => {
                format!("lead({value}, {offset})")
            }
            SortedWindowFunction::Shift { value, offset, .. } => {
                format!("lag({value}, {})", offset.unsigned_abs())
            }
        }
    }
}

pub fn sorted_window_supports_output_type(
    function: &SortedWindowFunction,
    data_type: &DataType,
    input_schema: &Schema,
) -> Result<bool> {
    Ok(match function {
        SortedWindowFunction::Shift {
            value,
            offset,
            default,
        } => {
            offset.unsigned_abs() <= SORTED_WINDOW_MAX_OFFSET as u64
                && &value.data_type(input_schema)? == data_type
                && &default.data_type() == data_type
                && !data_type.is_null()
        }
        _ => matches!(
            data_type,
            DataType::Int32 | DataType::Int64 | DataType::UInt64
        ),
    })
}

#[derive(Debug)]
pub struct SortedWindowExec {
    input: Arc<dyn ExecutionPlan>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    functions: Vec<SortedWindowFunction>,
    fields: Vec<FieldRef>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SortedWindowExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Vec<PhysicalSortExpr>,
        functions: Vec<SortedWindowFunction>,
        fields: Vec<FieldRef>,
    ) -> Result<Self> {
        if functions.len() != fields.len() {
            return internal_err!("SortedWindowExec needs one output field per function");
        }
        let input_schema = input.schema();
        for (function, field) in functions.iter().zip(fields.iter()) {
            if !sorted_window_supports_output_type(function, field.data_type(), &input_schema)? {
                return internal_err!(
                    "SortedWindowExec does not support {} with output type {}",
                    function.name(),
                    field.data_type()
                );
            }
        }
        let mut schema_fields: Vec<FieldRef> = input_schema.fields().iter().cloned().collect();
        schema_fields.extend(fields.iter().cloned());
        let schema = Arc::new(Schema::new_with_metadata(
            schema_fields,
            input_schema.metadata().clone(),
        ));
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        if let Some(ordering) = input.output_ordering() {
            eq_properties.add_ordering(ordering.iter().cloned());
        }
        let cache = Arc::new(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            partition_by,
            order_by,
            functions,
            fields,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn required_ordering(&self) -> Option<LexOrdering> {
        let sort_exprs: Vec<PhysicalSortExpr> = self
            .partition_by
            .iter()
            .map(|e| PhysicalSortExpr {
                expr: Arc::clone(e),
                options: SortOptions::default(),
            })
            .chain(self.order_by.iter().cloned())
            .collect();
        LexOrdering::new(sort_exprs)
    }
}

impl DisplayAs for SortedWindowExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let functions = self
                    .functions
                    .iter()
                    .map(|e| e.name())
                    .collect::<Vec<_>>()
                    .join(", ");
                let partition = self
                    .partition_by
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let order = self
                    .order_by
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "CometSortedWindowExec: functions=[{functions}], partition_by=[{partition}], order_by=[{order}]"
                )
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

impl ExecutionPlan for SortedWindowExec {
    fn name(&self) -> &str {
        "CometSortedWindowExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let values = self.functions.iter().filter_map(|e| match e {
            SortedWindowFunction::Shift { value, .. } => Some(value),
            _ => None,
        });
        apply_expression_roots(
            self.partition_by
                .iter()
                .chain(self.order_by.iter().map(|e| &e.expr))
                .chain(values),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("SortedWindowExec takes exactly one child");
        }
        Ok(Arc::new(SortedWindowExec::try_new(
            Arc::clone(&children[0]),
            self.partition_by.clone(),
            self.order_by.clone(),
            self.functions.clone(),
            self.fields.clone(),
        )?))
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.required_ordering().map(OrderingRequirements::from)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let needs_peers = self.functions.iter().any(|f| {
            matches!(
                f,
                SortedWindowFunction::Rank | SortedWindowFunction::DenseRank
            )
        });
        let max_lead = self
            .functions
            .iter()
            .map(|f| f.offset().max(0) as usize)
            .max()
            .unwrap_or(0);
        let max_lag = self
            .functions
            .iter()
            .map(|f| (-f.offset()).max(0) as usize)
            .max()
            .unwrap_or(0);
        let mut defaults = Vec::with_capacity(self.functions.len());
        let mut context_values = Vec::with_capacity(self.functions.len());
        for (function, field) in self.functions.iter().zip(self.fields.iter()) {
            match function {
                SortedWindowFunction::Shift { default, .. } => {
                    defaults.push(Some(default.to_array_of_size(1)?));
                    context_values.push(Some(new_empty_array(field.data_type())));
                }
                _ => {
                    defaults.push(None);
                    context_values.push(None);
                }
            }
        }
        let reservation = MemoryConsumer::new(format!("SortedWindowExec[{partition}]"))
            .register(context.memory_pool());
        Ok(Box::pin(SortedWindowStream {
            input,
            schema: Arc::clone(&self.schema),
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.iter().map(|e| Arc::clone(&e.expr)).collect(),
            functions: self.functions.clone(),
            output_types: self.fields.iter().map(|f| f.data_type().clone()).collect(),
            needs_peers,
            max_lead,
            max_lag,
            defaults,
            seen_rows: false,
            last_partition_key: None,
            last_order_key: None,
            row_number: 0,
            rank: 0,
            dense_rank: 0,
            pending: VecDeque::new(),
            pending_rows: 0,
            context_values,
            context_starts: Vec::new(),
            finished: false,
            reservation,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }
}

struct PendingBatch {
    batch: RecordBatch,
    starts: BooleanBuffer,
    columns: Vec<Option<ArrayRef>>,
    values: Vec<Option<ArrayRef>>,
}

struct SortedWindowStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<Arc<dyn PhysicalExpr>>,
    functions: Vec<SortedWindowFunction>,
    output_types: Vec<DataType>,
    needs_peers: bool,
    max_lead: usize,
    max_lag: usize,
    defaults: Vec<Option<ArrayRef>>,
    seen_rows: bool,
    last_partition_key: Option<Vec<ArrayRef>>,
    last_order_key: Option<Vec<ArrayRef>>,
    row_number: u64,
    rank: u64,
    dense_rank: u64,
    pending: VecDeque<PendingBatch>,
    pending_rows: usize,
    context_values: Vec<Option<ArrayRef>>,
    context_starts: Vec<bool>,
    finished: bool,
    reservation: MemoryReservation,
    baseline_metrics: BaselineMetrics,
}

fn supports_distinct(data_type: &DataType) -> bool {
    let leaf = match data_type {
        DataType::Dictionary(_, v) => v.as_ref(),
        dt => dt,
    };
    !leaf.is_nested()
        && !matches!(
            leaf,
            DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _)
        )
}

fn adjacent_changes(column: &ArrayRef) -> Result<BooleanBuffer> {
    let len = column.len() - 1;
    let previous = column.slice(0, len);
    let current = column.slice(1, len);
    if supports_distinct(column.data_type()) {
        return Ok(distinct(&previous, &current)?.values().clone());
    }
    let cmp = make_comparator(previous.as_ref(), current.as_ref(), SortOptions::default())?;
    Ok((0..len).map(|i| !cmp(i, i).is_eq()).collect())
}

fn group_starts(
    columns: &[ArrayRef],
    last: Option<&[ArrayRef]>,
    num_rows: usize,
) -> Result<Option<BooleanBuffer>> {
    let mut acc: Option<BooleanBuffer> = None;
    for (idx, column) in columns.iter().enumerate() {
        let first = match last {
            None => true,
            Some(last) => {
                let cmp =
                    make_comparator(last[idx].as_ref(), column.as_ref(), SortOptions::default())?;
                !cmp(0, 0).is_eq()
            }
        };
        let mut builder = BooleanBufferBuilder::new(num_rows);
        builder.append(first);
        if num_rows > 1 {
            builder.append_buffer(&adjacent_changes(column)?);
        }
        let starts = builder.finish();
        acc = Some(match acc {
            None => starts,
            Some(acc) => &acc | &starts,
        });
    }
    Ok(acc)
}

fn evaluate_columns(exprs: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    let num_rows = batch.num_rows();
    exprs
        .iter()
        .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
        .collect()
}

fn last_row(columns: &[ArrayRef]) -> Vec<ArrayRef> {
    columns.iter().map(|c| c.slice(c.len() - 1, 1)).collect()
}

fn counter_array(values: &[u64], data_type: &DataType) -> Result<ArrayRef> {
    Ok(match data_type {
        DataType::Int32 => Arc::new(Int32Array::from_iter_values(
            values.iter().map(|v| *v as i32),
        )),
        DataType::Int64 => Arc::new(Int64Array::from_iter_values(
            values.iter().map(|v| *v as i64),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from_iter_values(values.iter().copied())),
        other => return internal_err!("SortedWindowExec cannot produce {other} counters"),
    })
}

impl SortedWindowStream {
    fn ingest(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let partition_columns = evaluate_columns(&self.partition_by, &batch)?;
        let starts = match group_starts(
            &partition_columns,
            self.last_partition_key.as_deref(),
            num_rows,
        )? {
            Some(starts) => starts,
            None => {
                let mut builder = BooleanBufferBuilder::new(num_rows);
                builder.append(!self.seen_rows);
                builder.append_n(num_rows - 1, false);
                builder.finish()
            }
        };
        self.seen_rows = true;
        if !partition_columns.is_empty() {
            self.last_partition_key = Some(last_row(&partition_columns));
        }

        let peers = if self.needs_peers && !self.order_by.is_empty() {
            let order_columns = evaluate_columns(&self.order_by, &batch)?;
            let order_starts =
                group_starts(&order_columns, self.last_order_key.as_deref(), num_rows)?;
            self.last_order_key = Some(last_row(&order_columns));
            order_starts.map(|o| &o | &starts)
        } else {
            None
        };

        let wants = |kind: fn(&SortedWindowFunction) -> bool| self.functions.iter().any(kind);
        let wants_row_number = wants(|f| matches!(f, SortedWindowFunction::RowNumber));
        let wants_rank = wants(|f| matches!(f, SortedWindowFunction::Rank));
        let wants_dense_rank = wants(|f| matches!(f, SortedWindowFunction::DenseRank));
        let mut row_numbers = Vec::with_capacity(if wants_row_number { num_rows } else { 0 });
        let mut ranks = Vec::with_capacity(if wants_rank { num_rows } else { 0 });
        let mut dense_ranks = Vec::with_capacity(if wants_dense_rank { num_rows } else { 0 });
        let mut row_number = self.row_number;
        let mut rank = self.rank;
        let mut dense_rank = self.dense_rank;
        for i in 0..num_rows {
            if starts.value(i) {
                row_number = 0;
                dense_rank = 0;
            }
            row_number += 1;
            if self.needs_peers && (starts.value(i) || peers.as_ref().is_some_and(|p| p.value(i))) {
                rank = row_number;
                dense_rank += 1;
            }
            if wants_row_number {
                row_numbers.push(row_number);
            }
            if wants_rank {
                ranks.push(rank);
            }
            if wants_dense_rank {
                dense_ranks.push(dense_rank);
            }
        }
        self.row_number = row_number;
        self.rank = rank;
        self.dense_rank = dense_rank;

        let mut columns = Vec::with_capacity(self.functions.len());
        let mut values = Vec::with_capacity(self.functions.len());
        for (idx, function) in self.functions.iter().enumerate() {
            let counters = match function {
                SortedWindowFunction::Shift { value, .. } => {
                    columns.push(None);
                    values.push(Some(value.evaluate(&batch)?.into_array(num_rows)?));
                    continue;
                }
                SortedWindowFunction::RowNumber => &row_numbers,
                SortedWindowFunction::Rank => &ranks,
                SortedWindowFunction::DenseRank => &dense_ranks,
            };
            columns.push(Some(counter_array(counters, &self.output_types[idx])?));
            values.push(None);
        }

        self.pending_rows += num_rows;
        self.pending.push_back(PendingBatch {
            batch,
            starts,
            columns,
            values,
        });
        self.update_reservation();
        Ok(())
    }

    fn update_reservation(&mut self) {
        let size: usize = self
            .pending
            .iter()
            .map(|p| p.batch.get_array_memory_size())
            .sum();
        self.reservation.resize(size);
    }

    fn front_ready(&self) -> bool {
        match self.pending.front() {
            None => false,
            Some(front) => {
                self.finished
                    || self.max_lead == 0
                    || self.pending_rows - front.batch.num_rows() >= self.max_lead
            }
        }
    }

    fn emit_front(&mut self) -> Result<RecordBatch> {
        let front = match self.pending.pop_front() {
            Some(front) => front,
            None => return internal_err!("SortedWindowExec has no pending batch"),
        };
        let num_rows = front.batch.num_rows();
        self.pending_rows -= num_rows;

        let context_rows = self.context_starts.len();
        let mut starts: Vec<bool> = Vec::with_capacity(context_rows + num_rows + self.max_lead);
        starts.extend_from_slice(&self.context_starts);
        starts.extend(front.starts.iter());
        let mut ahead: Vec<(usize, &PendingBatch)> = Vec::new();
        let mut ahead_rows = 0;
        for next in self.pending.iter() {
            if ahead_rows >= self.max_lead {
                break;
            }
            let take = (self.max_lead - ahead_rows).min(next.batch.num_rows());
            starts.extend(next.starts.iter().take(take));
            ahead.push((take, next));
            ahead_rows += take;
        }
        let total = starts.len();
        let mut partition_ids: Vec<u32> = Vec::with_capacity(total);
        let mut current: u32 = 0;
        for (idx, start) in starts.iter().enumerate() {
            if idx > 0 && *start {
                current += 1;
            }
            partition_ids.push(current);
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        columns.extend(front.batch.columns().iter().cloned());
        for (idx, function) in self.functions.iter().enumerate() {
            match function {
                SortedWindowFunction::Shift { offset, .. } => {
                    let context = self.context_values[idx].as_ref().unwrap();
                    let current_values = front.values[idx].as_ref().unwrap();
                    let default = self.defaults[idx].as_ref().unwrap();
                    let mut arrays: Vec<&dyn Array> = Vec::with_capacity(ahead.len() + 3);
                    arrays.push(context.as_ref());
                    arrays.push(current_values.as_ref());
                    let mut ahead_ends: Vec<usize> = Vec::with_capacity(ahead.len());
                    let mut end = context_rows + num_rows;
                    for (take, next) in ahead.iter() {
                        arrays.push(next.values[idx].as_ref().unwrap().as_ref());
                        end += take;
                        ahead_ends.push(end);
                    }
                    let default_idx = arrays.len();
                    arrays.push(default.as_ref());
                    let mut indices: Vec<(usize, usize)> = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let row = context_rows + i;
                        let target = row as i64 + offset;
                        if target < 0
                            || target >= total as i64
                            || partition_ids[target as usize] != partition_ids[row]
                        {
                            indices.push((default_idx, 0));
                            continue;
                        }
                        let target = target as usize;
                        if target < context_rows {
                            indices.push((0, target));
                        } else if target < context_rows + num_rows {
                            indices.push((1, target - context_rows));
                        } else {
                            let mut begin = context_rows + num_rows;
                            for (segment, segment_end) in ahead_ends.iter().enumerate() {
                                if target < *segment_end {
                                    indices.push((segment + 2, target - begin));
                                    break;
                                }
                                begin = *segment_end;
                            }
                        }
                    }
                    columns.push(interleave(&arrays, &indices)?);
                }
                _ => columns.push(Arc::clone(front.columns[idx].as_ref().unwrap())),
            }
        }

        if self.max_lag > 0 {
            let keep = self.max_lag.min(context_rows + num_rows);
            for (idx, value) in front.values.iter().enumerate() {
                if let Some(value) = value {
                    let context = self.context_values[idx].as_ref().unwrap();
                    let combined = if keep <= num_rows {
                        value.slice(num_rows - keep, keep)
                    } else {
                        let from_context = keep - num_rows;
                        concat(&[
                            context
                                .slice(context_rows - from_context, from_context)
                                .as_ref(),
                            value.as_ref(),
                        ])?
                    };
                    self.context_values[idx] = Some(combined);
                }
            }
            self.context_starts =
                starts[context_rows + num_rows - keep..context_rows + num_rows].to_vec();
        }

        self.update_reservation();
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &options,
        )?)
    }
}

impl Stream for SortedWindowStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.front_ready() {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let emitted = {
                    let _timer = elapsed_compute.timer();
                    self.emit_front()
                };
                return match emitted {
                    Ok(batch) => self
                        .baseline_metrics
                        .record_poll(Poll::Ready(Some(Ok(batch)))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                };
            }
            if self.finished {
                self.reservation.free();
                return Poll::Ready(None);
            }
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                    let ingested = {
                        let _timer = elapsed_compute.timer();
                        self.ingest(batch)
                    };
                    if let Err(e) = ingested {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => self.finished = true,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for SortedWindowStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructArray};
    use arrow::compute::{cast, concat_batches};
    use arrow::datatypes::{Field, Fields};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
    use datafusion::functions_window::rank::{dense_rank_udwf, rank_udwf};
    use datafusion::functions_window::row_number::row_number_udwf;
    use datafusion::logical_expr::{WindowFrame, WindowFunctionDefinition};
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::window::WindowExpr;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::windows::{create_window_expr, BoundedWindowAggExec};
    use datafusion::physical_plan::InputOrderMode;
    use datafusion::prelude::SessionContext;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    #[derive(Clone, Copy)]
    enum Sizes {
        Ones,
        Twos,
        Mixed,
        Geometric,
        Huge,
    }

    #[derive(Clone)]
    enum Func {
        RowNumber,
        Rank,
        DenseRank,
        Lead(&'static str, i64, ScalarValue),
        Lag(&'static str, i64, ScalarValue),
    }

    fn struct_fields() -> Fields {
        Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new_list("b", Field::new_list_field(DataType::Int64, true), true),
        ])
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int32, true),
            Field::new("k2", DataType::Utf8, true),
            Field::new("st", DataType::Struct(struct_fields()), true),
            Field::new("o", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]))
    }

    fn partition_sizes(sizes: Sizes, rng: &mut StdRng) -> Vec<usize> {
        match sizes {
            Sizes::Ones => vec![1; 700],
            Sizes::Twos => vec![2; 350],
            Sizes::Mixed => (0..320).map(|_| rng.random_range(1..=4)).collect(),
            Sizes::Geometric => {
                let mut out = vec![];
                while out.iter().sum::<usize>() < 900 {
                    let mut n = 1;
                    while n < 60 && rng.random_bool(0.8) {
                        n += 1;
                    }
                    out.push(n);
                }
                out
            }
            Sizes::Huge => {
                let mut out = vec![1, 2, 1];
                out.push(2500);
                out.extend([1, 3, 1]);
                out
            }
        }
    }

    fn generate(sizes: Sizes, seed: u64) -> RecordBatch {
        let mut rng = StdRng::seed_from_u64(seed);
        let sizes = partition_sizes(sizes, &mut rng);
        let mut k1 = Int32Builder::new();
        let mut k2 = StringBuilder::new();
        let mut st_a = Int32Builder::new();
        let mut st_b = ListBuilder::new(Int64Builder::new());
        let mut st_valid = vec![];
        let mut o = Int64Builder::new();
        let mut v = Int64Builder::new();
        let mut s = StringBuilder::new();
        for (p, size) in sizes.iter().enumerate() {
            let tie = rng.random_range(1..=3);
            for j in 0..*size {
                if p < 2 {
                    k1.append_null();
                } else {
                    k1.append_value((p / 2) as i32);
                }
                if p % 2 == 0 {
                    k2.append_null();
                } else {
                    k2.append_value(format!("k{p}"));
                }
                if p == 0 {
                    st_a.append_null();
                    st_b.append_null();
                    st_valid.push(false);
                } else {
                    if p % 3 == 0 {
                        st_a.append_null();
                    } else {
                        st_a.append_value((p % 7) as i32);
                    }
                    st_b.values().append_value(p as i64);
                    if p % 4 == 0 {
                        st_b.values().append_null();
                    }
                    st_b.append(true);
                    st_valid.push(true);
                }
                if j < 1 && rng.random_bool(0.2) {
                    o.append_null();
                } else {
                    o.append_value((j / tie) as i64);
                }
                if rng.random_bool(0.2) {
                    v.append_null();
                } else {
                    v.append_value(rng.random_range(-1000..1000));
                }
                if rng.random_bool(0.2) {
                    s.append_null();
                } else {
                    s.append_value(format!("s{}", rng.random_range(0..100)));
                }
            }
        }
        let st = StructArray::new(
            struct_fields(),
            vec![
                Arc::new(st_a.finish()) as ArrayRef,
                Arc::new(st_b.finish()) as ArrayRef,
            ],
            Some(st_valid.into()),
        );
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(k1.finish()),
                Arc::new(k2.finish()),
                Arc::new(st),
                Arc::new(o.finish()),
                Arc::new(v.finish()),
                Arc::new(s.finish()),
            ],
        )
        .unwrap()
    }

    fn split(batch: &RecordBatch, sizes: &[usize], seed: u64) -> Vec<RecordBatch> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut out = vec![];
        let mut offset = 0;
        let mut i = 0;
        while offset < batch.num_rows() {
            let size = if sizes.is_empty() {
                rng.random_range(1..=40)
            } else {
                sizes[i % sizes.len()]
            };
            let len = size.min(batch.num_rows() - offset);
            out.push(batch.slice(offset, len));
            offset += len;
            i += 1;
        }
        out
    }

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        let schema = schema();
        Arc::new(Column::new(name, schema.index_of(name).unwrap()))
    }

    fn lit(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(value))
    }

    fn order_by(options: SortOptions) -> Vec<PhysicalSortExpr> {
        vec![PhysicalSortExpr {
            expr: col("o"),
            options,
        }]
    }

    fn df_window_expr(
        func: &Func,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order: &[PhysicalSortExpr],
    ) -> Arc<dyn WindowExpr> {
        let (def, name, args) = match func {
            Func::RowNumber => (row_number_udwf(), "row_number", vec![]),
            Func::Rank => (rank_udwf(), "rank", vec![]),
            Func::DenseRank => (dense_rank_udwf(), "dense_rank", vec![]),
            Func::Lead(c, n, d) => (
                lead_udwf(),
                "lead",
                vec![col(c), lit(ScalarValue::Int64(Some(*n))), lit(d.clone())],
            ),
            Func::Lag(c, n, d) => (
                lag_udwf(),
                "lag",
                vec![col(c), lit(ScalarValue::Int64(Some(*n))), lit(d.clone())],
            ),
        };
        create_window_expr(
            &WindowFunctionDefinition::WindowUDF(def),
            name.to_string(),
            &args,
            partition_by,
            order,
            Arc::new(WindowFrame::new(Some(true))),
            schema(),
            false,
            false,
            None,
        )
        .unwrap()
    }

    fn sorted_function(func: &Func) -> SortedWindowFunction {
        match func {
            Func::RowNumber => SortedWindowFunction::RowNumber,
            Func::Rank => SortedWindowFunction::Rank,
            Func::DenseRank => SortedWindowFunction::DenseRank,
            Func::Lead(c, n, d) | Func::Lag(c, n, d) => {
                let value = col(c);
                let value_type = value.data_type(&schema()).unwrap();
                let default = if d.is_null() {
                    ScalarValue::try_from(&value_type).unwrap()
                } else {
                    d.cast_to(&value_type).unwrap()
                };
                SortedWindowFunction::Shift {
                    value,
                    offset: if matches!(func, Func::Lead(..)) {
                        *n
                    } else {
                        -*n
                    },
                    default,
                }
            }
        }
    }

    async fn run_both(
        batches: Vec<RecordBatch>,
        funcs: &[Func],
        partition_by: &[Arc<dyn PhysicalExpr>],
        options: SortOptions,
        counter_type: DataType,
    ) -> (RecordBatch, RecordBatch, usize) {
        let order = order_by(options);
        let input_batches = batches.len();
        let df_exprs: Vec<_> = funcs
            .iter()
            .map(|f| df_window_expr(f, partition_by, &order))
            .collect();
        let ordering: Vec<PhysicalSortExpr> = partition_by
            .iter()
            .map(|e| PhysicalSortExpr {
                expr: Arc::clone(e),
                options: SortOptions::default(),
            })
            .chain(order.iter().cloned())
            .collect();
        let config = MemorySourceConfig::try_new(&[batches], schema(), None)
            .unwrap()
            .try_with_sort_information(vec![LexOrdering::new(ordering).unwrap()])
            .unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(config)));
        let expected = Arc::new(
            BoundedWindowAggExec::try_new(
                df_exprs.clone(),
                Arc::clone(&input) as Arc<dyn ExecutionPlan>,
                InputOrderMode::Sorted,
                !partition_by.is_empty(),
            )
            .unwrap(),
        );
        let functions: Vec<_> = funcs.iter().map(sorted_function).collect();
        let fields: Vec<FieldRef> = df_exprs
            .iter()
            .zip(functions.iter())
            .map(|(e, f)| {
                let field = e.field().unwrap().as_ref().clone();
                let field = match f {
                    SortedWindowFunction::Shift { .. } => field,
                    _ => field
                        .with_data_type(counter_type.clone())
                        .with_nullable(false),
                };
                Arc::new(field)
            })
            .collect();
        let actual = Arc::new(
            SortedWindowExec::try_new(input, partition_by.to_vec(), order, functions, fields)
                .unwrap(),
        );
        let ctx = SessionContext::new().task_ctx();
        let expected_batches = collect(expected, Arc::clone(&ctx)).await.unwrap();
        let actual_batches = collect(Arc::clone(&actual) as _, ctx).await.unwrap();
        assert_eq!(actual_batches.len(), input_batches);
        let metrics = actual.metrics().unwrap();
        assert_eq!(
            metrics.output_rows().unwrap(),
            actual_batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );
        let expected = concat_batches(&expected_batches[0].schema(), &expected_batches).unwrap();
        let actual = concat_batches(&actual.schema(), &actual_batches).unwrap();
        (expected, actual, input_batches)
    }

    fn assert_same(expected: &RecordBatch, actual: &RecordBatch, context: &str) {
        assert_eq!(expected.num_columns(), actual.num_columns(), "{context}");
        assert_eq!(expected.num_rows(), actual.num_rows(), "{context}");
        for idx in 0..expected.num_columns() {
            let e = expected.column(idx);
            let a = cast(actual.column(idx), e.data_type()).unwrap();
            if e.as_ref() != a.as_ref() {
                for row in 0..e.len() {
                    let ev = ScalarValue::try_from_array(e, row).unwrap();
                    let av = ScalarValue::try_from_array(&a, row).unwrap();
                    assert_eq!(ev, av, "{context}: column {idx} row {row}");
                }
            }
        }
    }

    fn all_functions() -> Vec<Func> {
        vec![
            Func::RowNumber,
            Func::Rank,
            Func::DenseRank,
            Func::Lead("v", 1, ScalarValue::Null),
            Func::Lead("v", 2, ScalarValue::Int64(Some(42))),
            Func::Lead("v", 3, ScalarValue::Int32(Some(-7))),
            Func::Lag("v", 1, ScalarValue::Null),
            Func::Lag("v", 3, ScalarValue::Int64(Some(-1))),
            Func::Lead("s", 1, ScalarValue::Utf8(Some("x".to_string()))),
            Func::Lag("s", 2, ScalarValue::Null),
            Func::Lead("st", 1, ScalarValue::Null),
            Func::Lag("o", 1, ScalarValue::Int64(Some(0))),
            Func::Lead("o", 0, ScalarValue::Null),
        ]
    }

    #[tokio::test]
    async fn matches_bounded_window_agg_exec() {
        let function_sets: Vec<Vec<Func>> = vec![
            all_functions(),
            vec![Func::Lead("o", 1, ScalarValue::Int64(Some(i64::MAX)))],
            vec![Func::RowNumber],
            vec![Func::Rank, Func::DenseRank],
            vec![Func::Lag("v", 2, ScalarValue::Null)],
        ];
        let partition_bys: Vec<Vec<Arc<dyn PhysicalExpr>>> = vec![
            vec![col("k1")],
            vec![col("k1"), col("k2")],
            vec![col("st")],
            vec![],
        ];
        let splits: Vec<Vec<usize>> = vec![
            vec![1],
            vec![2],
            vec![3],
            vec![5, 1, 2],
            vec![64],
            vec![4096],
            vec![],
        ];
        let options = [
            SortOptions::default(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];
        let mut seed = 0;
        for sizes in [
            Sizes::Ones,
            Sizes::Twos,
            Sizes::Mixed,
            Sizes::Geometric,
            Sizes::Huge,
        ] {
            for (f, funcs) in function_sets.iter().enumerate() {
                for (p, partition_by) in partition_bys.iter().enumerate() {
                    for (s, split_sizes) in splits.iter().enumerate() {
                        seed += 1;
                        let data = generate(sizes, seed);
                        let batches = split(&data, split_sizes, seed);
                        let option = options[seed as usize % 2];
                        let counter_type = if seed % 2 == 0 {
                            DataType::UInt64
                        } else {
                            DataType::Int32
                        };
                        let (expected, actual, _) =
                            run_both(batches, funcs, partition_by, option, counter_type).await;
                        assert_same(
                            &expected,
                            &actual,
                            &format!("seed {seed} functions {f} partition_by {p} split {s}"),
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn empty_batches_and_empty_input() {
        let data = generate(Sizes::Mixed, 7);
        let mut batches = vec![data.slice(0, 0)];
        for b in split(&data, &[3], 7) {
            batches.push(b);
            batches.push(data.slice(0, 0));
        }
        let non_empty = batches.iter().filter(|b| b.num_rows() > 0).count();
        let order = order_by(SortOptions::default());
        let funcs = all_functions();
        let df_exprs: Vec<_> = funcs
            .iter()
            .map(|f| df_window_expr(f, &[col("k1")], &order))
            .collect();
        let fields: Vec<FieldRef> = df_exprs
            .iter()
            .zip(funcs.iter())
            .map(|(e, f)| match f {
                Func::Lead(..) | Func::Lag(..) => e.field().unwrap(),
                _ => Arc::new(
                    e.field()
                        .unwrap()
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Int32)
                        .with_nullable(false),
                ),
            })
            .collect();
        let input = MemorySourceConfig::try_new_exec(&[batches], schema(), None).unwrap();
        let plan = Arc::new(
            SortedWindowExec::try_new(
                input,
                vec![col("k1")],
                order.clone(),
                funcs.iter().map(sorted_function).collect(),
                fields.clone(),
            )
            .unwrap(),
        );
        let out = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert_eq!(out.len(), non_empty);
        assert_eq!(
            out.iter().map(|b| b.num_rows()).sum::<usize>(),
            data.num_rows()
        );

        let input = MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
        let plan = Arc::new(
            SortedWindowExec::try_new(
                input,
                vec![col("k1")],
                order,
                funcs.iter().map(sorted_function).collect(),
                fields,
            )
            .unwrap(),
        );
        let out = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn input_columns_pass_through() {
        let data = generate(Sizes::Twos, 3);
        let batches = split(&data, &[100], 3);
        let (_, actual, _) = run_both(
            batches,
            &[Func::Lead("v", 1, ScalarValue::Null), Func::RowNumber],
            &[col("k1"), col("k2")],
            SortOptions::default(),
            DataType::Int32,
        )
        .await;
        for idx in 0..data.num_columns() {
            assert_eq!(actual.column(idx).as_ref(), data.column(idx).as_ref());
        }
        let lead = actual
            .column(data.num_columns())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lead.len(), data.num_rows());
    }

    #[test]
    fn rejects_unsupported_output_types() {
        let schema = schema();
        assert!(!sorted_window_supports_output_type(
            &SortedWindowFunction::RowNumber,
            &DataType::Utf8,
            &schema
        )
        .unwrap());
        assert!(!sorted_window_supports_output_type(
            &SortedWindowFunction::Shift {
                value: col("v"),
                offset: SORTED_WINDOW_MAX_OFFSET + 1,
                default: ScalarValue::Int64(None),
            },
            &DataType::Int64,
            &schema
        )
        .unwrap());
        assert!(!sorted_window_supports_output_type(
            &SortedWindowFunction::Shift {
                value: col("v"),
                offset: 1,
                default: ScalarValue::Int32(None),
            },
            &DataType::Int64,
            &schema
        )
        .unwrap());
        assert!(sorted_window_supports_output_type(
            &SortedWindowFunction::Shift {
                value: col("v"),
                offset: -SORTED_WINDOW_MAX_OFFSET,
                default: ScalarValue::Int64(None),
            },
            &DataType::Int64,
            &schema
        )
        .unwrap());
    }
}
