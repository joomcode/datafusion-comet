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

//! Hash join filter evaluated conjunct by conjunct over the surviving pairs,
//! with the subexpressions over one input alone evaluated per input row.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, PrimitiveArray, RecordBatch,
    RecordBatchOptions, UInt32Array, UInt64Array, downcast_array, new_null_array,
};
use arrow::compute::{self, filter_record_batch, take};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinSide, Result, internal_err};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{collect_columns, split_conjunction};
use datafusion_physical_expr_common::physical_expr::{PhysicalExprRef, is_volatile};

use crate::joins::utils::JoinFilter;

/// A hash join filter `c1 AND c2 AND ...` evaluated one conjunct at a time,
/// each over the pairs every earlier conjunct kept, like Spark's
/// short-circuiting `AND`.
///
/// The largest non-volatile subexpressions that read the columns of one input
/// alone (e.g. `parts[3]` or `parts[3] IS NULL` of one side) are lifted out
/// and evaluated per input row rather than per pair:
/// - once per run of pairs sharing a row (a probe row with many matches);
/// - over the build batch once the stream has evaluated as many build-side
///   pairs as it has rows, cached for the rest of the stream;
/// - otherwise per pair, over the columns they read taken once per lookup.
///
/// A conjunct copies per pair only the lifted results and bare columns it
/// reads.
#[derive(Debug)]
pub(super) struct LazyJoinFilter {
    /// The conjuncts in their original order
    conjuncts: Vec<Conjunct>,
    build: Side,
    probe: Side,
    build_cache: BuildCache,
}

/// The lifted subexpressions of one input
#[derive(Debug)]
struct Side {
    inputs: Vec<SideInput>,
    /// The input's columns the inputs that are not bare columns read
    projection: Vec<usize>,
    /// Schema of `projection`
    schema: SchemaRef,
}

#[derive(Debug)]
struct SideInput {
    /// Over `Side::schema`
    expr: PhysicalExprRef,
    /// Set when the input is a bare column of the input, to its index
    column: Option<usize>,
}

#[derive(Debug)]
struct Conjunct {
    /// Over `schema`
    expr: PhysicalExprRef,
    schema: SchemaRef,
    /// Source of each column of `schema`
    inputs: Vec<InputRef>,
}

#[derive(Debug, Clone, Copy)]
enum InputRef {
    Build(usize),
    Probe(usize),
}

#[derive(Debug, Default)]
struct BuildCache {
    /// The arrays of the build batch the fields below are for, kept alive so
    /// the batch is recognized by pointer
    key: Vec<ArrayRef>,
    /// Pairs whose build-side inputs were evaluated without the cache
    pairs_evaluated: usize,
    /// Results of the build-side inputs that are not bare columns over every
    /// build row
    values: Option<Vec<Option<ArrayRef>>>,
    /// Evaluating them over every build row failed on some row; they are
    /// evaluated for pairs only from then on, so only pairs raise errors
    failed: bool,
}

/// One side of the current pairs of an `apply`
struct Pairs<'a, T: ArrowPrimitiveType> {
    batch: &'a RecordBatch,
    indices: PrimitiveArray<T>,
    /// `Side::projection` taken for `indices`, once needed
    projection: Option<RecordBatch>,
}

impl LazyJoinFilter {
    /// Returns `None` when the filter reads no column, or reads a column of
    /// neither input.
    pub fn try_new(filter: &JoinFilter, build_side: JoinSide) -> Result<Option<Self>> {
        let column_indices = filter.column_indices();
        if column_indices.is_empty()
            || column_indices
                .iter()
                .any(|c| !matches!(c.side, JoinSide::Left | JoinSide::Right))
        {
            return Ok(None);
        }
        let filter_schema = filter.schema();

        // Lift the largest single-input subexpressions, bare columns included,
        // numbered over the whole filter.
        let mut lifted: Vec<(bool, PhysicalExprRef)> = vec![];
        let expression = Arc::clone(filter.expression())
            .transform_down(|expr| {
                let columns = collect_columns(&expr);
                let Some(side) = columns
                    .iter()
                    .next()
                    .map(|c| column_indices[c.index()].side)
                else {
                    return Ok(Transformed::no(expr));
                };
                if columns
                    .iter()
                    .any(|c| column_indices[c.index()].side != side)
                    || (expr.downcast_ref::<Column>().is_none() && is_volatile(&expr))
                {
                    return Ok(Transformed::no(expr));
                }
                let is_build = side == build_side;
                let position = match lifted
                    .iter()
                    .position(|(b, e)| *b == is_build && **e == *expr)
                {
                    Some(position) => position,
                    None => {
                        lifted.push((is_build, Arc::clone(&expr)));
                        lifted.len() - 1
                    }
                };
                Ok(Transformed::new(
                    Arc::new(Column::new(&format!("__lifted_{position}"), position)),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            })?
            .data;

        let fields = lifted
            .iter()
            .enumerate()
            .map(|(position, (_, expr))| {
                Ok(Field::new(
                    format!("__lifted_{position}"),
                    expr.data_type(filter_schema)?,
                    expr.nullable(filter_schema)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let side = |is_build: bool| -> Result<(Side, Vec<Option<usize>>)> {
            // Filter columns the side's non-bare-column inputs read
            let mut read: Vec<usize> = lifted
                .iter()
                .filter(|(b, e)| *b == is_build && e.downcast_ref::<Column>().is_none())
                .flat_map(|(_, e)| collect_columns(e))
                .map(|c| c.index())
                .collect();
            read.sort_unstable();
            read.dedup();
            let mut projection: Vec<usize> =
                read.iter().map(|&i| column_indices[i].index).collect();
            projection.sort_unstable();
            projection.dedup();
            let mut inputs = vec![];
            let mut positions = vec![None; lifted.len()];
            for (position, (b, expr)) in lifted.iter().enumerate() {
                if *b != is_build {
                    continue;
                }
                let column = expr
                    .downcast_ref::<Column>()
                    .map(|c| column_indices[c.index()].index);
                let expr = Arc::clone(expr)
                    .transform_up(|e| {
                        let Some(c) = e.downcast_ref::<Column>() else {
                            return Ok(Transformed::no(e));
                        };
                        let index = column_indices[c.index()].index;
                        let index = projection.binary_search(&index).unwrap_or(0);
                        Ok(Transformed::yes(
                            Arc::new(Column::new(c.name(), index)) as PhysicalExprRef
                        ))
                    })?
                    .data;
                positions[position] = Some(inputs.len());
                inputs.push(SideInput { expr, column });
            }
            let schema = Arc::new(Schema::new(
                projection
                    .iter()
                    .map(|&index| {
                        let i = read
                            .iter()
                            .copied()
                            .find(|&i| column_indices[i].index == index)
                            .unwrap();
                        filter_schema.field(i).clone()
                    })
                    .collect::<Vec<_>>(),
            ));
            Ok((
                Side {
                    inputs,
                    projection,
                    schema,
                },
                positions,
            ))
        };
        let (build, build_positions) = side(true)?;
        let (probe, probe_positions) = side(false)?;
        let lifted_inputs: Vec<InputRef> = (0..lifted.len())
            .map(|i| match build_positions[i] {
                Some(j) => InputRef::Build(j),
                None => InputRef::Probe(probe_positions[i].unwrap()),
            })
            .collect();

        let conjuncts = split_conjunction(&expression)
            .into_iter()
            .map(|conjunct| {
                let mut used: Vec<usize> = collect_columns(conjunct)
                    .iter()
                    .map(|c| c.index())
                    .collect();
                used.sort_unstable();
                used.dedup();
                let expr = Arc::clone(conjunct)
                    .transform_up(|e| {
                        let Some(column) = e.downcast_ref::<Column>() else {
                            return Ok(Transformed::no(e));
                        };
                        let index = used.binary_search(&column.index()).unwrap();
                        Ok(Transformed::yes(
                            Arc::new(Column::new(column.name(), index)) as PhysicalExprRef,
                        ))
                    })?
                    .data;
                Ok(Conjunct {
                    expr,
                    schema: Arc::new(Schema::new(
                        used.iter().map(|&i| fields[i].clone()).collect::<Vec<_>>(),
                    )),
                    inputs: used.iter().map(|&i| lifted_inputs[i]).collect(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(Self {
            conjuncts,
            build,
            probe,
            build_cache: BuildCache::default(),
        }))
    }

    /// Keeps the pairs for which every conjunct is true, in their order.
    pub fn apply(
        &mut self,
        build_batch: &RecordBatch,
        probe_batch: &RecordBatch,
        build_indices: UInt64Array,
        probe_indices: UInt32Array,
    ) -> Result<(UInt64Array, UInt32Array)> {
        let mut build_pairs = Pairs {
            batch: build_batch,
            indices: build_indices,
            projection: None,
        };
        let mut probe_pairs = Pairs {
            batch: probe_batch,
            indices: probe_indices,
            projection: None,
        };
        for k in 0..self.conjuncts.len() {
            let num_pairs = build_pairs.indices.len();
            if num_pairs == 0 {
                break;
            }
            let mask = self.evaluate_conjunct(k, &mut build_pairs, &mut probe_pairs)?;
            if mask.null_count() == 0 && mask.true_count() == num_pairs {
                continue;
            }
            build_pairs.retain(&mask)?;
            probe_pairs.retain(&mask)?;
        }
        Ok((build_pairs.indices, probe_pairs.indices))
    }

    fn evaluate_conjunct(
        &mut self,
        k: usize,
        build_pairs: &mut Pairs<'_, arrow::datatypes::UInt64Type>,
        probe_pairs: &mut Pairs<'_, arrow::datatypes::UInt32Type>,
    ) -> Result<BooleanArray> {
        let num_pairs = build_pairs.indices.len();
        let conjunct = &self.conjuncts[k];
        if conjunct
            .inputs
            .iter()
            .any(|i| matches!(i, InputRef::Build(j) if self.build.inputs[*j].column.is_none()))
        {
            self.build_cache
                .prepare(&self.build, build_pairs.batch, num_pairs);
        }

        let mut columns = Vec::with_capacity(conjunct.inputs.len());
        for input in &conjunct.inputs {
            let column = match *input {
                InputRef::Build(j) => match self.build_cache.values(j, build_pairs.batch) {
                    Some(values) => take_or_nulls(values, &build_pairs.indices)?,
                    None => build_pairs.evaluate(&self.build, j)?,
                },
                InputRef::Probe(j) => probe_pairs.evaluate(&self.probe, j)?,
            };
            columns.push(column);
        }

        let batch = projected_batch(&conjunct.schema, columns, num_pairs)?;
        let result = conjunct.expr.evaluate(&batch)?.into_array(num_pairs)?;
        let mask = as_boolean_array(&result)?;
        if mask.len() != num_pairs {
            return internal_err!(
                "join filter conjunct returned {} rows for {num_pairs} pairs",
                mask.len()
            );
        }
        Ok(mask.clone())
    }
}

impl BuildCache {
    /// Evaluates the build-side inputs over every row of `batch` once the
    /// pairs evaluated without the cache reach its number of rows.
    fn prepare(&mut self, side: &Side, batch: &RecordBatch, num_pairs: usize) {
        if !same_batch(&self.key, batch) {
            *self = BuildCache {
                key: batch.columns().to_vec(),
                ..Default::default()
            };
        }
        if self.values.is_some() || self.failed {
            return;
        }
        if self.pairs_evaluated + num_pairs < batch.num_rows() {
            self.pairs_evaluated += num_pairs;
            return;
        }
        let num_rows = batch.num_rows();
        let columns = side
            .projection
            .iter()
            .map(|&c| Arc::clone(batch.column(c)))
            .collect();
        let values = projected_batch(&side.schema, columns, num_rows).and_then(|batch| {
            side.inputs
                .iter()
                .map(|input| match input.column {
                    Some(_) => Ok(None),
                    None => Ok(Some(input.expr.evaluate(&batch)?.into_array(num_rows)?)),
                })
                .collect::<Result<Vec<_>>>()
        });
        match values {
            Ok(values) => self.values = Some(values),
            Err(_) => self.failed = true,
        }
    }

    /// The results of input `j` over every row of `batch`, when cached
    fn values(&self, j: usize, batch: &RecordBatch) -> Option<&ArrayRef> {
        self.values
            .as_ref()
            .filter(|_| same_batch(&self.key, batch))
            .and_then(|values| values[j].as_ref())
    }
}

impl<T: ArrowPrimitiveType> Pairs<'_, T> {
    /// Input `j` of `side` for every pair: a bare column taken per pair,
    /// otherwise evaluated once per run of pairs of one row, or per pair over
    /// the side's columns taken once for all inputs.
    fn evaluate(&mut self, side: &Side, j: usize) -> Result<ArrayRef> {
        let input = &side.inputs[j];
        if let Some(column) = input.column {
            return take_or_nulls(self.batch.column(column), &self.indices);
        }
        if let Some((rows, positions)) = runs_of(&self.indices) {
            let num_rows = rows.len();
            let columns = side
                .projection
                .iter()
                .map(|&c| take_or_nulls(self.batch.column(c), &rows))
                .collect::<Result<Vec<_>>>()?;
            let values = input
                .expr
                .evaluate(&projected_batch(&side.schema, columns, num_rows)?)?
                .into_array(num_rows)?;
            return Ok(take(values.as_ref(), &positions, None)?);
        }
        if self.projection.is_none() {
            let columns = side
                .projection
                .iter()
                .map(|&c| take_or_nulls(self.batch.column(c), &self.indices))
                .collect::<Result<Vec<_>>>()?;
            self.projection = Some(projected_batch(&side.schema, columns, self.indices.len())?);
        }
        let projection = self.projection.as_ref().unwrap();
        input
            .expr
            .evaluate(projection)?
            .into_array(projection.num_rows())
    }

    /// Keeps the pairs of `mask`
    fn retain(&mut self, mask: &BooleanArray) -> Result<()> {
        self.indices = downcast_array(compute::filter(&self.indices, mask)?.as_ref());
        if let Some(projection) = &self.projection {
            self.projection = Some(filter_record_batch(projection, mask)?);
        }
        Ok(())
    }
}

/// Whether `batch` holds the very arrays of `key`
fn same_batch(key: &[ArrayRef], batch: &RecordBatch) -> bool {
    key.len() == batch.num_columns()
        && key
            .iter()
            .zip(batch.columns())
            .all(|(a, b)| Arc::ptr_eq(a, b))
}

fn take_or_nulls<I: Array>(array: &ArrayRef, indices: &I) -> Result<ArrayRef> {
    if array.is_empty() || indices.null_count() == indices.len() {
        return Ok(new_null_array(array.data_type(), indices.len()));
    }
    Ok(take(array.as_ref(), indices, None)?)
}

/// For indices with runs of equal consecutive values averaging two pairs or
/// more: the value of each run and, per pair, the position of its run.
fn runs_of<T: ArrowPrimitiveType>(
    indices: &PrimitiveArray<T>,
) -> Option<(PrimitiveArray<T>, UInt32Array)> {
    if indices.null_count() > 0 || indices.len() < 2 {
        return None;
    }
    let values = indices.values();
    let num_runs = 1 + values.windows(2).filter(|w| w[0] != w[1]).count();
    if num_runs * 2 > values.len() {
        return None;
    }
    let mut rows = Vec::with_capacity(num_runs);
    let mut positions = Vec::with_capacity(values.len());
    for &value in values.iter() {
        if rows.last() != Some(&value) {
            rows.push(value);
        }
        positions.push((rows.len() - 1) as u32);
    }
    Some((
        PrimitiveArray::<T>::from_iter_values(rows),
        UInt32Array::from(positions),
    ))
}

fn projected_batch(
    schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    num_rows: usize,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

#[cfg(test)]
mod tests {
    use std::fmt::{self, Display, Formatter};
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{AsArray, Int32Array, ListBuilder, StringArray, StringBuilder};
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_common::{JoinType, NullEquality};
    use datafusion_execution::TaskContext;
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, CaseExpr, CastExpr, IsNullExpr, Literal, binary, lit,
    };

    use super::*;
    use crate::common;
    use crate::joins::utils::{ColumnIndex, apply_join_filter_to_indices};
    use crate::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
    use crate::test::TestMemoryExec;
    use crate::{ExecutionPlan, execution_plan::collect};

    /// `child[index]` over a list of strings, null when out of range; counts
    /// the rows it is evaluated over
    #[derive(Debug)]
    struct Item {
        child: PhysicalExprRef,
        index: usize,
        rows: Arc<AtomicUsize>,
    }

    impl Hash for Item {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.child.hash(state);
            self.index.hash(state);
        }
    }

    impl PartialEq for Item {
        fn eq(&self, other: &Self) -> bool {
            self.child.eq(&other.child) && self.index == other.index
        }
    }

    impl Eq for Item {}

    impl Display for Item {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{}[{}]", self.child, self.index)
        }
    }

    impl PhysicalExpr for Item {
        fn data_type(&self, _: &Schema) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn nullable(&self, _: &Schema) -> Result<bool> {
            Ok(true)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            let lists = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
            let lists = lists.as_list::<i32>();
            self.rows.fetch_add(lists.len(), Ordering::Relaxed);
            let values: StringArray = (0..lists.len())
                .map(|i| {
                    if lists.is_null(i) {
                        return None;
                    }
                    let list = lists.value(i);
                    let list = list.as_string::<i32>();
                    (self.index < list.len() && list.is_valid(self.index))
                        .then(|| list.value(self.index).to_string())
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(values)))
        }

        fn children(&self) -> Vec<&PhysicalExprRef> {
            vec![&self.child]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<PhysicalExprRef>,
        ) -> Result<PhysicalExprRef> {
            Ok(Arc::new(Item {
                child: Arc::clone(&children[0]),
                index: self.index,
                rows: Arc::clone(&self.rows),
            }))
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{self}")
        }
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new(
                "parts",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            Field::new("x", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]))
    }

    /// `k` skewed to 0, `parts` of 0..6 strings over a small alphabet with
    /// nulls, nullable `x`, and `s` a number as text, `"bad"` on some rows
    fn table(rng: &mut Rng, rows: usize, bad_s: bool) -> RecordBatch {
        let mut parts = ListBuilder::new(StringBuilder::new());
        for _ in 0..rows {
            if rng.next(20) == 0 {
                parts.append_null();
                continue;
            }
            for _ in 0..rng.next(7) {
                match rng.next(5) {
                    0 => parts.values().append_null(),
                    v => parts
                        .values()
                        .append_value(["a", "b", "c", "d"][v as usize - 1]),
                }
            }
            parts.append(true);
        }
        let k: Int32Array = (0..rows)
            .map(|_| (rng.next(10) != 0).then(|| (rng.next(10) as i32).saturating_sub(6)))
            .collect();
        let x: Int32Array = (0..rows)
            .map(|_| (rng.next(8) != 0).then(|| rng.next(20) as i32))
            .collect();
        let s: StringArray = (0..rows)
            .map(|i| match rng.next(10) {
                0 => None,
                1 if bad_s && i % 2 == 1 => Some("bad".to_string()),
                v => Some(v.to_string()),
            })
            .collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(k),
                Arc::new(parts.finish()) as ArrayRef,
                Arc::new(x),
                Arc::new(s),
            ],
        )
        .unwrap()
    }

    fn col(name: &str, index: usize) -> PhysicalExprRef {
        Arc::new(Column::new(name, index))
    }

    fn item(child: PhysicalExprRef, index: usize, rows: &Arc<AtomicUsize>) -> PhysicalExprRef {
        Arc::new(Item {
            child,
            index,
            rows: Arc::clone(rows),
        })
    }

    fn and(l: PhysicalExprRef, r: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(BinaryExpr::new(l, Operator::And, r))
    }

    fn or(l: PhysicalExprRef, r: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(BinaryExpr::new(l, Operator::Or, r))
    }

    fn op(l: PhysicalExprRef, o: Operator, r: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(BinaryExpr::new(l, o, r))
    }

    fn is_null(e: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(IsNullExpr::new(e))
    }

    /// Strict (erroring) string to int cast
    fn to_int(e: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(CastExpr::new(e, DataType::Int32, None))
    }

    /// Filter columns, in this order, over left (build) and right (probe)
    /// inputs of `schema()`: l.k, r.k, l.parts, r.parts, l.x, r.x, l.s, r.s
    fn filter(expr: PhysicalExprRef) -> JoinFilter {
        let s = schema();
        let mut column_indices = vec![];
        let mut fields = vec![];
        for index in 0..4 {
            for side in [JoinSide::Left, JoinSide::Right] {
                column_indices.push(ColumnIndex { index, side });
                fields.push(s.field(index).clone());
            }
        }
        JoinFilter::new(expr, column_indices, Arc::new(Schema::new(fields)))
    }

    /// The table location filter of gcs_cost_insights_v2: `AND` over
    /// `i in 1..=depth` of `l.parts[i] IS NULL OR l.parts[i] = r.parts[i]`
    fn parts_filter(depth: usize, rows: &Arc<AtomicUsize>) -> PhysicalExprRef {
        let mut e: Option<PhysicalExprRef> = None;
        for i in 1..=depth {
            let l = item(col("l.parts", 2), i, rows);
            let r = item(col("r.parts", 3), i, rows);
            let term = or(is_null(Arc::clone(&l)), op(l, Operator::Eq, r));
            e = Some(match e {
                None => term,
                Some(e) => and(e, term),
            });
        }
        e.unwrap()
    }

    fn filters(rows: &Arc<AtomicUsize>) -> Vec<(&'static str, PhysicalExprRef)> {
        let x_l = || col("l.x", 4);
        let x_r = || col("r.x", 5);
        vec![
            ("parts", parts_filter(5, rows)),
            (
                "parts_and_x",
                and(parts_filter(3, rows), op(x_l(), Operator::Lt, x_r())),
            ),
            (
                "or_top",
                or(
                    op(op(x_l(), Operator::Plus, x_r()), Operator::Gt, lit(15i32)),
                    is_null(x_l()),
                ),
            ),
            (
                "case",
                and(
                    Arc::new(
                        CaseExpr::try_new(
                            None,
                            vec![(
                                op(x_r(), Operator::Gt, lit(9i32)),
                                op(x_l(), Operator::Lt, x_r()),
                            )],
                            Some(is_null(item(col("l.parts", 2), 0, rows))),
                        )
                        .unwrap(),
                    ),
                    op(x_r(), Operator::NotEq, lit(7i32)),
                ),
            ),
            (
                "nullable_conjuncts",
                and(
                    and(
                        op(x_l(), Operator::GtEq, lit(3i32)),
                        op(x_l(), Operator::LtEq, x_r()),
                    ),
                    op(
                        item(col("l.parts", 2), 0, rows),
                        Operator::Eq,
                        item(col("r.parts", 3), 0, rows),
                    ),
                ),
            ),
            (
                "literal_conjunct",
                and(
                    lit(true),
                    and(
                        Arc::new(Literal::new(ScalarValue::Boolean(None))),
                        op(x_l(), Operator::Eq, x_r()),
                    ),
                ),
            ),
            ("probe_only", op(x_r(), Operator::Gt, lit(4i32))),
            (
                "build_only_and_probe_only",
                and(
                    op(x_l(), Operator::Lt, lit(12i32)),
                    is_null(item(col("r.parts", 3), 2, rows)),
                ),
            ),
            (
                "strict_cast_of_unpaired_build_rows",
                and(
                    op(x_l(), Operator::Gt, lit(-1i32)),
                    op(to_int(col("l.s", 6)), Operator::Lt, x_r()),
                ),
            ),
        ]
    }

    /// Candidate pairs grouped by probe row, as the hash join produces them,
    /// with a long run of one probe row
    fn pairs(rng: &mut Rng, build_rows: usize, probe_rows: usize) -> (UInt64Array, UInt32Array) {
        let mut b = vec![];
        let mut p = vec![];
        for probe in 0..probe_rows as u32 {
            let n = if probe == 3 {
                3 * build_rows
            } else {
                rng.next(6) as usize
            };
            for _ in 0..n {
                b.push(rng.next(build_rows as u64));
                p.push(probe);
            }
        }
        (UInt64Array::from(b), UInt32Array::from(p))
    }

    fn old_path(
        build: &RecordBatch,
        probe: &RecordBatch,
        b: &UInt64Array,
        p: &UInt32Array,
        filter: &JoinFilter,
    ) -> Result<(UInt64Array, UInt32Array)> {
        apply_join_filter_to_indices(
            build,
            probe,
            b.clone(),
            p.clone(),
            filter,
            JoinSide::Left,
            None,
            JoinType::Inner,
        )
    }

    /// Pairs whose build row has a non-numeric `s`
    fn without_bad_build_s(
        build: &RecordBatch,
        b: &UInt64Array,
        p: &UInt32Array,
    ) -> (UInt64Array, UInt32Array) {
        let s = build.column(3).as_string::<i32>();
        let keep: BooleanArray = b
            .values()
            .iter()
            .map(|&i| Some(!(s.is_valid(i as usize) && s.value(i as usize) == "bad")))
            .collect();
        (
            downcast_array(compute::filter(b, &keep).unwrap().as_ref()),
            downcast_array(compute::filter(p, &keep).unwrap().as_ref()),
        )
    }

    #[test]
    fn keeps_the_pairs_of_the_unlifted_filter_in_order() -> Result<()> {
        let rows = Arc::new(AtomicUsize::new(0));
        let mut rng = Rng(7);
        let build = table(&mut rng, 300, true);
        for (name, expr) in filters(&rows) {
            let filter = filter(expr);
            let mut lazy = LazyJoinFilter::try_new(&filter, JoinSide::Left)?.unwrap();
            // Several calls over the same build batch: the first ones before,
            // the later ones after the build-side cache is filled.
            for call in 0..6 {
                let probe = table(&mut rng, 40 + call * 10, false);
                let (b, p) = pairs(&mut rng, build.num_rows(), probe.num_rows());
                let (b, p) = without_bad_build_s(&build, &b, &p);
                let expected = old_path(&build, &probe, &b, &p, &filter)?;
                let actual = lazy.apply(&build, &probe, b.clone(), p.clone())?;
                assert_eq!(actual, expected, "filter {name}, call {call}");
                // Unsorted pairs: no runs to share
                let order: UInt32Array = (0..b.len() as u32)
                    .map(|i| (i * 7919) % b.len() as u32)
                    .collect();
                if b.len() > 1 && b.len() % 7919 != 0 {
                    let b = downcast_array(take(&b, &order, None)?.as_ref());
                    let p = downcast_array(take(&p, &order, None)?.as_ref());
                    let expected = old_path(&build, &probe, &b, &p, &filter)?;
                    let actual = lazy.apply(&build, &probe, b, p)?;
                    assert_eq!(actual, expected, "filter {name}, call {call}, shuffled");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn errors_only_for_pairs_the_filter_reaches() -> Result<()> {
        let rows = Arc::new(AtomicUsize::new(0));
        let mut rng = Rng(11);
        let build = table(&mut rng, 200, true);
        let probe = table(&mut rng, 50, false);
        let filter = filter(and(
            op(col("r.x", 5), Operator::Lt, lit(100i32)),
            op(to_int(col("l.s", 6)), Operator::Lt, col("r.x", 5)),
        ));
        let mut lazy = LazyJoinFilter::try_new(&filter, JoinSide::Left)?.unwrap();
        // Enough pairs to fill the build-side cache, which fails on "bad"
        // build rows no pair reaches: no error, results as before.
        for _ in 0..4 {
            let (b, p) = pairs(&mut rng, build.num_rows(), probe.num_rows());
            let (b, p) = without_bad_build_s(&build, &b, &p);
            let expected = old_path(&build, &probe, &b, &p, &filter)?;
            assert_eq!(lazy.apply(&build, &probe, b, p)?, expected);
        }
        assert!(lazy.build_cache.failed);
        // A pair of a "bad" build row still raises the cast error.
        let s = build.column(3).as_string::<i32>();
        let bad = (0..build.num_rows())
            .find(|&i| s.is_valid(i) && s.value(i) == "bad")
            .unwrap() as u64;
        let b = UInt64Array::from(vec![0, bad]);
        let p = UInt32Array::from(vec![0, 0]);
        assert!(old_path(&build, &probe, &b, &p, &filter).is_err());
        assert!(lazy.apply(&build, &probe, b, p).is_err());
        let _ = rows;
        Ok(())
    }

    #[test]
    fn evaluates_lifted_subexpressions_per_row_not_per_pair() -> Result<()> {
        let rows = Arc::new(AtomicUsize::new(0));
        let mut rng = Rng(5);
        let build = table(&mut rng, 1000, false);
        let probe = table(&mut rng, 4, false);
        let filter = filter(parts_filter(20, &rows));
        let mut lazy = LazyJoinFilter::try_new(&filter, JoinSide::Left)?.unwrap();
        // 25 calls of 8000 pairs, all of probe row 1 (a skewed key)
        let calls = 25;
        let mut pairs_total = 0;
        for call in 0..calls {
            let b: UInt64Array = (0..8000u64).map(|i| (i * 13 + call) % 1000).collect();
            let p = UInt32Array::from(vec![1u32; 8000]);
            pairs_total += b.len();
            let expected = old_path(&build, &probe, &b, &p, &filter)?;
            let before = rows.load(Ordering::Relaxed);
            let actual = lazy.apply(&build, &probe, b, p)?;
            assert_eq!(actual, expected);
            if call >= 1 {
                // Cache filled: only one probe row per lifted probe item
                assert!(rows.load(Ordering::Relaxed) - before <= 20, "call {call}");
            }
        }
        rows.store(0, Ordering::Relaxed);
        let _ = old_path(
            &build,
            &probe,
            &(0..8000u64).map(|i| i % 1000).collect(),
            &UInt32Array::from(vec![1u32; 8000]),
            &filter,
        )?;
        // The unlifted filter evaluates the items per pair (pre-selection of
        // DataFusion's AND skips some of them)
        assert!(rows.load(Ordering::Relaxed) >= 2 * 8000);
        assert!(pairs_total > 0);
        Ok(())
    }

    #[test]
    fn not_used_without_filter_columns() -> Result<()> {
        let f = JoinFilter::new(lit(true), vec![], Arc::new(Schema::empty()));
        assert!(LazyJoinFilter::try_new(&f, JoinSide::Left)?.is_none());
        Ok(())
    }

    fn memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        TestMemoryExec::try_new_exec(&[batches], schema(), None).unwrap()
    }

    fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
        let mut rows: Vec<String> = arrow::util::pretty::pretty_format_batches(batches)
            .unwrap()
            .to_string()
            .lines()
            .filter(|l| l.starts_with('|'))
            .skip(1)
            .map(|l| l.to_string())
            .collect();
        rows.sort();
        rows
    }

    /// The hash join with each filter, for every join type and batch size,
    /// returns the rows of a nested loop join over `l.k = r.k AND filter`.
    #[tokio::test]
    async fn hash_join_matches_nested_loop_join_for_every_join_type() -> Result<()> {
        let rows = Arc::new(AtomicUsize::new(0));
        let mut rng = Rng(3);
        let left: Vec<RecordBatch> = (0..3).map(|_| table(&mut rng, 120, false)).collect();
        let right: Vec<RecordBatch> = (0..4).map(|_| table(&mut rng, 90, false)).collect();
        let s = schema();
        for (name, expr) in filters(&rows) {
            for join_type in [
                JoinType::Inner,
                JoinType::Left,
                JoinType::Right,
                JoinType::Full,
                JoinType::LeftSemi,
                JoinType::LeftAnti,
                JoinType::RightSemi,
                JoinType::RightAnti,
                JoinType::LeftMark,
                JoinType::RightMark,
            ] {
                for batch_size in [7, 64, 8192] {
                    let ctx = Arc::new(
                        TaskContext::default().with_session_config(
                            datafusion_execution::config::SessionConfig::new()
                                .with_batch_size(batch_size),
                        ),
                    );
                    let hash_join = HashJoinExec::try_new(
                        memory_exec(left.clone()),
                        memory_exec(right.clone()),
                        vec![(
                            Arc::new(Column::new_with_schema("k", &s)?) as _,
                            Arc::new(Column::new_with_schema("k", &s)?) as _,
                        )],
                        Some(filter(Arc::clone(&expr))),
                        &join_type,
                        None,
                        PartitionMode::CollectLeft,
                        NullEquality::NullEqualsNothing,
                        false,
                    )?;
                    let nested_loop_join = NestedLoopJoinExec::try_new(
                        memory_exec(left.clone()),
                        memory_exec(right.clone()),
                        Some(filter(and(
                            binary(col("l.k", 0), Operator::Eq, col("r.k", 1), &s)?,
                            Arc::clone(&expr),
                        ))),
                        &join_type,
                        None,
                    )?;
                    let expected =
                        sorted_rows(&collect(Arc::new(nested_loop_join), Arc::clone(&ctx)).await?);
                    let actual = sorted_rows(&common::collect(hash_join.execute(0, ctx)?).await?);
                    assert_eq!(
                        actual, expected,
                        "filter {name}, {join_type}, batch size {batch_size}"
                    );
                }
            }
        }
        Ok(())
    }
}
