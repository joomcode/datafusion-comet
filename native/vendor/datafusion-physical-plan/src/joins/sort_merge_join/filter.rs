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

//! Filter handling for Sort-Merge Join
//!
//! This module encapsulates the complexity of join filter evaluation, including:
//! - Immediate filtering for INNER joins
//! - Deferred filtering for outer joins
//! - Metadata tracking for grouping output rows by input row
//! - Correcting filter masks to handle multiple matches per input row

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBufferBuilder, BooleanBuilder,
    RecordBatch, RecordBatchOptions, UInt64Array, UInt64Builder, new_null_array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{self, filter_record_batch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinSide, JoinType, Result, internal_err};
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
    TryCastExpr,
};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{
    PhysicalExpr, PhysicalExprRef, is_volatile,
};

use crate::joins::utils::{ColumnIndex, JoinFilter};

/// Metadata for tracking filter results during deferred filtering
///
/// When a join filter is present and we need to ensure each input row produces
/// at least one output (outer joins), we can't filter immediately. Instead,
/// we accumulate all joined rows with metadata, then post-process to determine
/// which rows to output.
#[derive(Debug)]
pub struct FilterMetadata {
    /// Did each output row pass the join filter?
    /// Used to detect if an input row found ANY match
    pub filter_mask: BooleanBuilder,

    /// Which input row (within batch) produced each output row?
    /// Used for grouping output rows by input row
    pub row_indices: UInt64Builder,

    /// Which input batch did each output row come from?
    /// Used to disambiguate row_indices across multiple batches
    pub batch_ids: Vec<usize>,
}

impl FilterMetadata {
    /// Create new empty filter metadata
    pub fn new() -> Self {
        Self {
            filter_mask: BooleanBuilder::new(),
            row_indices: UInt64Builder::new(),
            batch_ids: vec![],
        }
    }

    /// Returns (row_indices, filter_mask, batch_ids_ref) and clears builders
    pub fn finish_metadata(&mut self) -> (UInt64Array, BooleanArray, &[usize]) {
        let row_indices = self.row_indices.finish();
        let filter_mask = self.filter_mask.finish();
        (row_indices, filter_mask, &self.batch_ids)
    }

    /// Add metadata for null-joined rows (no filter applied)
    pub fn append_nulls(&mut self, num_rows: usize) {
        self.filter_mask.append_nulls(num_rows);
        self.row_indices.append_nulls(num_rows);
        self.batch_ids.resize(
            self.batch_ids.len() + num_rows,
            0, // batch_id = 0 for null-joined rows
        );
    }

    /// Add metadata for filtered rows
    pub fn append_filter_metadata(
        &mut self,
        row_indices: &UInt64Array,
        filter_mask: &BooleanArray,
        batch_id: usize,
    ) {
        debug_assert_eq!(
            row_indices.len(),
            filter_mask.len(),
            "row_indices and filter_mask must have same length"
        );

        self.filter_mask.extend(filter_mask);
        self.row_indices.extend(row_indices);
        self.batch_ids
            .resize(self.batch_ids.len() + row_indices.len(), batch_id);
    }

    /// Verify that metadata arrays are aligned (same length)
    pub fn debug_assert_metadata_aligned(&self) {
        if self.filter_mask.len() > 0 {
            debug_assert_eq!(
                self.filter_mask.len(),
                self.row_indices.len(),
                "filter_mask and row_indices must have same length when metadata is used"
            );
            debug_assert_eq!(
                self.filter_mask.len(),
                self.batch_ids.len(),
                "filter_mask and batch_ids must have same length when metadata is used"
            );
        } else {
            debug_assert_eq!(
                self.filter_mask.len(),
                0,
                "filter_mask should be empty when batches is empty"
            );
        }
    }
}

impl Default for FilterMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Determines if a join type needs deferred filtering
///
/// Deferred filtering is required when:
/// - A filter exists AND
/// - The join type requires ensuring each input row produces at least one output
pub fn needs_deferred_filtering(
    filter: &Option<JoinFilter>,
    join_type: JoinType,
) -> bool {
    filter.is_some()
        && matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full)
}

/// Marks the entries that are the last of their input row
///
/// Used during filter mask correction to detect row boundaries when grouping
/// output rows by input row. Entries without a row index (null-joined rows
/// that belong to no input row group) are not marked and do not end a
/// group: a FULL join stages the null-joined rows of unmatched buffered rows
/// ahead of the pairs a freeze materializes, so they can fall between two
/// runs of one streamed row's pairs.
fn last_entries_of_rows(indices: &UInt64Array, batch_ids: &[usize]) -> Vec<bool> {
    debug_assert_eq!(
        batch_ids.len(),
        indices.len(),
        "batch_ids.len() should match indices.len()"
    );
    let mut last = vec![false; indices.len()];
    let mut next_row: Option<(usize, u64)> = None;
    for i in (0..indices.len()).rev() {
        if indices.is_null(i) {
            continue;
        }
        let row = (batch_ids[i], indices.value(i));
        last[i] = next_row != Some(row);
        next_row = Some(row);
    }
    last
}

/// Corrects the filter mask for joins with deferred filtering
///
/// When an input row joins with multiple buffered rows, we get multiple output rows.
/// This function groups them by input row and applies join-type-specific logic:
///
/// - **Outer joins**: Keep first matching row, convert rest to nulls, add null-joined for unmatched
///
/// # Arguments
/// * `join_type` - The type of join being performed
/// * `row_indices` - Which input row produced each output row
/// * `batch_ids` - Which batch each output row came from
/// * `filter_mask` - Whether each output row passed the filter
/// * `expected_size` - Total number of input rows (for adding unmatched)
///
/// # Returns
/// Corrected mask indicating which rows to include in final output:
/// - `true`: Include this row
/// - `false`: Convert to null-joined row (outer joins)
/// - `null`: Discard this row
pub fn get_corrected_filter_mask(
    join_type: JoinType,
    row_indices: &UInt64Array,
    batch_ids: &[usize],
    filter_mask: &BooleanArray,
    expected_size: usize,
) -> Option<BooleanArray> {
    let row_indices_length = row_indices.len();
    let mut corrected_mask: BooleanBuilder =
        BooleanBuilder::with_capacity(row_indices_length);
    let mut seen_true = false;

    match join_type {
        JoinType::Left | JoinType::Right | JoinType::Full => {
            // For each input row group: keep first filter-passing row,
            // discard (null) remaining matches, null-join if none passed.
            // Null metadata entries are already-null-joined rows that
            // flow through unchanged to preserve output ordering.
            let last_entries = last_entries_of_rows(row_indices, batch_ids);
            for (i, &last_index) in last_entries.iter().enumerate() {
                if filter_mask.is_null(i) {
                    corrected_mask.append_value(true);
                } else if filter_mask.value(i) {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null();
                } else {
                    corrected_mask.append_value(false);
                }

                if last_index {
                    seen_true = false;
                }
            }

            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::LeftMark
        | JoinType::RightMark
        | JoinType::LeftSemi
        | JoinType::RightSemi
        | JoinType::LeftAnti
        | JoinType::RightAnti => {
            unreachable!("Semi/anti/mark joins are handled by BitwiseSortMergeJoinStream")
        }
        JoinType::Inner => None,
    }
}

/// Selects the pairs of one freeze that deferred filtering may output
///
/// For each streamed row, `get_corrected_filter_mask` keeps every pair that
/// passed the filter, or null-joins the row's last pair when none did, and
/// discards the rest. A row's pairs can span several freezes, so a freeze
/// cannot tell which of its failing pairs ends up null-joined. Within each
/// run of one row's pairs it therefore keeps the passing pairs, or only the
/// run's last pair when none passed. Over the runs of one row this keeps
/// all of its passing pairs and its overall last pair, which is all
/// `get_corrected_filter_mask` outputs from it, and the discarded failing
/// pairs never decide which pair is null-joined.
///
/// # Arguments
/// * `row_indices` - Which streamed row produced each pair (no nulls)
/// * `filter_mask` - Whether each pair passed the filter (no nulls)
///
/// # Returns
/// A mask that is `true` for the pairs to materialize and keep
pub fn deferred_filter_candidates(
    row_indices: &UInt64Array,
    filter_mask: &BooleanArray,
) -> BooleanArray {
    debug_assert_eq!(
        row_indices.len(),
        filter_mask.len(),
        "row_indices and filter_mask must have same length"
    );
    debug_assert_eq!(row_indices.null_count(), 0);
    debug_assert_eq!(filter_mask.null_count(), 0);

    let rows = row_indices.values();
    let passed = filter_mask.values();
    let mut keep = BooleanBufferBuilder::new(rows.len());
    let mut run_start = 0;
    for i in 0..rows.len() {
        if i + 1 < rows.len() && rows[i + 1] == rows[i] {
            continue;
        }
        let run_len = i + 1 - run_start;
        if run_len == 1 {
            keep.append(true);
        } else {
            let run = passed.slice(run_start, run_len);
            if run.count_set_bits() > 0 {
                keep.append_buffer(&run);
            } else {
                keep.append_n(run_len - 1, false);
                keep.append(true);
            }
        }
        run_start = i + 1;
    }
    BooleanArray::new(keep.finish(), None)
}

/// Applies corrected filter mask to record batch based on join type
///
/// The corrected mask has three possible values per row:
/// - `true`: Keep the row as-is (matched and passed filter)
/// - `false`: Convert to null-joined row (all filter matches failed for this input row)
/// - `null`: Discard the row entirely (duplicate match for an already-output input row)
///
/// This function preserves input row ordering by processing each row in place
/// rather than separating matched/unmatched rows.
pub fn filter_record_batch_by_join_type(
    record_batch: &RecordBatch,
    corrected_mask: &BooleanArray,
    join_type: JoinType,
    schema: &SchemaRef,
    buffered_schema: &SchemaRef,
) -> Result<RecordBatch> {
    match join_type {
        JoinType::Left | JoinType::Right | JoinType::Full => {
            if record_batch.num_rows() == 0 {
                return Ok(record_batch.clone());
            }

            // Discard null-masked rows (keep true + false only)
            let keep_mask = compute::is_not_null(corrected_mask)?;
            let kept_batch = filter_record_batch(record_batch, &keep_mask)?;

            if kept_batch.num_rows() == 0 {
                return Ok(kept_batch);
            }

            let kept_corrected = compute::filter(corrected_mask, &keep_mask)?;
            let kept_corrected = kept_corrected
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();

            // All rows passed the filter — no null-joining needed
            if !kept_corrected.has_false() {
                return Ok(kept_batch);
            }

            // For false entries: replace the non-preserved side with nulls.
            // This preserves row ordering unlike filter+concat.
            let (null_side_start, null_side_len) = match join_type {
                JoinType::Left => {
                    // Left join: null out right (buffered) columns
                    let left_cols =
                        schema.fields().len() - buffered_schema.fields().len();
                    (left_cols, buffered_schema.fields().len())
                }
                JoinType::Right => {
                    // Right join: null out left (buffered) columns
                    (0, buffered_schema.fields().len())
                }
                JoinType::Full => {
                    // Full join: null out buffered columns for streamed rows
                    // that matched but failed the filter. Unmatched buffered
                    // rows are null-joined on the streamed side separately
                    // when the buffered batch is drained.
                    let left_cols =
                        schema.fields().len() - buffered_schema.fields().len();
                    (left_cols, buffered_schema.fields().len())
                }
                _ => unreachable!(),
            };

            let num_rows = kept_batch.num_rows();
            let mut columns: Vec<ArrayRef> = kept_batch.columns().to_vec();

            for col in columns.iter_mut().skip(null_side_start).take(null_side_len) {
                let null_array = new_null_array(col.data_type(), num_rows);
                *col = zip(kept_corrected, &*col, &null_array)?;
            }

            let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
            Ok(RecordBatch::try_new_with_options(
                Arc::clone(schema),
                columns,
                &options,
            )?)
        }
        JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::LeftMark
        | JoinType::RightMark => {
            unreachable!(
                "Semi/anti/mark joins are handled by SemiAntiMarkSortMergeJoinStream"
            )
        }
        JoinType::Inner => Ok(filter_record_batch(record_batch, corrected_mask)?),
    }
}

/// An expression the caller has proved safe to precompute for extra input rows.
///
/// A join can evaluate this expression before a conditional branch selects rows,
/// and only once for repeated rows. The complete wrapped expression must therefore
/// be deterministic, row-local, and unable to fail for any value allowed by the
/// input schema. The marker is intentionally affirmative: unknown expressions,
/// including UDFs that omit volatility metadata, are never safe by default.
///
/// This lets an embedding engine prove safety of its own expressions without a
/// dependency from this crate back to that engine. It must not wrap a throwing
/// expression merely because that expression is under a lazy CASE/IF/AND/OR.
#[derive(Debug)]
pub struct PrecomputeSafeExpr {
    expression: PhysicalExprRef,
}

impl PartialEq for PrecomputeSafeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expression.eq(&other.expression)
    }
}

impl Eq for PrecomputeSafeExpr {}

impl Hash for PrecomputeSafeExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expression.hash(state);
    }
}

impl PrecomputeSafeExpr {
    /// Marks an expression after the caller has established the contract above.
    pub fn new(expression: PhysicalExprRef) -> Self {
        Self { expression }
    }
}

impl Display for PrecomputeSafeExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.expression, f)
    }
}

impl PhysicalExpr for PrecomputeSafeExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.expression.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expression.nullable(input_schema)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.expression.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.expression.evaluate(batch)
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        self.expression.evaluate_selection(batch, selection)
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.expression]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        if children.len() != 1 {
            return internal_err!("PrecomputeSafeExpr expects one child");
        }
        // A safety assertion is about this exact expression. Rewrites must
        // establish it again; never let a substituted throwing child inherit
        // it. SMJ's column rebinding happens after hoist selection, so it no
        // longer needs the marker on the selected expression.
        Ok(children.remove(0))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.expression.fmt_sql(f)
    }
}

/// Whether a DataFusion expression is known safe outside a lazy branch.
///
/// PhysicalExpr has no can-throw property. Keep this an affirmative whitelist,
/// checking every child as well as this node; non-volatility alone is not enough
/// for ANSI casts, integer division or checked arithmetic. Embedders can use
/// PrecomputeSafeExpr for their own independently proved expression classes.
fn safe_to_precompute(expression: &PhysicalExprRef, schema: &Schema) -> bool {
    if is_volatile(expression) {
        return false;
    }
    if expression.is::<PrecomputeSafeExpr>()
        || expression.is::<Column>()
        || expression.is::<Literal>()
    {
        return true;
    }
    let safe_node = if let Some(binary) = expression.downcast_ref::<BinaryExpr>() {
        let (Ok(left), Ok(right)) = (
            binary.left().data_type(schema),
            binary.right().data_type(schema),
        ) else {
            return false;
        };
        match binary.op() {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom => {
                left == right
                    && (left.is_numeric()
                        || matches!(
                            left,
                            DataType::Boolean
                                | DataType::Utf8
                                | DataType::LargeUtf8
                                | DataType::Binary
                                | DataType::LargeBinary
                                | DataType::Date32
                                | DataType::Date64
                                | DataType::Timestamp(_, _)
                        ))
            }
            Operator::And | Operator::Or => {
                left == DataType::Boolean && right == DataType::Boolean
            }
            Operator::Plus | Operator::Minus | Operator::Multiply => {
                // BinaryExpr exposes its checked-arithmetic flag through equality.
                let wrapping = BinaryExpr::new(
                    Arc::clone(binary.left()),
                    *binary.op(),
                    Arc::clone(binary.right()),
                );
                left == right
                    && (left.is_integer() || left.is_floating())
                    && *binary == wrapping
            }
            Operator::Divide | Operator::Modulo => left == right && left.is_floating(),
            _ => false,
        }
    } else if let Some(cast) = expression.downcast_ref::<CastExpr>() {
        cast.expr().data_type(schema).is_ok_and(|from| {
            use DataType::*;
            let to = cast.cast_type();
            !matches!(to, Dictionary(_, _))
                && (from == *to
                    || from == Null
                    || matches!(
                        (&from, to),
                        (Int8, Int16 | Int32 | Int64 | Float32 | Float64)
                            | (Int16, Int32 | Int64 | Float32 | Float64)
                            | (Int32, Int64 | Float32 | Float64)
                            | (Int64, Float32 | Float64)
                            | (UInt8, UInt16 | UInt32 | UInt64 | Float32 | Float64)
                            | (UInt16, UInt32 | UInt64 | Float32 | Float64)
                            | (UInt32, UInt64 | Float32 | Float64)
                            | (UInt64, Float32 | Float64)
                            | (Float32, Float64)
                    ))
        })
    } else if let Some(cast) = expression.downcast_ref::<TryCastExpr>() {
        // TRY_CAST parsing and primitive numeric conversion replace invalid
        // values with NULL. Unsupported/nested conversions stay pair-evaluated.
        cast.expr().data_type(schema).is_ok_and(|from| {
            let to = cast.cast_type();
            (from.is_integer()
                || from.is_floating()
                || matches!(from, DataType::Utf8 | DataType::LargeUtf8))
                && (to.is_integer() || to.is_floating())
        })
    } else if let Some(not) = expression.downcast_ref::<NotExpr>() {
        not.arg()
            .data_type(schema)
            .is_ok_and(|t| t == DataType::Boolean)
    } else {
        expression.is::<IsNullExpr>()
            || expression.is::<IsNotNullExpr>()
            || expression.is::<CaseExpr>()
    };
    safe_node
        && expression
            .children()
            .into_iter()
            .all(|child| safe_to_precompute(child, schema))
}

/// A join filter whose largest subexpressions over the columns of one input
/// alone are lifted out, so they can be evaluated once per buffered or
/// streamed row instead of once per pair.
#[derive(Debug)]
pub(super) struct HoistedJoinFilter {
    /// The filter over `schema`, reading each lifted subexpression's result
    /// from a column
    pub expression: PhysicalExprRef,
    /// Schema of the batch `expression` is evaluated against
    pub schema: SchemaRef,
    /// Where each column of `schema` comes from
    pub inputs: Vec<HoistedFilterInput>,
    /// The lifted subexpressions over buffered columns, over the buffered
    /// input's columns
    pub buffered_exprs: Vec<PhysicalExprRef>,
    /// The lifted subexpressions over streamed columns, over the streamed
    /// input's columns in `streamed_projection`
    pub streamed_exprs: Vec<PhysicalExprRef>,
    /// The streamed input's columns `streamed_exprs` read
    pub streamed_projection: Vec<usize>,
}

#[derive(Debug)]
pub(super) enum HoistedFilterInput {
    /// A column of the streamed or the buffered input
    Column(ColumnIndex),
    /// The result of `buffered_exprs[i]`
    Buffered(usize),
    /// The result of `streamed_exprs[i]`
    Streamed(usize),
}

impl HoistedJoinFilter {
    /// Lifts deterministic, infallible subexpressions of `filter` that read
    /// the columns of one input and none of the other. Returns `None` when there
    /// is none besides bare columns.
    pub fn try_new(
        filter: &JoinFilter,
        buffered_side: JoinSide,
        streamed_schema: &Schema,
        buffered_schema: &Schema,
    ) -> Result<Option<Self>> {
        let column_indices = filter.column_indices();
        let num_filter_columns = column_indices.len();
        let mut lifted: Vec<(JoinSide, PhysicalExprRef)> = vec![];
        let expression = Arc::clone(filter.expression())
            .transform_down(|expr| {
                if expr.downcast_ref::<Column>().is_some()
                    || !safe_to_precompute(&expr, filter.schema())
                {
                    return Ok(Transformed::no(expr));
                }
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
                {
                    return Ok(Transformed::no(expr));
                }
                let position = match lifted.iter().position(|(_, e)| **e == *expr) {
                    Some(position) => position,
                    None => {
                        lifted.push((side, Arc::clone(&expr)));
                        lifted.len() - 1
                    }
                };
                Ok(Transformed::new(
                    Arc::new(Column::new(
                        &lifted_name(&lifted, position, buffered_side),
                        num_filter_columns + position,
                    )),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            })?
            .data;
        if lifted.is_empty() {
            return Ok(None);
        }

        let mut used: Vec<usize> = collect_columns(&expression)
            .iter()
            .map(|c| c.index())
            .filter(|&index| index < num_filter_columns)
            .collect();
        used.sort_unstable();
        used.dedup();
        let mut fields: Vec<Field> = used
            .iter()
            .map(|&index| filter.schema().field(index).clone())
            .collect();
        let mut inputs: Vec<HoistedFilterInput> = used
            .iter()
            .map(|&index| HoistedFilterInput::Column(column_indices[index].clone()))
            .collect();
        let mut buffered_exprs = vec![];
        let mut streamed_exprs = vec![];
        for (position, (side, expr)) in lifted.iter().enumerate() {
            fields.push(Field::new(
                lifted_name(&lifted, position, buffered_side),
                expr.data_type(filter.schema())?,
                expr.nullable(filter.schema())?,
            ));
            if *side == buffered_side {
                inputs.push(HoistedFilterInput::Buffered(buffered_exprs.len()));
                buffered_exprs.push(Arc::clone(expr));
            } else {
                inputs.push(HoistedFilterInput::Streamed(streamed_exprs.len()));
                streamed_exprs.push(Arc::clone(expr));
            }
        }

        let expression = expression
            .transform_up(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let index = match used.binary_search(&column.index()) {
                    Ok(position) => position,
                    Err(_) => used.len() + column.index() - num_filter_columns,
                };
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), index)) as PhysicalExprRef,
                ))
            })?
            .data;
        let rebind = |exprs: Vec<PhysicalExprRef>,
                      schema: &Schema,
                      projection: Option<&[usize]>| {
            exprs
                .into_iter()
                .map(|expr| {
                    expr.transform_up(|expr| {
                        let Some(column) = expr.downcast_ref::<Column>() else {
                            return Ok(Transformed::no(expr));
                        };
                        let index = column_indices[column.index()].index;
                        let position = match projection {
                            Some(projection) => projection.binary_search(&index).unwrap(),
                            None => index,
                        };
                        Ok(Transformed::yes(Arc::new(Column::new(
                            schema.field(index).name(),
                            position,
                        ))
                            as PhysicalExprRef))
                    })
                    .map(|transformed| transformed.data)
                })
                .collect::<Result<Vec<_>>>()
        };
        let mut streamed_projection: Vec<usize> = streamed_exprs
            .iter()
            .flat_map(collect_columns)
            .map(|c| column_indices[c.index()].index)
            .collect();
        streamed_projection.sort_unstable();
        streamed_projection.dedup();
        let buffered_exprs = rebind(buffered_exprs, buffered_schema, None)?;
        let streamed_exprs =
            rebind(streamed_exprs, streamed_schema, Some(&streamed_projection))?;

        Ok(Some(Self {
            expression,
            schema: Arc::new(Schema::new(fields)),
            inputs,
            buffered_exprs,
            streamed_exprs,
            streamed_projection,
        }))
    }
}

/// Name of the column holding the result of `lifted[position]`
fn lifted_name(
    lifted: &[(JoinSide, PhysicalExprRef)],
    position: usize,
    buffered_side: JoinSide,
) -> String {
    let side = lifted[position].0;
    let side_position = lifted[..position]
        .iter()
        .filter(|(s, _)| *s == side)
        .count();
    if side == buffered_side {
        format!("__buffered_{side_position}")
    } else {
        format!("__streamed_{side_position}")
    }
}
