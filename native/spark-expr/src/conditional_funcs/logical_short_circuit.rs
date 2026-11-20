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

use super::case_when::cannot_fail;
use crate::SparkContains;
use arrow::array::{Array, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and_kleene, or_kleene};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err, Result, ScalarValue};
use datafusion::functions::string::{ends_with::EndsWithFunc, starts_with::StartsWithFunc};
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal, NotExpr};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_expr_common::utils::scatter;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// An extra proof local to boolean evaluation: these concrete native string predicates compare
/// existing strings, without parsing or constructing an unbounded result. Retain the original
/// vectorized BinaryExpr for their ordinary Boolean combinations. Do not relax the separate
/// global CASE/null-intolerant/SMJ proof or trust a function's name: user UDFs can reuse it.
fn logical_cannot_fail(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    if cannot_fail(expr, schema) {
        return true;
    }
    let known_node = if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let implementation = function.fun().inner();
        let concrete_predicate = implementation.downcast_ref::<StartsWithFunc>().is_some()
            || implementation.downcast_ref::<EndsWithFunc>().is_some()
            || implementation.downcast_ref::<SparkContains>().is_some();
        let supported_pair = function.args().len() == 2
            && match (
                function.args()[0].data_type(schema),
                function.args()[1].data_type(schema),
            ) {
                (Ok(left), Ok(right)) => {
                    let string = |dt: &DataType| {
                        matches!(
                            dt,
                            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                        )
                    };
                    let aligned_scalar = implementation.downcast_ref::<SparkContains>().is_some()
                        && function.args().iter().any(bounded_string_literal);
                    string(&left) && string(&right) && (left == right || aligned_scalar)
                }
                _ => false,
            };
        // Array/array requires equal types. SparkContains alone has an explicit scalar-to-array
        // alignment path; a bounded literal can safely be cast to any supported string layout.
        // Never infer scalar-ness from an arbitrary expression or narrow an oversized literal.

        concrete_predicate && supported_pair
    } else if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        matches!(binary.op(), Operator::And | Operator::Or)
            && binary
                .left()
                .data_type(schema)
                .is_ok_and(|dt| dt == DataType::Boolean)
            && binary
                .right()
                .data_type(schema)
                .is_ok_and(|dt| dt == DataType::Boolean)
    } else if let Some(not) = expr.downcast_ref::<NotExpr>() {
        not.arg()
            .data_type(schema)
            .is_ok_and(|dt| dt == DataType::Boolean)
    } else {
        false
    };
    known_node
        && expr
            .children()
            .into_iter()
            .all(|child| logical_cannot_fail(child, schema))
}

fn bounded_string_literal(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Literal>()
        .is_some_and(|literal| match literal.value() {
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s) => s
                .as_ref()
                .is_none_or(|value| value.len() <= i32::MAX as usize),
            _ => false,
        })
}

/// Spark's expression-level AND/OR evaluation. Unlike DataFusion's selectivity heuristic,
/// every mixed batch skips the RHS exactly where the LHS already determines the answer.
/// NULL does not determine the answer: NULL AND FALSE is FALSE and NULL OR TRUE is TRUE,
/// so RHS is still evaluated on NULL-LHS rows. Top-level whole-stage Spark filters additionally
/// discard NULL conjuncts; that distinct operator-level policy is handled when serializing them.
#[derive(Debug)]
pub struct LogicalShortCircuit {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    is_and: bool,
    /// Both sides may fail or advance state. Evaluate row by row to preserve their ordering:
    /// a right-side error/state update on row0 must precede evaluating the left side on row1.
    rowwise: bool,
    projection: Vec<usize>,
    projected_right: Arc<dyn PhysicalExpr>,
}

impl LogicalShortCircuit {
    pub fn try_new(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        is_and: bool,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if left.data_type(input_schema)? != DataType::Boolean
            || right.data_type(input_schema)? != DataType::Boolean
        {
            return internal_err!("Spark logical operands must be Boolean");
        }
        // Preserve the existing vectorized kernel when eager RHS evaluation cannot fail or
        // advance state. Unknown/JVM-UDF/random expressions are never accepted by this proof.
        if logical_cannot_fail(&right, input_schema) {
            let op = if is_and { Operator::And } else { Operator::Or };
            return Ok(Arc::new(BinaryExpr::new(left, op, right)));
        }
        let rowwise = !logical_cannot_fail(&left, input_schema);
        Self::build(left, right, is_and, rowwise)
    }

    fn build(
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        is_and: bool,
        rowwise: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let mut projection = collect_columns(&right)
            .iter()
            .map(Column::index)
            .collect::<Vec<_>>();
        projection.sort_unstable();
        projection.dedup();
        // Build the projected occurrence once. Rebuilding it per batch would reset any
        // nondeterministic/native/JVM state retained by with_new_children.
        let projected_right = Arc::clone(&right)
            .transform_down(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let index = projection
                    .binary_search(&column.index())
                    .map_err(|_| internal_datafusion_err!("{column} is not projected"))?;
                if index == column.index() {
                    return Ok(Transformed::no(expr));
                }
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
                ))
            })?
            .data;
        Ok(Arc::new(Self {
            left,
            right,
            is_and,
            rowwise,
            projection,
            projected_right,
        }))
    }

    fn determined(&self, value: Option<bool>) -> bool {
        value == Some(!self.is_and)
    }

    fn merge(&self, left: Option<bool>, right: Option<bool>) -> Option<bool> {
        if self.determined(left) || self.determined(right) {
            Some(!self.is_and)
        } else if left.is_some() && right.is_some() {
            Some(self.is_and)
        } else {
            None
        }
    }
}

impl Hash for LogicalShortCircuit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
        self.is_and.hash(state);
        self.rowwise.hash(state);
    }
}
impl PartialEq for LogicalShortCircuit {
    fn eq(&self, other: &Self) -> bool {
        self.left.eq(&other.left)
            && self.right.eq(&other.right)
            && self.is_and == other.is_and
            && self.rowwise == other.rowwise
    }
}
impl Eq for LogicalShortCircuit {}
impl Display for LogicalShortCircuit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Spark{}({}, {})",
            if self.is_and { "And" } else { "Or" },
            self.left,
            self.right
        )
    }
}
impl PhysicalExpr for LogicalShortCircuit {
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn nullable(&self, schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(schema)? || self.right.nullable(schema)?)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(Vec::<
                Option<bool>,
            >::new(
            )))));
        }
        let right_batch = batch.project(&self.projection)?;
        if self.rowwise {
            let mut output = Vec::with_capacity(rows);
            for row in 0..rows {
                let left = self.left.evaluate(&batch.slice(row, 1))?.into_array(1)?;
                if left.len() != 1 {
                    return internal_err!("Logical LHS returned {} rows for one", left.len());
                }
                let left = as_boolean_array(&left)?
                    .iter()
                    .next()
                    .ok_or_else(|| internal_datafusion_err!("Logical LHS returned no row"))?;
                if self.determined(left) {
                    output.push(left);
                    continue;
                }
                let right = self
                    .projected_right
                    .evaluate(&right_batch.slice(row, 1))?
                    .into_array(1)?;
                if right.len() != 1 {
                    return internal_err!("Logical RHS returned {} rows for one", right.len());
                }
                let right = as_boolean_array(&right)?
                    .iter()
                    .next()
                    .ok_or_else(|| internal_datafusion_err!("Logical RHS returned no row"))?;
                output.push(self.merge(left, right));
            }
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(output))));
        }
        let left = self.left.evaluate(batch)?.into_array(rows)?;
        if left.len() != rows {
            return internal_err!("Logical LHS returned {} rows for {rows}", left.len());
        }
        let left = as_boolean_array(&left)?;
        let mask = BooleanArray::from_iter(left.iter().map(|value| Some(!self.determined(value))));
        if mask.values().count_set_bits() == 0 {
            return Ok(ColumnarValue::Array(Arc::new(left.clone())));
        }
        let selected_rows = mask.true_count();
        let selected_batch = if selected_rows == rows {
            right_batch
        } else {
            filter_record_batch(&right_batch, &mask)?
        };
        // The default evaluate_selection scatters before checking the child's length. A short
        // result can panic inside scatter, while a long result can silently discard values.
        // Validate the compact result first, including scalar RHS and zero-column batches.
        let right = self
            .projected_right
            .evaluate(&selected_batch)?
            .into_array(selected_rows)?;
        if right.len() != selected_rows {
            return internal_err!(
                "Logical RHS returned {} rows for {selected_rows}",
                right.len()
            );
        }
        as_boolean_array(&right)?;
        let right = if selected_rows == rows {
            right
        } else {
            scatter(&mask, right.as_ref())?
        };
        let right = as_boolean_array(&right)?;
        let result = if self.is_and {
            and_kleene(left, right)?
        } else {
            or_kleene(left, right)?
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 2 {
            return internal_err!("Spark logical expression needs two children");
        }
        if Arc::ptr_eq(&children[0], &self.left) && Arc::ptr_eq(&children[1], &self.right) {
            return Ok(self);
        }
        // A rewrite can also renumber columns into a new schema. Do not use the old schema
        // to prove a new complex expression infallible. Columns/literals have no effects;
        // every other rewritten LHS takes the conservative rowwise path.
        let rowwise = !(children[0].is::<Column>()
            || children[0].is::<datafusion::physical_expr::expressions::Literal>());
        Self::build(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.is_and,
            rowwise,
        )
    }
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({} {} {})",
            self.left,
            if self.is_and { "AND" } else { "OR" },
            self.right
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cast, EvalMode, SparkCastOptions};
    use arrow::array::{ArrayRef, Int32Array, LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::physical_expr::expressions::{col, lit, CaseExpr};
    use std::sync::Mutex;

    type Events = Arc<Mutex<Vec<(&'static str, i32)>>>;

    #[derive(Debug)]
    struct Probe {
        name: &'static str,
        ids: Arc<dyn PhysicalExpr>,
        values: Vec<Option<bool>>,
        error: Option<i32>,
        events: Events,
        widths: Option<Arc<Mutex<Vec<usize>>>>,
    }
    impl Hash for Probe {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.name.hash(state);
            self.ids.hash(state);
            self.values.hash(state);
            self.error.hash(state);
        }
    }
    impl PartialEq for Probe {
        fn eq(&self, rhs: &Self) -> bool {
            self.name == rhs.name
                && self.ids.eq(&rhs.ids)
                && self.values == rhs.values
                && self.error == rhs.error
        }
    }
    impl Eq for Probe {}
    impl Display for Probe {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Probe({})", self.name)
        }
    }
    impl PhysicalExpr for Probe {
        fn data_type(&self, _: &Schema) -> Result<DataType> {
            Ok(DataType::Boolean)
        }
        fn nullable(&self, _: &Schema) -> Result<bool> {
            Ok(self.values.iter().any(Option::is_none))
        }
        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            if let Some(widths) = &self.widths {
                widths.lock().unwrap().push(batch.num_columns());
            }
            let ids = self.ids.evaluate(batch)?.into_array(batch.num_rows())?;
            let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
            let mut values = vec![];
            for id in ids.values() {
                self.events.lock().unwrap().push((self.name, *id));
                if self.error == Some(*id) {
                    return datafusion::common::exec_err!("{} error row {id}", self.name);
                }
                values.push(self.values[*id as usize]);
            }
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.ids]
        }
        fn with_new_children(
            self: Arc<Self>,
            c: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(Self {
                name: self.name,
                ids: Arc::clone(&c[0]),
                values: self.values.clone(),
                error: self.error,
                events: Arc::clone(&self.events),
                widths: self.widths.clone(),
            }))
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
        fn is_volatile_node(&self) -> bool {
            true
        }
    }
    fn batch(values: Vec<Option<bool>>) -> RecordBatch {
        let n = values.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("unused", DataType::Int32, false),
                Field::new("left", DataType::Boolean, true),
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![99; n])),
                Arc::new(BooleanArray::from(values)),
                Arc::new(Int32Array::from_iter_values(0..n as i32)),
            ],
        )
        .unwrap()
    }
    fn probe(
        name: &'static str,
        b: &RecordBatch,
        values: Vec<Option<bool>>,
        error: Option<i32>,
        events: &Events,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(Probe {
            name,
            ids: col("id", b.schema_ref()).unwrap(),
            values,
            error,
            events: Arc::clone(events),
            widths: None,
        })
    }
    fn evaluate(expr: &Arc<dyn PhysicalExpr>, b: &RecordBatch) -> Result<Vec<Option<bool>>> {
        let a = expr.evaluate(b)?.into_array(b.num_rows())?;
        Ok(as_boolean_array(&a)?.iter().collect())
    }
    fn expected(a: Option<bool>, b: Option<bool>, is_and: bool) -> Option<bool> {
        match (a, b, is_and) {
            (Some(false), _, true) | (_, Some(false), true) => Some(false),
            (Some(true), Some(true), true) => Some(true),
            (Some(true), _, false) | (_, Some(true), false) => Some(true),
            (Some(false), Some(false), false) => Some(false),
            _ => None,
        }
    }

    #[test]
    fn full_three_valued_truth_table_masks_exact_rows_and_sliced_boolean_offsets() {
        let left: Vec<_> = [Some(false), Some(true), None]
            .into_iter()
            .flat_map(|x| vec![x; 3])
            .collect();
        let right = vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
        ];
        let b = batch(left.clone());
        for is_and in [true, false] {
            let events = Arc::new(Mutex::new(vec![]));
            let expr = LogicalShortCircuit::try_new(
                col("left", b.schema_ref()).unwrap(),
                probe("right", &b, right.clone(), None, &events),
                is_and,
                b.schema_ref(),
            )
            .unwrap();
            let wanted: Vec<_> = left
                .iter()
                .zip(&right)
                .map(|(&a, &b)| expected(a, b, is_and))
                .collect();
            assert_eq!(evaluate(&expr, &b).unwrap(), wanted);
            let ids: Vec<_> = left
                .iter()
                .enumerate()
                .filter_map(|(i, &x)| (x != Some(!is_and)).then_some(("right", i as i32)))
                .collect();
            assert_eq!(*events.lock().unwrap(), ids);
            events.lock().unwrap().clear();
            assert_eq!(evaluate(&expr, &b.slice(1, 7)).unwrap(), wanted[1..8]);
            assert!(events
                .lock()
                .unwrap()
                .iter()
                .all(|(_, id)| *id >= 1 && *id < 8));
        }
    }
    #[test]
    fn mixed_mask_above_datafusion_threshold_does_not_evaluate_determined_rhs_rows() {
        for is_and in [true, false] {
            // 75% of rows require the RHS, beyond DF's20% preselection threshold.
            let b = batch(vec![
                Some(is_and),
                Some(is_and),
                Some(!is_and),
                Some(is_and),
            ]);
            let events = Arc::new(Mutex::new(vec![]));
            let rhs = probe("right", &b, vec![Some(true); 4], Some(2), &events);
            let old: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                col("left", b.schema_ref()).unwrap(),
                if is_and { Operator::And } else { Operator::Or },
                Arc::clone(&rhs),
            ));
            assert!(evaluate(&old, &b)
                .unwrap_err()
                .to_string()
                .contains("right error row 2"));
            events.lock().unwrap().clear();
            let expr = LogicalShortCircuit::try_new(
                col("left", b.schema_ref()).unwrap(),
                rhs,
                is_and,
                b.schema_ref(),
            )
            .unwrap();
            assert_eq!(evaluate(&expr, &b).unwrap().len(), 4);
            assert_eq!(
                *events.lock().unwrap(),
                vec![("right", 0), ("right", 1), ("right", 3)]
            );
        }
    }
    #[test]
    fn null_lhs_still_evaluates_rhs_and_preserves_mandatory_error() {
        let b = batch(vec![Some(false), None, Some(true)]);
        for is_and in [true, false] {
            let events = Arc::new(Mutex::new(vec![]));
            let expr = LogicalShortCircuit::try_new(
                col("left", b.schema_ref()).unwrap(),
                probe("right", &b, vec![Some(true); 3], Some(1), &events),
                is_and,
                b.schema_ref(),
            )
            .unwrap();
            assert!(evaluate(&expr, &b)
                .unwrap_err()
                .to_string()
                .contains("right error row 1"));
            assert!(events.lock().unwrap().contains(&("right", 1)));
        }
    }
    #[test]
    fn scalar_lhs_and_rhs_broadcast_without_skipping_null_lhs() {
        let b = batch(vec![None, Some(false), Some(true)]);
        for is_and in [true, false] {
            for lhs in [Some(false), Some(true), None] {
                let events = Arc::new(Mutex::new(vec![]));
                let rhs = vec![Some(false), None, Some(true)];
                let expr = LogicalShortCircuit::try_new(
                    lit(ScalarValue::Boolean(lhs)),
                    probe("right", &b, rhs.clone(), None, &events),
                    is_and,
                    b.schema_ref(),
                )
                .unwrap();
                assert_eq!(
                    evaluate(&expr, &b).unwrap(),
                    rhs.into_iter()
                        .map(|r| expected(lhs, r, is_and))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    events.lock().unwrap().len(),
                    if lhs == Some(!is_and) { 0 } else { 3 }
                );
            }
        }
    }
    #[test]
    fn both_fallible_operands_keep_row_order_and_first_error() {
        let b = batch(vec![Some(true); 3]);
        let events = Arc::new(Mutex::new(vec![]));
        let lhs = probe("left", &b, vec![Some(true); 3], Some(1), &events);
        let rhs = probe("right", &b, vec![Some(true); 3], Some(0), &events);
        let expr = LogicalShortCircuit::try_new(lhs, rhs, true, b.schema_ref()).unwrap();
        assert!(evaluate(&expr, &b)
            .unwrap_err()
            .to_string()
            .contains("right error row 0"));
        assert_eq!(*events.lock().unwrap(), vec![("left", 0), ("right", 0)]);
    }
    #[test]
    fn stateful_operands_remain_interleaved_across_batches_and_selection() {
        let b = batch(vec![Some(true); 3]);
        let events = Arc::new(Mutex::new(vec![]));
        let lhs = probe(
            "left",
            &b,
            vec![Some(true), Some(false), None],
            None,
            &events,
        );
        let rhs = probe(
            "right",
            &b,
            vec![Some(false), Some(false), Some(true)],
            None,
            &events,
        );
        let expr = LogicalShortCircuit::try_new(lhs, rhs, true, b.schema_ref()).unwrap();
        assert_eq!(
            evaluate(&expr, &b).unwrap(),
            vec![Some(false), Some(false), None]
        );
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                ("left", 0),
                ("right", 0),
                ("left", 1),
                ("left", 2),
                ("right", 2)
            ]
        );
        events.lock().unwrap().clear();
        assert_eq!(
            evaluate(&expr, &b.slice(1, 2)).unwrap(),
            vec![Some(false), None]
        );
        assert_eq!(
            *events.lock().unwrap(),
            vec![("left", 1), ("left", 2), ("right", 2)]
        );
        let selected = BooleanArray::from(vec![false, true, true]);
        let values = expr
            .evaluate_selection(&b, &selected)
            .unwrap()
            .into_array(3)
            .unwrap();
        assert_eq!(
            as_boolean_array(&values)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(false), None]
        );
    }
    #[test]
    fn empty_or_fully_determined_batch_evaluates_no_rhs() {
        let b = batch(vec![Some(false); 3]);
        let events = Arc::new(Mutex::new(vec![]));
        let expr = LogicalShortCircuit::try_new(
            col("left", b.schema_ref()).unwrap(),
            probe("right", &b, vec![Some(true); 3], Some(0), &events),
            true,
            b.schema_ref(),
        )
        .unwrap();
        assert_eq!(evaluate(&expr, &b).unwrap(), vec![Some(false); 3]);
        assert!(evaluate(&expr, &b.slice(0, 0)).unwrap().is_empty());
        assert!(events.lock().unwrap().is_empty());
    }
    #[test]
    fn rewritten_column_indices_under_case_keep_values_and_state() {
        let b = batch(vec![Some(false), None, Some(true)]);
        let events = Arc::new(Mutex::new(vec![]));
        let expr = LogicalShortCircuit::try_new(
            col("left", b.schema_ref()).unwrap(),
            probe(
                "right",
                &b,
                vec![Some(true), Some(false), Some(true)],
                None,
                &events,
            ),
            true,
            b.schema_ref(),
        )
        .unwrap();
        let case: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                None,
                vec![(col("left", b.schema_ref()).unwrap(), expr)],
                Some(lit(false)),
            )
            .unwrap(),
        );
        assert_eq!(
            evaluate(&case, &b).unwrap(),
            vec![Some(false), Some(false), Some(true)]
        );
        assert_eq!(*events.lock().unwrap(), vec![("right", 2)]);
    }
    #[test]
    fn positively_infallible_rhs_retains_upstream_binary_kernel() {
        let b = batch(vec![Some(true), None]);
        for rhs in [lit(true), col("left", b.schema_ref()).unwrap()] {
            let expr = LogicalShortCircuit::try_new(
                col("left", b.schema_ref()).unwrap(),
                rhs,
                true,
                b.schema_ref(),
            )
            .unwrap();
            assert!(expr.is::<BinaryExpr>());
            assert_eq!(evaluate(&expr, &b).unwrap(), vec![Some(true), None]);
        }
    }
    #[derive(Debug)]
    struct Opaque {
        id: &'static str,
        value: ColumnarValue,
        calls: Arc<Mutex<Vec<(usize, usize)>>>,
    }
    impl Hash for Opaque {
        fn hash<H: Hasher>(&self, s: &mut H) {
            self.id.hash(s);
        }
    }
    impl PartialEq for Opaque {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }
    impl Eq for Opaque {}
    impl Display for Opaque {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Opaque({})", self.id)
        }
    }
    impl PhysicalExpr for Opaque {
        fn data_type(&self, _: &Schema) -> Result<DataType> {
            Ok(self.value.data_type())
        }
        fn nullable(&self, _: &Schema) -> Result<bool> {
            Ok(true)
        }
        fn evaluate(&self, b: &RecordBatch) -> Result<ColumnarValue> {
            self.calls
                .lock()
                .unwrap()
                .push((b.num_rows(), b.num_columns()));
            Ok(self.value.clone())
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            c: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            assert!(c.is_empty());
            Ok(self)
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }
    #[test]
    fn unknown_scalar_rhs_is_broadcast_into_the_selected_zero_column_batch() {
        for is_and in [true, false] {
            let left = vec![Some(!is_and), Some(is_and), None, Some(!is_and)];
            let b = batch(left.clone());
            for scalar in [Some(false), Some(true), None] {
                let calls = Arc::new(Mutex::new(vec![]));
                let right: Arc<dyn PhysicalExpr> = Arc::new(Opaque {
                    id: "rhs",
                    value: ColumnarValue::Scalar(ScalarValue::Boolean(scalar)),
                    calls: Arc::clone(&calls),
                });
                let expr = LogicalShortCircuit::try_new(
                    col("left", b.schema_ref()).unwrap(),
                    right,
                    is_and,
                    b.schema_ref(),
                )
                .unwrap();
                assert!(expr.is::<LogicalShortCircuit>());
                assert_eq!(
                    evaluate(&expr, &b).unwrap(),
                    left.iter()
                        .map(|&l| expected(l, scalar, is_and))
                        .collect::<Vec<_>>()
                );
                assert_eq!(*calls.lock().unwrap(), vec![(2, 0)]);
            }
        }
    }
    #[test]
    fn explicit_zero_column_row_count_survives_both_opaque_scalar_operands() {
        let b = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(5)),
        )
        .unwrap();
        let lhs_calls = Arc::new(Mutex::new(vec![]));
        let rhs_calls = Arc::new(Mutex::new(vec![]));
        let left: Arc<dyn PhysicalExpr> = Arc::new(Opaque {
            id: "lhs",
            value: ColumnarValue::Scalar(ScalarValue::Boolean(None)),
            calls: Arc::clone(&lhs_calls),
        });
        let right: Arc<dyn PhysicalExpr> = Arc::new(Opaque {
            id: "rhs",
            value: ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            calls: Arc::clone(&rhs_calls),
        });
        let expr = LogicalShortCircuit::try_new(left, right, true, b.schema_ref()).unwrap();
        assert_eq!(evaluate(&expr, &b).unwrap(), vec![Some(false); 5]);
        assert_eq!(*lhs_calls.lock().unwrap(), vec![(1, 0); 5]);
        assert_eq!(*rhs_calls.lock().unwrap(), vec![(1, 0); 5]);
    }
    #[test]
    fn rejects_malformed_child_lengths_before_exposing_any_successful_answer() {
        let b = batch(vec![Some(true); 3]);
        let opaque = |name, value| -> Arc<dyn PhysicalExpr> {
            Arc::new(Opaque {
                id: name,
                value,
                calls: Arc::new(Mutex::new(vec![])),
            })
        };
        let malformed = || ColumnarValue::Array(Arc::new(BooleanArray::from(vec![Some(true); 2])));
        let expr = LogicalShortCircuit::try_new(
            opaque("lhs", malformed()),
            opaque(
                "rhs",
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ),
            true,
            b.schema_ref(),
        )
        .unwrap();
        assert!(evaluate(&expr, &b)
            .unwrap_err()
            .to_string()
            .contains("Logical LHS returned 2 rows for one"));
        let expr = LogicalShortCircuit::try_new(
            col("left", b.schema_ref()).unwrap(),
            opaque("rhs", malformed()),
            true,
            b.schema_ref(),
        )
        .unwrap();
        assert!(evaluate(&expr, &b)
            .unwrap_err()
            .to_string()
            .contains("Logical RHS returned 2 rows for 3"));
    }
    #[test]
    fn masked_rhs_filters_only_its_referenced_column_not_unused_payload_columns() {
        let base = batch(vec![Some(true), Some(false), None, Some(true)]);
        let mut fields = base.schema().fields().to_vec();
        let mut columns = base.columns().to_vec();
        for index in 0..64 {
            fields.push(Arc::new(Field::new(
                format!("payload{index}"),
                DataType::Int32,
                false,
            )));
            columns.push(Arc::new(Int32Array::from(vec![index; 4])));
        }
        let b = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let widths = Arc::new(Mutex::new(vec![]));
        let rhs: Arc<dyn PhysicalExpr> = Arc::new(Probe {
            name: "rhs",
            ids: col("id", b.schema_ref()).unwrap(),
            values: vec![Some(true); 4],
            error: None,
            events: Arc::new(Mutex::new(vec![])),
            widths: Some(Arc::clone(&widths)),
        });
        let expr = LogicalShortCircuit::try_new(
            col("left", b.schema_ref()).unwrap(),
            rhs,
            true,
            b.schema_ref(),
        )
        .unwrap();
        assert_eq!(b.num_columns(), 67);
        assert_eq!(
            evaluate(&expr, &b).unwrap(),
            vec![Some(true), Some(false), None, Some(true)]
        );
        assert_eq!(*widths.lock().unwrap(), vec![1]);
    }

    #[test]
    fn mixed_mask_validates_compact_rhs_before_scattering_a_zero_column_batch() {
        for is_and in [true, false] {
            let b = batch(vec![Some(!is_and), Some(is_and), None, Some(!is_and)]);
            for malformed_rows in [1, 3] {
                let calls = Arc::new(Mutex::new(vec![]));
                // No RHS columns are referenced: projection/filtering must retain the two
                // selected rows, and a malformed array must fail before scatter can consume it.
                let right: Arc<dyn PhysicalExpr> = Arc::new(Opaque {
                    id: "malformed mixed rhs",
                    value: ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                        Some(true);
                        malformed_rows
                    ]))),
                    calls: Arc::clone(&calls),
                });
                let expr = LogicalShortCircuit::try_new(
                    col("left", b.schema_ref()).unwrap(),
                    right,
                    is_and,
                    b.schema_ref(),
                )
                .unwrap();
                let error = evaluate(&expr, &b).unwrap_err().to_string();
                assert!(
                    error.contains(&format!("Logical RHS returned {malformed_rows} rows for 2")),
                    "{error}"
                );
                assert_eq!(*calls.lock().unwrap(), vec![(2, 0)]);
            }
        }
    }

    fn string_function(
        udf: ScalarUDF,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(udf),
                args,
                schema,
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        )
    }

    #[test]
    fn concrete_string_predicates_and_boolean_wrappers_retain_upstream_kernel() {
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![
                Some("12"),
                Some("21"),
                None,
                Some(""),
                Some("2"),
                Some("1"),
            ]))],
        )
        .unwrap();
        let text = || col("text", b.schema_ref()).unwrap();
        let left = string_function(
            ScalarUDF::new_from_impl(StartsWithFunc::new()),
            vec![text(), lit("1")],
            b.schema_ref(),
        );
        let ends = string_function(
            ScalarUDF::new_from_impl(EndsWithFunc::new()),
            vec![text(), lit("2")],
            b.schema_ref(),
        );
        let contains = string_function(
            ScalarUDF::new_from_impl(SparkContains::new()),
            vec![text(), lit("1")],
            b.schema_ref(),
        );
        let left_values = vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(true),
        ];
        assert_eq!(evaluate(&left, &b).unwrap(), left_values);
        let right_cases = vec![
            (
                Arc::clone(&ends),
                vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(false),
                    Some(true),
                    Some(false),
                ],
            ),
            (
                Arc::clone(&contains),
                vec![
                    Some(true),
                    Some(true),
                    None,
                    Some(false),
                    Some(false),
                    Some(true),
                ],
            ),
            (
                Arc::new(NotExpr::new(Arc::clone(&ends))),
                vec![
                    Some(false),
                    Some(true),
                    None,
                    Some(true),
                    Some(false),
                    Some(true),
                ],
            ),
            (
                Arc::new(BinaryExpr::new(
                    Arc::clone(&ends),
                    Operator::Or,
                    Arc::clone(&contains),
                )),
                vec![
                    Some(true),
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                    Some(true),
                ],
            ),
            (
                Arc::new(BinaryExpr::new(
                    Arc::clone(&ends),
                    Operator::And,
                    Arc::clone(&contains),
                )),
                vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(false),
                    Some(false),
                    Some(false),
                ],
            ),
        ];
        for (right, right_values) in right_cases {
            assert_eq!(evaluate(&right, &b).unwrap(), right_values);
            for is_and in [false, true] {
                let result = LogicalShortCircuit::try_new(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    is_and,
                    b.schema_ref(),
                )
                .unwrap();
                assert!(
                    result.is::<BinaryExpr>(),
                    "ordinary strings must stay vectorized: {result}"
                );
                let expected_values: Vec<_> = left_values
                    .iter()
                    .zip(&right_values)
                    .map(|(a, b)| expected(*a, *b, is_and))
                    .collect();
                assert_eq!(evaluate(&result, &b).unwrap(), expected_values);
            }
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FailingNamedFunction {
        name: &'static str,
        output: DataType,
        signature: Signature,
    }
    impl FailingNamedFunction {
        fn new(name: &'static str, output: DataType) -> Self {
            Self {
                name,
                output,
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }
    impl ScalarUDFImpl for FailingNamedFunction {
        fn name(&self) -> &str {
            self.name
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(self.output.clone())
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            datafusion::common::exec_err!("Unknown function {} must not be evaluated", self.name)
        }
    }

    #[test]
    fn same_name_fake_udf_and_unknown_string_child_never_gain_the_pure_fastpath() {
        // A zero-column batch still carries one explicit row.
        let b = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let fake = string_function(
            ScalarUDF::new_from_impl(FailingNamedFunction::new("starts_with", DataType::Boolean)),
            vec![lit("text"), lit("t")],
            b.schema_ref(),
        );
        let unknown_child = string_function(
            ScalarUDF::new_from_impl(FailingNamedFunction::new("concat", DataType::Utf8)),
            vec![lit("text")],
            b.schema_ref(),
        );
        let known_with_unknown_child = string_function(
            ScalarUDF::new_from_impl(StartsWithFunc::new()),
            vec![unknown_child, lit("t")],
            b.schema_ref(),
        );
        for rhs in [fake, known_with_unknown_child] {
            let skipped =
                LogicalShortCircuit::try_new(lit(false), Arc::clone(&rhs), true, b.schema_ref())
                    .unwrap();
            assert!(skipped.is::<LogicalShortCircuit>());
            assert_eq!(evaluate(&skipped, &b).unwrap(), vec![Some(false)]);
            let required =
                LogicalShortCircuit::try_new(lit(true), rhs, true, b.schema_ref()).unwrap();
            assert!(evaluate(&required, &b).is_err());
        }
    }

    #[test]
    fn known_string_predicate_keeps_ansi_parsing_child_lazy() {
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["bad"]))],
        )
        .unwrap();
        let integer: Arc<dyn PhysicalExpr> = Arc::new(Cast::new(
            col("text", b.schema_ref()).unwrap(),
            DataType::Int64,
            SparkCastOptions::new_without_timezone(EvalMode::Ansi),
            None,
            None,
        ));
        let back_to_text: Arc<dyn PhysicalExpr> = Arc::new(Cast::new(
            integer,
            DataType::Utf8,
            SparkCastOptions::new_without_timezone(EvalMode::Ansi),
            None,
            None,
        ));
        let rhs = string_function(
            ScalarUDF::new_from_impl(StartsWithFunc::new()),
            vec![back_to_text, lit("1")],
            b.schema_ref(),
        );
        let skipped =
            LogicalShortCircuit::try_new(lit(false), Arc::clone(&rhs), true, b.schema_ref())
                .unwrap();
        assert!(skipped.is::<LogicalShortCircuit>());
        assert_eq!(evaluate(&skipped, &b).unwrap(), vec![Some(false)]);
        let required = LogicalShortCircuit::try_new(lit(true), rhs, true, b.schema_ref()).unwrap();
        assert!(evaluate(&required, &b).is_err());
    }

    #[test]
    fn mixed_string_array_types_do_not_gain_a_false_infallibility_proof() {
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("haystack", DataType::LargeUtf8, false),
                Field::new("needle", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(LargeStringArray::from(vec!["prefix"])),
                Arc::new(StringArray::from(vec!["pre"])),
            ],
        )
        .unwrap();
        for fun in [
            ScalarUDF::new_from_impl(SparkContains::new()),
            ScalarUDF::new_from_impl(StartsWithFunc::new()),
            ScalarUDF::new_from_impl(EndsWithFunc::new()),
        ] {
            let rhs = string_function(
                fun,
                vec![
                    col("haystack", b.schema_ref()).unwrap(),
                    col("needle", b.schema_ref()).unwrap(),
                ],
                b.schema_ref(),
            );
            assert!(!logical_cannot_fail(&rhs, b.schema_ref()));
            let expr = LogicalShortCircuit::try_new(lit(false), rhs, true, b.schema_ref()).unwrap();
            assert!(expr.is::<LogicalShortCircuit>());
            assert_eq!(evaluate(&expr, &b).unwrap(), vec![Some(false)]);
        }
        // This concrete implementation has no array/array type-alignment path, unlike its
        // scalar argument path. Confirm the error that the safety proof must not overlook.
        let rhs = string_function(
            ScalarUDF::new_from_impl(SparkContains::new()),
            vec![
                col("haystack", b.schema_ref()).unwrap(),
                col("needle", b.schema_ref()).unwrap(),
            ],
            b.schema_ref(),
        );
        assert!(evaluate(&rhs, &b).is_err());
        let required = LogicalShortCircuit::try_new(lit(true), rhs, true, b.schema_ref()).unwrap();
        assert!(evaluate(&required, &b).is_err());
    }

    #[test]
    fn contains_aligns_only_proven_scalar_literals_across_string_layouts() {
        let values = vec![Some("prefix"), None, Some("other"), Some("p")];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values)),
        ];
        for array in arrays {
            let b = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "text",
                    array.data_type().clone(),
                    true,
                )])),
                vec![array],
            )
            .unwrap();
            for scalar_first in [false, true] {
                let column = col("text", b.schema_ref()).unwrap();
                let args = if scalar_first {
                    vec![lit("prefix"), column]
                } else {
                    vec![column, lit("pre")]
                };
                let rhs = string_function(
                    ScalarUDF::new_from_impl(SparkContains::new()),
                    args,
                    b.schema_ref(),
                );
                let expected = if scalar_first {
                    vec![Some(true), None, Some(false), Some(true)]
                } else {
                    vec![Some(true), None, Some(false), Some(false)]
                };
                for is_and in [false, true] {
                    let expr = LogicalShortCircuit::try_new(
                        lit(is_and),
                        Arc::clone(&rhs),
                        is_and,
                        b.schema_ref(),
                    )
                    .unwrap();
                    assert!(
                        expr.is::<BinaryExpr>(),
                        "known scalar alignment must remain vectorized"
                    );
                    assert_eq!(evaluate(&expr, &b).unwrap(), expected);
                }
            }
        }
    }
}
