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

//! Builders for the native window operators (`Window` and `WindowGroupLimit`), and the
//! conversion of Spark window expressions into DataFusion window expressions.

use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalDayTime, SchemaRef};
use datafusion::{
    arrow::compute::SortOptions,
    common::ScalarValue,
    execution::FunctionRegistry,
    logical_expr::{
        AggregateUDF, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    },
    physical_expr::{
        expressions::{CastExpr, Column, Literal},
        window::WindowExpr,
        LexOrdering, PhysicalExpr, PhysicalSortExpr,
    },
    physical_plan::{
        limit::LocalLimitExec,
        projection::ProjectionExec,
        windows::{BoundedWindowAggExec, WindowAggExec},
        ExecutionPlan, InputOrderMode,
    },
};
use datafusion_comet_operators::{
    sorted_window_supports_output_type, PartitionAggregateWindowEnabled,
    PartitionAggregateWindowExec, PartitionedRankLimitExec, SortedWindowEnabled, SortedWindowExec,
    SortedWindowFunction, WindowFnKind,
};
use datafusion_comet_proto::{
    spark_expression::{
        self, agg_expr::ExprStruct as AggExprStruct, expr::ExprStruct, literal::Value, AggExpr,
    },
    spark_operator::{
        self, lower_window_frame_bound::LowerFrameBoundStruct,
        upper_window_frame_bound::UpperFrameBoundStruct, Operator, RankLikeFunction,
        WindowFrameType,
    },
};
use datafusion_comet_spark_expr::{Avg, AvgDecimal, SumDecimal, SumInteger};
use jni::objects::{Global, JObject};
use num::{BigInt, ToPrimitive};

use super::{
    aggregate::min_max_udaf, from_protobuf_eval_mode, operator_registry::OperatorBuilder,
    PhysicalPlanner, PlanCreationResult,
};
use crate::{
    execution::{operators::ExecutionError, serde::to_arrow_datatype, spark_plan::SparkPlan},
    extract_op,
};

use ExecutionError::GeneralError;

/// Builder for `Window`.
pub struct WindowBuilder;

impl OperatorBuilder for WindowBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let wnd = extract_op!(spark_plan, Window);
        let children = &spark_plan.children;

        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;
        let input_schema = child.schema();
        let sort_exprs: Result<Vec<PhysicalSortExpr>, ExecutionError> = wnd
            .order_by_list
            .iter()
            .map(|expr| planner.create_sort_expr(expr, Arc::clone(&input_schema)))
            .collect();

        let partition_exprs: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = wnd
            .partition_by_list
            .iter()
            .map(|expr| planner.create_normalized_key_expr(expr, Arc::clone(&input_schema)))
            .collect();

        let sort_exprs = &sort_exprs?;
        let partition_exprs = &partition_exprs?;

        let window_expr: Result<Vec<Arc<dyn WindowExpr>>, ExecutionError> = wnd
            .window_expr
            .iter()
            .map(|expr| {
                planner.create_window_expr(
                    expr,
                    Arc::clone(&input_schema),
                    partition_exprs,
                    sort_exprs,
                )
            })
            .collect();

        // Route to `BoundedWindowAggExec` when every window expression can
        // run with bounded memory. This uses DataFusion's
        // `evaluate_stateful` / row-by-row `evaluate` path, which is the
        // correct implementation for `LEAD` / `LAG` with `IGNORE NULLS`
        // (`WindowAggExec` calls `evaluate_all`, whose
        // `evaluate_all_with_ignore_null` has a sign-wrap bug for `LEAD`
        // that produces all-NULL output).
        //
        // Fall back to `WindowAggExec` otherwise. That covers
        // `PERCENT_RANK` / `CUME_DIST` / `NTILE`
        // (`!uses_bounded_memory()` — "Can not execute X in a streaming
        // fashion") and keeps the Spark-compatible Comet UDAFs
        // (`SumDecimal` / `SumInteger` / `AvgDecimal` / `Avg`) on the
        // non-streaming path since they don't implement `retract_batch`.
        // Because `process_agg_func` already picks DataFusion's
        // retract-capable built-ins for sliding aggregate frames,
        // ever-expanding aggregate frames (all that route to
        // `BoundedWindowAggExec` as `PlainAggregateWindowExpr`) never
        // trigger a retract call.
        let window_expr = window_expr?;
        let all_bounded = window_expr.iter().all(|e| e.uses_bounded_memory());
        // These flags are resolved by the JVM before crossing JNI. The spilling partition
        // path is opt-in; simple sorted windows are enabled by default in CometConf.
        let config = planner.session_ctx.copied_config();
        let sorted_window = if config.get_extension::<SortedWindowEnabled>().is_some() {
            planner.plan_sorted_window(
                wnd,
                &window_expr,
                &child.native_plan,
                partition_exprs,
                sort_exprs,
                &input_schema,
            )?
        } else {
            None
        };
        // Only non-streaming nodes need the spilling partition implementation. Bounded
        // expressions in a mixed node retain the bounded evaluator below that operator.
        let partition_aggregate = if !all_bounded
            && config
                .get_extension::<PartitionAggregateWindowEnabled>()
                .is_some()
        {
            PartitionAggregateWindowExec::try_plan(
                window_expr.clone(),
                Arc::clone(&child.native_plan),
                !partition_exprs.is_empty(),
                wnd.window_expr.iter().map(|e| e.ignore_nulls).collect(),
            )?
        } else {
            None
        };
        let window_agg: Arc<dyn ExecutionPlan> = if let Some(plan) = sorted_window {
            plan
        } else if all_bounded {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr,
                Arc::clone(&child.native_plan),
                InputOrderMode::Sorted,
                !partition_exprs.is_empty(),
            )?)
        } else if let Some(plan) = partition_aggregate {
            plan
        } else {
            Arc::new(WindowAggExec::try_new(
                window_expr,
                Arc::clone(&child.native_plan),
                !partition_exprs.is_empty(),
            )?)
        };

        // DataFusion's window functions don't always return the same Arrow
        // type that Spark expects (e.g. `row_number` returns UInt64 while
        // Spark expects Int32). If any window expression carries a
        // `result_type` that differs from the actual output type, wrap the
        // aggregate in a projection that casts the mismatched columns.
        let plan = {
            let agg_schema = window_agg.schema();
            let input_field_count = input_schema.fields().len();
            let mut needs_cast = false;
            let mut proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
                Vec::with_capacity(agg_schema.fields().len());
            for (idx, field) in agg_schema.fields().iter().enumerate() {
                let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
                let expr: Arc<dyn PhysicalExpr> = if idx >= input_field_count {
                    let w = &wnd.window_expr[idx - input_field_count];
                    match &w.result_type {
                        Some(t) => {
                            let expected = to_arrow_datatype(t);
                            if &expected != field.data_type() {
                                needs_cast = true;
                                Arc::new(CastExpr::new(col, expected, None))
                            } else {
                                col
                            }
                        }
                        None => col,
                    }
                } else {
                    col
                };
                proj_exprs.push((expr, field.name().to_string()));
            }
            if needs_cast {
                let projection: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                    proj_exprs,
                    Arc::clone(&window_agg),
                )?);
                // The window is an additional plan so that its time is reported with the
                // projection's rather than left out.
                SparkPlan::new_with_additional(
                    spark_plan.plan_id,
                    projection,
                    vec![child],
                    vec![window_agg],
                )
            } else {
                SparkPlan::new(spark_plan.plan_id, window_agg, vec![child])
            }
        };

        Ok((scans, shuffle_scans, Arc::new(plan)))
    }
}

/// Builder for `WindowGroupLimit`.
pub struct WindowGroupLimitBuilder;

impl OperatorBuilder for WindowGroupLimitBuilder {
    fn build(
        &self,
        spark_plan: &Operator,
        inputs: &mut Vec<Arc<Global<JObject<'static>>>>,
        partition_count: usize,
        planner: &PhysicalPlanner,
    ) -> PlanCreationResult {
        let wgl = extract_op!(spark_plan, WindowGroupLimit);
        let children = &spark_plan.children;

        // Route:
        //   * ROW_NUMBER + empty PARTITION BY -> `LocalLimitExec`. Spark's WGL requires
        //     the child to be sorted by ORDER BY, so `first K rows per input partition`
        //     IS the global RANK top-K (Partial phase); the Final phase runs on the
        //     single-partition post-shuffle stream and stays correct.
        //   * Everything else (ROW_NUMBER partitioned, RANK / DENSE_RANK with any
        //     PARTITION BY) -> Comet's streaming `PartitionedRankLimitExec`. Relies on
        //     the Spark-injected sort of `[partition_keys, order_keys]`, which lets it
        //     preserve input order for rows tied on the ORDER BY keys (matching Spark's
        //     `SimpleLimitIterator` for ROW_NUMBER and `RankLimitIterator` for RANK /
        //     DENSE_RANK). `partition_prefix_len == 0` degenerates to one big partition
        //     per DF input partition.
        assert_eq!(children.len(), 1);
        let (scans, shuffle_scans, child) =
            planner.create_plan(&children[0], inputs, partition_count)?;
        let input_schema = child.schema();

        let kind = match RankLikeFunction::try_from(wgl.rank_like_function).map_err(|_| {
            GeneralError(format!(
                "Unsupported WindowGroupLimit rank function tag: {}",
                wgl.rank_like_function
            ))
        })? {
            RankLikeFunction::RowNumber => WindowFnKind::RowNumber,
            RankLikeFunction::Rank => WindowFnKind::Rank,
            RankLikeFunction::DenseRank => WindowFnKind::DenseRank,
        };

        let partition_prefix_len = wgl.partition_by_list.len();
        let fetch = wgl.limit as usize;

        // Fast path: ROW_NUMBER without PARTITION BY collapses to `first K rows per
        // input DF partition`, which is exactly what `LocalLimitExec` does. No
        // row-encoding overhead, no ORDER BY tie detection needed.
        let topk: Arc<dyn ExecutionPlan> = if partition_prefix_len == 0
            && kind == WindowFnKind::RowNumber
        {
            Arc::new(LocalLimitExec::new(Arc::clone(&child.native_plan), fetch))
        } else {
            // Partition keys arrive as bare exprs (no direction). Spark's WGL
            // requires the child to be sorted by partition-keys ASCENDING then by
            // order-by keys, so materialize partition keys with SortOptions matching
            // Spark's `SortOrder(_, Ascending)` (ascending, nulls_first).
            let mut partition_keys: Vec<PhysicalSortExpr> =
                Vec::with_capacity(partition_prefix_len);
            for expr in &wgl.partition_by_list {
                let phys = planner.create_normalized_key_expr(expr, Arc::clone(&input_schema))?;
                partition_keys.push(PhysicalSortExpr {
                    expr: phys,
                    options: SortOptions::default(),
                });
            }
            let mut order_keys: Vec<PhysicalSortExpr> = Vec::with_capacity(wgl.order_by_list.len());
            for expr in &wgl.order_by_list {
                order_keys.push(planner.create_sort_expr(expr, Arc::clone(&input_schema))?);
            }

            // Always route to the streaming `PartitionedRankLimitExec`. Spark's
            // `WindowGroupLimitExec.requiredChildOrdering` guarantees the child is
            // sorted by `[partition_keys..., order_keys...]`, so a single pass
            // suffices. A heap-based top-K would be faster asymptotically but would
            // reorder rows tied on the ORDER BY keys, which breaks Spark's
            // `SimpleLimitIterator` / `RankLimitIterator` semantics: for
            // `ROW_NUMBER()`, Spark assigns ranks in input order to tied rows, and
            // any deviation surfaces as wrong-row diffs in SPARK-37099-shaped tests.
            Arc::new(PartitionedRankLimitExec::try_new(
                Arc::clone(&child.native_plan),
                partition_keys,
                order_keys,
                fetch,
                kind,
            )?)
        };

        Ok((
            scans,
            shuffle_scans,
            Arc::new(SparkPlan::new(
                spark_plan.plan_id,
                topk,
                vec![Arc::clone(&child)],
            )),
        ))
    }
}

impl PhysicalPlanner {
    /// Create a DataFusion windows physical expression from Spark physical expression
    fn create_window_expr<'a>(
        &'a self,
        spark_expr: &'a spark_operator::WindowExpr,
        input_schema: SchemaRef,
        partition_by: &[Arc<dyn PhysicalExpr>],
        sort_exprs: &[PhysicalSortExpr],
    ) -> Result<Arc<dyn WindowExpr>, ExecutionError> {
        let window_func: WindowFunctionDefinition;
        let window_func_name: String;
        let window_args: Vec<Arc<dyn PhysicalExpr>>;
        if let Some(func) = &spark_expr.built_in_window_function {
            match &func.expr_struct {
                Some(ExprStruct::ScalarFunc(f)) => {
                    window_func_name = f.func.clone();
                    window_args = f
                        .args
                        .iter()
                        .map(|expr| self.create_expr(expr, Arc::clone(&input_schema)))
                        .collect::<Result<Vec<_>, ExecutionError>>()?;
                    window_func =
                        self.find_df_window_function(&window_func_name)
                            .ok_or_else(|| {
                                GeneralError(format!(
                                    "{window_func_name} not supported for window function"
                                ))
                            })?;
                }
                other => {
                    return Err(GeneralError(format!(
                        "{other:?} not supported for window function"
                    )))
                }
            };
        } else if let Some(agg_func) = &spark_expr.agg_func {
            // Scala normally falls back before serialization. Reject a filter at the native
            // boundary too: create_window_expr below does not pass one to DataFusion, and its
            // public WindowExpr API cannot expose it later to custom window operators.
            if agg_func.filter.is_some() {
                return Err(GeneralError(
                    "FILTER (WHERE ...) is not supported for window aggregates".to_string(),
                ));
            }
            // Is the frame ever-expanding (start = UnboundedPreceding)? When it is,
            // DataFusion uses `PlainAggregateWindowExpr` which does not call
            // `retract_batch`, so we can safely use Comet's Spark-compatible
            // UDAFs (SumDecimal/SumInteger/AvgDecimal/Avg). Otherwise it uses
            // `SlidingAggregateWindowExpr` which requires retract — Comet's UDAFs
            // don't implement it, so the caller must fall back to DataFusion's
            // built-ins (which do).
            let is_ever_expanding = spark_expr
                .spec
                .as_ref()
                .and_then(|s| s.frame_specification.as_ref())
                .and_then(|f| f.lower_bound.as_ref())
                .and_then(|lb| lb.lower_frame_bound_struct.as_ref())
                .map(|inner| matches!(inner, LowerFrameBoundStruct::UnboundedPreceding(_)))
                .unwrap_or(true);
            let (func, args) =
                self.process_agg_func(agg_func, Arc::clone(&input_schema), is_ever_expanding)?;
            window_func_name = func.name().to_string();
            window_args = args;
            window_func = func;
        } else {
            return Err(GeneralError(
                "Both func and agg_func are not set".to_string(),
            ));
        }

        let spark_window_frame = match spark_expr
            .spec
            .as_ref()
            .and_then(|inner| inner.frame_specification.as_ref())
        {
            Some(frame) => frame,
            _ => {
                return Err(ExecutionError::DeserializeError(
                    "Cannot deserialize window frame".to_string(),
                ))
            }
        };

        let units = match spark_window_frame.frame_type() {
            WindowFrameType::Rows => WindowFrameUnits::Rows,
            WindowFrameType::Range => WindowFrameUnits::Range,
        };

        // For RANGE frames the offset scalar must be arithmetic-compatible with
        // the ORDER BY column, not just numeric. arrow-arith rejects e.g.
        // `Date32 + Int32` — it requires an Interval RHS. Capture the ORDER BY
        // datatype once so both bound branches can coerce accordingly.
        let range_order_by_type = if matches!(units, WindowFrameUnits::Range) {
            sort_exprs
                .first()
                .map(|s| s.expr.data_type(&input_schema))
                .transpose()
                .map_err(|e| GeneralError(format!("Failed to resolve ORDER BY type: {e}")))?
        } else {
            None
        };

        let lower_bound: WindowFrameBound = match spark_window_frame
            .lower_bound
            .as_ref()
            .and_then(|inner| inner.lower_frame_bound_struct.as_ref())
        {
            Some(l) => match l {
                LowerFrameBoundStruct::UnboundedPreceding(_) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Preceding(ScalarValue::Int64(None))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                LowerFrameBoundStruct::Preceding(offset) => {
                    // Spark encodes ROWS bound direction via the sign of the offset:
                    // negative => PRECEDING, positive => FOLLOWING. The proto
                    // `LowerWindowFrameBound` only carries a `Preceding` variant, so
                    // Comet overloads it for both cases. Route to the matching
                    // DataFusion `WindowFrameBound` based on the sign.
                    match units {
                        WindowFrameUnits::Rows => {
                            let abs = offset.offset.unsigned_abs();
                            if offset.offset < 0 {
                                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(abs)))
                            } else {
                                WindowFrameBound::Following(ScalarValue::UInt64(Some(abs)))
                            }
                        }
                        WindowFrameUnits::Range => {
                            let raw = match offset.range_offset.as_ref() {
                                Some(lit) => numeric_literal_to_scalar(lit)?,
                                None => ScalarValue::Int64(Some(offset.offset.abs())),
                            };
                            let scalar = coerce_range_offset_for_order_by(
                                raw,
                                range_order_by_type.as_ref(),
                            )?;
                            WindowFrameBound::Preceding(scalar)
                        }
                        WindowFrameUnits::Groups => {
                            return Err(GeneralError(
                                "WindowFrameUnits::Groups is not supported.".to_string(),
                            ));
                        }
                    }
                }
                LowerFrameBoundStruct::CurrentRow(_) => WindowFrameBound::CurrentRow,
            },
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Preceding(ScalarValue::Int64(None)),
                WindowFrameUnits::Groups => {
                    return Err(GeneralError(
                        "WindowFrameUnits::Groups is not supported.".to_string(),
                    ));
                }
            },
        };

        let upper_bound: WindowFrameBound = match spark_window_frame
            .upper_bound
            .as_ref()
            .and_then(|inner| inner.upper_frame_bound_struct.as_ref())
        {
            Some(u) => match u {
                UpperFrameBoundStruct::UnboundedFollowing(_) => match units {
                    WindowFrameUnits::Rows => {
                        WindowFrameBound::Following(ScalarValue::UInt64(None))
                    }
                    WindowFrameUnits::Range => {
                        WindowFrameBound::Following(ScalarValue::Int64(None))
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                UpperFrameBoundStruct::Following(offset) => match units {
                    WindowFrameUnits::Rows => {
                        // Mirror the lower-bound sign handling: the upper proto
                        // variant is `Following`, but Spark encodes the bound
                        // direction via sign. Negative => PRECEDING, positive =>
                        // FOLLOWING.
                        let abs = offset.offset.unsigned_abs();
                        if offset.offset < 0 {
                            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(abs)))
                        } else {
                            WindowFrameBound::Following(ScalarValue::UInt64(Some(abs)))
                        }
                    }
                    WindowFrameUnits::Range => {
                        let raw = match offset.range_offset.as_ref() {
                            Some(lit) => numeric_literal_to_scalar(lit)?,
                            None => ScalarValue::Int64(Some(offset.offset)),
                        };
                        let scalar =
                            coerce_range_offset_for_order_by(raw, range_order_by_type.as_ref())?;
                        WindowFrameBound::Following(scalar)
                    }
                    WindowFrameUnits::Groups => {
                        return Err(GeneralError(
                            "WindowFrameUnits::Groups is not supported.".to_string(),
                        ));
                    }
                },
                UpperFrameBoundStruct::CurrentRow(_) => WindowFrameBound::CurrentRow,
            },
            None => match units {
                WindowFrameUnits::Rows => WindowFrameBound::Following(ScalarValue::UInt64(None)),
                WindowFrameUnits::Range => WindowFrameBound::Following(ScalarValue::Int64(None)),
                WindowFrameUnits::Groups => {
                    return Err(GeneralError(
                        "WindowFrameUnits::Groups is not supported.".to_string(),
                    ));
                }
            },
        };

        let window_frame = WindowFrame::new_bounds(units, lower_bound, upper_bound);
        let lex_orderings = LexOrdering::new(sort_exprs.to_vec());
        let sort_phy_exprs = lex_orderings.as_deref().unwrap_or(&[]);

        datafusion::physical_plan::windows::create_window_expr(
            &window_func,
            window_func_name,
            &window_args,
            partition_by,
            sort_phy_exprs,
            window_frame.into(),
            input_schema,
            spark_expr.ignore_nulls,
            false, // TODO: Spark does not support DISTINCT ... OVER
            None,
        )
        .map_err(|e| ExecutionError::DataFusionError(e.to_string()))
    }

    fn plan_sorted_window(
        &self,
        wnd: &spark_operator::Window,
        window_expr: &[Arc<dyn WindowExpr>],
        input: &Arc<dyn ExecutionPlan>,
        partition_exprs: &[Arc<dyn PhysicalExpr>],
        sort_exprs: &[PhysicalSortExpr],
        input_schema: &SchemaRef,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, ExecutionError> {
        let mut functions = Vec::with_capacity(window_expr.len());
        let mut fields = Vec::with_capacity(window_expr.len());
        for (spark_expr, df_expr) in wnd.window_expr.iter().zip(window_expr.iter()) {
            if spark_expr.agg_func.is_some() {
                return Ok(None);
            }
            let Some(ExprStruct::ScalarFunc(func)) = spark_expr
                .built_in_window_function
                .as_ref()
                .and_then(|f| f.expr_struct.as_ref())
            else {
                return Ok(None);
            };
            let function = match func.func.as_str() {
                "row_number" => SortedWindowFunction::RowNumber,
                "rank" => SortedWindowFunction::Rank,
                "dense_rank" => SortedWindowFunction::DenseRank,
                name @ ("lead" | "lag") => {
                    if spark_expr.ignore_nulls || func.args.len() != 3 {
                        return Ok(None);
                    }
                    let value = self.create_expr(&func.args[0], Arc::clone(input_schema))?;
                    let offset = self.create_expr(&func.args[1], Arc::clone(input_schema))?;
                    let default = self.create_expr(&func.args[2], Arc::clone(input_schema))?;
                    let (Some(offset), Some(default)) = (
                        offset.downcast_ref::<Literal>(),
                        default.downcast_ref::<Literal>(),
                    ) else {
                        return Ok(None);
                    };
                    let offset = match offset.value() {
                        ScalarValue::Int8(Some(v)) => *v as i64,
                        ScalarValue::Int16(Some(v)) => *v as i64,
                        ScalarValue::Int32(Some(v)) => *v as i64,
                        ScalarValue::Int64(Some(v)) => *v,
                        _ => return Ok(None),
                    };
                    let Some(offset) = (if name == "lead" {
                        Some(offset)
                    } else {
                        offset.checked_neg()
                    }) else {
                        return Ok(None);
                    };
                    let value_type = value.data_type(input_schema)?;
                    let default = if default.value().is_null() {
                        ScalarValue::try_from(&value_type)?
                    } else {
                        match default.value().cast_to(&value_type) {
                            Ok(v) => v,
                            Err(_) => return Ok(None),
                        }
                    };
                    SortedWindowFunction::Shift {
                        value,
                        offset,
                        default,
                    }
                }
                _ => return Ok(None),
            };
            let result_type = spark_expr.result_type.as_ref().map(to_arrow_datatype);
            let data_type = match &function {
                SortedWindowFunction::Shift { value, .. } => {
                    let value_type = value.data_type(input_schema)?;
                    if result_type.as_ref().is_some_and(|t| t != &value_type) {
                        return Ok(None);
                    }
                    value_type
                }
                _ => result_type.unwrap_or(DataType::UInt64),
            };
            if !sorted_window_supports_output_type(&function, &data_type, input_schema)? {
                return Ok(None);
            }
            let nullable = matches!(function, SortedWindowFunction::Shift { .. });
            let field = df_expr
                .field()?
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_nullable(nullable);
            fields.push(Arc::new(field));
            functions.push(function);
        }
        Ok(Some(Arc::new(SortedWindowExec::try_new(
            Arc::clone(input),
            partition_exprs.to_vec(),
            sort_exprs.to_vec(),
            functions,
            fields,
        )?)))
    }

    fn process_agg_func(
        &self,
        agg_func: &AggExpr,
        schema: SchemaRef,
        is_ever_expanding: bool,
    ) -> Result<(WindowFunctionDefinition, Vec<Arc<dyn PhysicalExpr>>), ExecutionError> {
        // Wrap a freshly-constructed AggregateUDF impl as a WindowFunctionDefinition.
        fn udaf<U: datafusion::logical_expr::AggregateUDFImpl + 'static>(
            udaf: U,
        ) -> WindowFunctionDefinition {
            WindowFunctionDefinition::AggregateUDF(Arc::new(AggregateUDF::new_from_impl(udaf)))
        }

        // Resolve a window-capable function by name via the session registry, returning
        // a clean "X not supported for window function" error if missing.
        let by_name = |name: &str| -> Result<WindowFunctionDefinition, ExecutionError> {
            self.find_df_window_function(name)
                .ok_or_else(|| GeneralError(format!("{name} not supported for window function")))
        };

        match &agg_func.expr_struct {
            Some(AggExprStruct::Count(expr)) => {
                let children = expr
                    .children
                    .iter()
                    .map(|child| self.create_expr(child, Arc::clone(&schema)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((by_name("count")?, children))
            }
            Some(AggExprStruct::Min(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = min_max_udaf(&child.data_type(&schema)?, false);
                Ok((WindowFunctionDefinition::AggregateUDF(func), vec![child]))
            }
            Some(AggExprStruct::Max(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let func = min_max_udaf(&child.data_type(&schema)?, true);
                Ok((WindowFunctionDefinition::AggregateUDF(func), vec![child]))
            }
            Some(AggExprStruct::Sum(expr)) => {
                // For ever-expanding frames, use Comet's Spark-compatible Sum UDAFs
                // (SumDecimal / SumInteger) which enforce Spark overflow semantics.
                // For sliding frames, those UDAFs can't be used (no retract_batch),
                // so delegate to DataFusion's built-in `sum`, which supports retract
                // but doesn't enforce Spark's decimal precision overflow-to-NULL.
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let arrow_type = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                match arrow_type {
                    DataType::Decimal128(_, _) if is_ever_expanding => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = SumDecimal::try_new(
                            arrow_type,
                            eval_mode,
                            agg_func.expr_id,
                            Arc::clone(&self.query_context_registry),
                        )?;
                        Ok((udaf(func), vec![child]))
                    }
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                        if is_ever_expanding =>
                    {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = SumInteger::try_new(arrow_type, eval_mode)?;
                        Ok((udaf(func), vec![child]))
                    }
                    _ => {
                        let actual = child.data_type(&schema)?;
                        let child: Arc<dyn PhysicalExpr> = if actual != arrow_type {
                            Arc::new(CastExpr::new(child, arrow_type, None))
                        } else {
                            child
                        };
                        Ok((by_name("sum")?, vec![child]))
                    }
                }
            }
            Some(AggExprStruct::Avg(expr)) => {
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                let datatype = to_arrow_datatype(expr.datatype.as_ref().unwrap());
                let input_datatype = to_arrow_datatype(expr.sum_datatype.as_ref().unwrap());
                match datatype {
                    DataType::Decimal128(_, _) if is_ever_expanding => {
                        let eval_mode = from_protobuf_eval_mode(expr.eval_mode)?;
                        let func = AvgDecimal::new(
                            datatype,
                            input_datatype,
                            eval_mode,
                            agg_func.expr_id,
                            Arc::clone(&self.query_context_registry),
                        );
                        Ok((udaf(func), vec![child]))
                    }
                    _ if is_ever_expanding => {
                        let child: Arc<dyn PhysicalExpr> =
                            Arc::new(CastExpr::new(child, DataType::Float64, None));
                        let func = Avg::new("avg", DataType::Float64);
                        Ok((udaf(func), vec![child]))
                    }
                    _ => {
                        // Sliding frame — DataFusion's built-in `avg` handles retract.
                        // Cast non-decimal input to Float64 to match Spark's Avg result type.
                        let child: Arc<dyn PhysicalExpr> = match datatype {
                            DataType::Decimal128(_, _) => child,
                            _ => Arc::new(CastExpr::new(child, DataType::Float64, None)),
                        };
                        Ok((by_name("avg")?, vec![child]))
                    }
                }
            }
            Some(AggExprStruct::First(expr)) => {
                // Spark's FIRST_VALUE → DataFusion's `first_value` UDAF. The UDAF honors
                // ignore-nulls via the WindowExpr-level `ignore_nulls` flag.
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok((by_name("first_value")?, vec![child]))
            }
            Some(AggExprStruct::Last(expr)) => {
                // Spark's LAST_VALUE → DataFusion's `last_value` UDAF.
                let child = self.create_expr(expr.child.as_ref().unwrap(), Arc::clone(&schema))?;
                Ok((by_name("last_value")?, vec![child]))
            }
            other => Err(GeneralError(format!(
                "{other:?} not supported for window function"
            ))),
        }
    }

    /// Find DataFusion's built-in window function by name.
    fn find_df_window_function(&self, name: &str) -> Option<WindowFunctionDefinition> {
        let registry = &self.session_ctx.state();
        // Prefer the WindowUDF (frame-aware) over the AggregateUDF when both exist.
        // Functions like `nth_value` are registered twice in DataFusion: once as an
        // aggregate and once as a window function. The window variant has the
        // correct IGNORE NULLS / frame semantics for an OVER clause; the aggregate
        // variant wrapped into PlainAggregateWindowExpr does not. Aggregate-only
        // functions (count/sum/min/max/avg/...) have no UDWF and fall through.
        registry
            .udwf(name)
            .map(WindowFunctionDefinition::WindowUDF)
            .ok()
            .or_else(|| {
                registry
                    .udaf(name)
                    .map(WindowFunctionDefinition::AggregateUDF)
                    .ok()
            })
    }
}

/// Convert a Spark numeric Literal proto into a `ScalarValue` whose data type
/// matches the literal's declared type. Used for RANGE window frame offsets,
/// where the offset's type must match the ORDER BY column's type. Only numeric
/// types are supported; the Scala side rejects non-numeric RANGE offsets before
/// reaching here.
fn numeric_literal_to_scalar(
    lit: &spark_expression::Literal,
) -> Result<ScalarValue, ExecutionError> {
    let data_type = to_arrow_datatype(lit.datatype.as_ref().ok_or_else(|| {
        GeneralError("RANGE frame offset literal is missing datatype".to_string())
    })?);

    if lit.is_null {
        return Err(GeneralError(
            "RANGE frame offset must not be null".to_string(),
        ));
    }

    let value = lit
        .value
        .as_ref()
        .ok_or_else(|| GeneralError("RANGE frame offset literal has no value".to_string()))?;

    let scalar = match value {
        Value::ByteVal(v) => ScalarValue::Int8(Some(*v as i8)),
        Value::ShortVal(v) => ScalarValue::Int16(Some(*v as i16)),
        Value::IntVal(v) => ScalarValue::Int32(Some(*v)),
        Value::LongVal(v) => ScalarValue::Int64(Some(*v)),
        Value::FloatVal(v) => ScalarValue::Float32(Some(*v)),
        Value::DoubleVal(v) => ScalarValue::Float64(Some(*v)),
        Value::DecimalVal(bytes) => {
            let big_integer = BigInt::from_signed_bytes_be(bytes);
            let integer = big_integer.to_i128().ok_or_else(|| {
                GeneralError(format!(
                    "Cannot parse {big_integer:?} as i128 for Decimal RANGE frame offset"
                ))
            })?;
            match data_type {
                DataType::Decimal128(p, s) => ScalarValue::Decimal128(Some(integer), p, s),
                ref dt => {
                    return Err(GeneralError(format!(
                        "Decimal RANGE frame offset has non-Decimal128 datatype: {dt:?}"
                    )))
                }
            }
        }
        other => {
            return Err(GeneralError(format!(
                "Unsupported value variant for RANGE frame offset: {other:?}"
            )))
        }
    };

    Ok(scalar)
}

/// arrow-arith only provides `Date32 + Interval` kernels, not `Date32 + Int32`.
/// Spark ships `n PRECEDING/FOLLOWING` as an IntegerType literal, so a DATE
/// ORDER BY frame would blow up at execution with
/// "Invalid date arithmetic operation: Date32 + Int32". Wrap integer offsets
/// as IntervalDayTime (days component) so the frame comparator can compute
/// `current_date +/- offset` natively.
fn coerce_range_offset_for_order_by(
    scalar: ScalarValue,
    order_by_type: Option<&DataType>,
) -> Result<ScalarValue, ExecutionError> {
    let Some(dt) = order_by_type else {
        return Ok(scalar);
    };
    match (dt, &scalar) {
        (DataType::Date32, ScalarValue::Int32(Some(v))) => Ok(ScalarValue::IntervalDayTime(Some(
            IntervalDayTime::new(*v, 0),
        ))),
        (DataType::Date32, ScalarValue::Int64(Some(v))) => {
            let days = i32::try_from(*v).map_err(|_| {
                GeneralError(format!(
                    "DATE RANGE frame offset {v} does not fit in i32 days"
                ))
            })?;
            Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                days, 0,
            ))))
        }
        _ => Ok(scalar),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_comet_proto::spark_operator::operator::OpStruct;

    fn datatype(type_id: i32) -> spark_expression::DataType {
        spark_expression::DataType {
            type_id,
            type_info: None,
        }
    }

    fn column() -> spark_expression::Expr {
        spark_expression::Expr {
            expr_struct: Some(ExprStruct::Bound(spark_expression::BoundReference {
                index: 0,
                datatype: Some(datatype(3)),
            })),
            ..Default::default()
        }
    }

    fn literal(value: Option<i32>) -> spark_expression::Expr {
        spark_expression::Expr {
            expr_struct: Some(ExprStruct::Literal(spark_expression::Literal {
                value: value.map(Value::IntVal),
                datatype: Some(datatype(3)),
                is_null: value.is_none(),
            })),
            ..Default::default()
        }
    }

    fn window_expr(name: &str, offset: i32, ignore_nulls: bool) -> spark_operator::WindowExpr {
        spark_operator::WindowExpr {
            built_in_window_function: Some(spark_expression::Expr {
                expr_struct: Some(ExprStruct::ScalarFunc(spark_expression::ScalarFunc {
                    func: name.to_string(),
                    args: if matches!(name, "lead" | "lag") {
                        vec![column(), literal(Some(offset)), literal(None)]
                    } else {
                        vec![]
                    },
                    return_type: None,
                    fail_on_error: false,
                    ..Default::default()
                })),
                ..Default::default()
            }),
            spec: Some(spark_operator::WindowSpecDefinition {
                partition_spec: vec![],
                order_spec: vec![],
                frame_specification: Some(spark_operator::WindowFrame {
                    frame_type: WindowFrameType::Rows as i32,
                    lower_bound: Some(spark_operator::LowerWindowFrameBound {
                        lower_frame_bound_struct: Some(LowerFrameBoundStruct::UnboundedPreceding(
                            spark_operator::UnboundedPreceding {},
                        )),
                    }),
                    upper_bound: Some(spark_operator::UpperWindowFrameBound {
                        upper_frame_bound_struct: Some(UpperFrameBoundStruct::CurrentRow(
                            spark_operator::CurrentRow {},
                        )),
                    }),
                }),
            }),
            ignore_nulls,
            // The Spark counter/shift result is Int32; percent_rank is Double.
            result_type: Some(datatype(if name == "percent_rank" { 6 } else { 3 })),
            ..Default::default()
        }
    }

    fn window_operator(window_expr: Vec<spark_operator::WindowExpr>) -> Operator {
        Operator {
            children: vec![Operator {
                op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                    fields: vec![datatype(3)],
                    source: "".to_string(),
                })),
                ..Default::default()
            }],
            op_struct: Some(OpStruct::Window(Box::new(spark_operator::Window {
                window_expr,
                order_by_list: vec![spark_expression::Expr {
                    expr_struct: Some(ExprStruct::SortOrder(Box::new(
                        spark_expression::SortOrder {
                            child: Some(Box::new(column())),
                            direction: 0,
                            null_ordering: 0,
                        },
                    ))),
                    ..Default::default()
                }],
                partition_by_list: vec![column()],
                child: None,
            }))),
            ..Default::default()
        }
    }

    fn plan_string(op: &Operator, sorted: bool, partition: bool) -> String {
        let mut config = SessionConfig::new();
        if sorted {
            config = config.with_extension(Arc::new(SortedWindowEnabled));
        }
        if partition {
            config = config.with_extension(Arc::new(PartitionAggregateWindowEnabled));
        }
        let planner = PhysicalPlanner::new(Arc::new(SessionContext::new_with_config(config)), 0);
        let (_, _, plan) = planner.create_plan(op, &mut vec![], 1).unwrap();
        let display = datafusion::physical_plan::displayable(plan.native_plan.as_ref())
            .indent(false)
            .to_string();
        display
    }

    #[test]
    fn sorted_window_routing_preserves_flags_and_offset_eligibility() {
        let simple = window_operator(vec![
            window_expr("row_number", 0, false),
            window_expr("rank", 0, false),
            window_expr("dense_rank", 0, false),
            window_expr("lead", 1, false),
            window_expr("lag", 1, false),
        ]);
        assert!(plan_string(&simple, true, false).contains("CometSortedWindowExec"));
        assert!(plan_string(&simple, false, false).contains("BoundedWindowAggExec"));
        for name in ["lead", "lag"] {
            for offset in [0, 1, -1, 1024, -1024, 1025, -1025] {
                let op = window_operator(vec![window_expr(name, offset, false)]);
                let plan = plan_string(&op, true, false);
                assert_eq!(
                    plan.contains("CometSortedWindowExec"),
                    offset.unsigned_abs() <= 1024,
                    "{name} offset {offset}: {plan}"
                );
            }
            let op = window_operator(vec![window_expr(name, 1, true)]);
            let plan = plan_string(&op, true, true);
            assert!(plan.contains("BoundedWindowAggExec"), "{plan}");
            assert!(!plan.contains("CometSortedWindowExec"), "{plan}");
        }
    }

    #[test]
    fn partition_window_routing_is_opt_in_and_preserves_mixed_nodes() {
        for mixed in [false, true] {
            let mut expressions = vec![window_expr("percent_rank", 0, false)];
            if mixed {
                expressions.push(window_expr("row_number", 0, false));
            }
            let op = window_operator(expressions);
            let disabled = plan_string(&op, true, false);
            assert!(disabled.contains("WindowAggExec"), "{disabled}");
            assert!(
                !disabled.contains("PartitionAggregateWindowExec"),
                "{disabled}"
            );
            let enabled = plan_string(&op, true, true);
            assert!(
                enabled.contains("PartitionAggregateWindowExec"),
                "{enabled}"
            );
            if mixed {
                assert!(enabled.contains("BoundedWindowAggExec"), "{enabled}");
            }
        }
    }

    #[test]
    fn sorted_window_rejects_nonliteral_offset_and_default() {
        let planner = PhysicalPlanner::default();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None).unwrap();
        // The DF expression only supplies a field here; selection must reject a nonliteral
        // offset/default before constructing the custom operator.
        let field_expr = planner
            .create_window_expr(
                &window_expr("row_number", 0, false),
                Arc::clone(&schema),
                &[],
                &[],
            )
            .unwrap();
        for arg in [1, 2] {
            let mut expr = window_expr("lead", 1, false);
            let Some(ExprStruct::ScalarFunc(func)) = expr
                .built_in_window_function
                .as_mut()
                .and_then(|e| e.expr_struct.as_mut())
            else {
                unreachable!()
            };
            func.args[arg] = column();
            let window = spark_operator::Window {
                window_expr: vec![expr],
                ..Default::default()
            };
            assert!(planner
                .plan_sorted_window(
                    &window,
                    &[Arc::clone(&field_expr)],
                    &input,
                    &[],
                    &[],
                    &schema,
                )
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn window_aggregate_filter_is_rejected_at_native_boundary() {
        let planner = PhysicalPlanner::default();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let mut expr = window_expr("row_number", 0, false);
        expr.built_in_window_function = None;
        expr.agg_func = Some(AggExpr {
            expr_struct: Some(AggExprStruct::Count(spark_expression::Count {
                children: vec![column()],
            })),
            filter: Some(spark_expression::Expr {
                expr_struct: Some(ExprStruct::Literal(spark_expression::Literal {
                    value: Some(Value::BoolVal(false)),
                    datatype: Some(datatype(0)),
                    is_null: false,
                })),
                ..Default::default()
            }),
            ..Default::default()
        });
        let error = planner
            .create_window_expr(&expr, Arc::clone(&schema), &[], &[])
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("FILTER (WHERE ...) is not supported for window aggregates"));
        expr.agg_func.as_mut().unwrap().filter = None;
        assert!(planner.create_window_expr(&expr, schema, &[], &[]).is_ok());
    }
}
