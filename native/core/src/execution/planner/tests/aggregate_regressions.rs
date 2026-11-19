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

//! Exercise the actual Spark aggregate planner and state wrappers, not a substitute UDAF.

use super::*;
use arrow::array::{AsArray, Int64Array};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use std::collections::BTreeMap;

fn bound(index: i32, datatype: &spark_expression::DataType) -> Expr {
    Expr {
        expr_struct: Some(Bound(spark_expression::BoundReference {
            index,
            datatype: Some(datatype.clone()),
        })),
        ..Default::default()
    }
}

/// The stage preceding COUNT(DISTINCT) must emit every (g, k) exactly once. A Partial
/// aggregate would emit duplicates under pressure, silently overcounting downstream.
async fn partial_merge_case(sort_keys: &[i32], limit: usize, expect_spill: bool, fair: bool) {
    let datatype = create_int64_datatype();
    let merge_mode = spark_operator::AggregateMode::PartialMerge as i32;
    let scan = Operator {
        op_struct: Some(OpStruct::Scan(spark_operator::Scan {
            fields: vec![datatype.clone(); 4],
            source: String::new(),
        })),
        ..Default::default()
    };
    let input = if sort_keys.is_empty() {
        scan
    } else {
        Operator {
            children: vec![scan],
            op_struct: Some(OpStruct::Sort(spark_operator::Sort {
                sort_orders: sort_keys
                    .iter()
                    .map(|&index| create_sort_order(index, 4, false, true))
                    .collect(),
                ..Default::default()
            })),
            ..Default::default()
        }
    };
    let value = create_unbound_int64("value");
    let op = Operator {
        children: vec![input],
        op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
            grouping_exprs: vec![bound(0, &datatype), bound(1, &datatype)],
            agg_exprs: vec![create_max_agg(&value), create_count_agg(&value)],
            expr_modes: vec![merge_mode; 2],
            mode: merge_mode,
            initial_input_buffer_offset: 2,
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut config = SessionConfig::new().with_batch_size(127);
    config.options_mut().execution.sort_spill_reservation_bytes = 16 << 10;
    let context = Arc::new(SessionContext::new_with_config(config.clone()));
    let planner = PhysicalPlanner::new(Arc::clone(&context), 0);
    let (_, _, planned) = planner.create_plan(&op, &mut vec![], 1).unwrap();
    let aggregate = planned.additional_native_plans[0]
        .downcast_ref::<AggregateExec>()
        .unwrap();
    assert_eq!(
        aggregate.mode(),
        if sort_keys.len() == 1 {
            &AggregateMode::Final
        } else {
            &AggregateMode::Single
        }
    );
    const GROUPS: usize = 24_000;
    let schema = planned.children[0].schema();
    let batches = (0..3)
        .flat_map(|_| {
            (0..GROUPS)
                .step_by(512)
                .map(|start| {
                    let keys = start..(start + 512).min(GROUPS);
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from_iter_values(
                                keys.clone().map(|k| (k % 3) as i64),
                            )) as ArrayRef,
                            Arc::new(Int64Array::from_iter_values(keys.clone().map(|k| k as i64))),
                            Arc::new(Int64Array::from_iter_values(keys.clone().map(|k| k as i64))),
                            Arc::new(Int64Array::from_iter_values(keys.map(|_| 1))),
                        ],
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let source: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    let plan = Arc::clone(&planned.native_plan)
        .transform_up(|node| {
            Ok(if node.name() == "ScanExec" {
                Transformed::yes(Arc::clone(&source))
            } else {
                Transformed::no(node)
            })
        })
        .unwrap()
        .data;
    let (pool, fake): (Arc<dyn MemoryPool>, _) = if fair {
        let (pool, task) =
            crate::execution::memory_pools::fair_unified_pool_with_fake_spark(limit, limit);
        (pool, Some(task))
    } else {
        (Arc::new(GreedyMemoryPool::new(limit)), None)
    };
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let task = Arc::new(
        datafusion::execution::TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    );
    let mut stream = plan.execute(0, task).unwrap();
    let mut actual = BTreeMap::new();
    let mut first_output_buffer: Option<arrow::buffer::Buffer> = None;
    let mut slices_sharing_first_buffer = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        if sort_keys.len() == 1 && !expect_spill {
            let data = batch.column(0).to_data();
            let buffer = &data.buffers()[0];
            // Keep the allocation alive so independent per-batch materializations
            // cannot pass through allocator address reuse. Real wrapper output must
            // share the completed range's backing batch across successive slices.
            let first = first_output_buffer.get_or_insert_with(|| buffer.clone());
            if first.data_ptr() == buffer.data_ptr() {
                slices_sharing_first_buffer += 1;
            }
        }
        for row in 0..batch.num_rows() {
            let key = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row);
            let group = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row);
            let max = batch
                .column(2)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row);
            let count = batch
                .column(3)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row);
            assert!(
                actual.insert(key, (group, max, count)).is_none(),
                "duplicate key {key}"
            );
        }
    }
    assert_eq!(actual.len(), GROUPS);
    if sort_keys.len() == 1 && !expect_spill {
        assert!(
            slices_sharing_first_buffer > 1,
            "wrapper must slice one materialized range"
        );
    }
    for key in 0..GROUPS as i64 {
        assert_eq!(actual[&key], (key % 3, key, 3));
    }
    let mut aggregate_spills = 0;
    plan.apply(|node| {
        if node.downcast_ref::<AggregateExec>().is_some() {
            aggregate_spills += node.metrics().unwrap().spill_count().unwrap_or(0);
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    if expect_spill {
        assert!(aggregate_spills > 0, "aggregate must actually spill");
    }
    drop(stream);
    drop(plan);
    assert_eq!(pool.reserved(), 0);
    if let Some(task) = fake {
        assert_eq!(task.held(), 0);
    }
}

#[tokio::test]
async fn partial_merge_unordered_spills_without_duplicate_keys() {
    partial_merge_case(&[], 256 << 10, true, false).await;
}

#[tokio::test]
async fn partial_merge_prefix_sorted_spills_without_duplicate_keys() {
    partial_merge_case(&[0], 256 << 10, true, false).await;
}

#[tokio::test]
async fn partial_merge_prefix_sorted_drains_wrapper_states_once() {
    partial_merge_case(&[0], 64 << 20, false, false).await;
}

#[tokio::test]
async fn partial_merge_fully_sorted_preserves_single_wrapper() {
    partial_merge_case(&[0, 1], 64 << 20, false, false).await;
}

#[tokio::test]
async fn partial_merge_fully_sorted_primitive_states_survive_a_tight_pool() {
    partial_merge_case(&[0, 1], 256 << 10, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_merge_prefix_sorted_spills_with_production_fair_pool() {
    partial_merge_case(&[0], 256 << 10, true, true).await;
}

#[test]
fn specialized_aggregate_planner_uses_groups_without_changing_float_min_max() {
    use crate::execution::planner::aggregate::{grouped_min_max_udaf, min_max_udaf};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    for data_type in [DataType::Boolean, DataType::Float32, DataType::Float64] {
        let schema = Arc::new(Schema::new(vec![Field::new("v", data_type.clone(), true)]));
        for is_max in [false, true] {
            let udf = grouped_min_max_udaf(&data_type, is_max);
            let expr = AggregateExprBuilder::new(udf, vec![Arc::new(Column::new("v", 0))])
                .schema(Arc::clone(&schema))
                .alias("a")
                .build()
                .unwrap();
            assert!(expr.create_groups_accumulator().is_ok());
            if data_type == DataType::Boolean {
                assert_eq!(
                    min_max_udaf(&data_type, is_max).name(),
                    if is_max { "max" } else { "min" }
                );
                assert_eq!(
                    expr.fun().name(),
                    if is_max { "bool_or" } else { "bool_and" }
                );
            } else {
                assert!(expr
                    .fun()
                    .inner()
                    .downcast_ref::<datafusion_comet_spark_expr::SparkMinMax>()
                    .is_some());
            }
        }
    }
}

#[test]
fn first_last_planner_selects_unordered_groups_for_variable_width_values() {
    use spark_expression::agg_expr::ExprStruct as AggregateExpr;
    for type_id in [0, 4, 7, 8] {
        let datatype = spark_expression::DataType {
            type_id,
            type_info: None,
        };
        for ignore_nulls in [false, true] {
            for expr_struct in [
                AggregateExpr::First(spark_expression::First {
                    child: Some(bound(0, &datatype)),
                    datatype: Some(datatype.clone()),
                    ignore_nulls,
                }),
                AggregateExpr::Last(spark_expression::Last {
                    child: Some(bound(0, &datatype)),
                    datatype: Some(datatype.clone()),
                    ignore_nulls,
                }),
            ] {
                let op = Operator {
                    children: vec![Operator {
                        op_struct: Some(OpStruct::Scan(spark_operator::Scan {
                            fields: vec![datatype.clone()],
                            source: String::new(),
                        })),
                        ..Default::default()
                    }],
                    op_struct: Some(OpStruct::HashAgg(spark_operator::HashAggregate {
                        agg_exprs: vec![spark_expression::AggExpr {
                            expr_struct: Some(expr_struct),
                            ..Default::default()
                        }],
                        mode: spark_operator::AggregateMode::Partial as i32,
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                let (_, _, planned) = PhysicalPlanner::default()
                    .create_plan(&op, &mut vec![], 1)
                    .unwrap();
                let aggregate = planned.native_plan.downcast_ref::<AggregateExec>().unwrap();
                let expr = &aggregate.aggr_expr()[0];
                assert!(expr
                    .fun()
                    .inner()
                    .downcast_ref::<datafusion_comet_spark_expr::SparkFirstLast>()
                    .is_some());
                assert!(expr.groups_accumulator_supported());
                assert_eq!(expr.state_fields().unwrap().len(), 2);
                assert_eq!(expr.ignore_nulls(), ignore_nulls);
            }
        }
    }
}

#[test]
fn boolean_min_max_windows_still_retract_departing_rows() {
    use crate::execution::planner::aggregate::min_max_udaf;
    use arrow::array::BooleanArray;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Boolean, true)]));
    for is_max in [false, true] {
        let expr = AggregateExprBuilder::new(
            min_max_udaf(&DataType::Boolean, is_max),
            vec![Arc::new(Column::new("v", 0))],
        )
        .schema(Arc::clone(&schema))
        .alias("a")
        .build()
        .unwrap();
        let mut acc = expr.create_sliding_accumulator().unwrap();
        acc.update_batch(&[Arc::new(BooleanArray::from(vec![
            Some(false),
            None,
            Some(true),
        ]))])
        .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Boolean(Some(is_max)));
        acc.retract_batch(&[Arc::new(BooleanArray::from(vec![Some(false), None]))])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Boolean(Some(true)));
        acc.retract_batch(&[Arc::new(BooleanArray::from(vec![true]))])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Boolean(None));
    }
}

#[derive(Clone, Copy)]
struct VariableReplayCase {
    groups: usize,
    rows: usize,
    variants: usize,
    repeats: usize,
    limit: usize,
    short_values: bool,
    bounded_partial: bool,
}

async fn variable_first_last_replay_case(case: VariableReplayCase) {
    use arrow::array::{Array, StringArray};
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_comet_spark_expr::{CometCollectList, SparkFirstLast};
    use std::collections::BTreeSet;

    type GroupResults = BTreeMap<i64, (Vec<Option<String>>, Vec<i64>)>;

    fn value(case: VariableReplayCase, key: usize, column: usize) -> Option<String> {
        if key.is_multiple_of(29) {
            None
        } else if case.short_values {
            Some(char::from(b'a' + ((key + column) % 26) as u8).to_string())
        } else if column == 0 && key.is_multiple_of(149) {
            // One independently wide field keeps each individual group's state
            // feasible; multiplying a wide value across every field would test
            // an unsplittable memory minimum instead of replay headroom.
            Some(format!("wide-{key}-{}", "x".repeat(1500 + key % 1000)))
        } else {
            Some(format!("v{column}-{key}"))
        }
    }

    fn aggregate(
        mode: AggregateMode,
        input: Arc<dyn ExecutionPlan>,
        schema: &Arc<Schema>,
        variants: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let mut expressions = (0..variants)
            .map(|column| {
                let udf = if column.is_multiple_of(2) {
                    SparkFirstLast::first()
                } else {
                    SparkFirstLast::last()
                };
                Arc::new(
                    AggregateExprBuilder::new(
                        Arc::new(AggregateUDF::new_from_impl(udf)),
                        vec![Arc::new(Column::new(&format!("v{column}"), column + 1))],
                    )
                    .schema(Arc::clone(schema))
                    .alias(format!("a{column}"))
                    .with_ignore_nulls(column.is_multiple_of(3))
                    .build()
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        expressions.push(Arc::new(
            AggregateExprBuilder::new(
                Arc::new(AggregateUDF::new_from_impl(CometCollectList::new())),
                vec![Arc::new(Column::new("id", variants + 1))],
            )
            .schema(Arc::clone(schema))
            .alias("ids")
            .build()
            .unwrap(),
        ));
        Arc::new(
            AggregateExec::try_new(
                mode,
                PhysicalGroupBy::new_single(vec![(
                    Arc::new(Column::new("key", 0)),
                    "key".to_string(),
                )]),
                expressions,
                vec![None; variants + 1],
                input,
                Arc::clone(schema),
            )
            .unwrap(),
        )
    }

    async fn execute(
        plan: Arc<dyn ExecutionPlan>,
        config: &SessionConfig,
        limit: usize,
    ) -> (Vec<RecordBatch>, usize) {
        let (pool, spark): (Arc<dyn MemoryPool>, _) = if limit == usize::MAX {
            (Arc::new(GreedyMemoryPool::new(limit)), None)
        } else {
            let (pool, spark) =
                crate::execution::memory_pools::fair_unified_pool_with_fake_spark(limit, limit);
            (pool, Some(spark))
        };
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()
            .unwrap();
        let context = Arc::new(
            datafusion::execution::TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(config.clone()),
        );
        let mut stream = plan.execute(0, context).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        let spills = plan.metrics().unwrap().spill_count().unwrap_or(0);
        drop(stream);
        drop(plan);
        assert_eq!(
            pool.reserved(),
            0,
            "the stage must release every reservation"
        );
        if let Some(spark) = spark {
            assert_eq!(spark.held(), 0, "the stage must release Spark's real grant");
        }
        (batches, spills)
    }

    fn results(batches: Vec<RecordBatch>, case: VariableReplayCase) -> GroupResults {
        let mut groups = BTreeMap::new();
        let mut all_ids = BTreeSet::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let lists = batch.column(case.variants + 1).as_list::<i32>();
            for row in 0..batch.num_rows() {
                let key = keys.value(row);
                let values = (0..case.variants)
                    .map(|column| {
                        let values = batch.column(column + 1).as_string::<i32>();
                        (!values.is_null(row)).then(|| values.value(row).to_owned())
                    })
                    .collect::<Vec<_>>();
                for (column, actual) in values.iter().enumerate() {
                    assert_eq!(actual, &value(case, key as usize, column));
                }
                assert!(!lists.is_null(row));
                let list = lists.value(row);
                let mut ids = list
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .values()
                    .to_vec();
                ids.sort_unstable();
                assert_eq!(ids.len(), case.repeats);
                for &id in &ids {
                    assert_eq!(
                        (id as usize % case.groups * 337) % case.groups,
                        key as usize
                    );
                    assert!(all_ids.insert(id), "duplicate input identity {id}");
                }
                assert!(
                    groups.insert(key, (values, ids)).is_none(),
                    "duplicate group {key}"
                );
            }
        }
        assert_eq!(groups.len(), case.groups);
        assert_eq!(all_ids, (0..(case.groups * case.repeats) as i64).collect());
        groups
    }

    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    fields.extend(
        (0..case.variants).map(|column| Field::new(format!("v{column}"), DataType::Utf8, true)),
    );
    fields.push(Field::new("id", DataType::Int64, false));
    let schema = Arc::new(Schema::new(fields));
    let batches = (0..case.groups * case.repeats)
        .step_by(case.rows)
        .map(|start| {
            let rows = start..start + case.rows;
            let mut columns = vec![Arc::new(Int64Array::from_iter_values(
                rows.clone()
                    .map(|id| ((id % case.groups * 337) % case.groups) as i64),
            )) as ArrayRef];
            columns.extend((0..case.variants).map(|column| {
                Arc::new(StringArray::from_iter(rows.clone().map(|id| {
                    value(case, (id % case.groups * 337) % case.groups, column)
                }))) as ArrayRef
            }));
            columns.push(Arc::new(Int64Array::from_iter_values(
                rows.map(|id| id as i64),
            )));
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
        })
        .collect::<Vec<_>>();
    let source = || {
        MemorySourceConfig::try_new_exec(std::slice::from_ref(&batches), Arc::clone(&schema), None)
            .unwrap()
    };
    let mut config = SessionConfig::new().with_batch_size(case.rows);
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_ratio_threshold = 1.1;
    let (reference, _) = execute(
        aggregate(AggregateMode::Single, source(), &schema, case.variants),
        &config,
        usize::MAX,
    )
    .await;
    let reference = results(reference, case);

    // Materialize the real Partial state first: each bounded stage has one live
    // aggregate consumer rather than halving the artificial pool between stages.
    let partial = aggregate(AggregateMode::Partial, source(), &schema, case.variants);
    let partial_schema = partial.schema();
    let (states, _) = execute(
        partial,
        &config,
        if case.bounded_partial {
            case.limit
        } else {
            usize::MAX
        },
    )
    .await;
    let input = MemorySourceConfig::try_new_exec(&[states], partial_schema, None).unwrap();
    let (actual, spills) = execute(
        aggregate(AggregateMode::Final, input, &schema, case.variants),
        &config,
        case.limit,
    )
    .await;
    assert!(spills > 0, "the finite Final aggregate must actually spill");
    assert_eq!(results(actual, case), reference);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn variable_first_last_and_collect_list_final_spill_with_replay_headroom() {
    variable_first_last_replay_case(VariableReplayCase {
        groups: 768,
        rows: 64,
        variants: 7,
        repeats: 3,
        limit: 107_374,
        short_values: false,
        bounded_partial: true,
    })
    .await;
}

/// A one-byte UTF8 value has a small IPC representation, while each owned
/// variable accumulator still needs a Vec header for every group. Keep this
/// independent of wide payloads and their genuinely unsplittable minimum.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_short_first_last_states_replay_with_a_bounded_pool() {
    variable_first_last_replay_case(VariableReplayCase {
        groups: 16_384,
        rows: 8_192,
        variants: 14,
        repeats: 2,
        limit: 7 << 20,
        short_values: true,
        // Produce the real state without imposing a separate coarse-batch
        // Partial pressure condition on this isolated Final replay regression.
        bounded_partial: false,
    })
    .await;
}
