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

//! COMET PATCH: an aggregate whose reservation is starved to zero spills on its first
//! batch, and what the emptied table still holds must not fail the task.

use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::pretty::pretty_format_batches;
use datafusion_common::{Constraint, Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation, TrackConsumersPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{col, lit};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use futures::StreamExt;

use super::{AggregateExec, AggregateMode, PhysicalGroupBy, StreamType};
use crate::SendableRecordBatchStream;
use crate::common::collect;
use crate::execution_plan::ExecutionPlan;
use crate::metrics::MetricValue;
use crate::stream::RecordBatchStreamAdapter;
use crate::streaming::{PartitionStream, StreamingTableExec};
use crate::test::TestMemoryExec;

const POOL_SIZE: usize = 64 * 1024 * 1024;
const BATCHES: u32 = 4;
const ROWS_PER_BATCH: u32 = 2_000;

/// Yields `batches`, and drops the reservation that fills the pool when the second
/// batch is requested, so the aggregate gets no memory for its first batch only.
struct HogReleasingPartition {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    hog: Arc<Mutex<Option<MemoryReservation>>>,
}

impl Debug for HogReleasingPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HogReleasingPartition").finish()
    }
}

impl PartitionStream for HogReleasingPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let hog = Arc::clone(&self.hog);
        let stream = futures::stream::iter(self.batches.clone().into_iter().enumerate())
            .map(move |(index, batch)| {
                if index == 1 {
                    hog.lock().unwrap().take();
                }
                Ok(batch)
            });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Rows sorted by `a`, with every value of `a` in one batch only and several values of
/// `b` for each `a`.
fn raw_batches(schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    (0..BATCHES)
        .map(|batch| {
            let rows = (0..ROWS_PER_BATCH).map(|row| batch * ROWS_PER_BATCH + row);
            let a = rows.clone().map(|row| row / 4).collect::<Vec<_>>();
            let b = rows
                .clone()
                .map(|row| format!("b{}", row % 3))
                .collect::<Vec<_>>();
            let v = rows.map(i64::from).collect::<Vec<_>>();
            Ok(RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(UInt32Array::from(a)),
                    Arc::new(StringArray::from(b)),
                    Arc::new(Int64Array::from(v)),
                ],
            )?)
        })
        .collect()
}

fn group_by(schema: &SchemaRef, keys: &[&str]) -> Result<PhysicalGroupBy> {
    Ok(PhysicalGroupBy::new_single(
        keys.iter()
            .map(|key| Ok((col(key, schema)?, (*key).to_string())))
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn aggregate(
    mode: AggregateMode,
    keys: &[&str],
    input: Arc<dyn ExecutionPlan>,
    raw_schema: &SchemaRef,
) -> Result<Arc<AggregateExec>> {
    let aggr_expr = vec![
        Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("v", raw_schema)?])
                .schema(Arc::clone(raw_schema))
                .alias("count_v")
                .build()?,
        ),
        Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![col("v", raw_schema)?])
                .schema(Arc::clone(raw_schema))
                .alias("sum_v")
                .build()?,
        ),
    ];
    let group_by = if mode == AggregateMode::Final {
        group_by(&input.schema(), keys)?
    } else {
        group_by(raw_schema, keys)?
    };
    Ok(Arc::new(AggregateExec::try_new(
        mode,
        group_by,
        aggr_expr,
        vec![None, None],
        input,
        Arc::clone(raw_schema),
    )?))
}

fn task_ctx(pool: Option<Arc<dyn MemoryPool>>) -> Result<Arc<TaskContext>> {
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(pool) = pool {
        runtime = runtime.with_memory_pool(pool);
    }
    Ok(Arc::new(
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(512)
                    .set_bool("datafusion.execution.enable_migration_aggregate", true),
            )
            .with_runtime(runtime.build_arc()?),
    ))
}

/// Input for the aggregate under test: the raw rows for `Single`, the partial states
/// computed without a memory limit for `Final`, optionally declared sorted on `a`.
async fn input_batches(
    mode: AggregateMode,
    keys: &[&str],
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let raw_schema = raw_schema();
    let raw = raw_batches(&raw_schema)?;
    if mode != AggregateMode::Final {
        return Ok((raw_schema, raw));
    }
    let mut partial_states = vec![];
    for batch in raw {
        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&raw_schema), None)?;
        let partial = aggregate(AggregateMode::Partial, keys, input, &raw_schema)?;
        let states = collect(partial.execute(0, task_ctx(None)?)?).await?;
        let states = concat_batches(&partial.schema(), &states)?;
        partial_states.push(sort_on_keys(&states, keys.len())?);
    }
    Ok((partial_states[0].schema(), partial_states))
}

fn sorted_on(schema: &SchemaRef, key: &str) -> Result<Vec<LexOrdering>> {
    Ok(vec![
        LexOrdering::new(vec![PhysicalSortExpr::new_default(col(key, schema)?)]).unwrap(),
    ])
}

fn spill_count(aggregate: &AggregateExec) -> usize {
    aggregate
        .metrics()
        .unwrap()
        .iter()
        .filter_map(|metric| match metric.value() {
            MetricValue::SpillCount(count) => Some(count.value()),
            _ => None,
        })
        .sum()
}

fn sort_on_keys(batch: &RecordBatch, keys: usize) -> Result<RecordBatch> {
    let columns = (0..keys)
        .map(|index| SortColumn {
            values: Arc::clone(batch.column(index)),
            options: None,
        })
        .collect::<Vec<_>>();
    let indices = lexsort_to_indices(&columns, None)?;
    Ok(take_record_batch(batch, &indices)?)
}

fn sorted_output(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<String> {
    let batch = concat_batches(schema, batches)?;
    let sorted = sort_on_keys(&batch, schema.fields().len() - 2)?;
    Ok(pretty_format_batches(&[sorted])?.to_string())
}

/// Runs the aggregate with the whole pool held by another consumer until the second
/// input batch is requested, and compares its output with an unconstrained run.
async fn run_starved(
    mode: AggregateMode,
    keys: &[&str],
    sorted_input: bool,
    expected_stream: fn(&StreamType) -> bool,
) -> Result<()> {
    let raw_schema = raw_schema();
    let (input_schema, batches) = input_batches(mode, keys).await?;
    let ordering = if sorted_input {
        sorted_on(&input_schema, "a")?
    } else {
        vec![]
    };

    let unconstrained_input = TestMemoryExec::try_new(
        std::slice::from_ref(&batches),
        Arc::clone(&input_schema),
        None,
    )?
    .try_with_sort_information(ordering.clone())?;
    let unconstrained_input =
        Arc::new(TestMemoryExec::update_cache(&Arc::new(unconstrained_input)));
    let unconstrained = aggregate(mode, keys, unconstrained_input, &raw_schema)?;
    let expected = collect(unconstrained.execute(0, task_ctx(None)?)?).await?;
    assert_eq!(spill_count(&unconstrained), 0);

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(POOL_SIZE));
    let hog = MemoryConsumer::new("hog").register(&pool);
    hog.try_grow(POOL_SIZE)?;
    let partition = Arc::new(HogReleasingPartition {
        schema: Arc::clone(&input_schema),
        batches,
        hog: Arc::new(Mutex::new(Some(hog))),
    });
    let input = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&input_schema),
        vec![partition],
        None,
        ordering,
        false,
        None,
    )?);
    let starved = aggregate(mode, keys, input, &raw_schema)?;
    let ctx = task_ctx(Some(Arc::clone(&pool)))?;
    assert!(expected_stream(&starved.execute_typed(0, &ctx)?));
    let actual = collect(starved.execute(0, ctx)?).await?;

    assert!(
        spill_count(&starved) > 0,
        "the starved aggregate must spill"
    );
    assert_eq!(
        sorted_output(&starved.schema(), &actual)?,
        sorted_output(&unconstrained.schema(), &expected)?
    );
    drop(starved);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn final_hash_aggregate_survives_a_starved_first_spill() -> Result<()> {
    run_starved(AggregateMode::Final, &["a", "b"], false, |stream| {
        matches!(stream, StreamType::FinalHash(_))
    })
    .await
}

#[tokio::test]
async fn single_hash_aggregate_survives_a_starved_first_spill() -> Result<()> {
    run_starved(AggregateMode::Single, &["a", "b"], false, |stream| {
        matches!(stream, StreamType::SingleHash(_))
    })
    .await
}

#[tokio::test]
async fn ordered_final_aggregate_survives_a_starved_first_spill() -> Result<()> {
    run_starved(AggregateMode::Final, &["a", "b"], true, |stream| {
        matches!(stream, StreamType::OrderedFinalAggregate(_))
    })
    .await
}

/// Partial states of `count(v)` and `sum(v)` grouped by `(a, b)`, for 200k groups in
/// random order.
fn many_group_states(batches: u32, rows: u32) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("count_v[count]", DataType::Int64, false),
        Field::new("sum_v[sum]", DataType::Int64, true),
    ]));
    let mut state = 11u64;
    let batches = (0..batches)
        .map(|_| {
            let keys: Vec<u64> = (0..rows)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    (state >> 33) % 200_000
                })
                .collect();
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt32Array::from_iter_values(
                        keys.iter().map(|k| (k % 1000) as u32),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        keys.iter().map(|k| format!("key-{k:020}")),
                    )),
                    Arc::new(Int64Array::from_iter_values(keys.iter().map(|_| 1))),
                    Arc::new(Int64Array::from_iter_values(
                        keys.iter().map(|k| *k as i64),
                    )),
                ],
            )?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((schema, batches))
}

/// Reads `stream` as the native shuffle writer does: it keeps every batch reserved until
/// the pool refuses one, and then spills them all.
async fn read_as_shuffle_writer(
    mut stream: SendableRecordBatchStream,
    pool: &Arc<dyn MemoryPool>,
) -> Result<(Vec<RecordBatch>, usize)> {
    let writer = MemoryConsumer::new("ShuffleRepartitioner[0]")
        .with_can_spill(true)
        .register(pool);
    let mut output = vec![];
    let mut spills = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if writer.try_grow(batch.get_array_memory_size()).is_err() {
            spills += 1;
            writer.free();
        }
        output.push(batch);
    }
    Ok((output, spills))
}

/// A final aggregate that spilled replays its runs through a fully ordered aggregate,
/// which cannot spill. The native shuffle writer reading it buffers its output until the
/// pool refuses it, so it soon holds nearly all the memory the aggregate released, and
/// the replay then failed with "Additional allocation failed for
/// FinalHashAggregateStream[0]" while the writer, which can spill, kept the rest.
#[tokio::test]
async fn spilled_final_aggregate_survives_a_shuffle_writer_that_takes_the_pool()
-> Result<()> {
    let raw_schema = raw_schema();
    let (input_schema, batches) = many_group_states(100, 4096)?;
    let unconstrained_input = TestMemoryExec::try_new_exec(
        std::slice::from_ref(&batches),
        Arc::clone(&input_schema),
        None,
    )?;
    let unconstrained = aggregate(
        AggregateMode::Final,
        &["a", "b"],
        unconstrained_input,
        &raw_schema,
    )?;
    let expected = collect(unconstrained.execute(0, task_ctx(None)?)?).await?;
    let expected = sorted_output(&unconstrained.schema(), &expected)?;

    for limit in [4 << 20, 6 << 20, 8 << 20, 12 << 20] {
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(limit),
            NonZeroUsize::new(5).unwrap(),
        ));
        let input = TestMemoryExec::try_new_exec(
            std::slice::from_ref(&batches),
            Arc::clone(&input_schema),
            None,
        )?;
        let starved = aggregate(AggregateMode::Final, &["a", "b"], input, &raw_schema)?;
        let stream = starved.execute(0, task_ctx(Some(Arc::clone(&pool)))?)?;
        let (actual, writer_spills) = read_as_shuffle_writer(stream, &pool).await?;
        assert!(spill_count(&starved) > 0, "the aggregate must spill");
        assert!(writer_spills > 0, "the writer must spill");
        assert_eq!(sorted_output(&starved.schema(), &actual)?, expected);
        drop(starved);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

/// Use a small test-only offset threshold with an ample memory pool: memory pressure
/// must not be the reason these tables emit or spill. The physical offset guard and
/// stream state transitions are identical to the production i32 threshold.
#[tokio::test]
async fn group_key_offset_budget_emits_or_spills_without_losing_groups() -> Result<()> {
    use arrow::array::{ArrayRef, ListBuilder, StringBuilder};
    for nested in [false, true] {
        let key_type = if nested {
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        } else {
            DataType::Utf8
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", key_type, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut batches = Vec::new();
        for batch in 0..8 {
            let strings: Vec<_> = (0..512)
                .map(|row| format!("{:064}", (batch * 512 + row) % 2400))
                .collect();
            let key: ArrayRef = if nested {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for value in &strings {
                    builder.values().append_value(value);
                    builder.append(true);
                }
                Arc::new(builder.finish())
            } else {
                Arc::new(StringArray::from(strings))
            };
            batches.push(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt32Array::from(vec![0; 512])),
                    key,
                    Arc::new(Int64Array::from(vec![1; 512])),
                ],
            )?);
        }
        for keys in [&["b"][..], &["a", "b"][..]] {
            // 0 = unordered, 1 = ordered prefix, 2 = all grouping keys ordered.
            let make_input = |source: &[RecordBatch],
                              order: usize|
             -> Result<Arc<dyn ExecutionPlan>> {
                let schema = source[0].schema();
                let (source, ordering) = if order == 2 {
                    let batch = concat_batches(&schema, source)?;
                    let columns = keys
                        .iter()
                        .map(|key| {
                            Ok(SortColumn {
                                values: Arc::clone(batch.column(schema.index_of(key)?)),
                                options: None,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let indices = lexsort_to_indices(&columns, None)?;
                    let batch = take_record_batch(&batch, &indices)?;
                    let batches = (0..batch.num_rows())
                        .step_by(512)
                        .map(|start| {
                            batch.slice(start, 512.min(batch.num_rows() - start))
                        })
                        .collect();
                    let order = keys
                        .iter()
                        .map(|key| col(key, &schema).map(PhysicalSortExpr::new_default))
                        .collect::<Result<Vec<_>>>()?;
                    (batches, vec![LexOrdering::new(order).unwrap()])
                } else {
                    (
                        source.to_vec(),
                        if order == 1 {
                            sorted_on(&schema, "a")?
                        } else {
                            vec![]
                        },
                    )
                };
                let exec = TestMemoryExec::try_new(&[source], schema, None)?
                    .try_with_sort_information(ordering)?;
                Ok(Arc::new(TestMemoryExec::update_cache(&Arc::new(exec))))
            };
            let expected_agg = aggregate(
                AggregateMode::Single,
                keys,
                make_input(&batches, 0)?,
                &schema,
            )?;
            let expected = collect(expected_agg.execute(0, task_ctx(None)?)?).await?;
            let expected = sorted_output(&expected_agg.schema(), &expected)?;
            for mode in [
                AggregateMode::Partial,
                AggregateMode::Final,
                AggregateMode::Single,
            ] {
                for order in 0..=2 {
                    if order == 1 && keys.len() != 2 {
                        continue;
                    }
                    let source = if mode == AggregateMode::Final {
                        let mut states = vec![];
                        for batch in &batches {
                            let partial = aggregate(
                                AggregateMode::Partial,
                                keys,
                                make_input(std::slice::from_ref(batch), 0)?,
                                &schema,
                            )?;
                            states.extend(
                                collect(partial.execute(0, task_ctx(None)?)?).await?,
                            );
                        }
                        states
                    } else {
                        batches.clone()
                    };
                    for migrated in [true, false] {
                        let mut tested = Arc::unwrap_or_clone(aggregate(
                            mode,
                            keys,
                            make_input(&source, order)?,
                            &schema,
                        )?);
                        tested.group_key_offset_limit = 96 * 1024;
                        let pool: Arc<dyn MemoryPool> =
                            Arc::new(GreedyMemoryPool::new(64 << 20));
                        let context = Arc::try_unwrap(task_ctx(Some(Arc::clone(&pool)))?)
                            .unwrap_or_else(|_| unreachable!("fresh task context"));
                        let config = context.session_config().clone().set_bool(
                            "datafusion.execution.enable_migration_aggregate",
                            migrated,
                        );
                        let context = Arc::new(context.with_session_config(config));
                        if !migrated {
                            assert!(matches!(
                                tested.execute_typed(0, &context)?,
                                StreamType::GroupedHash(_)
                            ));
                        }
                        let output = collect(tested.execute(0, context)?).await?;
                        if order != 2 {
                            if mode != AggregateMode::Partial {
                                assert!(
                                    spill_count(&tested) > 0,
                                    "offset budget must force spill: {mode:?}, nested={nested}, order={order}, migrated={migrated}, keys={keys:?}"
                                );
                            } else {
                                assert!(
                                    output
                                        .iter()
                                        .map(RecordBatch::num_rows)
                                        .sum::<usize>()
                                        > 2400,
                                    "offset budget must force early emission"
                                );
                            }
                        }
                        let actual = if mode == AggregateMode::Partial {
                            let final_agg = aggregate(
                                AggregateMode::Final,
                                keys,
                                make_input(&output, 0)?,
                                &schema,
                            )?;
                            collect(final_agg.execute(0, task_ctx(None)?)?).await?
                        } else {
                            output
                        };
                        assert_eq!(
                            sorted_output(&expected_agg.schema(), &actual)?,
                            expected,
                            "{mode:?}, nested={nested}, order={order}, migrated={migrated}, keys={keys:?}"
                        );
                        assert_eq!(pool.reserved(), 0);
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn fully_ordered_final_refuses_ungranted_variable_emit_headroom() -> Result<()> {
    use datafusion_functions_aggregate::min_max::min_udaf;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt32, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(0..32)),
            Arc::new(StringArray::from_iter_values(
                (0..32).map(|row| format!("{row:064}")),
            )),
        ],
    )?;
    let min = Arc::new(
        AggregateExprBuilder::new(min_udaf(), vec![col("value", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("min_value")
            .build()?,
    );
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let group_by =
        PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".into())]);
    let partial = AggregateExec::try_new(
        AggregateMode::Partial,
        group_by,
        vec![Arc::clone(&min)],
        vec![None],
        input,
        Arc::clone(&schema),
    )?;
    let states = collect(partial.execute(0, task_ctx(None)?)?).await?;
    let state_schema = partial.schema();
    let input = TestMemoryExec::try_new(&[states], Arc::clone(&state_schema), None)?
        .try_with_sort_information(sorted_on(&state_schema, "key")?)?;
    let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
    let group_by =
        PhysicalGroupBy::new_single(vec![(col("key", &state_schema)?, "key".into())]);
    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        group_by,
        vec![min],
        vec![None],
        input,
        schema,
    )?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let context = task_ctx(Some(Arc::clone(&pool)))?;
    assert!(matches!(
        final_agg.execute_typed(0, &context)?,
        StreamType::OrderedFinalAggregate(_)
    ));
    let result = collect(final_agg.execute(0, context)?).await;
    assert!(
        matches!(
            result.unwrap_err().find_root(),
            datafusion_common::DataFusionError::ResourcesExhausted(_)
        ),
        "new output allocation must not be funded by infallible accounting"
    );
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn ordered_final_retains_variable_emit_workspace_until_range_is_drained()
-> Result<()> {
    use arrow::array::Array;
    use datafusion_functions_aggregate::min_max::min_udaf;
    use std::collections::HashSet;
    let rows = 8192u32;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt32, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("{row:064}")),
            )),
        ],
    )?;
    let min = Arc::new(
        AggregateExprBuilder::new(min_udaf(), vec![col("value", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("min_value")
            .build()?,
    );
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let group_by =
        PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".into())]);
    let partial = AggregateExec::try_new(
        AggregateMode::Partial,
        group_by,
        vec![Arc::clone(&min)],
        vec![None],
        input,
        Arc::clone(&schema),
    )?;
    let states = collect(partial.execute(0, task_ctx(None)?)?).await?;
    let state_schema = partial.schema();
    let states = concat_batches(&state_schema, &states)?;
    let input =
        TestMemoryExec::try_new(&[vec![states]], Arc::clone(&state_schema), None)?
            .try_with_sort_information(sorted_on(&state_schema, "key")?)?;
    let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
    let group_by =
        PhysicalGroupBy::new_single(vec![(col("key", &state_schema)?, "key".into())]);
    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        group_by,
        vec![min],
        vec![None],
        input,
        schema,
    )?;
    let limit = 16 << 20;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
    let context = task_ctx(Some(Arc::clone(&pool)))?;
    let mut stream = final_agg.execute(0, context)?;
    let first = stream
        .next()
        .await
        .transpose()?
        .expect("first completed groups");
    assert!(first.num_rows() < rows as usize);
    let granted = pool.reserved();
    assert!(granted > 0);
    let hog = MemoryConsumer::new("competing JVM writer").register(&pool);
    hog.try_grow(limit - granted)?;
    let mut actual = HashSet::new();
    let check = |batch: &RecordBatch, actual: &mut HashSet<u32>| {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(actual.insert(keys.value(row)), "duplicate completed key");
            assert_eq!(values.value(row), format!("{:064}", keys.value(row)));
        }
    };
    check(&first, &mut actual);
    while let Some(batch) = stream.next().await.transpose()? {
        check(&batch, &mut actual);
        if actual.len() < rows as usize - 1 {
            assert_eq!(
                pool.reserved(),
                limit,
                "retain the materialization workspace until the completed range drains"
            );
        }
    }
    assert_eq!(actual.len(), rows as usize);
    drop(stream);
    drop(hog);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn fully_ordered_partial_first_last_emits_states_when_emit_headroom_is_refused()
-> Result<()> {
    use datafusion_expr::AggregateUDF;
    use datafusion_functions_aggregate::first_last::{FirstValue, LastValue};
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let batches = (0..4)
        .map(|batch| {
            let values: Vec<Option<String>> = (0..32)
                .map(|row| {
                    if (batch == 0 && row == 0) || (batch == 3 && row == 31) {
                        None
                    } else {
                        Some(format!("batch-{batch}-row-{row:02}-{}", "x".repeat(4096)))
                    }
                })
                .collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt32Array::from(vec![batch / 2; 32])),
                    Arc::new(StringArray::from(values)),
                ],
            )
            .map_err(datafusion_common::DataFusionError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    for ignore_nulls in [false, true] {
        let expressions = vec![
            Arc::new(
                AggregateExprBuilder::new(
                    Arc::new(AggregateUDF::new_from_impl(FirstValue::new())),
                    vec![col("value", &schema)?],
                )
                .schema(Arc::clone(&schema))
                .alias("first_value")
                .with_ignore_nulls(ignore_nulls)
                .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(
                    Arc::new(AggregateUDF::new_from_impl(LastValue::new())),
                    vec![col("value", &schema)?],
                )
                .schema(Arc::clone(&schema))
                .alias("last_value")
                .with_ignore_nulls(ignore_nulls)
                .build()?,
            ),
        ];
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".into())]);
        let source = TestMemoryExec::try_new_exec(
            std::slice::from_ref(&batches),
            Arc::clone(&schema),
            None,
        )?;
        let reference = AggregateExec::try_new(
            AggregateMode::Single,
            group_by.clone(),
            expressions.clone(),
            vec![None; 2],
            source,
            Arc::clone(&schema),
        )?;
        let expected = collect(reference.execute(0, task_ctx(None)?)?).await?;
        let input = TestMemoryExec::try_new(
            std::slice::from_ref(&batches),
            Arc::clone(&schema),
            None,
        )?
        .try_with_sort_information(sorted_on(&schema, "key")?)?;
        let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            expressions.clone(),
            vec![None; 2],
            input,
            Arc::clone(&schema),
        )?;
        let state_schema = partial.schema();
        let secondary_order = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(col("key", &state_schema)?),
            PhysicalSortExpr::new(
                col(state_schema.field(1).name(), &state_schema)?,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ])
        .unwrap();
        assert!(
            partial
                .properties()
                .equivalence_properties()
                .constraints()
                .is_empty()
        );
        assert!(
            partial
                .properties()
                .equivalence_properties()
                .ordering_satisfy(sorted_on(&state_schema, "key")?.remove(0))?
        );
        assert!(
            !partial
                .properties()
                .equivalence_properties()
                .ordering_satisfy(secondary_order.clone())?
        );
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(if ignore_nulls {
                12 << 10
            } else {
                6 << 10
            }));
        let context = task_ctx(Some(Arc::clone(&pool)))?;
        assert!(matches!(
            partial.execute_typed(0, &context)?,
            StreamType::OrderedPartialAggregate(_)
        ));
        let states = collect(partial.execute(0, context)?).await?;
        assert!(
            states.iter().map(RecordBatch::num_rows).sum::<usize>() > 2,
            "two keys repeat only through pressure-triggered partial emission"
        );
        assert_eq!(pool.reserved(), 0);
        let state_batch = concat_batches(&state_schema, &states)?;
        let requested_order = lexsort_to_indices(
            &[
                SortColumn {
                    values: Arc::clone(state_batch.column(0)),
                    options: None,
                },
                SortColumn {
                    values: Arc::clone(state_batch.column(1)),
                    options: Some(SortOptions {
                        descending: true,
                        nulls_first: false,
                    }),
                },
            ],
            None,
        )?;
        assert_ne!(
            state_batch,
            take_record_batch(&state_batch, &requested_order)?,
            "actual duplicate-key states do not satisfy the falsely inferred secondary ordering"
        );
        let state_input =
            TestMemoryExec::try_new(&[states], Arc::clone(&state_schema), None)?
                .try_with_sort_information(sorted_on(&state_schema, "key")?)?;
        let state_input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::update_cache(&Arc::new(state_input)));
        let final_group_by =
            PhysicalGroupBy::new_single(vec![(col("key", &state_schema)?, "key".into())]);
        let partial_reduce = AggregateExec::try_new(
            AggregateMode::PartialReduce,
            final_group_by.clone(),
            expressions.clone(),
            vec![None; 2],
            Arc::clone(&state_input),
            Arc::clone(&schema),
        )?;
        assert!(
            partial_reduce
                .properties()
                .equivalence_properties()
                .constraints()
                .is_empty()
        );
        assert!(
            !partial_reduce
                .properties()
                .equivalence_properties()
                .ordering_satisfy(secondary_order)?
        );
        let final_aggregate = AggregateExec::try_new(
            AggregateMode::Final,
            final_group_by,
            expressions,
            vec![None; 2],
            state_input,
            Arc::clone(&schema),
        )?;
        let actual = collect(final_aggregate.execute(0, task_ctx(None)?)?).await?;
        assert!(final_aggregate.properties().equivalence_properties().constraints().iter().any(
            |constraint| matches!(constraint, Constraint::Unique(keys) if keys.as_slice() == [0])
        ));
        assert_eq!(
            sorted_output(&final_aggregate.schema(), &actual)?,
            sorted_output(&reference.schema(), &expected)?,
            "ignore_nulls={ignore_nulls}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn grouping_sets_do_not_inherit_unique_keys_constants_or_ordering() -> Result<()> {
    // A first aggregate provides a genuine UNIQUE(a,b) source constraint. NULL b
    // makes (a,b) and (a) produce identical visible keys, distinguished only by GID.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )?;
    let source = TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
        .try_with_sort_information(sorted_on(&schema, "a")?)?;
    let source = Arc::new(TestMemoryExec::update_cache(&Arc::new(source)));
    let unique_source = aggregate(AggregateMode::Single, &["a", "b"], source, &schema)?;
    let source_schema = unique_source.schema();
    let count = Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("count_v", &source_schema)?])
            .schema(Arc::clone(&source_schema))
            .alias("count")
            .build()?,
    );
    let keys = PhysicalGroupBy::new(
        vec![
            (col("a", &source_schema)?, "a".into()),
            (col("b", &source_schema)?, "b".into()),
        ],
        vec![
            (lit(ScalarValue::UInt32(None)), "a".into()),
            (lit(ScalarValue::Utf8(None)), "b".into()),
        ],
        vec![vec![false, false], vec![false, true]],
        true,
    );
    let expanded = AggregateExec::try_new(
        AggregateMode::Single,
        keys,
        vec![count],
        vec![None],
        unique_source,
        source_schema,
    )?;
    assert_eq!(
        expanded
            .properties()
            .equivalence_properties()
            .constraints()
            .to_vec(),
        vec![Constraint::Unique(vec![0, 1, 2])]
    );
    assert!(
        expanded
            .properties()
            .equivalence_properties()
            .output_ordering()
            .is_none()
    );
    let output = collect(expanded.execute(0, task_ctx(None)?)?).await?;
    let output = concat_batches(&expanded.schema(), &output)?;
    assert_eq!(output.num_rows(), 4);
    let keys = output
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(
        (0..output.num_rows())
            .filter(|&row| keys.value(row) == 1)
            .count(),
        2
    );
    Ok(())
}
