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
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::pretty::pretty_format_batches;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation, TrackConsumersPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
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
