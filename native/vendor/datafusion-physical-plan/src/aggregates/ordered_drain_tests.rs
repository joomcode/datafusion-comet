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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

use super::{AggregateExec, AggregateMode, PhysicalGroupBy, StreamType};
use crate::common::collect;
use crate::execution_plan::ExecutionPlan;
use crate::test::TestMemoryExec;

const BATCH_SIZE: usize = 256;

fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p", DataType::Utf8, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn raw_batches(
    schema: &SchemaRef,
    prefixes: &[&str],
    rows_per_prefix: usize,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    for (i, prefix) in prefixes.iter().enumerate() {
        for start in (0..rows_per_prefix).step_by(4096) {
            let end = (start + 4096).min(rows_per_prefix);
            let rows = start..end;
            batches.push(RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(StringArray::from_iter_values(
                        rows.clone().map(|_| *prefix),
                    )),
                    Arc::new(StringArray::from_iter_values(rows.clone().map(|r| {
                        format!("ranking-{i}-{:012}", (r * 7919) % rows_per_prefix)
                    }))),
                    Arc::new(Int64Array::from_iter_values(rows.map(|r| r as i64))),
                ],
            )?);
        }
    }
    Ok(batches)
}

fn aggregate(
    mode: AggregateMode,
    input_schema: &SchemaRef,
    raw_schema: &SchemaRef,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<AggregateExec>> {
    let group_by = PhysicalGroupBy::new_single(vec![
        (col("p", input_schema)?, "p".to_string()),
        (col("s", input_schema)?, "s".to_string()),
    ]);
    let count = Arc::new(
        AggregateExprBuilder::new(count_udaf(), vec![col("v", raw_schema)?])
            .schema(Arc::clone(raw_schema))
            .alias("count_v")
            .build()?,
    );
    Ok(Arc::new(AggregateExec::try_new(
        mode,
        group_by,
        vec![count],
        vec![None],
        input,
        Arc::clone(raw_schema),
    )?))
}

fn input(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    sorted: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ordering = if sorted {
        vec![
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("p", schema)?)])
                .unwrap(),
        ]
    } else {
        vec![]
    };
    let exec = TestMemoryExec::try_new(&[batches.to_vec()], Arc::clone(schema), None)?
        .try_with_sort_information(ordering)?;
    Ok(Arc::new(TestMemoryExec::update_cache(&Arc::new(exec))))
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
                    .with_batch_size(BATCH_SIZE)
                    .set_bool("datafusion.execution.enable_migration_aggregate", true)
                    .set_str(
                        "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                        "1.1",
                    ),
            )
            .with_runtime(runtime.build_arc()?),
    ))
}

struct Run {
    elapsed: Duration,
    groups: HashSet<(String, String)>,
    total_count: i64,
    rows: usize,
}

async fn run(
    aggregate: Arc<AggregateExec>,
    pool: Option<Arc<dyn MemoryPool>>,
    expected_stream: fn(&StreamType) -> bool,
    bounded_batches: bool,
) -> Result<Run> {
    let ctx = task_ctx(pool)?;
    assert!(expected_stream(&aggregate.execute_typed(0, &ctx)?));
    let start = Instant::now();
    let output = collect(aggregate.execute(0, ctx)?).await?;
    let elapsed = start.elapsed();
    let mut groups = HashSet::new();
    let mut total_count = 0;
    let mut rows = 0;
    for batch in &output {
        assert!(
            !bounded_batches || batch.num_rows() <= BATCH_SIZE,
            "batch of {} rows",
            batch.num_rows()
        );
        rows += batch.num_rows();
        let p = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            groups.insert((p.value(i).to_string(), s.value(i).to_string()));
            total_count += c.value(i);
        }
    }
    Ok(Run {
        elapsed,
        groups,
        total_count,
        rows,
    })
}

fn is_ordered_partial(stream: &StreamType) -> bool {
    matches!(stream, StreamType::OrderedPartialAggregate(_))
}

fn is_partial_hash(stream: &StreamType) -> bool {
    matches!(stream, StreamType::PartialHash(_))
}

fn is_ordered_final(stream: &StreamType) -> bool {
    matches!(stream, StreamType::OrderedFinalAggregate(_))
}

fn is_any(_: &StreamType) -> bool {
    true
}

#[tokio::test]
async fn ordered_partial_aggregate_drains_a_huge_ordered_range_in_linear_time()
-> Result<()> {
    let schema = raw_schema();
    let rows = 300_000;
    let batches = raw_batches(&schema, &["x"], rows)?;

    let sorted = run(
        aggregate(
            AggregateMode::Partial,
            &schema,
            &schema,
            input(&batches, &schema, true)?,
        )?,
        None,
        is_ordered_partial,
        true,
    )
    .await?;
    let unsorted = run(
        aggregate(
            AggregateMode::Partial,
            &schema,
            &schema,
            input(&batches, &schema, false)?,
        )?,
        None,
        is_partial_hash,
        true,
    )
    .await?;

    assert_eq!(sorted.rows, rows);
    assert_eq!(sorted.groups.len(), rows);
    assert_eq!(sorted.total_count, rows as i64);
    assert_eq!(sorted.groups, unsorted.groups);
    assert!(
        sorted.elapsed < unsorted.elapsed * 5 + Duration::from_millis(500),
        "ordered {:?}, unordered {:?}",
        sorted.elapsed,
        unsorted.elapsed
    );
    Ok(())
}

#[tokio::test]
async fn ordered_partial_reduce_drains_a_huge_ordered_range_in_linear_time() -> Result<()>
{
    let schema = raw_schema();
    let rows = 300_000;
    let batches = raw_batches(&schema, &["x"], rows)?;
    let partial = aggregate(
        AggregateMode::Partial,
        &schema,
        &schema,
        input(&batches, &schema, false)?,
    )?;
    let states = collect(partial.execute(0, task_ctx(None)?)?).await?;
    let state_schema = partial.schema();

    let reduce = |sorted: bool| -> Result<Arc<AggregateExec>> {
        aggregate(
            AggregateMode::PartialReduce,
            &state_schema,
            &schema,
            input(&states, &state_schema, sorted)?,
        )
    };
    let pool = || -> Option<Arc<dyn MemoryPool>> {
        Some(Arc::new(GreedyMemoryPool::new(1 << 30)))
    };
    let sorted = run(reduce(true)?, pool(), is_ordered_final, true).await?;
    let unsorted = run(reduce(false)?, pool(), is_any, true).await?;

    assert_eq!(sorted.rows, rows);
    assert_eq!(sorted.total_count, rows as i64);
    assert_eq!(sorted.groups, unsorted.groups);
    assert!(
        sorted.elapsed < unsorted.elapsed * 5 + Duration::from_millis(500),
        "ordered {:?}, unordered {:?}",
        sorted.elapsed,
        unsorted.elapsed
    );
    Ok(())
}

#[tokio::test]
async fn ordered_partial_aggregate_keeps_drained_groups_when_memory_runs_out()
-> Result<()> {
    let schema = raw_schema();
    let rows = 60_000;
    let batches = raw_batches(&schema, &["a", "b", "c", "d"], rows)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(6 * 1024 * 1024));
    let constrained = run(
        aggregate(
            AggregateMode::Partial,
            &schema,
            &schema,
            input(&batches, &schema, true)?,
        )?,
        Some(Arc::clone(&pool)),
        is_ordered_partial,
        false,
    )
    .await?;

    assert_eq!(constrained.total_count, 4 * rows as i64);
    assert_eq!(constrained.groups.len(), 4 * rows);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
