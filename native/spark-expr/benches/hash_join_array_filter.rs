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

//! Broadcast hash join with a filter over array elements, shaped like
//! gcs_cost_insights_v2's table location lookup: build rows are table
//! locations split into path parts, probe rows are object paths, the join
//! keys are the bucket and the first part, and the filter is
//! `AND` over `i in 1..21` of
//! `loc.parts[i] IS NULL OR loc.parts[i] = obj.parts[i]`.
//! Reports time per candidate pair in the `pairs` throughput.

use arrow::array::{ArrayRef, ListBuilder, RecordBatch, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::common::{JoinSide, JoinType, NullEquality};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column, IsNullExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use datafusion_comet_spark_expr::{create_query_context_map, ListExtract};
use std::sync::Arc;
use tokio::runtime::Runtime;

const DEPTH: usize = 21;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Utf8, false),
        Field::new("first", DataType::Utf8, true),
        Field::new(
            "parts",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

/// `rows` rows of one or `keys` keys, `parts` given per row
fn table(rows: usize, keys: usize, parts_of: impl Fn(usize) -> Vec<String>) -> RecordBatch {
    let mut parts = ListBuilder::new(StringBuilder::new());
    let mut first = vec![];
    for i in 0..rows {
        let p = parts_of(i);
        first.push(format!("k{}", i % keys));
        for s in &p {
            parts.values().append_value(s);
        }
        parts.append(true);
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec!["joom-analytics-ml-infra"; rows])) as ArrayRef,
            Arc::new(StringArray::from(first)),
            Arc::new(parts.finish()),
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("payload-{i}"))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Locations `tables/db{d}/t{t}` (some one level deeper); objects
/// `tables/db{d}/t{t}/date=.../hour=.../part-*.parquet`, so most pairs fail on part 1
/// or 2 and every object has one location.
fn inputs(locations: usize, objects: usize, keys: usize) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let location = |i: usize| {
        let mut p = vec![
            "tables".to_string(),
            format!("analytics_db_{}", i % 97),
            format!("events_table_{}_daily", i / 97),
        ];
        if i.is_multiple_of(5) {
            p.push("v2".to_string());
        }
        p
    };
    let build = table(locations, keys, location);
    let probe: Vec<RecordBatch> = (0..objects.div_ceil(8192))
        .map(|b| {
            let n = 8192.min(objects - b * 8192);
            table(n, keys, |j| {
                let i = (b * 8192 + j) * 7919 % locations;
                let mut p = location(i);
                p.push(format!("date=2026-10-{:02}", j % 28 + 1));
                p.push(format!("hour={:02}", j % 24));
                p.push(format!(
                    "part-{j:05}-6d2f9a1c-0b7e-4c1f-9a39-{:012x}.c000.zstd.parquet",
                    j * 7919
                ));
                p
            })
        })
        .collect();
    (vec![build], probe)
}

fn item(column: usize, i: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(ListExtract::new(
        Arc::new(Column::new("parts", column)),
        lit(i as i32),
        None,
        false,
        false,
        None,
        create_query_context_map(),
    ))
}

/// Over `(loc.parts, obj.parts)`
fn location_filter() -> JoinFilter {
    let mut expr: Option<Arc<dyn PhysicalExpr>> = None;
    for i in 1..DEPTH {
        let term: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(IsNullExpr::new(item(0, i))),
            Operator::Or,
            Arc::new(BinaryExpr::new(item(0, i), Operator::Eq, item(1, i))),
        ));
        expr = Some(match expr {
            None => term,
            Some(e) => Arc::new(BinaryExpr::new(e, Operator::And, term)),
        });
    }
    let parts = schema().field(2).clone();
    JoinFilter::new(
        expr.unwrap(),
        vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![parts.clone(), parts])),
    )
}

fn make_exec(batches: &[RecordBatch]) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[batches.to_vec()], schema(), None).unwrap()
}

fn bench_hash_join_array_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("hash_join_array_filter");
    group.sample_size(10);
    for (shape, locations, objects, keys) in [
        ("skewed_12k_locations", 12_000, 200, 1),
        ("unique_keys", 12_000, 200_000, 12_000),
    ] {
        let (build, probe) = inputs(locations, objects, keys);
        let pairs = (locations / keys) * objects;
        group.throughput(Throughput::Elements(pairs as u64));
        group.bench_function(BenchmarkId::new(shape, pairs), |b| {
            b.iter(|| {
                let s = schema();
                let on = vec![
                    (
                        Arc::new(Column::new_with_schema("bucket", &s).unwrap()) as _,
                        Arc::new(Column::new_with_schema("bucket", &s).unwrap()) as _,
                    ),
                    (
                        Arc::new(Column::new_with_schema("first", &s).unwrap()) as _,
                        Arc::new(Column::new_with_schema("first", &s).unwrap()) as _,
                    ),
                ];
                let join = HashJoinExec::try_new(
                    make_exec(&build),
                    make_exec(&probe),
                    on,
                    Some(location_filter()),
                    &JoinType::Right,
                    None,
                    PartitionMode::Partitioned,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap();
                let task_ctx = Arc::new(
                    TaskContext::default()
                        .with_session_config(SessionConfig::new().with_batch_size(8192)),
                );
                let rows: usize = rt.block_on(async {
                    let batches = collect(Arc::new(join), task_ctx).await.unwrap();
                    batches.iter().map(|b| b.num_rows()).sum()
                });
                assert!(rows >= objects);
                rows
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_hash_join_array_filter);
criterion_main!(benches);
