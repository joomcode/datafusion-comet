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

//! Criterion benchmarks for Sort Merge Join
//!
//! These benchmarks measure the join kernel in isolation by feeding
//! pre-sorted RecordBatches directly into SortMergeJoinExec, avoiding
//! sort / scan overhead.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinSide, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, col};
use datafusion_physical_plan::collect;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{SortMergeJoinExec, utils::JoinOn};
use datafusion_physical_plan::test::TestMemoryExec;
use tokio::runtime::Runtime;

/// Build pre-sorted RecordBatches (split into ~8192-row chunks).
///
/// Schema: (key: Int64, data: Int64, payload: Utf8)
///
/// `key_mod` controls distinct key count: key = row_index % key_mod.
fn build_sorted_batches(
    num_rows: usize,
    key_mod: usize,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let mut rows: Vec<(i64, i64)> = (0..num_rows)
        .map(|i| ((i % key_mod) as i64, i as i64))
        .collect();
    rows.sort();

    let keys: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let data: Vec<i64> = rows.iter().map(|(_, d)| *d).collect();
    let payload: Vec<String> = data.iter().map(|d| format!("val_{d}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(data)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap();

    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

fn make_exec(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> Arc<dyn datafusion_physical_plan::ExecutionPlan> {
    TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("data", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn do_join(
    left: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    right: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    join_type: datafusion_common::JoinType,
    rt: &Runtime,
) -> usize {
    let on: JoinOn = vec![(
        col("key", &left.schema()).unwrap(),
        col("key", &right.schema()).unwrap(),
    )];
    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        None,
        join_type,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )
    .unwrap();

    let task_ctx = Arc::new(TaskContext::default());
    rt.block_on(async {
        let batches = collect(Arc::new(join), task_ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn bench_smj(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    let mut group = c.benchmark_group("sort_merge_join");

    // 1:1 Inner Join — 100K rows each, unique keys
    // Best case for contiguous-range optimization: every index array is [0,1,2,...].
    {
        let n = 100_000;
        let left_batches = build_sorted_batches(n, n, &s);
        let right_batches = build_sorted_batches(n, n, &s);
        group.bench_function(BenchmarkId::new("inner_1to1", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, datafusion_common::JoinType::Inner, &rt)
            })
        });
    }

    // 1:10 Inner Join — 100K left, 100K right, 10K distinct keys
    {
        let n = 100_000;
        let key_mod = 10_000;
        let left_batches = build_sorted_batches(n, key_mod, &s);
        let right_batches = build_sorted_batches(n, key_mod, &s);
        group.bench_function(BenchmarkId::new("inner_1to10", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, datafusion_common::JoinType::Inner, &rt)
            })
        });
    }

    // Left Join — 100K each, ~5% unmatched on left
    {
        let n = 100_000;
        let left_batches = build_sorted_batches(n, n + n / 20, &s);
        let right_batches = build_sorted_batches(n, n, &s);
        group.bench_function(BenchmarkId::new("left_1to1_unmatched", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, datafusion_common::JoinType::Left, &rt)
            })
        });
    }

    // Left Semi Join — 100K left, 100K right, 10K keys
    {
        let n = 100_000;
        let key_mod = 10_000;
        let left_batches = build_sorted_batches(n, key_mod, &s);
        let right_batches = build_sorted_batches(n, key_mod, &s);
        group.bench_function(BenchmarkId::new("left_semi_1to10", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, datafusion_common::JoinType::LeftSemi, &rt)
            })
        });
    }

    // Left Anti Join — 100K left, 100K right, partial match
    {
        let n = 100_000;
        let left_batches = build_sorted_batches(n, n + n / 5, &s);
        let right_batches = build_sorted_batches(n, n, &s);
        group.bench_function(BenchmarkId::new("left_anti_partial", n), |b| {
            b.iter(|| {
                let left = make_exec(&left_batches, &s);
                let right = make_exec(&right_batches, &s);
                do_join(left, right, datafusion_common::JoinType::LeftAnti, &rt)
            })
        });
    }

    group.finish();
}

/// Streamed side of the validity-interval join: `key`, the lookup time `t`
/// and `payload_cols` payload columns alternating Int64 and Utf8.
fn build_interval_streamed(
    keys: usize,
    rows_per_key: usize,
    group_rows: usize,
    payload_cols: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new("t", DataType::Int64, false),
    ];
    for c in 0..payload_cols {
        let data_type = if c % 2 == 0 {
            DataType::Int64
        } else {
            DataType::Utf8
        };
        fields.push(Field::new(format!("p{c}"), data_type, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let num_rows = keys * rows_per_key;
    let key: Vec<i64> = (0..num_rows).map(|i| (i / rows_per_key) as i64).collect();
    let t: Vec<i64> = (0..num_rows)
        .map(|i| ((i * 7919) % (group_rows * 10)) as i64 + 1)
        .collect();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(key)),
        Arc::new(Int64Array::from(t)),
    ];
    for c in 0..payload_cols {
        if c % 2 == 0 {
            columns.push(Arc::new(Int64Array::from_iter_values(
                (0..num_rows).map(|i| (i * c) as i64),
            )));
        } else {
            columns.push(Arc::new(StringArray::from_iter_values(
                (0..num_rows).map(|i| format!("payload_{c}_{i:012}")),
            )));
        }
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    (schema, split_batch(&batch, 8192))
}

/// Buffered side of the validity-interval join: `group_rows` consecutive
/// intervals `(eff, next_eff]` per key.
fn build_interval_buffered(
    keys: usize,
    group_rows: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("eff", DataType::Int64, false),
        Field::new("next_eff", DataType::Int64, false),
        Field::new("rate", DataType::Utf8, false),
    ]));
    let num_rows = keys * group_rows;
    let key: Vec<i64> = (0..num_rows).map(|i| (i / group_rows) as i64).collect();
    let eff: Vec<i64> = (0..num_rows)
        .map(|i| ((i % group_rows) * 10) as i64)
        .collect();
    let next_eff: Vec<i64> = eff.iter().map(|e| e + 10).collect();
    let rate = StringArray::from_iter_values((0..num_rows).map(|i| format!("rate_{i}")));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(key)),
            Arc::new(Int64Array::from(eff)),
            Arc::new(Int64Array::from(next_eff)),
            Arc::new(rate),
        ],
    )
    .unwrap();
    (schema, split_batch(&batch, 8192))
}

fn split_batch(batch: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    (0..batch.num_rows())
        .step_by(batch_size)
        .map(|offset| batch.slice(offset, (batch.num_rows() - offset).min(batch_size)))
        .collect()
}

/// `streamed.t > buffered.eff AND streamed.t <= buffered.next_eff`
fn interval_filter(streamed: &Schema, buffered: &Schema) -> JoinFilter {
    let t = Arc::new(Column::new("t", 0));
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(
            Arc::clone(&t) as _,
            Operator::Gt,
            Arc::new(Column::new("eff", 1)),
        )),
        Operator::And,
        Arc::new(BinaryExpr::new(
            t,
            Operator::LtEq,
            Arc::new(Column::new("next_eff", 2)),
        )),
    ));
    JoinFilter::new(
        expression,
        vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            streamed.field(1).clone(),
            buffered.field(1).clone(),
            buffered.field(2).clone(),
        ])),
    )
}

/// Validity-interval join where every streamed row passes the filter for one
/// buffered row of its key group: over large key groups, where almost every
/// pair fails, and over unique keys, where every pair passes.
fn bench_smj_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sort_merge_join_filter");
    group.sample_size(10);
    for (shape, keys, rows_per_key, group_rows) in [
        ("interval", 4, 100, 11_000),
        ("interval_unique", 100_000, 1, 1),
    ] {
        let (streamed_schema, streamed_batches) =
            build_interval_streamed(keys, rows_per_key, group_rows, 98);
        let (buffered_schema, buffered_batches) =
            build_interval_buffered(keys, group_rows);
        let pairs = keys * rows_per_key * group_rows;
        for join_type in [
            datafusion_common::JoinType::Left,
            datafusion_common::JoinType::Full,
            datafusion_common::JoinType::Inner,
        ] {
            group.bench_function(
                BenchmarkId::new(format!("{shape}_{join_type:?}"), pairs),
                |b| {
                    b.iter(|| {
                        let left = make_exec(&streamed_batches, &streamed_schema);
                        let right = make_exec(&buffered_batches, &buffered_schema);
                        let on: JoinOn = vec![(
                            col("key", &streamed_schema).unwrap(),
                            col("key", &buffered_schema).unwrap(),
                        )];
                        let join = SortMergeJoinExec::try_new(
                            left,
                            right,
                            on,
                            Some(interval_filter(&streamed_schema, &buffered_schema)),
                            join_type,
                            vec![SortOptions::default()],
                            NullEquality::NullEqualsNothing,
                        )
                        .unwrap();
                        let task_ctx = Arc::new(TaskContext::default());
                        let rows: usize = rt.block_on(async {
                            let batches =
                                collect(Arc::new(join), task_ctx).await.unwrap();
                            batches.iter().map(|b| b.num_rows()).sum()
                        });
                        if join_type != datafusion_common::JoinType::Full {
                            assert_eq!(rows, keys * rows_per_key);
                        }
                        rows
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_smj, bench_smj_filter);
criterion_main!(benches);
