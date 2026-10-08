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

//! Sort-merge join with a validity-interval join filter, shaped like a Spark
//! `o.t > cast(r.eff as timestamp) AND o.t <= cast(r.next_eff as timestamp)`
//! lookup: a string key, large buffered key groups spread over several input
//! batches, a wide streamed side, and Spark's date-to-timestamp casts.

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::{JoinSide, JoinType, NullEquality};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::sync::Arc;
use tokio::runtime::Runtime;

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn key_of(k: usize) -> String {
    format!("C{k:06}")
}

/// Streamed side: `key`, the lookup time `t` and `payload_cols` payload
/// columns alternating Int64 and Utf8. Every row falls into one interval of
/// its key group.
fn build_streamed(
    keys: usize,
    rows_per_key: usize,
    group_rows: usize,
    payload_cols: usize,
    batch_size: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("t", timestamp_type(), true),
    ];
    for c in 0..payload_cols {
        let data_type = if c % 2 == 0 {
            DataType::Int64
        } else {
            DataType::Utf8
        };
        fields.push(Field::new(format!("p{c}"), data_type, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let num_rows = keys * rows_per_key;
    let mut rows: Vec<(usize, i64)> = (0..num_rows)
        .map(|i| {
            let day = (i * 7919) % group_rows;
            let offset = ((i * 104_729) as i64 % MICROS_PER_DAY) + 1;
            (i / rows_per_key, day as i64 * MICROS_PER_DAY + offset)
        })
        .collect();
    rows.sort();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|(k, _)| key_of(*k)),
        )),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(rows.iter().map(|(_, t)| *t))
                .with_timezone("UTC"),
        ),
    ];
    for c in 0..payload_cols {
        if c % 2 == 0 {
            columns.push(Arc::new(Int64Array::from_iter_values(
                (0..num_rows).map(|i| (i * c) as i64),
            )));
        } else {
            columns.push(Arc::new(StringArray::from_iter_values(
                (0..num_rows).map(|i| format!("payload_{c}_{i:08}")),
            )));
        }
    }
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    (schema, split_batch(&batch, batch_size))
}

/// Buffered side: `group_rows` consecutive one-day intervals
/// `(eff, next_eff]` per key, as dates.
fn build_buffered(
    keys: usize,
    group_rows: usize,
    batch_size: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("eff", DataType::Date32, true),
        Field::new("next_eff", DataType::Date32, true),
        Field::new("rate", DataType::Decimal128(18, 6), true),
    ]));
    let num_rows = keys * group_rows;
    let key = StringArray::from_iter_values((0..num_rows).map(|i| key_of(i / group_rows)));
    let eff = Date32Array::from_iter_values((0..num_rows).map(|i| (i % group_rows) as i32));
    let next_eff =
        Date32Array::from_iter_values((0..num_rows).map(|i| (i % group_rows) as i32 + 1));
    let rate = Decimal128Array::from_iter_values((0..num_rows).map(|i| i as i128))
        .with_precision_and_scale(18, 6)
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(key),
            Arc::new(eff),
            Arc::new(next_eff),
            Arc::new(rate),
        ],
    )
    .unwrap();
    (schema, split_batch(&batch, batch_size))
}

fn split_batch(batch: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    (0..batch.num_rows())
        .step_by(batch_size)
        .map(|offset| batch.slice(offset, (batch.num_rows() - offset).min(batch_size)))
        .collect()
}

fn to_timestamp(column: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Cast::new(
        Arc::new(Column::new(column, index)),
        timestamp_type(),
        SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        None,
        None,
    ))
}

/// `t > cast(eff as timestamp) AND t <= cast(next_eff as timestamp)`
fn interval_filter(streamed: &Schema, buffered: &Schema) -> JoinFilter {
    let t: Arc<dyn PhysicalExpr> = Arc::new(Column::new("t", 0));
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(
            Arc::clone(&t),
            Operator::Gt,
            to_timestamp("eff", 1),
        )),
        Operator::And,
        Arc::new(BinaryExpr::new(
            t,
            Operator::LtEq,
            to_timestamp("next_eff", 2),
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

fn make_exec(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn bench_smj_interval_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("smj_interval_filter");
    group.sample_size(10);
    for (shape, keys, rows_per_key, group_rows, input_batch_size) in [
        ("group11k_b8192", 4, 100, 11_000, 8192),
        ("group11k_b1024", 4, 100, 11_000, 1024),
        ("unique", 100_000, 1, 1, 8192),
    ] {
        let (streamed_schema, streamed_batches) =
            build_streamed(keys, rows_per_key, group_rows, 98, input_batch_size);
        let (buffered_schema, buffered_batches) =
            build_buffered(keys, group_rows, input_batch_size);
        let pairs = keys * rows_per_key * group_rows;
        for join_type in [JoinType::Left, JoinType::Full, JoinType::Inner] {
            group.bench_function(
                BenchmarkId::new(format!("{shape}_{join_type:?}"), pairs),
                |b| {
                    b.iter(|| {
                        let left = make_exec(&streamed_batches, &streamed_schema);
                        let right = make_exec(&buffered_batches, &buffered_schema);
                        let on = vec![(
                            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
                            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
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
                        let task_ctx = Arc::new(
                            TaskContext::default()
                                .with_session_config(SessionConfig::new().with_batch_size(8192)),
                        );
                        let rows: usize = rt.block_on(async {
                            let batches = collect(Arc::new(join), task_ctx).await.unwrap();
                            batches.iter().map(|b| b.num_rows()).sum()
                        });
                        if join_type != JoinType::Full {
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

criterion_group!(benches, bench_smj_interval_filter);
criterion_main!(benches);
