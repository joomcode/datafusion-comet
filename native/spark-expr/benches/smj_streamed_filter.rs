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

//! Sort-merge join whose filter casts a streamed column, shaped like a Spark
//! `cast(o.completed as timestamp) < b.t AND o.t > b.t` lookup of earlier
//! events: a string key, large key groups on both sides, a wide streamed
//! side and a narrow buffered one. `completed` is either a
//! `yyyy-MM-dd HH:mm:ss` string or a date.

use arrow::array::{
    ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
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
const BASE_MICROS: i64 = 20_000 * MICROS_PER_DAY;
const BATCH_SIZE: usize = 8192;

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn key_of(k: usize) -> String {
    format!("V{k:08}")
}

#[derive(Clone, Copy)]
enum Completed {
    String,
    Date,
}

/// Streamed side: `key`, the event time `t`, `completed` and
/// `payload_cols` payload columns alternating Int64 and Utf8.
///
/// With `all_pass`, `completed` precedes and `t` follows every buffered
/// time of the key; otherwise `completed` lies `window_days` before `t`.
fn build_streamed(
    keys: usize,
    rows_per_key: usize,
    group_days: i64,
    window_days: i64,
    all_pass: bool,
    completed: Completed,
    payload_cols: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let completed_type = match completed {
        Completed::String => DataType::Utf8,
        Completed::Date => DataType::Date32,
    };
    let mut fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("t", timestamp_type(), true),
        Field::new("completed", completed_type, true),
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
    let rows: Vec<(usize, i64, i64)> = (0..num_rows)
        .map(|i| {
            if all_pass {
                let t = BASE_MICROS + (group_days + 1) * MICROS_PER_DAY + i as i64;
                (i / rows_per_key, t, BASE_MICROS - MICROS_PER_DAY)
            } else {
                let day = window_days + (i as i64 * 7919) % (group_days - window_days);
                let t = BASE_MICROS + day * MICROS_PER_DAY + (i as i64 * 104_729) % MICROS_PER_DAY;
                (i / rows_per_key, t, t - window_days * MICROS_PER_DAY)
            }
        })
        .collect();
    let completed_column: ArrayRef = match completed {
        Completed::String => Arc::new(StringArray::from_iter_values(rows.iter().map(|r| {
            chrono::DateTime::from_timestamp_micros(r.2)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        }))),
        Completed::Date => Arc::new(Date32Array::from_iter_values(
            rows.iter().map(|r| r.2.div_euclid(MICROS_PER_DAY) as i32),
        )),
    };
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|r| key_of(r.0)),
        )),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.1))
                .with_timezone("UTC"),
        ),
        completed_column,
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
    (schema, split_batch(&batch, BATCH_SIZE))
}

/// Buffered side: `key`, `t` spread over `group_days` days and `qty`.
fn build_buffered(
    keys: usize,
    group_rows: usize,
    group_days: i64,
    batch_size: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("t", timestamp_type(), true),
        Field::new("qty", DataType::Int64, true),
    ]));
    let num_rows = keys * group_rows;
    let span = group_days * MICROS_PER_DAY;
    let key = StringArray::from_iter_values((0..num_rows).map(|i| key_of(i / group_rows)));
    let t = TimestampMicrosecondArray::from_iter_values(
        (0..num_rows).map(|i| BASE_MICROS + (i % group_rows) as i64 * span / group_rows as i64),
    )
    .with_timezone("UTC");
    let qty = Int64Array::from_iter_values((0..num_rows).map(|i| i as i64));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(key), Arc::new(t), Arc::new(qty)],
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

/// `cast(completed as timestamp) < b.t AND o.t > b.t`
fn streamed_cast_filter(streamed: &Schema, buffered: &Schema) -> JoinFilter {
    let completed = Arc::new(Cast::new(
        Arc::new(Column::new("completed", 0)),
        timestamp_type(),
        SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        None,
        None,
    ));
    let buffered_t: Arc<dyn PhysicalExpr> = Arc::new(Column::new("t", 2));
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(BinaryExpr::new(
            completed,
            Operator::Lt,
            Arc::clone(&buffered_t),
        )),
        Operator::And,
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("t", 1)),
            Operator::Gt,
            buffered_t,
        )),
    ));
    JoinFilter::new(
        expression,
        vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            streamed.field(2).clone(),
            streamed.field(1).clone(),
            buffered.field(1).clone(),
        ])),
    )
}

fn make_exec(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn bench_smj_streamed_filter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("smj_streamed_filter");
    group.sample_size(10);
    for (shape, keys, rows_per_key, group_rows, all_pass, completed) in [
        ("few_string", 4, 1000, 2000, false, Completed::String),
        ("few_date", 4, 1000, 2000, false, Completed::Date),
        ("all_string", 4, 200, 2000, true, Completed::String),
    ] {
        let group_days = 400;
        let window_days = 4;
        let (streamed_schema, streamed_batches) = build_streamed(
            keys,
            rows_per_key,
            group_days,
            window_days,
            all_pass,
            completed,
            16,
        );
        let (buffered_schema, buffered_batches) =
            build_buffered(keys, group_rows, group_days, BATCH_SIZE);
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
                            Some(streamed_cast_filter(&streamed_schema, &buffered_schema)),
                            join_type,
                            vec![SortOptions::default()],
                            NullEquality::NullEqualsNothing,
                        )
                        .unwrap();
                        let task_ctx =
                            Arc::new(TaskContext::default().with_session_config(
                                SessionConfig::new().with_batch_size(BATCH_SIZE),
                            ));
                        let rows: usize = rt.block_on(async {
                            let batches = collect(Arc::new(join), task_ctx).await.unwrap();
                            batches.iter().map(|b| b.num_rows()).sum()
                        });
                        if all_pass {
                            assert_eq!(rows, pairs);
                        } else {
                            assert!(rows > keys * rows_per_key && rows < pairs / 20);
                        }
                        rows
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_smj_streamed_filter);
criterion_main!(benches);
