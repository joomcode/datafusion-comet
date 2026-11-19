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

use arrow::array::builder::{BooleanBuilder, Int64Builder, StringBuilder};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::bool_and_or::bool_or_udaf;
use datafusion::functions_aggregate::first_last::{FirstValue, LastValue};
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_comet_spark_expr::SparkFirstLast;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

const NUM_ROWS: usize = 10_000_000;
const NUM_GROUPS: u64 = 1_000_000;
const BATCH_SIZE: usize = 8192;

fn batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("i", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("f", DataType::Boolean, false),
    ]));
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut out = Vec::new();
    let mut row = 0;
    while row < NUM_ROWS {
        let n = BATCH_SIZE.min(NUM_ROWS - row);
        let mut k = Int64Builder::with_capacity(n);
        let mut i = Int64Builder::with_capacity(n);
        let mut s = StringBuilder::with_capacity(n, n * 16);
        let mut b = BooleanBuilder::with_capacity(n);
        let mut f = BooleanBuilder::with_capacity(n);
        for _ in 0..n {
            let r = next();
            k.append_value((r % NUM_GROUPS) as i64);
            let null = (r >> 40) % 5 == 0;
            if null {
                i.append_null();
                s.append_null();
                b.append_null();
            } else {
                i.append_value((r >> 20) as i64);
                s.append_value(format!("user-{}", (r >> 24) % 100_000));
                b.append_value((r >> 50) % 2 == 0);
            }
            f.append_value((r >> 33) % 2 == 0);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(k.finish()),
            Arc::new(i.finish()),
            Arc::new(s.finish()),
            Arc::new(b.finish()),
            Arc::new(f.finish()),
        ];
        out.push(RecordBatch::try_new(Arc::clone(&schema), columns).unwrap());
        row += n;
    }
    out
}

fn task_context() -> TaskContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_ratio_threshold = 1.1;
    TaskContext::default().with_session_config(config)
}

async fn run(
    partitions: &[Vec<RecordBatch>],
    udaf: Arc<AggregateUDF>,
    column: &str,
    ignore_nulls: bool,
    filtered: bool,
) -> usize {
    let schema = partitions[0][0].schema();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
    )));
    let index = schema.index_of(column).unwrap();
    let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new(column, index));
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
    let filter: Arc<dyn PhysicalExpr> = Arc::new(Column::new("f", 4));
    let aggr = AggregateExprBuilder::new(udaf, vec![value])
        .schema(Arc::clone(&schema))
        .alias("a")
        .with_ignore_nulls(ignore_nulls)
        .build()
        .unwrap();
    let aggregate = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::new_single(vec![(key, "k".to_string())]),
        vec![aggr.into()],
        vec![filtered.then_some(filter)],
        scan,
        schema,
    )
    .unwrap();
    let mut stream = aggregate.execute(0, Arc::new(task_context())).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    rows
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions = vec![batches()];
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("first_last_10m_rows_1m_groups");

    let cases: Vec<(&str, Arc<AggregateUDF>, &str, bool, bool)> = vec![
        (
            "first_ignore_nulls_filter_i64_adapter",
            Arc::new(AggregateUDF::new_from_impl(FirstValue::new())),
            "i",
            true,
            true,
        ),
        (
            "first_ignore_nulls_filter_i64_groups",
            Arc::new(AggregateUDF::new_from_impl(SparkFirstLast::first())),
            "i",
            true,
            true,
        ),
        (
            "last_ignore_nulls_filter_utf8_adapter",
            Arc::new(AggregateUDF::new_from_impl(LastValue::new())),
            "s",
            true,
            true,
        ),
        (
            "last_ignore_nulls_filter_utf8_groups",
            Arc::new(AggregateUDF::new_from_impl(SparkFirstLast::last())),
            "s",
            true,
            true,
        ),
        (
            "first_utf8_adapter",
            Arc::new(AggregateUDF::new_from_impl(FirstValue::new())),
            "s",
            false,
            false,
        ),
        (
            "first_utf8_groups",
            Arc::new(AggregateUDF::new_from_impl(SparkFirstLast::first())),
            "s",
            false,
            false,
        ),
        ("max_boolean_adapter", max_udaf(), "b", false, false),
        ("max_boolean_bool_or", bool_or_udaf(), "b", false, false),
    ];

    for (name, udaf, column, ignore_nulls, filtered) in cases {
        let rows = rt.block_on(run(
            &partitions,
            Arc::clone(&udaf),
            column,
            ignore_nulls,
            filtered,
        ));
        assert!(rows > 0, "{name} produced no rows");
        println!("{name}: {rows} output rows");
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| {
                run(
                    &partitions,
                    Arc::clone(&udaf),
                    column,
                    ignore_nulls,
                    filtered,
                )
            })
        });
    }
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(20))
        .warm_up_time(Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
