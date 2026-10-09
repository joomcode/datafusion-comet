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

//! `SortedWindowExec` against DataFusion's `BoundedWindowAggExec` on sorted input with tiny
//! window partitions, for `LEAD` and `ROW_NUMBER`, over narrow and wide nested rows.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use comet::execution::operators::{SortedWindowExec, SortedWindowFunction};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::functions_window::lead_lag::lead_udwf;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::{WindowFrame, WindowFunctionDefinition};
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::windows::{create_window_expr, BoundedWindowAggExec};
use datafusion::physical_plan::{collect, ExecutionPlan, InputOrderMode};
use datafusion::prelude::SessionContext;
use tokio::runtime::Runtime;

const ROWS_PER_BATCH: usize = 8192;
const BATCHES: usize = 8;
const WIDE_COLUMNS: usize = 8;

#[derive(Clone, Copy)]
enum Function {
    Lead,
    RowNumber,
}

fn schema(wide: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ];
    if wide {
        for i in 0..WIDE_COLUMNS {
            fields.push(Field::new(format!("s{i}"), DataType::Utf8, true));
            fields.push(Field::new(
                format!("n{i}"),
                DataType::Struct(nested_fields()),
                true,
            ));
        }
    }
    Arc::new(Schema::new(fields))
}

fn nested_fields() -> Fields {
    Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new_list("b", Field::new_list_field(DataType::Utf8, true), true),
    ])
}

fn batches(sizes: &[usize], wide: bool) -> Vec<RecordBatch> {
    let total = ROWS_PER_BATCH * BATCHES;
    let mut keys = Vec::with_capacity(total);
    let mut key = 0i64;
    let mut i = 0;
    while keys.len() < total {
        for _ in 0..sizes[i % sizes.len()] {
            keys.push(key);
        }
        key += 1;
        i += 1;
    }
    keys.truncate(total);
    let schema = schema(wide);
    keys.chunks(ROWS_PER_BATCH)
        .map(|chunk| {
            let n = chunk.len();
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(chunk.to_vec())),
                Arc::new(Int64Array::from_iter_values((0..n as i64).map(|v| v * 7))),
            ];
            if wide {
                for c in 0..WIDE_COLUMNS {
                    columns.push(Arc::new(StringArray::from_iter_values(
                        (0..n).map(|r| format!("value-{c}-{r}-padding")),
                    )));
                    let strings =
                        StringArray::from_iter_values((0..n * 3).map(|r| format!("element-{r}")));
                    let list = ListArray::new(
                        Arc::new(Field::new_list_field(DataType::Utf8, true)),
                        OffsetBuffer::from_lengths(std::iter::repeat_n(3, n)),
                        Arc::new(strings),
                        None,
                    );
                    columns.push(Arc::new(StructArray::new(
                        nested_fields(),
                        vec![
                            Arc::new(Int64Array::from_iter_values(0..n as i64)),
                            Arc::new(list),
                        ],
                        None,
                    )));
                }
            }
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
        })
        .collect()
}

fn input(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr {
            expr: Arc::new(Column::new("key", 0)),
            options: SortOptions::default(),
        },
        PhysicalSortExpr {
            expr: Arc::new(Column::new("ts", 1)),
            options: SortOptions::default(),
        },
    ])
    .unwrap();
    let config = MemorySourceConfig::try_new(&[batches.to_vec()], Arc::clone(schema), None)
        .unwrap()
        .try_with_sort_information(vec![ordering])
        .unwrap();
    Arc::new(DataSourceExec::new(Arc::new(config)))
}

fn plan(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    function: Function,
    sorted: bool,
) -> Arc<dyn ExecutionPlan> {
    let partition_by: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("key", 0))];
    let order_by = vec![PhysicalSortExpr {
        expr: Arc::new(Column::new("ts", 1)),
        options: SortOptions::default(),
    }];
    let ts: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 1));
    let default = ScalarValue::Int64(Some(i64::MAX));
    let (def, name, args) = match function {
        Function::Lead => (
            lead_udwf(),
            "lead",
            vec![
                Arc::clone(&ts),
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>,
                Arc::new(Literal::new(default.clone())),
            ],
        ),
        Function::RowNumber => (row_number_udwf(), "row_number", vec![]),
    };
    let window_expr = create_window_expr(
        &WindowFunctionDefinition::WindowUDF(def),
        name.to_string(),
        &args,
        &partition_by,
        &order_by,
        Arc::new(WindowFrame::new(Some(true))),
        Arc::clone(schema),
        false,
        false,
        None,
    )
    .unwrap();
    let input = input(batches, schema);
    if !sorted {
        return Arc::new(
            BoundedWindowAggExec::try_new(vec![window_expr], input, InputOrderMode::Sorted, true)
                .unwrap(),
        );
    }
    let (function, field): (SortedWindowFunction, FieldRef) = match function {
        Function::Lead => (
            SortedWindowFunction::Shift {
                value: ts,
                offset: 1,
                default,
            },
            window_expr.field().unwrap(),
        ),
        Function::RowNumber => (
            SortedWindowFunction::RowNumber,
            Arc::new(
                window_expr
                    .field()
                    .unwrap()
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Int32),
            ),
        ),
    };
    Arc::new(
        SortedWindowExec::try_new(input, partition_by, order_by, vec![function], vec![field])
            .unwrap(),
    )
}

fn bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sorted_window");
    group.sample_size(10);
    for (shape, sizes) in [("1row", vec![1usize]), ("2.2rows", vec![2, 2, 2, 3, 2])] {
        for wide in [false, true] {
            let schema = schema(wide);
            let data = batches(&sizes, wide);
            for (function, fname) in [
                (Function::Lead, "lead"),
                (Function::RowNumber, "row_number"),
            ] {
                for sorted in [false, true] {
                    let id = format!(
                        "{fname}/{shape}/{}/{}",
                        if wide { "wide" } else { "narrow" },
                        if sorted { "sorted" } else { "bounded" }
                    );
                    group.bench_function(BenchmarkId::from_parameter(id), |b| {
                        b.iter(|| {
                            let plan = plan(&data, &schema, function, sorted);
                            rt.block_on(collect(plan, SessionContext::new().task_ctx()))
                                .unwrap()
                        })
                    });
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
