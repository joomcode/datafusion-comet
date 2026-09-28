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

//! Isolate the payload copies in sorting wide binary rows. This is not an end-to-end
//! sort benchmark: key comparisons, reservations, spill and JVM conversion are excluded.
//! Both pipelines return identical, materialized Binary arrays. The view pipeline must
//! pay for conversion at both boundaries; it cannot win by returning a different format.

use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, UInt32Array};
use arrow::compute::{cast, interleave, lexsort_to_indices, take, SortColumn};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROWS: usize = 256;
const RUNS: usize = 4;
const OUTPUT_ROWS: usize = 128;

fn input(width: usize) -> Vec<ArrayRef> {
    (0..RUNS)
        .map(|run| {
            // Distinct values prevent a bogus representation-only equality check.
            let values: Vec<Vec<u8>> = (0..ROWS)
                .map(|row| {
                    let mut value = vec![((row + run) % 251) as u8; width];
                    value[..8].copy_from_slice(&((run * ROWS + row) as u64).to_le_bytes());
                    value
                })
                .collect();
            Arc::new(BinaryArray::from_iter_values(
                values.iter().map(Vec::as_slice),
            )) as ArrayRef
        })
        .collect()
}

fn gather(runs: &[ArrayRef], indices: &[(usize, usize)], materialize: bool) -> Vec<ArrayRef> {
    let arrays: Vec<&dyn Array> = runs.iter().map(|a| a.as_ref()).collect();
    indices
        .chunks(OUTPUT_ROWS)
        .map(|chunk| {
            let output = interleave(&arrays, chunk).unwrap();
            if materialize {
                cast(&output, &DataType::Binary).unwrap()
            } else {
                output
            }
        })
        .collect()
}

fn pipeline(
    input: &[ArrayRef],
    order: &UInt32Array,
    merge_order: &[(usize, usize)],
    views: bool,
) -> Vec<ArrayRef> {
    let sorted: Vec<_> = input
        .iter()
        .map(|array| {
            let array = if views {
                cast(array, &DataType::BinaryView).unwrap()
            } else {
                Arc::clone(array)
            };
            take(&array, order, None).unwrap()
        })
        .collect();
    gather(&sorted, merge_order, views)
}

fn benchmark(c: &mut Criterion) {
    let keys: ArrayRef = Arc::new(Int32Array::from_iter_values((0..ROWS as i32).rev()));
    let columns = vec![SortColumn {
        values: keys,
        options: None,
    }];
    let order = lexsort_to_indices(&columns, None).unwrap();
    let merge_order: Vec<_> = (0..ROWS)
        .flat_map(|row| (0..RUNS).map(move |run| (run, row)))
        .collect();
    let mut keys_group = c.benchmark_group("sort_payload_keys");
    keys_group.bench_function("lexsort_256", |b| {
        b.iter(|| black_box(lexsort_to_indices(black_box(&columns), None).unwrap()))
    });
    keys_group.finish();

    for width in [32, 192 * 1024] {
        let input = input(width);
        let views: Vec<_> = input
            .iter()
            .map(|a| cast(a, &DataType::BinaryView).unwrap())
            .collect();
        let expected = pipeline(&input, &order, &merge_order, false);
        let actual = pipeline(&input, &order, &merge_order, true);
        for (expected, actual) in expected.iter().zip(&actual) {
            assert_eq!(expected.to_data(), actual.to_data());
        }
        drop((expected, actual));

        let mut group = c.benchmark_group(format!("sort_payload_{width}b"));
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(3));
        group.throughput(Throughput::Bytes((ROWS * RUNS * width) as u64));
        for (name, arrays) in [("binary", &input), ("view", &views)] {
            group.bench_function(format!("take/{name}"), |b| {
                b.iter(|| {
                    black_box(
                        arrays
                            .iter()
                            .map(|a| take(a, &order, None).unwrap())
                            .collect::<Vec<_>>(),
                    )
                })
            });
            group.bench_function(format!("interleave/{name}"), |b| {
                b.iter(|| black_box(gather(arrays, &merge_order, false)))
            });
        }
        for (name, use_views) in [("binary", false), ("view_then_binary", true)] {
            group.bench_function(format!("pipeline/{name}"), |b| {
                b.iter(|| black_box(pipeline(black_box(&input), &order, &merge_order, use_views)))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
