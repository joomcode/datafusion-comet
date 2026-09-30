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

//! COMET PATCH: `SortExec` over a narrow key and a binary payload of a fixed width,
//! with unbounded memory and with a pool that makes it spill. Also times the least
//! a sort can copy: sort the keys, then gather every payload once. Prints the
//! wall time, the bytes allocated per input byte and the spills of each case.
//!
//! `cargo bench --bench sort_wide_payload [-- <max width>]`

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch};
use arrow::compute::{SortColumn, concat, interleave, lexsort_to_indices};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::config::SpillCompression;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

struct Counting;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATED.fetch_add(new_size.saturating_sub(layout.size()), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const PAYLOAD_BYTES: usize = 256 << 20;
const BATCH_SIZE: usize = 8192;
const INPUT_BATCH_BYTES: usize = 8 << 20;
const RUNS: usize = 3;

#[derive(Clone, Copy)]
enum Keys {
    Long,
    IntLong,
}

impl Keys {
    fn name(self) -> &'static str {
        match self {
            Keys::Long => "i64",
            Keys::IntLong => "i32,i64",
        }
    }
}

fn schema(keys: Keys) -> SchemaRef {
    let mut fields = vec![Field::new("k1", DataType::Int64, false)];
    if let Keys::IntLong = keys {
        fields.insert(0, Field::new("k0", DataType::Int32, true));
    }
    fields.push(Field::new("payload", DataType::Binary, true));
    Arc::new(Schema::new(fields))
}

fn input(keys: Keys, width: usize, per_batch: usize) -> Vec<RecordBatch> {
    let schema = schema(keys);
    let rows = PAYLOAD_BYTES / width;
    let per_batch = match per_batch {
        0 => (INPUT_BATCH_BYTES / width).clamp(16, BATCH_SIZE),
        rows => rows,
    };
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut batches = vec![];
    let mut start = 0;
    while start < rows {
        let len = per_batch.min(rows - start);
        let values: Vec<u64> = (0..len).map(|_| next()).collect();
        let mut columns: Vec<ArrayRef> = vec![];
        if let Keys::IntLong = keys {
            columns.push(Arc::new(Int32Array::from_iter(
                values
                    .iter()
                    .map(|v| (v % 17 != 0).then_some((v % 64) as i32)),
            )));
        }
        columns.push(Arc::new(Int64Array::from_iter_values(
            values.iter().map(|v| (v >> 8) as i64),
        )));
        let payload: Vec<Vec<u8>> = values
            .iter()
            .map(|v| {
                let mut bytes = vec![0; width];
                for (i, chunk) in bytes.chunks_mut(8).enumerate() {
                    let word = if i % 2 == 0 { next() } else { *v };
                    chunk.copy_from_slice(&word.to_le_bytes()[..chunk.len()]);
                }
                bytes
            })
            .collect();
        columns.push(Arc::new(BinaryArray::from_iter_values(
            payload.iter().map(Vec::as_slice),
        )));
        batches.push(RecordBatch::try_new(Arc::clone(&schema), columns).unwrap());
        start += len;
    }
    batches
}

fn ordering(keys: Keys, schema: &SchemaRef) -> LexOrdering {
    let names: &[&str] = match keys {
        Keys::Long => &["k1"],
        Keys::IntLong => &["k0", "k1"],
    };
    LexOrdering::new(
        names
            .iter()
            .map(|name| PhysicalSortExpr::new_default(col(name, schema).unwrap())),
    )
    .unwrap()
}

struct Measurement {
    time: Duration,
    allocated: usize,
    spills: usize,
    spilled_bytes: usize,
    rows: usize,
}

fn sort(
    runtime: &tokio::runtime::Runtime,
    batches: &[RecordBatch],
    keys: Keys,
    memory_limit: Option<usize>,
) -> Result<Measurement, String> {
    let schema = batches[0].schema();
    let mut builder = RuntimeEnvBuilder::new();
    if let Some(limit) = memory_limit {
        builder = builder.with_memory_pool(Arc::new(GreedyMemoryPool::new(limit)));
    }
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(BATCH_SIZE)
                    .with_spill_compression(SpillCompression::Zstd),
            )
            .with_runtime(builder.build_arc().unwrap()),
    );
    let source =
        TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(&schema), None)
            .unwrap();
    let sort = Arc::new(SortExec::new(ordering(keys, &schema), source));
    let before = ALLOCATED.load(Ordering::Relaxed);
    let start = Instant::now();
    let output = runtime
        .block_on(collect(
            Arc::clone(&sort) as Arc<dyn ExecutionPlan>,
            context,
        ))
        .map_err(|e| e.to_string())?;
    let time = start.elapsed();
    let allocated = ALLOCATED.load(Ordering::Relaxed) - before;
    let rows = output.iter().map(RecordBatch::num_rows).sum();
    let metrics = sort.metrics().unwrap();
    Ok(Measurement {
        time,
        allocated,
        spills: metrics.spill_count().unwrap_or(0),
        spilled_bytes: metrics.spilled_bytes().unwrap_or(0),
        rows,
    })
}

fn gather_once(batches: &[RecordBatch], keys: Keys) -> Measurement {
    let schema = batches[0].schema();
    let ordering = ordering(keys, &schema);
    let before = ALLOCATED.load(Ordering::Relaxed);
    let start = Instant::now();
    let columns: Vec<SortColumn> = ordering
        .iter()
        .map(|sort| {
            let arrays: Vec<ArrayRef> = batches
                .iter()
                .map(|batch| sort.evaluate_to_sort_column(batch).unwrap().values)
                .collect();
            let arrays: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
            SortColumn {
                values: concat(&arrays).unwrap(),
                options: Some(sort.options),
            }
        })
        .collect();
    let order = lexsort_to_indices(&columns, None).unwrap();
    let mut position = vec![];
    for (index, batch) in batches.iter().enumerate() {
        position.extend((0..batch.num_rows()).map(|row| (index, row)));
    }
    let indices: Vec<(usize, usize)> = order
        .values()
        .iter()
        .map(|&i| position[i as usize])
        .collect();
    let mut rows = 0;
    for chunk in indices.chunks(BATCH_SIZE) {
        let columns: Vec<ArrayRef> = (0..schema.fields().len())
            .map(|column| {
                let arrays: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(column).as_ref()).collect();
                interleave(&arrays, chunk).unwrap()
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        rows += batch.num_rows();
    }
    Measurement {
        time: start.elapsed(),
        allocated: ALLOCATED.load(Ordering::Relaxed) - before,
        spills: 0,
        spilled_bytes: 0,
        rows,
    }
}

fn best(
    mut run: impl FnMut() -> Result<Measurement, String>,
) -> Result<Measurement, String> {
    let mut best: Option<Measurement> = None;
    for _ in 0..RUNS {
        let m = run()?;
        if best.as_ref().is_none_or(|b| m.time < b.time) {
            best = Some(m);
        }
    }
    Ok(best.unwrap())
}

fn main() {
    let max_width = std::env::args()
        .skip(1)
        .find_map(|arg| arg.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    println!(
        "{:<8} {:>6} {:>5} {:<7} {:>9} {:>8} {:>7} {:>7} {:>9}",
        "keys", "width", "rows", "case", "ms", "MB/s", "alloc/x", "spills", "spill MB"
    );
    let cases = [
        (Keys::Long, 32, 0),
        (Keys::Long, 128, 0),
        (Keys::Long, 1024, 0),
        (Keys::Long, 4096, 0),
        (Keys::Long, 16384, 0),
        (Keys::Long, 65536, 0),
        (Keys::IntLong, 1024, 0),
        (Keys::IntLong, 16384, 0),
        (Keys::Long, 1024, 8),
        (Keys::IntLong, 16384, 8),
    ];
    for (keys, width, per_batch) in cases {
        if width > max_width {
            continue;
        }
        let batches = input(keys, width, per_batch);
        let per_batch = batches[0].num_rows();
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let limited = bytes / 4;
        let results = [
            ("gather", best(|| Ok(gather_once(&batches, keys)))),
            ("memory", best(|| sort(&runtime, &batches, keys, None))),
            (
                "spill",
                best(|| sort(&runtime, &batches, keys, Some(limited))),
            ),
        ];
        for (case, m) in results {
            let m = match m {
                Ok(m) => m,
                Err(e) => {
                    println!(
                        "{:<8} {:>6} {:>5} {:<7} failed: {e}",
                        keys.name(),
                        width,
                        per_batch,
                        case
                    );
                    continue;
                }
            };
            assert_eq!(m.rows, rows);
            println!(
                "{:<8} {:>6} {:>5} {:<7} {:>9.1} {:>8.0} {:>7.2} {:>7} {:>9.1}",
                keys.name(),
                width,
                per_batch,
                case,
                m.time.as_secs_f64() * 1e3,
                bytes as f64 / 1e6 / m.time.as_secs_f64(),
                m.allocated as f64 / bytes as f64,
                m.spills,
                m.spilled_bytes as f64 / 1e6,
            );
        }
    }
}
