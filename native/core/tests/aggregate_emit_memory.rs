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

//! What a hash aggregate allocates against what it reserves, measured with the accounting
//! allocator `libcomet` installs. Its balance is process-wide, so this file is its own test
//! binary and runs its cases one after another in a single test.

use arrow::array::builder::{Int64Builder, StringBuilder};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use comet::alloc_accounting::current_balance;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::AggregateUDF;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_comet_spark_expr::CometCollectList;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct PeakPool {
    inner: GreedyMemoryPool,
    peak: AtomicUsize,
}
impl std::fmt::Display for PeakPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peak")
    }
}
impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "peak"
    }
    fn grow(&self, r: &MemoryReservation, n: usize) {
        self.inner.grow(r, n);
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }
    fn shrink(&self, r: &MemoryReservation, n: usize) {
        self.inner.shrink(r, n)
    }
    fn try_grow(&self, r: &MemoryReservation, n: usize) -> datafusion::common::Result<()> {
        self.inner.try_grow(r, n)?;
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

const ROWS: usize = 8192;
const SRC_ROWS: usize = 1024;

fn fields() -> Fields {
    (0..16)
        .map(|i| {
            Arc::new(Field::new(
                format!("f{i}"),
                if i % 2 == 0 {
                    DataType::Utf8
                } else {
                    DataType::Int64
                },
                true,
            ))
        })
        .collect::<Vec<_>>()
        .into()
}

fn input(batches: usize, groups: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Struct(fields()), true),
    ]));
    (0..batches)
        .map(|b| {
            let rows = b * SRC_ROWS..(b + 1) * SRC_ROWS;
            let mut k = Int64Builder::new();
            rows.clone()
                .for_each(|i| k.append_value(((i * 7919) % groups) as i64));
            let cols: Vec<ArrayRef> = (0..16)
                .map(|f| -> ArrayRef {
                    if f % 2 == 0 {
                        let mut s = StringBuilder::new();
                        rows.clone()
                            .for_each(|i| s.append_value(format!("v{f:02}-{i:016x}")));
                        Arc::new(s.finish())
                    } else {
                        let mut n = Int64Builder::new();
                        rows.clone().for_each(|i| n.append_value((i * f) as i64));
                        Arc::new(n.finish())
                    }
                })
                .collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(k.finish()),
                    Arc::new(StructArray::new(fields(), cols, None)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn agg(
    mode: AggregateMode,
    input: Arc<dyn ExecutionPlan>,
    schema: &Arc<Schema>,
) -> Arc<dyn ExecutionPlan> {
    let udf = Arc::new(AggregateUDF::new_from_impl(CometCollectList::new()));
    let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 1));
    let expr = Arc::new(
        AggregateExprBuilder::new(udf, vec![value])
            .schema(Arc::clone(schema))
            .alias("agg")
            .build()
            .unwrap(),
    );
    let group_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    Arc::new(
        AggregateExec::try_new(
            mode,
            PhysicalGroupBy::new_single(vec![(group_key, "key".to_string())]),
            vec![expr],
            vec![None],
            input,
            Arc::clone(schema),
        )
        .unwrap(),
    )
}

struct Measured {
    output: Vec<RecordBatch>,
    rows: usize,
    values: usize,
    peak_allocated: usize,
    peak_reserved: usize,
}

async fn measure(
    label: &str,
    plan: Arc<dyn ExecutionPlan>,
    pool_size: usize,
    keep: bool,
) -> Measured {
    let pool = Arc::new(PeakPool {
        inner: GreedyMemoryPool::new(pool_size),
        peak: AtomicUsize::new(0),
    });
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()
        .unwrap();
    let ctx = Arc::new(
        TaskContext::default()
            .with_runtime(runtime)
            .with_session_config({
                let mut c = SessionConfig::new().with_batch_size(ROWS);
                c.options_mut()
                    .execution
                    .skip_partial_aggregation_probe_ratio_threshold = 1.1;
                c
            }),
    );
    let base = current_balance();
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicUsize::new(0));
    let sampler = {
        let (stop, peak) = (Arc::clone(&stop), Arc::clone(&peak));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(current_balance().saturating_sub(base), Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        })
    };
    let mut stream = plan.execute(0, ctx).unwrap();
    let mut out = vec![];
    let mut rows = 0;
    let mut values = 0;
    let mut kept = 0usize;
    while let Some(b) = stream.next().await {
        let b = b.unwrap();
        rows += b.num_rows();
        let lists = b.column(1).as_list::<i32>();
        let offsets = lists.value_offsets();
        values += (offsets[lists.len()] - offsets[0]) as usize;
        if keep {
            kept += b.get_array_memory_size();
            out.push(b);
        }
    }
    drop(stream);
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let mib = |x: usize| x as f64 / (1 << 20) as f64;
    println!(
        "{label}: rows={rows} peak allocated {:.1} MiB, peak reserved {:.1} MiB, kept {:.1} MiB",
        mib(peak.load(Ordering::Relaxed)),
        mib(pool.peak.load(Ordering::Relaxed)),
        mib(kept)
    );
    Measured {
        output: out,
        rows,
        values,
        peak_allocated: peak.load(Ordering::Relaxed),
        peak_reserved: pool.peak.load(Ordering::Relaxed),
    }
}

fn scan(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    let schema = batches[0].schema();
    let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[batches], schema, None).unwrap(),
    )));
    // Joining the small source batches allocates the aggregate's input during the
    // measurement, as a scan or a shuffle read would.
    #[allow(deprecated)]
    let joined =
        datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec::new(source, ROWS);
    Arc::new(joined)
}

/// Small, separately allocated copies of `batches`.
fn small_copies(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .step_by(SRC_ROWS)
                .map(move |offset| batch.slice(offset, SRC_ROWS.min(batch.num_rows() - offset)))
        })
        .map(|slice| arrow::compute::concat_batches(&slice.schema(), &[slice]).unwrap())
        .collect()
}

/// collect_list(struct of 16 fields), as in dim_device_loader: materializing the groups used
/// to copy everything the accumulator retained outside the pool, once more than the pool
/// held for a partial aggregate and twice for a final one, where a slice per run of a
/// 16-field struct cost more than the values.
#[test]
fn collect_list_materializes_its_groups_in_reserved_memory() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (batches, groups) = (512, 512 * SRC_ROWS / 8);
        let data = input(batches, groups);
        let schema = data[0].schema();
        let within = |m: &Measured| m.peak_allocated <= m.peak_reserved / 10 * 11 + (8 << 20);

        let partial = measure(
            "partial",
            agg(AggregateMode::Partial, scan(data.clone()), &schema),
            1 << 40,
            true,
        )
        .await;
        assert!(
            within(&partial),
            "partial allocated {} with {} reserved",
            partial.peak_allocated,
            partial.peak_reserved
        );
        assert_eq!(partial.values, batches * SRC_ROWS);

        let states = small_copies(partial.output);
        let unbounded = measure(
            "final",
            agg(AggregateMode::Final, scan(states.clone()), &schema),
            1 << 40,
            false,
        )
        .await;
        assert!(
            within(&unbounded),
            "final allocated {} with {} reserved",
            unbounded.peak_allocated,
            unbounded.peak_reserved
        );

        // With a pool below what the final aggregate takes unbounded, it spills instead of
        // materializing more than the pool grants.
        let limit = unbounded.peak_reserved / 2;
        let bounded = measure(
            "final, half the memory",
            agg(AggregateMode::Final, scan(states), &schema),
            limit,
            false,
        )
        .await;
        assert!(
            bounded.peak_allocated <= limit / 10 * 11 + (8 << 20),
            "allocated {} of {limit}",
            bounded.peak_allocated
        );
        assert_eq!((bounded.rows, bounded.values), (groups, batches * SRC_ROWS));
        assert_eq!(
            (unbounded.rows, unbounded.values),
            (groups, batches * SRC_ROWS)
        );
    });
}
