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

//! COMET PATCH: tests for the external sort's memory accounting, following the
//! reproductions in apache/datafusion#25804.

use super::*;
use crate::metrics::ExecutionPlanMetricsSet;
use arrow::array::{AsArray, Int32Array};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryLimit};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::expressions::Column;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn new_sorter(
    schema: &SchemaRef,
    pool: &Arc<dyn MemoryPool>,
    batch_size: usize,
    sort_spill_reservation_bytes: usize,
) -> Result<ExternalSorter> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool))
        .build_arc()?;
    ExternalSorter::new(
        0,
        Arc::clone(schema),
        [PhysicalSortExpr::new_default(Arc::new(Column::new("x", 0)))].into(),
        batch_size,
        sort_spill_reservation_bytes,
        usize::MAX,
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        runtime,
    )
}

/// Once armed, hands every released byte to another consumer, standing in for other
/// partitions or Spark tasks that take whatever the sort gives back.
#[derive(Debug)]
struct StealingPool {
    inner: GreedyMemoryPool,
    armed: AtomicBool,
    stolen: AtomicUsize,
}

impl StealingPool {
    fn new(size: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: GreedyMemoryPool::new(size),
            armed: AtomicBool::new(false),
            stolen: AtomicUsize::new(0),
        })
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Relaxed);
    }

    fn stolen(&self) -> usize {
        self.stolen.load(Ordering::Relaxed)
    }
}

impl fmt::Display for StealingPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "stealing({})", self.inner)
    }
}

impl MemoryPool for StealingPool {
    fn name(&self) -> &str {
        "stealing"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        if self.armed.load(Ordering::Relaxed) {
            self.stolen.fetch_add(shrink, Ordering::Relaxed);
        } else {
            self.inner.shrink(reservation, shrink)
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn reversed_batch(schema: &SchemaRef, i: i32) -> Result<RecordBatch> {
    let values: Vec<i32> = ((i * 100)..((i + 1) * 100)).rev().collect();
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int32Array::from(values))],
    )?)
}

fn assert_sorted_ints(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    rows: usize,
) -> Result<()> {
    let merged = concat_batches(schema, batches)?;
    assert_eq!(merged.num_rows(), rows);
    let col = merged.column(0).as_primitive::<Int32Type>();
    for i in 1..col.len() {
        assert!(col.value(i - 1) <= col.value(i), "output not sorted at {i}");
    }
    Ok(())
}

/// Finding 2 of apache/datafusion#25804: the headroom `sort()` hands the spill merge used
/// to go back to the pool at the end of the first pass (and on the read-ahead fallback),
/// so a later pass had to win it back from a pool that no longer had it.
#[tokio::test]
async fn spill_merge_keeps_its_headroom_across_passes() -> Result<()> {
    // Two runs of 128-row Int32 batches need 2 * 2 KiB per pass with read-ahead. With
    // 3 KiB the first pass also falls back to a smaller read-ahead.
    for headroom in [4 * 1024, 3 * 1024] {
        let pool_size = headroom + 40 * 1024;
        let stealing = StealingPool::new(pool_size);
        let pool: Arc<dyn MemoryPool> = Arc::clone(&stealing) as _;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let mut sorter = new_sorter(&schema, &pool, 128, headroom)?;
        for i in 0..200 {
            sorter.insert_batch(reversed_batch(&schema, i)?).await?;
        }
        assert!(sorter.spill_count() >= 3, "need a multi-pass merge");
        let merge_stream = sorter.sort().await?;
        drop(sorter);

        let contender = MemoryConsumer::new("CompetingPartition").register(&pool);
        contender.try_grow(pool_size - pool.reserved())?;
        stealing.arm();

        let batches: Vec<RecordBatch> = merge_stream.try_collect().await?;
        assert_sorted_ints(&schema, &batches, 200 * 100)?;
        // Whatever the sort still holds is neither the contender's nor handed over.
        assert_eq!(pool.reserved(), contender.size() + stealing.stolen());
    }
    Ok(())
}
