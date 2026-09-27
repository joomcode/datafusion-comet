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
use arrow::array::{
    ArrayRef, AsArray, DictionaryArray, Int32Array, Int64Array, StringArray,
    StringViewArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
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
    new_sorter_with_threshold(
        schema,
        pool,
        batch_size,
        sort_spill_reservation_bytes,
        usize::MAX,
    )
}

fn new_sorter_with_threshold(
    schema: &SchemaRef,
    pool: &Arc<dyn MemoryPool>,
    batch_size: usize,
    sort_spill_reservation_bytes: usize,
    sort_in_place_threshold_bytes: usize,
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
        sort_in_place_threshold_bytes,
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

/// Finding 9 of apache/datafusion#25804: the final pass of a spill merge used to grow
/// until the pool refused, leaving nothing for the operators reading its output. It now
/// runs only if the pool could grant as much again, and merges more passes otherwise.
#[tokio::test]
async fn final_spill_merge_leaves_as_much_again_for_its_consumer() -> Result<()> {
    let pool_size = 44 * 1024;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let mut sorter = new_sorter(&schema, &pool, 128, 4 * 1024)?;
    let batches = 2000;
    for i in 0..batches {
        sorter.insert_batch(reversed_batch(&schema, i)?).await?;
    }
    assert!(
        sorter.spill_count() >= 20,
        "need more runs than one pass can seat"
    );
    let mut merge_stream = sorter.sort().await?;
    drop(sorter);

    let first = merge_stream.try_next().await?.expect("rows");
    let merge = pool.reserved();
    assert!(merge > 0, "the final pass reserves its buffers");
    // An operator reading the output can reserve as much as the merge holds.
    let consumer = MemoryConsumer::new("Downstream").register(&pool);
    consumer.try_grow(merge)?;
    drop(consumer);

    let mut output = vec![first];
    output.extend(merge_stream.try_collect::<Vec<_>>().await?);
    assert_sorted_ints(&schema, &output, batches as usize * 100)?;
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

/// Records the pool's reservation whenever batch data is written to a spill file.
struct RecordingTempFileFactory {
    pool: Arc<dyn MemoryPool>,
    reserved_during_writes: Arc<parking_lot::Mutex<Vec<usize>>>,
}

impl datafusion_execution::TempFileFactory for RecordingTempFileFactory {
    fn create_temp_file(
        &self,
        _description: &str,
    ) -> Result<Arc<dyn datafusion_execution::SpillFile>> {
        Ok(Arc::new(RecordingSpillFile {
            pool: Arc::clone(&self.pool),
            reserved_during_writes: Arc::clone(&self.reserved_during_writes),
        }))
    }
}

struct RecordingSpillFile {
    pool: Arc<dyn MemoryPool>,
    reserved_during_writes: Arc<parking_lot::Mutex<Vec<usize>>>,
}

impl datafusion_execution::SpillFile for RecordingSpillFile {
    fn size(&self) -> Option<u64> {
        Some(0)
    }

    fn read_stream(
        &self,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes>> + Send>>>
    {
        Ok(Box::pin(futures::stream::empty()))
    }

    fn open_writer(&self) -> Result<Box<dyn datafusion_execution::SpillWriter>> {
        Ok(Box::new(RecordingSpillWriter {
            pool: Arc::clone(&self.pool),
            reserved_during_writes: Arc::clone(&self.reserved_during_writes),
        }))
    }
}

struct RecordingSpillWriter {
    pool: Arc<dyn MemoryPool>,
    reserved_during_writes: Arc<parking_lot::Mutex<Vec<usize>>>,
}

impl std::io::Write for RecordingSpillWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Skip the 4-byte continuation and length prefixes.
        if buf.len() > 8 {
            self.reserved_during_writes
                .lock()
                .push(self.pool.reserved());
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl datafusion_execution::SpillWriter for RecordingSpillWriter {
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Finding 3 of apache/datafusion#25804: `consume_and_spill_append` freed the sorted
/// batches' reservation before writing them. The spill workspace still holds the
/// buffered input until the spill ends, so the sorted batch must be reserved on top.
#[tokio::test]
async fn sorted_batches_stay_reserved_while_they_are_spilled() -> Result<()> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reserved_during_writes = Arc::new(parking_lot::Mutex::new(vec![]));
    let disk_manager = datafusion_execution::disk_manager::DiskManagerBuilder::default()
        .with_temp_file_factory(Arc::new(RecordingTempFileFactory {
            pool: Arc::clone(&pool),
            reserved_during_writes: Arc::clone(&reserved_during_writes),
        }));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_disk_manager_builder(disk_manager)
        .build_arc()?;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let expr: LexOrdering =
        [PhysicalSortExpr::new_default(Arc::new(Column::new("x", 0)))].into();
    let mut sorter = ExternalSorter::new(
        0,
        Arc::clone(&schema),
        expr.clone(),
        128,
        0,
        usize::MAX,
        SpillCompression::Uncompressed,
        &ExecutionPlanMetricsSet::new(),
        runtime,
    )?;
    let batch = reversed_batch(&schema, 0)?;
    let input = get_reserved_bytes_for_record_batch(&batch)?;
    let sorted = get_reserved_bytes_for_record_batch(&sort_batch(&batch, &expr, None)?)?;
    sorter.reservation.try_grow(input)?;
    sorter.in_mem_batches.push(batch);

    sorter.sort_and_spill_in_mem_batches().await?;

    let reserved_during_writes = reserved_during_writes.lock();
    assert!(!reserved_during_writes.is_empty());
    assert!(
        reserved_during_writes
            .iter()
            .all(|&reserved| reserved >= input + sorted),
        "sorted batches must stay reserved while they are written: {reserved_during_writes:?}"
    );
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

/// Finding 6 of apache/datafusion#25804: the chunks `sort_batch_stream` sorts a batch
/// into share its view data or dictionary values, which were charged once per chunk.
/// Sorts one batch in a pool that holds only its reservation, in four chunks.
#[tokio::test]
async fn sorted_chunks_charge_shared_buffers_once() -> Result<()> {
    let rows = 4096;
    let long = |i: usize| format!("row-{i:08}-{}", "x".repeat(87));
    let views: ArrayRef =
        Arc::new(StringViewArray::from_iter_values((0..rows).rev().map(long)));
    let dictionary: ArrayRef = Arc::new(DictionaryArray::new(
        Int32Array::from_iter_values((0..rows as i32).rev().map(|i| i % 1024)),
        Arc::new(StringArray::from_iter_values((0..1024).map(long))),
    ));
    for values in [views, dictionary] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            values.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;
        let shared = match batch.column(0).data_type() {
            DataType::Utf8View => batch
                .column(0)
                .as_string_view()
                .data_buffers()
                .iter()
                .map(|buffer| buffer.capacity())
                .sum(),
            _ => batch
                .column(0)
                .as_any_dictionary()
                .values()
                .to_data()
                .buffers()[1]
                .capacity(),
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
            get_reserved_bytes_for_record_batch(&batch)?,
        ));
        let mut sorter = new_sorter(&schema, &pool, 1024, 0)?;
        sorter.insert_batch(batch).await?;
        let mut stream = sorter.sort().await?;
        drop(sorter);

        let mut output = vec![stream.try_next().await?.expect("rows")];
        // The chunks still to come hold the shared buffer, so it stays reserved.
        assert!(pool.reserved() >= shared);
        output.extend(stream.try_collect::<Vec<_>>().await?);
        assert_eq!(output.len(), 4);
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows
        );
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

/// Finding 5 of apache/datafusion#25804: each zero-copy slice of one parent batch, as
/// `AggregateExec` emits for `EmitTo::All`, was charged the parent's whole buffer, so
/// sorting 64 slices of a 512 KiB batch in 2 MiB spilled 21 times. Sorts them by
/// concatenating and by merging the slices as runs.
#[tokio::test]
async fn slices_of_one_batch_reserve_the_parent_once() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let rows = 64 * 1024;
    let parent = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..rows as i64).rev(),
        ))],
    )?;
    let parent_bytes = get_record_batch_memory_size(&parent);
    for threshold in [usize::MAX, 0] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4 * parent_bytes));
        let mut sorter = new_sorter_with_threshold(&schema, &pool, 1024, 0, threshold)?;
        for i in 0..64 {
            sorter.insert_batch(parent.slice(i * 1024, 1024)).await?;
        }
        assert_eq!(sorter.spill_count(), 0);
        // The parent once, and each slice's own rows.
        assert_eq!(sorter.used(), 2 * parent_bytes);

        let output: Vec<RecordBatch> = sorter.sort().await?.try_collect().await?;
        drop(sorter);
        let merged = concat_batches(&schema, &output)?;
        let values = merged.column(0).as_primitive::<Int64Type>();
        assert!(values.values().iter().copied().eq(0..rows as i64));
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
