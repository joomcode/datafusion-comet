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

use crate::sorts::cursor::{ArrayValues, CursorArray, RowValues};
use crate::{EmptyRecordBatchStream, SendableRecordBatchStream};
use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::array::{Array, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_ord::sort::lexsort_to_indices;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::stream::{Fuse, StreamExt};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// A [`Stream`](futures::Stream) that has multiple partitions that can
/// be polled separately but not concurrently
///
/// Used by sort preserving merge to decouple the cursor merging logic from
/// the source of the cursors, the intention being to allow preserving
/// any row encoding performed for intermediate sorts
pub trait PartitionedStream: std::fmt::Debug + Send {
    type Output;

    /// Returns the number of partitions
    fn partitions(&self) -> usize;

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>>;
}

/// A new type wrapper around a set of fused [`SendableRecordBatchStream`]
/// that implements debug, and skips over empty [`RecordBatch`]
struct FusedStreams(Vec<Fuse<SendableRecordBatchStream>>);

impl std::fmt::Debug for FusedStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedStreams")
            .field("num_streams", &self.0.len())
            .finish()
    }
}

impl FusedStreams {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            let poll_result = self.0[stream_idx].poll_next_unpin(cx);
            match &poll_result {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(b))) if b.num_rows() == 0 => continue,
                Poll::Ready(Some(Ok(_))) => return poll_result,
                Poll::Ready(None) | Poll::Ready(Some(Err(_))) => {
                    let stream_schema = self.0[stream_idx].get_ref().schema();

                    // Replace the stream with an empty stream, so we can drop memory usage
                    let empty_stream: SendableRecordBatchStream =
                        Box::pin(EmptyRecordBatchStream::new(stream_schema));
                    self.0[stream_idx] = empty_stream.fuse();

                    return poll_result;
                }
            }
        }
    }
}

/// A pair of `Arc<Rows>` that can be reused
#[derive(Debug)]
struct ReusableRows {
    // inner[stream_idx] holds a two Arcs:
    // at start of a new poll
    // .0 is the rows from the previous poll (at start),
    // .1 is the one that is being written to
    // at end of a poll, .0 will be swapped with .1,
    inner: Vec<[Option<Arc<Rows>>; 2]>,
    /// COMET PATCH: covers every buffer in `inner` for as long as it is kept, so a
    /// buffer stays reserved after its cursor, which gets an empty reservation, is
    /// dropped. Follows apache/datafusion#25372.
    reservation: MemoryReservation,
    /// COMET PATCH
    kept: usize,
}

impl ReusableRows {
    // return a Rows for writing,
    // does not clone if the existing rows can be reused
    fn take_next(&mut self, stream_idx: usize) -> Result<Rows> {
        let rows = self.inner[stream_idx][1].take().unwrap();
        self.kept -= rows.size();
        Arc::try_unwrap(rows).map_err(|_| {
            internal_datafusion_err!(
                "Rows from RowCursorStream is still in use by consumer"
            )
        })
    }
    // save the Rows
    fn save(&mut self, stream_idx: usize, rows: &Arc<Rows>) -> Result<()> {
        self.kept += rows.size();
        if let Some(old) = self.inner[stream_idx][1].replace(Arc::clone(rows)) {
            self.kept -= old.size();
        }
        // swap the current with the previous one, so that the next poll can reuse the Rows from the previous poll
        let [a, b] = &mut self.inner[stream_idx];
        mem::swap(a, b);
        // COMET PATCH: reserve the buffer before the cursor gets it.
        self.reservation.try_resize(self.kept)
    }

    // COMET PATCH: a finished stream keeps only the rows its last cursors still hold.
    fn release(&mut self, stream_idx: usize) {
        for slot in &mut self.inner[stream_idx] {
            if slot
                .as_ref()
                .is_some_and(|rows| Arc::strong_count(rows) == 1)
            {
                self.kept -= slot.take().unwrap().size();
            }
        }
        let kept = self.kept;
        if kept < self.reservation.size() {
            self.reservation.shrink(self.reservation.size() - kept);
        }
    }
}

/// A [`PartitionedStream`] that wraps a set of [`SendableRecordBatchStream`]
/// and computes [`RowValues`] based on the provided [`PhysicalSortExpr`]
/// Note: the stream returns an error if the consumer buffers more than one RowValues (i.e. holds on to two RowValues
/// from the same partition at the same time).
#[derive(Debug)]
pub struct RowCursorStream {
    /// Converter to convert output of physical expressions
    converter: RowConverter,
    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// Input streams
    streams: FusedStreams,
    /// Tracks the memory used by `converter`
    reservation: MemoryReservation,
    /// Allocated rows for each partition, we keep two to allow for buffering one
    /// in the consumer of the stream
    rows: ReusableRows,
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expressions: &LexOrdering,
        streams: Vec<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;

        let streams: Vec<_> = streams.into_iter().map(|s| s.fuse()).collect();
        let converter = RowConverter::new(sort_fields)?;
        let mut rows = Vec::with_capacity(streams.len());
        for _ in &streams {
            // Initialize each stream with an empty Rows
            rows.push([
                Some(Arc::new(converter.empty_rows(0, 0))),
                Some(Arc::new(converter.empty_rows(0, 0))),
            ]);
        }
        let kept = rows.iter().flatten().flatten().map(|r| r.size()).sum();
        let rows = ReusableRows {
            inner: rows,
            reservation: reservation.new_empty(),
            kept,
        };
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| Arc::clone(&x.expr)).collect(),
            streams: FusedStreams(streams),
            rows,
        })
    }

    fn convert_batch(
        &mut self,
        batch: &RecordBatch,
        stream_idx: usize,
    ) -> Result<RowValues> {
        let cols = evaluate_expressions_to_arrays(&self.column_expressions, batch)?;

        // At this point, ownership should of this Rows should be unique
        let mut rows = self.rows.take_next(stream_idx)?;

        rows.clear();

        self.converter.append(&mut rows, &cols)?;
        self.reservation.try_resize(self.converter.size())?;

        let rows = Arc::new(rows);

        // COMET PATCH: `self.rows` reserves the buffer while it keeps it, which is at
        // least as long as the cursor does, so the cursor's reservation is empty.
        self.rows.save(stream_idx, &rows)?;
        Ok(RowValues::new(rows, self.reservation.new_empty()))
    }
}

impl PartitionedStream for RowCursorStream {
    type Output = Result<(RowValues, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        let polled = ready!(self.streams.poll_next(cx, stream_idx));
        // COMET PATCH: a finished stream's rows are never reused.
        if polled.is_none() {
            self.rows.release(stream_idx);
        }
        Poll::Ready(polled.map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch, stream_idx)?;
                Ok((cursor, batch))
            })
        }))
    }
}

/// Specialized stream for sorts on single primitive columns
pub struct FieldCursorStream<T: CursorArray> {
    /// The physical expressions to sort by
    sort: PhysicalSortExpr,
    /// Input streams
    streams: FusedStreams,
    /// Create new reservations for each array
    reservation: MemoryReservation,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: CursorArray> std::fmt::Debug for FieldCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream")
            .field("num_streams", &self.streams)
            .finish()
    }
}

impl<T: CursorArray> FieldCursorStream<T> {
    pub fn new(
        sort: PhysicalSortExpr,
        streams: Vec<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Self {
        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        Self {
            sort,
            streams: FusedStreams(streams),
            reservation,
            phantom: Default::default(),
        }
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<ArrayValues<T::Values>> {
        let value = self.sort.expr.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;
        // COMET PATCH: a column of the batch is charged with the batch, which the merge's
        // `BatchBuilder` holds at least as long as this cursor. Reserve only a key that
        // the sort expression computed.
        let size_in_mem = if batch.columns().iter().any(|c| Arc::ptr_eq(c, &array)) {
            0
        } else {
            array.get_buffer_memory_size()
        };
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        let array_reservation = self.reservation.new_empty();
        array_reservation.try_grow(size_in_mem)?;
        Ok(ArrayValues::new(
            self.sort.options,
            array,
            array_reservation,
        ))
    }
}

impl<T: CursorArray> PartitionedStream for FieldCursorStream<T> {
    type Output = Result<(ArrayValues<T::Values>, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}

/// A lazy, memory-efficient sort iterator used as a fallback during aggregate
/// spill when there is not enough memory for an eager sort (which requires ~2x
/// peak memory to hold both the unsorted and sorted copies simultaneously).
///
/// On the first call to `next()`, a sorted index array (`UInt32Array`) is
/// computed via `lexsort_to_indices`. Subsequent calls yield chunks of
/// `batch_size` rows by `take`-ing from the original batch using slices of
/// this index array. Each `take` copies data for the chunk (not zero-copy),
/// but only one chunk is live at a time since the caller consumes it before
/// requesting the next. Once all rows have been yielded, the original batch
/// and index array are dropped to free memory.
///
/// The caller must reserve `sizeof(batch) + sizeof(one chunk)` for this iterator,
/// and free the reservation once the iterator is depleted.
pub(crate) struct IncrementalSortIterator {
    batch: RecordBatch,
    expressions: LexOrdering,
    batch_size: usize,
    indices: Option<UInt32Array>,
    cursor: usize,
}

impl IncrementalSortIterator {
    pub(crate) fn new(
        batch: RecordBatch,
        expressions: LexOrdering,
        batch_size: usize,
    ) -> Self {
        Self {
            batch,
            expressions,
            batch_size,
            cursor: 0,
            indices: None,
        }
    }
}

impl Iterator for IncrementalSortIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.batch.num_rows() {
            return None;
        }

        match self.indices.as_ref() {
            None => {
                let sort_columns = match self
                    .expressions
                    .iter()
                    .map(|expr| expr.evaluate_to_sort_column(&self.batch))
                    .collect::<Result<Vec<_>>>()
                {
                    Ok(cols) => cols,
                    Err(e) => return Some(Err(e)),
                };

                let indices = match lexsort_to_indices(&sort_columns, None) {
                    Ok(indices) => indices,
                    Err(e) => return Some(Err(e.into())),
                };
                self.indices = Some(indices);

                // Call again, this time it will hit the Some(indices) branch and return the first batch
                self.next()
            }
            Some(indices) => {
                let batch_size = self.batch_size.min(self.batch.num_rows() - self.cursor);

                // Perform the take to produce the next batch
                let new_batch_indices = indices.slice(self.cursor, batch_size);
                let new_batch = match take_record_batch(&self.batch, &new_batch_indices) {
                    Ok(batch) => batch,
                    Err(e) => return Some(Err(e.into())),
                };

                self.cursor += batch_size;

                // If this is the last batch, we can release the memory
                if self.cursor >= self.batch.num_rows() {
                    let schema = self.batch.schema();
                    let _ = mem::replace(&mut self.batch, RecordBatch::new_empty(schema));
                    self.indices = None;
                }

                // Return the new batch
                Some(Ok(new_batch))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_rows = self.batch.num_rows();
        let batch_size = self.batch_size;
        let num_batches = num_rows.div_ceil(batch_size);
        (num_batches, Some(num_batches))
    }
}

impl FusedIterator for IncrementalSortIterator {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow_schema::SchemaRef;
    use datafusion_common::DataFusionError;
    use datafusion_execution::RecordBatchStream;
    use datafusion_physical_expr::expressions::col;
    use futures::Stream;
    use std::pin::Pin;

    /// Verifies that `take_record_batch` in `IncrementalSortIterator` actually
    /// copies the data into a new allocation rather than returning a zero-copy
    /// slice of the original batch. If the output arrays were slices, their
    /// underlying buffer length would match the original array's length; a true
    /// copy will have a buffer sized to fit only the chunk.
    #[test]
    fn incremental_sort_iterator_copies_data() -> Result<()> {
        let original_len = 10;
        let batch_size = 3;

        // Build a batch with a single Int32 column of descending values
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let col_a: Int32Array = Int32Array::from(vec![0; original_len]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col_a)])?;

        // Sort ascending on column "a"
        let expressions = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "a",
            &batch.schema(),
        )?)])
        .unwrap();

        let mut total_rows = 0;
        IncrementalSortIterator::new(batch.clone(), expressions, batch_size).try_for_each(
            |result| {
                let chunk = result?;
                total_rows += chunk.num_rows();

                // Every output column must be a fresh allocation whose length
                // equals the chunk size, NOT the original array length.
                chunk.columns().iter().zip(batch.columns()).for_each(|(arr, original_arr)| {
                    let (_, scalar_buf, _) = arr.as_primitive::<Int32Type>().clone().into_parts();
                    let (_, original_scalar_buf, _) = original_arr.as_primitive::<Int32Type>().clone().into_parts();

                    assert_ne!(scalar_buf.inner().data_ptr(), original_scalar_buf.inner().data_ptr(), "Expected a copy of the data for each chunk, but got a slice that shares the same buffer as the original array");
                });

                Result::<_, DataFusionError>::Ok(())
            },
        )?;

        assert_eq!(total_rows, original_len);
        Ok(())
    }

    #[test]
    fn test_fused_stream_drop_finished_streams() {
        #[derive(Clone)]
        struct SingleItemManualStream {
            // Held only so its `Arc` strong count reveals when the stream is dropped.
            #[expect(dead_code)]
            hold_ref: Arc<()>,
            record_batch: RecordBatch,
            should_finish: bool,
        }

        impl Stream for SingleItemManualStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if !self.should_finish {
                    self.should_finish = true;
                    return Poll::Ready(Some(Ok(self.record_batch.clone())));
                }

                Poll::Ready(None)
            }
        }

        impl RecordBatchStream for SingleItemManualStream {
            fn schema(&self) -> SchemaRef {
                self.record_batch.schema()
            }
        }

        let hold_ref = Arc::new(());
        let record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();

        let stream_1 = SingleItemManualStream {
            hold_ref: Arc::clone(&hold_ref),
            should_finish: false,
            record_batch: record_batch.clone(),
        };
        let stream_2 = stream_1.clone();

        let stream_1: SendableRecordBatchStream = Box::pin(stream_1);
        let stream_2: SendableRecordBatchStream = Box::pin(stream_2);

        let mut fused_stream = FusedStreams(vec![stream_1.fuse(), stream_2.fuse()]);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // The original plus one clone held by each of the two streams.
        assert_eq!(Arc::strong_count(&hold_ref), 3);

        // First fetch from stream 0 yields its single batch.
        // the stream is not finished yet, so nothing is dropped.
        let poll = fused_stream.poll_next(&mut cx, 0);
        assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));
        assert_eq!(Arc::strong_count(&hold_ref), 3);

        // Second fetch from stream 0 returns `None`, so it is replaced with an
        // empty stream and dropped, releasing its `hold_ref` clone.
        // running 3 times to make sure the stream is fused correctly
        for _ in 0..3 {
            let poll = fused_stream.poll_next(&mut cx, 0);
            assert!(matches!(poll, Poll::Ready(None)));
            assert_eq!(Arc::strong_count(&hold_ref), 2);
        }

        // First fetch from stream 1 yields its single batch
        // the stream is not finished yet, so nothing is dropped.
        let poll = fused_stream.poll_next(&mut cx, 1);
        assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));
        assert_eq!(Arc::strong_count(&hold_ref), 2);

        // Second fetch from stream 1 returns `None`, so it is replaced with an
        // empty stream and dropped, releasing its `hold_ref` clone.
        // running 3 times to make sure the stream is fused correctly
        for _ in 0..3 {
            let poll = fused_stream.poll_next(&mut cx, 1);
            assert!(matches!(poll, Poll::Ready(None)));
            assert_eq!(Arc::strong_count(&hold_ref), 1);
        }
    }

    // COMET PATCH: finding 4 of apache/datafusion#25804.
    fn two_column_streams(
        partitions: usize,
        batches: usize,
    ) -> (SchemaRef, LexOrdering, Vec<SendableRecordBatchStream>) {
        use crate::memory::MemoryStream;
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let streams = (0..partitions)
            .map(|_| {
                let batches = (0..batches)
                    .map(|i| {
                        let a = Int32Array::from_iter_values(
                            (0..100).map(|r| (i * 100 + r) as i32),
                        );
                        let b = StringArray::from_iter_values(
                            (0..100).map(|_| "x".repeat(50 * (i + 1))),
                        );
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(a), Arc::new(b)],
                        )
                        .unwrap()
                    })
                    .collect();
                Box::pin(
                    MemoryStream::try_new(batches, Arc::clone(&schema), None).unwrap(),
                ) as SendableRecordBatchStream
            })
            .collect();
        let expressions = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(col("a", &schema).unwrap()),
            PhysicalSortExpr::new_default(col("b", &schema).unwrap()),
        ])
        .unwrap();
        (schema, expressions, streams)
    }

    /// The encoded rows `RowCursorStream` keeps for reuse after their cursor is dropped
    /// stay reserved until it lets go of them.
    #[test]
    fn row_cursor_stream_reserves_the_rows_it_keeps() -> Result<()> {
        use datafusion_execution::memory_pool::{
            GreedyMemoryPool, MemoryConsumer, MemoryPool,
        };
        let (schema, expressions, streams) = two_column_streams(2, 3);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let reservation = MemoryConsumer::new("merge").register(&pool);
        let mut stream =
            RowCursorStream::try_new(&schema, &expressions, streams, reservation)?;
        let kept = |stream: &RowCursorStream| -> usize {
            stream
                .rows
                .inner
                .iter()
                .flatten()
                .flatten()
                .map(|rows| rows.size())
                .sum()
        };
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut poll = |stream: &mut RowCursorStream, idx: usize| match stream
            .poll_next(&mut cx, idx)
        {
            Poll::Ready(Some(Ok((cursor, _)))) => Some(cursor),
            Poll::Ready(None) => None,
            other => panic!("unexpected poll result {other:?}"),
        };

        // The merge keeps a stream's previous cursor while it reads the next batch.
        let first = poll(&mut stream, 0).unwrap();
        let second = poll(&mut stream, 0).unwrap();
        drop(first);
        let other = poll(&mut stream, 1).unwrap();
        assert_eq!(pool.reserved(), stream.converter.size() + kept(&stream));
        assert_eq!(stream.rows.kept, kept(&stream));
        drop(second);
        drop(other);
        assert!(kept(&stream) > 0);
        assert_eq!(pool.reserved(), stream.converter.size() + kept(&stream));
        assert_eq!(stream.rows.kept, kept(&stream));

        // A finished stream lets go of the rows no cursor holds.
        drop(poll(&mut stream, 0).unwrap());
        assert!(poll(&mut stream, 0).is_none());
        assert!(stream.rows.inner[0].iter().all(Option::is_none));
        while let Some(cursor) = poll(&mut stream, 1) {
            drop(cursor);
        }
        assert_eq!(kept(&stream), 0);
        assert_eq!(pool.reserved(), stream.converter.size());
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// The count of the rows `RowCursorStream` keeps follows every reuse, replacement
    /// and release of them, whichever cursors the merge still holds.
    #[test]
    fn row_cursor_stream_counts_the_rows_it_keeps() -> Result<()> {
        use datafusion_execution::memory_pool::{
            GreedyMemoryPool, MemoryConsumer, MemoryPool,
        };
        let (schema, expressions, _) = two_column_streams(0, 0);
        let partitions = 32;
        let streams = (0..partitions)
            .map(|p| {
                let (_, _, mut streams) = two_column_streams(1, p % 5);
                streams.pop().unwrap()
            })
            .collect();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let reservation = MemoryConsumer::new("merge").register(&pool);
        let mut stream =
            RowCursorStream::try_new(&schema, &expressions, streams, reservation)?;
        let kept = |stream: &RowCursorStream| -> usize {
            stream
                .rows
                .inner
                .iter()
                .flatten()
                .flatten()
                .map(|rows| rows.size())
                .sum()
        };
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let mut held: Vec<Option<RowValues>> = (0..partitions).map(|_| None).collect();
        let mut finished = vec![false; partitions];
        let mut state = 7u64;
        while finished.iter().any(|f| !f) {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let idx = (state >> 33) as usize % partitions;
            if (state >> 20).is_multiple_of(3) {
                held[idx] = None;
            }
            match stream.poll_next(&mut cx, idx) {
                Poll::Ready(Some(Ok((cursor, _)))) => {
                    held[idx] = Some(cursor);
                    assert_eq!(pool.reserved(), stream.converter.size() + kept(&stream));
                }
                Poll::Ready(None) => finished[idx] = true,
                other => panic!("unexpected poll result {other:?}"),
            }
            assert_eq!(stream.rows.kept, kept(&stream));
            assert!(pool.reserved() <= stream.converter.size() + kept(&stream));
        }
        held.clear();
        for idx in 0..partitions {
            assert!(matches!(stream.poll_next(&mut cx, idx), Poll::Ready(None)));
            assert_eq!(stream.rows.kept, kept(&stream));
        }
        assert_eq!(stream.rows.kept, 0);
        assert_eq!(pool.reserved(), stream.converter.size());
        Ok(())
    }

    /// A merge of many single-row streams takes time linear in the number of streams.
    #[tokio::test]
    async fn merge_of_many_single_row_streams_is_linear() -> Result<()> {
        use crate::memory::MemoryStream;
        use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
        use crate::sorts::streaming_merge::StreamingMergeBuilder;
        use arrow::array::StringArray;
        use futures::TryStreamExt;

        let (schema, expressions, _) = two_column_streams(0, 0);
        let merge = |partitions: usize| {
            let schema = Arc::clone(&schema);
            let expressions = expressions.clone();
            async move {
                let streams = (0..partitions)
                    .map(|p| {
                        let batch = RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![
                                Arc::new(Int32Array::from(vec![
                                    (p * 7919 % partitions) as i32,
                                ])),
                                Arc::new(StringArray::from(vec!["x"])),
                            ],
                        )
                        .unwrap();
                        Box::pin(
                            MemoryStream::try_new(vec![batch], Arc::clone(&schema), None)
                                .unwrap(),
                        ) as SendableRecordBatchStream
                    })
                    .collect();
                let start = std::time::Instant::now();
                let merged: Vec<RecordBatch> = StreamingMergeBuilder::new()
                    .with_streams(streams)
                    .with_schema(Arc::clone(&schema))
                    .with_expressions(&expressions)
                    .with_metrics(BaselineMetrics::new(
                        &ExecutionPlanMetricsSet::new(),
                        0,
                    ))
                    .with_batch_size(8192)
                    .with_bypass_mempool()
                    .build()?
                    .try_collect()
                    .await?;
                assert_eq!(
                    merged.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    partitions
                );
                Ok::<_, DataFusionError>(start.elapsed())
            }
        };
        let small = merge(4_000).await?;
        let large = merge(64_000).await?;
        assert!(
            large < small * 64 + std::time::Duration::from_secs(2),
            "16 times the streams took {large:?} against {small:?}"
        );
        Ok(())
    }

    // COMET PATCH: finding 7 of apache/datafusion#25804.
    #[derive(Debug)]
    struct PeakPool {
        inner: datafusion_execution::memory_pool::GreedyMemoryPool,
        peak: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for PeakPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "peak({})", self.inner)
        }
    }

    impl datafusion_execution::memory_pool::MemoryPool for PeakPool {
        fn name(&self) -> &str {
            "peak"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
            self.peak
                .fetch_max(self.inner.reserved(), std::sync::atomic::Ordering::Relaxed);
        }

        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.inner.shrink(reservation, shrink)
        }

        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> Result<()> {
            self.inner.try_grow(reservation, additional)?;
            self.peak
                .fetch_max(self.inner.reserved(), std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    /// A single-column merge charges its sort key once, as part of the batch.
    #[tokio::test]
    async fn field_cursor_merge_counts_the_key_once() -> Result<()> {
        use crate::memory::MemoryStream;
        use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
        use crate::sorts::streaming_merge::StreamingMergeBuilder;
        use arrow::array::Int64Array;
        use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool};
        use futures::TryStreamExt;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = |offset: i64| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(
                    (0..10_000).map(|i| 2 * i + offset),
                ))],
            )
            .unwrap()
        };
        let inputs = [batch(0), batch(1)];
        let input_size: usize = inputs
            .iter()
            .map(crate::spill::get_record_batch_memory_size)
            .sum();
        let streams = inputs
            .into_iter()
            .map(|b| {
                Box::pin(
                    MemoryStream::try_new(vec![b], Arc::clone(&schema), None).unwrap(),
                ) as SendableRecordBatchStream
            })
            .collect();
        let peak = Arc::new(PeakPool {
            inner: datafusion_execution::memory_pool::GreedyMemoryPool::new(usize::MAX),
            peak: Default::default(),
        });
        let pool: Arc<dyn MemoryPool> = Arc::clone(&peak) as _;
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("a", &schema)?)])
                .unwrap();
        let merged: Vec<RecordBatch> = StreamingMergeBuilder::new()
            .with_streams(streams)
            .with_schema(Arc::clone(&schema))
            .with_expressions(&ordering)
            .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
            .with_batch_size(100_000)
            .with_reservation(MemoryConsumer::new("merge").register(&pool))
            .build()?
            .try_collect()
            .await?;
        assert_eq!(
            merged.iter().map(RecordBatch::num_rows).sum::<usize>(),
            20_000
        );
        let peak = peak.peak.load(std::sync::atomic::Ordering::Relaxed);
        // Both batches are buffered at once, and their keys are the same buffers.
        assert!(peak >= input_size, "the batches are not accounted: {peak}");
        assert!(
            peak < input_size * 3 / 2,
            "the key is counted twice: {peak} for {input_size}"
        );
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
