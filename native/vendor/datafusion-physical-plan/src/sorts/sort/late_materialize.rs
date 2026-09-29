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

//! COMET PATCH: sort the keys of the buffered batches and gather each payload once.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::{
    SortColumn, concat, interleave, lexsort_to_indices, take_record_batch,
};
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::Result;
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::utils::collect_columns;

use crate::SendableRecordBatchStream;
use crate::metrics::Time;
use crate::spill::spill_manager::GetSlicedSize;
use crate::stream::RecordBatchStreamAdapter;

const MIN_PAYLOAD_BYTES_PER_ROW: usize = 64;
const MIN_PAYLOAD_PER_KEY_BYTE: usize = 2;
const ORDER_BYTES_PER_ROW: usize = 48;
const OUTPUT_BATCH_BYTES: usize = 4 << 20;
const MIN_SPILL_BATCH_BYTES: usize = 16 << 10;
const SPILL_BATCHES_PER_RUN: usize = 64;

#[derive(Debug)]
pub(super) struct LateMaterialization {
    key_columns: Vec<usize>,
}

impl LateMaterialization {
    pub(super) fn select(
        batch: &RecordBatch,
        ordering: &LexOrdering,
    ) -> Result<Option<Self>> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(None);
        }
        let mut key_columns: Vec<usize> = ordering
            .iter()
            .flat_map(|sort| collect_columns(&sort.expr))
            .map(|column| column.index())
            .collect();
        key_columns.sort_unstable();
        key_columns.dedup();
        let late = Self { key_columns };
        let keys = late.key_bytes(batch)?;
        let payload = batch.get_sliced_size()?.saturating_sub(keys);
        Ok((payload / rows >= MIN_PAYLOAD_BYTES_PER_ROW
            && payload >= MIN_PAYLOAD_PER_KEY_BYTE * keys)
            .then_some(late))
    }

    pub(super) fn reserved_bytes(
        &self,
        batch: &RecordBatch,
        counter: &mut RecordBatchMemoryCounter,
    ) -> Result<usize> {
        Ok(counter.count_batch(batch)
            + 2 * self.key_bytes(batch)?
            + ORDER_BYTES_PER_ROW * batch.num_rows())
    }

    fn key_bytes(&self, batch: &RecordBatch) -> Result<usize> {
        batch.project(&self.key_columns)?.get_sliced_size()
    }

    pub(super) fn applies_to(batches: &[RecordBatch]) -> bool {
        batches.iter().map(RecordBatch::num_rows).sum::<usize>() <= u32::MAX as usize
    }

    pub(super) fn output_rows(
        batches: &[RecordBatch],
        batch_size: usize,
    ) -> Result<usize> {
        Self::rows_per_batch(batches, OUTPUT_BATCH_BYTES, batch_size)
    }

    pub(super) fn spill_rows(
        batches: &[RecordBatch],
        buffered: usize,
        batch_size: usize,
    ) -> Result<usize> {
        let bytes = (buffered / SPILL_BATCHES_PER_RUN)
            .clamp(MIN_SPILL_BATCH_BYTES, OUTPUT_BATCH_BYTES);
        Self::rows_per_batch(batches, bytes, batch_size)
    }

    fn rows_per_batch(
        batches: &[RecordBatch],
        bytes: usize,
        batch_size: usize,
    ) -> Result<usize> {
        let mut rows = 0;
        let mut total = 0;
        for batch in batches {
            rows += batch.num_rows();
            total += batch.get_sliced_size()?;
        }
        let row_bytes = (total / rows.max(1)).max(1);
        Ok((bytes / row_bytes).clamp(1, batch_size.max(1)))
    }

    pub(super) fn sort_stream(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        ordering: LexOrdering,
        rows_per_batch: usize,
        reservation: MemoryReservation,
        elapsed_compute: Time,
    ) -> SendableRecordBatchStream {
        let stream = futures::stream::once({
            let schema = Arc::clone(&schema);
            async move {
                let gather = {
                    let _timer = elapsed_compute.timer();
                    Gather::try_new(
                        schema,
                        batches,
                        &ordering,
                        rows_per_batch,
                        reservation,
                        elapsed_compute.clone(),
                    )?
                };
                Ok::<_, datafusion_common::DataFusionError>(futures::stream::iter(gather))
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::TryStreamExt::try_flatten(stream),
        ))
    }
}

fn sort_order(batches: &[RecordBatch], ordering: &LexOrdering) -> Result<UInt32Array> {
    let columns = ordering
        .iter()
        .map(|expr| {
            let mut arrays = batches
                .iter()
                .map(|batch| Ok(expr.evaluate_to_sort_column(batch)?.values))
                .collect::<Result<Vec<ArrayRef>>>()?;
            let values = if arrays.len() == 1 {
                arrays.pop().unwrap()
            } else {
                let arrays: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
                concat(&arrays)?
            };
            Ok(SortColumn {
                values,
                options: Some(expr.options),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.len() > 1 {
        let fields: Vec<SortField> = columns
            .iter()
            .map(|column| {
                SortField::new_with_options(
                    column.values.data_type().clone(),
                    column.options.unwrap_or_default(),
                )
            })
            .collect();
        if RowConverter::supports_fields(&fields) {
            let converter = RowConverter::new(fields)?;
            let values: Vec<ArrayRef> =
                columns.into_iter().map(|column| column.values).collect();
            let rows = converter.convert_columns(&values)?;
            drop(values);
            return Ok(UInt32Array::from(row_order(&rows)));
        }
    }
    Ok(lexsort_to_indices(&columns, None)?)
}

fn row_order(rows: &Rows) -> Vec<u32> {
    if rows.num_rows() == 0 {
        return vec![];
    }
    let width = rows.row(0).as_ref().len();
    let fixed = width <= 16 && rows.iter().all(|row| row.as_ref().len() == width);
    if fixed {
        let mut keys: Vec<(u128, u32)> = rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let mut bytes = [0u8; 16];
                bytes[..width].copy_from_slice(row.as_ref());
                (u128::from_be_bytes(bytes), index as u32)
            })
            .collect();
        keys.sort_unstable();
        return keys.into_iter().map(|(_, index)| index).collect();
    }
    let mut keys: Vec<(&[u8], u32)> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| (row.data(), index as u32))
        .collect();
    keys.sort_unstable();
    keys.into_iter().map(|(_, index)| index).collect()
}

struct Gather {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    starts: Vec<usize>,
    remaining: Vec<usize>,
    order: UInt32Array,
    cursor: usize,
    rows_per_batch: usize,
    reservation: MemoryReservation,
    elapsed_compute: Time,
}

impl Gather {
    fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        ordering: &LexOrdering,
        rows_per_batch: usize,
        reservation: MemoryReservation,
        elapsed_compute: Time,
    ) -> Result<Self> {
        let order = sort_order(&batches, ordering)?;
        let mut starts = Vec::with_capacity(batches.len());
        let mut rows = 0;
        for batch in &batches {
            starts.push(rows);
            rows += batch.num_rows();
        }
        let mut gather = Self {
            schema,
            remaining: batches.iter().map(RecordBatch::num_rows).collect(),
            batches,
            starts,
            order,
            cursor: 0,
            rows_per_batch,
            reservation,
            elapsed_compute,
        };
        gather.release();
        Ok(gather)
    }

    fn release(&mut self) {
        let mut counter = RecordBatchMemoryCounter::new();
        for batch in &self.batches {
            counter.count_batch(batch);
        }
        let needed = counter.memory_usage() + self.order.get_array_memory_size();
        if self.reservation.size() > needed {
            self.reservation.shrink(self.reservation.size() - needed);
        }
    }

    fn next_batch(&mut self) -> Result<RecordBatch> {
        let elapsed_compute = self.elapsed_compute.clone();
        let _timer = elapsed_compute.timer();
        let end = (self.cursor + self.rows_per_batch).min(self.order.len());
        let order = self.order.slice(self.cursor, end - self.cursor);
        self.cursor = end;
        let indices: Vec<(usize, usize)> = order
            .values()
            .iter()
            .map(|&row| {
                let row = row as usize;
                let batch = self.starts.partition_point(|&start| start <= row) - 1;
                (batch, row - self.starts[batch])
            })
            .collect();
        let batch = if self.batches.len() == 1 {
            take_record_batch(&self.batches[0], &order)?
        } else {
            let columns = (0..self.schema.fields().len())
                .map(|column| {
                    let arrays: Vec<&dyn Array> = self
                        .batches
                        .iter()
                        .map(|batch| batch.column(column).as_ref())
                        .collect();
                    interleave(&arrays, &indices)
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(indices.len())),
            )?
        };
        let mut finished = false;
        for &(batch, _) in &indices {
            self.remaining[batch] -= 1;
            if self.remaining[batch] == 0 {
                self.batches[batch] = RecordBatch::new_empty(Arc::clone(&self.schema));
                finished = true;
            }
        }
        if finished {
            self.release();
        }
        Ok(batch)
    }
}

impl Iterator for Gather {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.cursor < self.order.len()).then(|| self.next_batch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute_stream;
    use crate::metrics::MetricsSet;
    use crate::sorts::sort::SortExec;
    use crate::test::TestMemoryExec;
    use crate::{ExecutionPlan, collect};
    use arrow::array::{
        BinaryArray, DictionaryArray, Int32Array, ListArray, StringArray, StringViewArray,
    };
    use arrow::compute::{SortOptions, concat_batches};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion_common::config::SpillCompression;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryLimit, MemoryPool};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::col;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct PeakPool {
        inner: GreedyMemoryPool,
        peak: AtomicUsize,
    }

    impl PeakPool {
        fn new(limit: usize) -> Arc<Self> {
            Arc::new(Self {
                inner: GreedyMemoryPool::new(limit),
                peak: AtomicUsize::new(0),
            })
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::Relaxed)
        }

        fn observe(&self) {
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        }
    }

    impl std::fmt::Display for PeakPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "peak({})", self.inner)
        }
    }

    impl MemoryPool for PeakPool {
        fn name(&self) -> &str {
            "peak"
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
            self.observe();
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
            self.observe();
            Ok(())
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }

        fn memory_limit(&self) -> MemoryLimit {
            self.inner.memory_limit()
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int32, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "list",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Field::new("view", DataType::Utf8View, true),
        ]))
    }

    fn batches(
        count: usize,
        rows: usize,
        width: usize,
        sorted: bool,
    ) -> Vec<RecordBatch> {
        let schema = schema();
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..count)
            .map(|b| {
                let values: Vec<u64> = (0..rows).map(|_| next()).collect();
                let k0 =
                    Int32Array::from_iter(values.iter().enumerate().map(|(i, v)| {
                        if sorted {
                            Some((b * rows + i) as i32 / 3)
                        } else {
                            (v % 11 != 0).then_some((v % 50) as i32)
                        }
                    }));
                let k1 = StringArray::from_iter(
                    values
                        .iter()
                        .map(|v| (v % 13 != 0).then(|| format!("k{}", (v >> 8) % 997))),
                );
                let payload = BinaryArray::from_iter(values.iter().map(|v| {
                    (v % 17 != 0).then(|| {
                        let mut bytes = vec![0u8; width];
                        for (i, chunk) in bytes.chunks_mut(8).enumerate() {
                            let word = v.rotate_left(i as u32);
                            chunk.copy_from_slice(&word.to_le_bytes()[..chunk.len()]);
                        }
                        bytes
                    })
                }));
                let dict: DictionaryArray<Int32Type> = values
                    .iter()
                    .map(|v| {
                        (v % 7 != 0)
                            .then_some(["a", "bb", "ccc", "dddd", "e"][(v % 5) as usize])
                    })
                    .collect();
                let list = ListArray::from_iter_primitive::<Int32Type, _, _>(
                    values.iter().map(|v| {
                        (v % 19 != 0).then(|| {
                            (0..(v % 4) as i32).map(|i| Some(i * (*v as i32 % 100)))
                        })
                    }),
                );
                let view = StringViewArray::from_iter(values.iter().map(|v| {
                    (v % 23 != 0).then(|| format!("view value longer than twelve {v}"))
                }));
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(k0),
                        Arc::new(k1),
                        Arc::new(payload),
                        Arc::new(dict),
                        Arc::new(list),
                        Arc::new(view),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    fn ordering(options: &[(&str, SortOptions)]) -> LexOrdering {
        let schema = schema();
        LexOrdering::new(options.iter().map(|(name, options)| {
            PhysicalSortExpr::new(col(name, &schema).unwrap(), *options)
        }))
        .unwrap()
    }

    fn two_keys() -> LexOrdering {
        ordering(&[
            (
                "k0",
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            ),
            (
                "k1",
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
        ])
    }

    fn one_key() -> LexOrdering {
        ordering(&[("k0", SortOptions::default())])
    }

    fn context(
        pool: Option<Arc<dyn MemoryPool>>,
        batch_size: usize,
        merge_bytes: usize,
    ) -> Arc<TaskContext> {
        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(pool) = pool {
            runtime = runtime.with_memory_pool(pool);
        }
        Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_sort_spill_reservation_bytes(merge_bytes)
                        .with_spill_compression(SpillCompression::Zstd),
                )
                .with_runtime(runtime.build_arc().unwrap()),
        )
    }

    fn sort_exec(input: &[RecordBatch], ordering: LexOrdering) -> Arc<SortExec> {
        let source =
            TestMemoryExec::try_new_exec(&[input.to_vec()], schema(), None).unwrap();
        Arc::new(SortExec::new(ordering, source))
    }

    async fn sort(
        input: &[RecordBatch],
        ordering: LexOrdering,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let sort = sort_exec(input, ordering);
        let output =
            collect(Arc::clone(&sort) as Arc<dyn ExecutionPlan>, context).await?;
        Ok((output, sort.metrics().unwrap()))
    }

    fn encoded_rows(
        batch: &RecordBatch,
        columns: &[ArrayRef],
        fields: Vec<SortField>,
    ) -> Vec<Vec<u8>> {
        let rows = RowConverter::new(fields)
            .unwrap()
            .convert_columns(columns)
            .unwrap();
        (0..batch.num_rows())
            .map(|i| rows.row(i).as_ref().to_vec())
            .collect()
    }

    fn assert_sorted_permutation(
        input: &[RecordBatch],
        output: &[RecordBatch],
        ordering: &LexOrdering,
    ) {
        let schema = schema();
        let input = concat_batches(&schema, input).unwrap();
        let output = concat_batches(&schema, output).unwrap();
        assert_eq!(input.num_rows(), output.num_rows());
        let keys: Vec<SortColumn> = ordering
            .iter()
            .map(|sort| sort.evaluate_to_sort_column(&output).unwrap())
            .collect();
        let fields = keys
            .iter()
            .map(|key| {
                SortField::new_with_options(
                    key.values.data_type().clone(),
                    key.options.unwrap(),
                )
            })
            .collect();
        let values: Vec<ArrayRef> = keys.into_iter().map(|key| key.values).collect();
        let sorted = encoded_rows(&output, &values, fields);
        assert!(sorted.windows(2).all(|pair| pair[0] <= pair[1]));
        let all = |batch: &RecordBatch| {
            let fields = schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect();
            let mut rows = encoded_rows(batch, batch.columns(), fields);
            rows.sort();
            rows
        };
        assert!(all(&input) == all(&output));
    }

    fn max_late_rows(input: &[RecordBatch]) -> usize {
        let rows: usize = input.iter().map(RecordBatch::num_rows).sum();
        let bytes: usize = input.iter().map(|b| b.get_sliced_size().unwrap()).sum();
        OUTPUT_BATCH_BYTES / (bytes / rows)
    }

    #[test]
    fn selects_payloads_wider_than_their_keys() -> Result<()> {
        let wide = &batches(1, 64, 256, false)[0];
        let narrow = &wide.project(&[0, 1, 3])?;
        assert!(LateMaterialization::select(wide, &two_keys())?.is_some());
        assert!(LateMaterialization::select(narrow, &two_keys())?.is_none());
        let payload_key = ordering(&[("payload", SortOptions::default())]);
        assert!(LateMaterialization::select(wide, &payload_key)?.is_none());
        let empty = wide.slice(0, 0);
        assert!(LateMaterialization::select(&empty, &two_keys())?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_sort_gathers_payloads_in_bounded_batches() -> Result<()> {
        for ordering in [one_key(), two_keys()] {
            let input = batches(8, 700, 2048, false);
            let (output, metrics) =
                sort(&input, ordering.clone(), context(None, 8192, 1 << 20)).await?;
            assert_eq!(metrics.spill_count(), Some(0));
            assert_sorted_permutation(&input, &output, &ordering);
            let bound = max_late_rows(&input);
            assert!(bound < 5600);
            assert!(output.iter().all(|batch| batch.num_rows() <= bound));
            assert!(output.len() > 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn single_small_and_view_inputs_match_the_reference() -> Result<()> {
        for (count, rows, width) in [(1, 1000, 512), (3, 5, 200), (4, 300, 8192)] {
            let input = batches(count, rows, width, false);
            let (output, _) =
                sort(&input, two_keys(), context(None, 64, 1 << 20)).await?;
            assert_sorted_permutation(&input, &output, &two_keys());
            assert!(output.iter().all(|batch| batch.num_rows() <= 64));
            assert!(
                output
                    .iter()
                    .all(|batch| batch.schema().field(2).data_type() == &DataType::Binary)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn spilled_sort_matches_the_reference() -> Result<()> {
        let input = batches(24, 200, 1024, false);
        let bytes: usize = input.iter().map(RecordBatch::get_array_memory_size).sum();
        for ordering in [one_key(), two_keys()] {
            let pool = PeakPool::new(bytes / 3);
            let (output, metrics) = sort(
                &input,
                ordering.clone(),
                context(Some(Arc::clone(&pool) as _), 8192, 1 << 20),
            )
            .await?;
            assert!(metrics.spill_count().unwrap() > 0);
            assert_sorted_permutation(&input, &output, &ordering);
            assert_eq!(pool.reserved(), 0);
            assert!(pool.peak() <= bytes / 3);
        }
        Ok(())
    }

    #[tokio::test]
    async fn multi_level_merge_of_many_spills_matches_the_reference() -> Result<()> {
        let input = batches(64, 100, 1024, false);
        let bytes: usize = input.iter().map(RecordBatch::get_array_memory_size).sum();
        let pool = PeakPool::new(bytes / 12);
        let (output, metrics) = sort(
            &input,
            two_keys(),
            context(Some(Arc::clone(&pool) as _), 256, 256 << 10),
        )
        .await?;
        assert!(metrics.spill_count().unwrap() >= 10);
        assert_sorted_permutation(&input, &output, &two_keys());
        assert_eq!(pool.reserved(), 0);
        assert!(pool.peak() <= bytes / 12);
        Ok(())
    }

    #[tokio::test]
    async fn fetch_matches_the_reference() -> Result<()> {
        let input = batches(6, 300, 1024, false);
        let source =
            TestMemoryExec::try_new_exec(std::slice::from_ref(&input), schema(), None)?;
        let sort = Arc::new(SortExec::new(two_keys(), source).with_fetch(Some(37)));
        let output = collect(sort, context(None, 8192, 1 << 20)).await?;
        let (full, _) = sort_all(&input).await?;
        let full = concat_batches(&schema(), &full)?;
        let output = concat_batches(&schema(), &output)?;
        assert_eq!(output.num_rows(), 37);
        for column in [0, 1] {
            assert_eq!(output.column(column), &full.column(column).slice(0, 37));
        }
        Ok(())
    }

    async fn sort_all(input: &[RecordBatch]) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        sort(input, two_keys(), context(None, 8192, 1 << 20)).await
    }

    #[tokio::test]
    async fn holds_the_input_once_instead_of_twice() -> Result<()> {
        let input = batches(16, 250, 4000, false);
        let bytes: usize = input.iter().map(RecordBatch::get_array_memory_size).sum();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(bytes * 3 / 2 + (4 << 20)));
        let (output, metrics) = sort(
            &input,
            one_key(),
            context(Some(Arc::clone(&pool)), 8192, 1 << 20),
        )
        .await?;
        assert!(bytes * 2 > bytes * 3 / 2 + (4 << 20));
        assert_eq!(metrics.spill_count(), Some(0));
        assert_sorted_permutation(&input, &output, &one_key());
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn releases_input_batches_as_their_rows_are_output() -> Result<()> {
        let input = batches(16, 250, 4000, true);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let sort = sort_exec(&input, one_key());
        let mut stream = execute_stream(sort, context(Some(Arc::clone(&pool)), 256, 0))?;
        let first = stream.next().await.unwrap()?;
        let held = pool.reserved();
        let mut rows = first.num_rows();
        let mut lowest = held;
        while let Some(batch) = stream.next().await {
            rows += batch?.num_rows();
            lowest = lowest.min(pool.reserved());
        }
        assert_eq!(rows, 4000);
        assert!(held > 0);
        assert!(lowest < held / 4);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
