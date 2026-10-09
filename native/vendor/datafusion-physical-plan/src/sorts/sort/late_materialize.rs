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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, RecordBatch, RecordBatchOptions, UInt32Array,
    new_empty_array,
};
use arrow::compute::{SortColumn, concat, interleave, lexsort_to_indices, take};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::HashMap;
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
const LOCAL_SOURCES: usize = 16;
const SAMPLED_CHUNKS: usize = 16;
const SPILL_COLUMN_SHARE: usize = 16;
const CHUNKS_PER_COLUMN: usize = 16;

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
        spilling: bool,
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
                        spilling,
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
    let mut keys: Vec<(u64, u32)> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| (prefix(row.data()), index as u32))
        .collect();
    keys.sort_unstable();
    let mut start = 0;
    while start < keys.len() {
        let mut end = start + 1;
        while end < keys.len() && keys[end].0 == keys[start].0 {
            end += 1;
        }
        if end - start > 1 {
            keys[start..end].sort_unstable_by(|a, b| {
                let left = rows.row(a.1 as usize);
                let right = rows.row(b.1 as usize);
                left.data().cmp(right.data()).then(a.1.cmp(&b.1))
            });
        }
        start = end;
    }
    keys.into_iter().map(|(_, index)| index).collect()
}

fn prefix(data: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    let len = data.len().min(8);
    bytes[..len].copy_from_slice(&data[..len]);
    u64::from_be_bytes(bytes)
}

struct Gather {
    schema: SchemaRef,
    columns: Vec<Pieces>,
    layouts: Vec<Layout>,
    holders: Vec<Vec<Vec<usize>>>,
    gathered_bytes: usize,
    local: bool,
    remaining: Vec<usize>,
    owners: HashMap<usize, (usize, usize)>,
    live_bytes: usize,
    order: UInt32Array,
    cursor: usize,
    rows_per_batch: usize,
    reservation: MemoryReservation,
    elapsed_compute: Time,
}

struct Pieces {
    arrays: Vec<ArrayRef>,
    starts: Vec<usize>,
    batches: Vec<Range<usize>>,
    layout: usize,
}

struct Layout {
    starts: Vec<usize>,
    slots: Vec<usize>,
    used: Vec<usize>,
    indices: Vec<(usize, usize)>,
    local: Option<UInt32Array>,
}

impl Layout {
    fn new(starts: Vec<usize>) -> Self {
        Self {
            slots: vec![usize::MAX; starts.len()],
            starts,
            used: vec![],
            indices: vec![],
            local: None,
        }
    }

    fn piece_of(starts: &[usize], row: usize) -> usize {
        starts.partition_point(|&start| start <= row) - 1
    }

    fn map(&mut self, order: &UInt32Array) {
        self.used.clear();
        self.indices.clear();
        self.local = None;
        if self.starts.len() == 1 {
            return;
        }
        for &row in order.values() {
            let row = row as usize;
            let piece = Self::piece_of(&self.starts, row);
            if self.slots[piece] == usize::MAX {
                self.slots[piece] = self.used.len();
                self.used.push(piece);
            }
            self.indices
                .push((self.slots[piece], row - self.starts[piece]));
        }
        for &piece in &self.used {
            self.slots[piece] = usize::MAX;
        }
        if self.used.len() == 1 {
            self.local = Some(UInt32Array::from_iter_values(
                self.indices.iter().map(|&(_, row)| row as u32),
            ));
        }
    }

    fn gather(&self, arrays: &[ArrayRef], order: &UInt32Array) -> Result<ArrayRef> {
        if self.starts.len() == 1 {
            return Ok(take(&arrays[0], order, None)?);
        }
        if let Some(local) = &self.local {
            return Ok(take(&arrays[self.used[0]], local, None)?);
        }
        let used: Vec<&dyn Array> = self
            .used
            .iter()
            .map(|&piece| arrays[piece].as_ref())
            .collect();
        Ok(interleave(&used, &self.indices)?)
    }
}

fn collect_buffers(data: &ArrayData, buffers: &mut Vec<(usize, usize)>) {
    for buffer in data.buffers() {
        buffers.push((buffer.data_ptr().as_ptr() as usize, buffer.capacity()));
    }
    if let Some(nulls) = data.nulls() {
        let buffer = nulls.inner().inner();
        buffers.push((buffer.data_ptr().as_ptr() as usize, buffer.capacity()));
    }
    for child in data.child_data() {
        collect_buffers(child, buffers);
    }
}

fn sliced_bytes(array: &dyn Array) -> Result<usize> {
    let mut bytes = array.to_data().get_slice_memory_size()?;
    match array.data_type() {
        DataType::Utf8View => {
            bytes += array
                .as_string_view()
                .data_buffers()
                .iter()
                .map(|b| b.len())
                .sum::<usize>()
        }
        DataType::BinaryView => {
            bytes += array
                .as_binary_view()
                .data_buffers()
                .iter()
                .map(|b| b.len())
                .sum::<usize>()
        }
        _ => {}
    }
    Ok(bytes)
}

fn offsets_fit(arrays: &[ArrayRef]) -> bool {
    match arrays[0].data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            let mut total = 0usize;
            let mut children = Vec::with_capacity(arrays.len());
            for array in arrays {
                let (offsets, values) = match array.as_list_opt::<i32>() {
                    Some(list) => (list.value_offsets(), Arc::clone(list.values())),
                    None => {
                        let map = array.as_map();
                        (
                            map.value_offsets(),
                            Arc::new(map.entries().clone()) as ArrayRef,
                        )
                    }
                };
                let start = offsets[0] as usize;
                let end = offsets[array.len()] as usize;
                total += end - start;
                children.push(values.slice(start, end - start));
            }
            total <= i32::MAX as usize && offsets_fit(&children)
        }
        DataType::LargeList(_) => {
            let children: Vec<ArrayRef> = arrays
                .iter()
                .map(|array| {
                    let list = array.as_list::<i64>();
                    let offsets = list.value_offsets();
                    let start = offsets[0] as usize;
                    let end = offsets[list.len()] as usize;
                    list.values().slice(start, end - start)
                })
                .collect();
            offsets_fit(&children)
        }
        DataType::FixedSizeList(_, _) => {
            let children: Vec<ArrayRef> = arrays
                .iter()
                .map(|array| Arc::clone(array.as_fixed_size_list().values()))
                .collect();
            offsets_fit(&children)
        }
        DataType::Struct(fields) => (0..fields.len()).all(|field| {
            let children: Vec<ArrayRef> = arrays
                .iter()
                .map(|array| Arc::clone(array.as_struct().column(field)))
                .collect();
            offsets_fit(&children)
        }),
        DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Union(_, _)
        | DataType::RunEndEncoded(_, _) => false,
        _ => true,
    }
}

impl Gather {
    fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        ordering: &LexOrdering,
        rows_per_batch: usize,
        spilling: bool,
        reservation: MemoryReservation,
        elapsed_compute: Time,
    ) -> Result<Self> {
        let order = sort_order(&batches, ordering)?;
        let width = schema.fields().len();
        let mut starts = Vec::with_capacity(batches.len());
        let mut holders = Vec::with_capacity(batches.len());
        let mut owners: HashMap<usize, (usize, usize)> = HashMap::new();
        let mut live_bytes = 0;
        let mut rows = 0;
        for batch in &batches {
            starts.push(rows);
            rows += batch.num_rows();
            let mut held = Vec::with_capacity(width);
            for column in batch.columns() {
                let mut found = vec![];
                collect_buffers(&column.to_data(), &mut found);
                found.sort_unstable();
                found.dedup_by_key(|(ptr, _)| *ptr);
                for &(ptr, capacity) in &found {
                    let owner = owners.entry(ptr).or_insert_with(|| {
                        live_bytes += capacity;
                        (0, capacity)
                    });
                    owner.0 += 1;
                }
                held.push(found.into_iter().map(|(ptr, _)| ptr).collect());
            }
            holders.push(held);
        }
        let columns = (0..width)
            .map(|column| Pieces {
                arrays: batches
                    .iter()
                    .map(|batch| Arc::clone(batch.column(column)))
                    .collect(),
                starts: starts.clone(),
                batches: (0..batches.len()).map(|batch| batch..batch + 1).collect(),
                layout: 0,
            })
            .collect();
        let mut gather = Self {
            schema,
            columns,
            layouts: vec![],
            holders,
            gathered_bytes: 0,
            local: true,
            remaining: batches.iter().map(RecordBatch::num_rows).collect(),
            owners,
            live_bytes,
            order,
            cursor: 0,
            rows_per_batch,
            reservation,
            elapsed_compute,
        };
        drop(batches);
        gather.fit();
        if starts.len() > 1 && !gather.scattered_sources_fit_locally(&starts) {
            gather.local = false;
            let buffered = gather.live_bytes;
            for column in 0..width {
                if spilling {
                    gather.concatenate(column, buffered / SPILL_COLUMN_SHARE)?;
                } else {
                    gather.concatenate(column, usize::MAX)?;
                    let total = gather.column_bytes(column)?;
                    gather.concatenate(column, total / CHUNKS_PER_COLUMN)?;
                }
            }
        }
        let mut layouts: Vec<Layout> = vec![];
        for column in gather.columns.iter_mut() {
            column.layout = match layouts
                .iter()
                .position(|layout| layout.starts == column.starts)
            {
                Some(layout) => layout,
                None => {
                    layouts.push(Layout::new(column.starts.clone()));
                    layouts.len() - 1
                }
            };
        }
        gather.layouts = layouts;
        Ok(gather)
    }

    fn needed(&self) -> usize {
        self.live_bytes + self.gathered_bytes + self.order.get_array_memory_size()
    }

    fn fit(&mut self) {
        let needed = self.needed();
        let size = self.reservation.size();
        if size > needed {
            self.reservation.shrink(size - needed);
        } else if size < needed {
            self.reservation.grow(needed - size);
        }
    }

    fn scattered_sources_fit_locally(&self, starts: &[usize]) -> bool {
        let chunks = self.order.len().div_ceil(self.rows_per_batch);
        let sampled = chunks.min(SAMPLED_CHUNKS);
        let mut slots = vec![false; starts.len()];
        let mut used = vec![];
        let mut sources = 0;
        for sample in 0..sampled {
            let start = sample * chunks / sampled * self.rows_per_batch;
            let end = (start + self.rows_per_batch).min(self.order.len());
            for &row in &self.order.values()[start..end] {
                let batch = Layout::piece_of(starts, row as usize);
                if !slots[batch] {
                    slots[batch] = true;
                    used.push(batch);
                }
            }
            sources += used.len();
            for batch in used.drain(..) {
                slots[batch] = false;
            }
        }
        sources <= LOCAL_SOURCES * sampled
    }

    fn column_bytes(&self, column: usize) -> Result<usize> {
        let mut bytes = 0;
        for array in &self.columns[column].arrays {
            bytes += sliced_bytes(array.as_ref())?;
        }
        Ok(bytes)
    }

    fn concatenate(&mut self, column: usize, budget: usize) -> Result<()> {
        let mut sizes = Vec::with_capacity(self.columns[column].arrays.len());
        for array in &self.columns[column].arrays {
            sizes.push(sliced_bytes(array.as_ref())?);
        }
        let mut groups = vec![];
        let mut start = 0;
        while start < sizes.len() {
            let mut end = start + 1;
            let mut bytes = sizes[start];
            while end < sizes.len() && bytes + sizes[end] <= budget {
                bytes += sizes[end];
                end += 1;
            }
            groups.push((start..end, bytes));
            start = end;
        }
        if groups.len() == sizes.len() {
            return Ok(());
        }
        let old = std::mem::replace(
            &mut self.columns[column],
            Pieces {
                arrays: vec![],
                starts: vec![],
                batches: vec![],
                layout: 0,
            },
        );
        let mut pieces = Pieces {
            arrays: Vec::with_capacity(groups.len()),
            starts: Vec::with_capacity(groups.len()),
            batches: Vec::with_capacity(groups.len()),
            layout: 0,
        };
        for (group, bytes) in groups {
            let merged = if group.len() > 1 {
                self.merge(&old.arrays[group.clone()], bytes)
            } else {
                None
            };
            match merged {
                Some(array) => {
                    let batches =
                        old.batches[group.start].start..old.batches[group.end - 1].end;
                    self.gathered_bytes += array.get_array_memory_size();
                    for batch in batches.clone() {
                        self.release(batch, column);
                    }
                    self.fit();
                    pieces.arrays.push(array);
                    pieces.starts.push(old.starts[group.start]);
                    pieces.batches.push(batches);
                }
                None => {
                    for piece in group {
                        pieces.arrays.push(Arc::clone(&old.arrays[piece]));
                        pieces.starts.push(old.starts[piece]);
                        pieces.batches.push(old.batches[piece].clone());
                    }
                }
            }
        }
        drop(old);
        self.columns[column] = pieces;
        self.fit();
        Ok(())
    }

    fn merge(&mut self, arrays: &[ArrayRef], bytes: usize) -> Option<ArrayRef> {
        if !offsets_fit(arrays) {
            return None;
        }
        let needed = self.needed() + bytes;
        let size = self.reservation.size();
        if size < needed && self.reservation.try_grow(needed - size).is_err() {
            return None;
        }
        let arrays: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
        let merged = concat(&arrays)
            .ok()
            .filter(|array| take(array, &UInt32Array::from(vec![0u32]), None).is_ok());
        if merged.is_none() {
            self.fit();
        }
        merged
    }

    fn release(&mut self, batch: usize, column: usize) {
        for ptr in std::mem::take(&mut self.holders[batch][column]) {
            if let Some(owner) = self.owners.get_mut(&ptr) {
                owner.0 -= 1;
                if owner.0 == 0 {
                    self.live_bytes -= owner.1;
                    self.owners.remove(&ptr);
                }
            }
        }
    }

    fn finish(&mut self, batch: usize) {
        for column in 0..self.columns.len() {
            let pieces = &mut self.columns[column];
            pieces.arrays[batch] = new_empty_array(pieces.arrays[batch].data_type());
            self.release(batch, column);
        }
    }

    fn next_batch(&mut self) -> Result<RecordBatch> {
        let elapsed_compute = self.elapsed_compute.clone();
        let _timer = elapsed_compute.timer();
        let end = (self.cursor + self.rows_per_batch).min(self.order.len());
        let order = self.order.slice(self.cursor, end - self.cursor);
        self.cursor = end;
        for layout in self.layouts.iter_mut() {
            layout.map(&order);
        }
        let columns = self
            .columns
            .iter()
            .map(|pieces| self.layouts[pieces.layout].gather(&pieces.arrays, &order))
            .collect::<Result<Vec<_>>>()?;
        let mut finished = vec![];
        if self.local {
            let layout = &self.layouts[0];
            if layout.starts.len() == 1 {
                self.remaining[0] -= order.len();
                if self.remaining[0] == 0 {
                    finished.push(0);
                }
            } else {
                for &(slot, _) in &layout.indices {
                    let batch = layout.used[slot];
                    self.remaining[batch] -= 1;
                    if self.remaining[batch] == 0 {
                        finished.push(batch);
                    }
                }
            }
        }
        for batch in &finished {
            self.finish(*batch);
        }
        let done = self.cursor == self.order.len();
        if done {
            for pieces in self.columns.iter_mut() {
                pieces.arrays.clear();
            }
            for batch in 0..self.holders.len() {
                for column in 0..self.columns.len() {
                    self.release(batch, column);
                }
            }
            self.gathered_bytes = 0;
        }
        if !finished.is_empty() || done {
            self.fit();
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(order.len())),
        )?)
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
    use arrow::compute::take_record_batch;
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
    async fn many_tiny_input_batches_match_the_reference() -> Result<()> {
        let input = batches(600, 3, 1024, false);
        let bytes: usize = input.iter().map(RecordBatch::get_array_memory_size).sum();
        let (output, _) = sort(&input, two_keys(), context(None, 8192, 1 << 20)).await?;
        assert_sorted_permutation(&input, &output, &two_keys());
        let pool = PeakPool::new(bytes / 3);
        let (output, metrics) = sort(
            &input,
            two_keys(),
            context(Some(Arc::clone(&pool) as _), 8192, 1 << 20),
        )
        .await?;
        assert!(metrics.spill_count().unwrap() > 0);
        assert_sorted_permutation(&input, &output, &two_keys());
        assert_eq!(pool.reserved(), 0);
        assert!(pool.peak() <= bytes / 3);
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

    fn mixed_schema() -> SchemaRef {
        let item = Arc::new(Field::new_list_field(DataType::Int32, true));
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("day", DataType::Date32, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("count", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("blob", DataType::Binary, true),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("list", DataType::List(Arc::clone(&item)), true),
            Field::new(
                "pair",
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::Int64, true),
                        Field::new("b", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("view", DataType::Utf8View, true),
        ]))
    }

    fn mixed_batches(count: usize, rows: usize, seed: u64) -> Vec<RecordBatch> {
        use arrow::array::{
            BooleanArray, Date32Array, Float64Array, Int64Array, StructArray,
        };
        let schema = mixed_schema();
        let mut state = seed | 1;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..count)
            .map(|_| {
                let values: Vec<u64> = (0..rows).map(|_| next()).collect();
                let k = Int32Array::from_iter(
                    values
                        .iter()
                        .map(|v| (v % 9 != 0).then_some((v % 7) as i32 - 3)),
                );
                let s = StringArray::from_iter(values.iter().map(|v| {
                    (v % 5 != 0).then(|| {
                        format!("{:0>w$}", (v >> 20) % 300, w = (v % 12) as usize)
                    })
                }));
                let flag = BooleanArray::from_iter(
                    values.iter().map(|v| (v % 3 != 0).then_some(v & 64 != 0)),
                );
                let day = Date32Array::from_iter(
                    values
                        .iter()
                        .map(|v| (v % 4 == 0).then_some((v % 400) as i32)),
                );
                let amount = Float64Array::from_iter(
                    values
                        .iter()
                        .map(|v| (v % 6 == 0).then_some((v % 1000) as f64 / 7.0)),
                );
                let count = Int64Array::from_iter(
                    values
                        .iter()
                        .map(|v| (v % 2 == 0).then_some((v >> 3) as i64)),
                );
                let name = StringArray::from_iter(values.iter().map(|v| {
                    (v % 8 == 0).then(|| {
                        format!("name-{}-{}", v % 977, "x".repeat((v % 40) as usize))
                    })
                }));
                let blob = BinaryArray::from_iter(
                    values
                        .iter()
                        .map(|v| (v % 10 != 0).then(|| v.to_le_bytes().repeat(20))),
                );
                let dict: DictionaryArray<arrow::datatypes::Int8Type> = values
                    .iter()
                    .map(|v| (v % 7 != 0).then_some(["p", "qq", "rrr"][(v % 3) as usize]))
                    .collect();
                let list = ListArray::from_iter_primitive::<Int32Type, _, _>(
                    values.iter().map(|v| {
                        (v % 5 != 1).then(|| (0..(v % 3) as i32).map(|i| Some(i + 1)))
                    }),
                );
                let pair = StructArray::from(vec![
                    (
                        Arc::new(Field::new("a", DataType::Int64, true)),
                        Arc::new(Int64Array::from_iter(
                            values.iter().map(|v| (v % 3 == 1).then_some(*v as i64)),
                        )) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("b", DataType::Utf8, true)),
                        Arc::new(StringArray::from_iter(
                            values
                                .iter()
                                .map(|v| (v % 4 == 1).then(|| format!("b{}", v % 13))),
                        )) as ArrayRef,
                    ),
                ]);
                let view = StringViewArray::from_iter(values.iter().map(|v| {
                    (v % 11 != 0)
                        .then(|| format!("a view of more than twelve bytes {}", v % 51))
                }));
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(k),
                        Arc::new(s),
                        Arc::new(flag),
                        Arc::new(day),
                        Arc::new(amount),
                        Arc::new(count),
                        Arc::new(name),
                        Arc::new(blob),
                        Arc::new(dict),
                        Arc::new(list),
                        Arc::new(pair),
                        Arc::new(view),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    fn mixed_ordering(options: &[(&str, bool, bool)]) -> LexOrdering {
        let schema = mixed_schema();
        LexOrdering::new(options.iter().map(|(name, descending, nulls_first)| {
            PhysicalSortExpr::new(
                col(name, &schema).unwrap(),
                SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                },
            )
        }))
        .unwrap()
    }

    fn mixed_orderings() -> Vec<LexOrdering> {
        vec![
            mixed_ordering(&[("k", false, false)]),
            mixed_ordering(&[("k", true, true)]),
            mixed_ordering(&[("k", true, false), ("s", false, true)]),
            mixed_ordering(&[
                ("s", true, true),
                ("day", false, false),
                ("k", false, true),
            ]),
            mixed_ordering(&[("flag", false, true), ("s", false, false)]),
        ]
    }

    fn reference(input: &[RecordBatch], ordering: &LexOrdering) -> RecordBatch {
        let batch = concat_batches(&input[0].schema(), input).unwrap();
        let columns: Vec<SortColumn> = ordering
            .iter()
            .map(|sort| sort.evaluate_to_sort_column(&batch).unwrap())
            .collect();
        let fields: Vec<SortField> = columns
            .iter()
            .map(|c| {
                SortField::new_with_options(
                    c.values.data_type().clone(),
                    c.options.unwrap(),
                )
            })
            .collect();
        let values: Vec<ArrayRef> = columns.into_iter().map(|c| c.values).collect();
        let rows = RowConverter::new(fields)
            .unwrap()
            .convert_columns(&values)
            .unwrap();
        let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
        order.sort_by(|a, b| {
            rows.row(*a as usize)
                .cmp(&rows.row(*b as usize))
                .then(a.cmp(b))
        });
        take_record_batch(&batch, &UInt32Array::from(order)).unwrap()
    }

    fn assert_matches_reference(
        input: &[RecordBatch],
        output: &[RecordBatch],
        ordering: &LexOrdering,
    ) {
        let expected = reference(input, ordering);
        let actual = concat_batches(&input[0].schema(), output).unwrap();
        assert_eq!(expected.num_rows(), actual.num_rows());
        let key_columns: Vec<usize> = ordering
            .iter()
            .flat_map(|sort| collect_columns(&sort.expr))
            .map(|column| column.index())
            .collect();
        for column in key_columns {
            assert_eq!(expected.column(column), actual.column(column));
        }
        let all = |batch: &RecordBatch| {
            let fields = batch
                .schema()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect();
            let mut rows = encoded_rows(batch, batch.columns(), fields);
            rows.sort();
            rows
        };
        assert!(all(&expected) == all(&actual));
        let fields = actual
            .schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect::<Vec<_>>();
        let expected_rows = encoded_rows(&expected, expected.columns(), fields.clone());
        let actual_rows = encoded_rows(&actual, actual.columns(), fields);
        let keys = |batch: &RecordBatch| {
            let columns: Vec<SortColumn> = ordering
                .iter()
                .map(|sort| sort.evaluate_to_sort_column(batch).unwrap())
                .collect();
            let fields = columns
                .iter()
                .map(|c| {
                    SortField::new_with_options(
                        c.values.data_type().clone(),
                        c.options.unwrap(),
                    )
                })
                .collect();
            let values: Vec<ArrayRef> = columns.into_iter().map(|c| c.values).collect();
            encoded_rows(batch, &values, fields)
        };
        let (expected_keys, actual_keys) = (keys(&expected), keys(&actual));
        let mut start = 0;
        while start < expected_keys.len() {
            let mut end = start + 1;
            while end < expected_keys.len() && expected_keys[end] == expected_keys[start]
            {
                end += 1;
            }
            assert!(
                actual_keys[start..end]
                    .iter()
                    .all(|k| k == &expected_keys[start])
            );
            let mut want = expected_rows[start..end].to_vec();
            let mut got = actual_rows[start..end].to_vec();
            want.sort();
            got.sort();
            assert!(want == got);
            start = end;
        }
    }

    fn mixed_sort_exec(input: &[RecordBatch], ordering: LexOrdering) -> Arc<SortExec> {
        let source =
            TestMemoryExec::try_new_exec(&[input.to_vec()], mixed_schema(), None)
                .unwrap();
        Arc::new(SortExec::new(ordering, source))
    }

    fn gather(
        input: Vec<RecordBatch>,
        ordering: &LexOrdering,
        rows_per_batch: usize,
        spilling: bool,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<Gather> {
        let reservation =
            datafusion_execution::memory_pool::MemoryConsumer::new("gather")
                .register(pool);
        let mut counter = RecordBatchMemoryCounter::new();
        let late = LateMaterialization::select(&input[0], ordering)?.unwrap();
        let mut size = 0;
        for batch in &input {
            size += late.reserved_bytes(batch, &mut counter)?;
        }
        reservation.try_grow(size)?;
        Gather::try_new(
            mixed_schema(),
            input,
            ordering,
            rows_per_batch,
            spilling,
            reservation,
            Time::new(),
        )
    }

    #[tokio::test]
    async fn wide_mixed_rows_in_many_batches_match_the_reference() -> Result<()> {
        for (count, rows, batch_size) in [(1, 3000, 512), (40, 97, 256), (300, 7, 8192)] {
            let input = mixed_batches(count, rows, count as u64 * 7919 + rows as u64);
            assert!(
                LateMaterialization::select(&input[0], &mixed_orderings()[0])?.is_some()
            );
            for ordering in mixed_orderings() {
                let sort = mixed_sort_exec(&input, ordering.clone());
                let output = collect(sort, context(None, batch_size, 1 << 20)).await?;
                assert_matches_reference(&input, &output, &ordering);
                assert!(output.iter().all(|batch| batch.num_rows() <= batch_size));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn spilled_wide_mixed_rows_match_the_reference() -> Result<()> {
        let input = mixed_batches(64, 150, 42);
        let bytes: usize = input.iter().map(RecordBatch::get_array_memory_size).sum();
        for ordering in mixed_orderings() {
            let pool = PeakPool::new(bytes / 3);
            let sort = mixed_sort_exec(&input, ordering.clone());
            let output = collect(
                Arc::clone(&sort) as Arc<dyn ExecutionPlan>,
                context(Some(Arc::clone(&pool) as _), 1024, 256 << 10),
            )
            .await?;
            assert!(sort.metrics().unwrap().spill_count().unwrap() > 0);
            assert_matches_reference(&input, &output, &ordering);
            assert_eq!(pool.reserved(), 0);
        }
        Ok(())
    }

    #[test]
    fn concatenates_every_column_of_a_scattered_order() -> Result<()> {
        let input = mixed_batches(50, 100, 7);
        let ordering = mixed_ordering(&[("k", false, true), ("s", true, false)]);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let mut gather = gather(input.clone(), &ordering, 256, false, &pool)?;
        assert!(!gather.local);
        assert!(gather.columns.iter().all(|pieces| pieces.arrays.len() == 1));
        assert_eq!(gather.layouts.len(), 1);
        assert_eq!(gather.live_bytes, 0);
        assert_eq!(pool.reserved(), gather.needed());
        let mut output = vec![];
        while gather.cursor < gather.order.len() {
            output.push(gather.next_batch()?);
            assert_eq!(pool.reserved(), gather.needed());
        }
        assert_eq!(gather.gathered_bytes, 0);
        assert_eq!(pool.reserved(), gather.order.get_array_memory_size());
        drop(gather);
        assert_eq!(pool.reserved(), 0);
        assert_matches_reference(&input, &output, &ordering);
        Ok(())
    }

    #[test]
    fn interleaves_an_order_that_reads_few_batches_at_a_time() -> Result<()> {
        let input = mixed_batches(50, 100, 7);
        let mut keyed = vec![];
        for (i, batch) in input.iter().enumerate() {
            let mut columns = batch.columns().to_vec();
            columns[0] = Arc::new(Int32Array::from(vec![i as i32; batch.num_rows()]));
            keyed.push(RecordBatch::try_new(mixed_schema(), columns)?);
        }
        let ordering = mixed_ordering(&[("k", false, true)]);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let mut gather = gather(keyed.clone(), &ordering, 64, false, &pool)?;
        assert!(gather.local);
        assert!(
            gather
                .columns
                .iter()
                .all(|pieces| pieces.arrays.len() == 50)
        );
        let held = pool.reserved();
        let mut output = vec![gather.next_batch()?];
        let mut lowest = held;
        while gather.cursor < gather.order.len() {
            output.push(gather.next_batch()?);
            lowest = lowest.min(pool.reserved());
        }
        assert!(lowest < held / 4);
        drop(gather);
        assert_matches_reference(&keyed, &output, &ordering);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn concatenates_in_chunks_a_column_the_pool_cannot_hold_twice() -> Result<()> {
        let input = mixed_batches(50, 100, 11);
        let ordering = mixed_ordering(&[("s", false, true), ("k", true, true)]);
        let late = LateMaterialization::select(&input[0], &ordering)?.unwrap();
        let mut counter = RecordBatchMemoryCounter::new();
        let mut held = 0;
        for batch in &input {
            held += late.reserved_bytes(batch, &mut counter)?;
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(held));
        let gather = gather(input.clone(), &ordering, 256, false, &pool)?;
        let blob = gather.columns[7].arrays.len();
        assert!(blob > 1 && blob < 50, "{blob} pieces");
        assert_eq!(gather.columns[2].arrays.len(), 1);
        assert_eq!(gather.layouts.len(), 2);
        assert_eq!(pool.reserved(), gather.needed());
        let output = gather.collect::<Result<Vec<_>>>()?;
        assert_matches_reference(&input, &output, &ordering);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn a_spill_concatenates_columns_in_chunks_of_a_share_of_its_input() -> Result<()> {
        let input = mixed_batches(50, 100, 13);
        let ordering = mixed_ordering(&[("k", false, false), ("s", false, false)]);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let gather = gather(input.clone(), &ordering, 256, true, &pool)?;
        let buffered: usize = input.iter().map(|b| b.get_sliced_size().unwrap()).sum();
        for pieces in &gather.columns {
            assert!(pieces.arrays.len() < 50);
            for (array, batches) in pieces.arrays.iter().zip(&pieces.batches) {
                if batches.len() > 1 {
                    let bytes = sliced_bytes(array.as_ref())?;
                    assert!(
                        bytes <= 2 * buffered / SPILL_COLUMN_SHARE,
                        "{bytes} of {buffered}"
                    );
                }
            }
        }
        assert!(gather.columns[7].arrays.len() > 1);
        assert_eq!(gather.columns[2].arrays.len(), 1);
        let output = gather.collect::<Result<Vec<_>>>()?;
        assert_matches_reference(&input, &output, &ordering);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn list_offsets_that_would_overflow_are_not_concatenated() -> Result<()> {
        let list_type =
            DataType::List(Arc::new(Field::new_list_field(DataType::Null, true)));
        let list = |len: i32| {
            ArrayData::builder(list_type.clone())
                .len(1)
                .add_buffer(arrow::buffer::Buffer::from_slice_ref([0i32, len]))
                .add_child_data(ArrayData::new_null(&DataType::Null, len as usize))
                .build()
        };
        let small = arrow::array::make_array(list(1)?);
        let big = arrow::array::make_array(list(i32::MAX - 1)?);
        assert!(offsets_fit(&[Arc::clone(&small), Arc::clone(&small)]));
        assert!(offsets_fit(&[Arc::clone(&big), Arc::clone(&small)]));
        assert!(!offsets_fit(&[Arc::clone(&big), Arc::clone(&small), small]));
        assert!(!offsets_fit(&[Arc::clone(&big), big]));
        Ok(())
    }

    #[test]
    fn row_order_matches_a_full_comparison_of_the_rows() -> Result<()> {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for options in [
            SortOptions::default(),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ] {
            let values: Vec<u64> = (0..20000).map(|_| next()).collect();
            let strings = StringArray::from_iter(values.iter().map(|v| {
                (v % 9 != 0).then(|| {
                    let len = (v >> 8) % 14;
                    let base = ["", "a", "ab", "abcdefg", "abcdefgh", "abcdefghij", "b"]
                        [(v % 7) as usize];
                    format!("{base}{}", "z".repeat(len as usize % 3))
                        .repeat((len / 5) as usize + 1)
                })
            }));
            let ints = Int32Array::from_iter(
                values
                    .iter()
                    .map(|v| (v % 5 != 0).then_some(((v >> 16) % 4) as i32 - 2)),
            );
            let bytes = BinaryArray::from_iter(
                values
                    .iter()
                    .map(|v| (v % 6 != 0).then(|| vec![0u8; ((v >> 24) % 10) as usize])),
            );
            let columns: Vec<ArrayRef> =
                vec![Arc::new(strings), Arc::new(ints), Arc::new(bytes)];
            for width in 1..=3 {
                let fields = columns[..width]
                    .iter()
                    .map(|c| SortField::new_with_options(c.data_type().clone(), options))
                    .collect();
                let rows =
                    RowConverter::new(fields)?.convert_columns(&columns[..width])?;
                let mut expected: Vec<u32> = (0..rows.num_rows() as u32).collect();
                expected.sort_by(|a, b| {
                    rows.row(*a as usize)
                        .cmp(&rows.row(*b as usize))
                        .then(a.cmp(b))
                });
                assert_eq!(row_order(&rows), expected);
            }
        }
        Ok(())
    }

    fn list_of(values: ArrayRef, lengths: Vec<usize>) -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            arrow::buffer::OffsetBuffer::from_lengths(lengths),
            values,
            None,
        ))
    }

    #[test]
    fn offsets_fit_reads_a_list_nested_in_a_sliced_list() {
        let inner = ListArray::from_iter_primitive::<Int32Type, _, _>(
            (0..17).map(|i| Some(vec![Some(i)])),
        );
        let outer = list_of(Arc::new(inner), vec![17, 0]);
        let tail = outer.slice(1, 1);
        assert!(offsets_fit(&[Arc::clone(&tail), tail]));
    }

    #[test]
    fn offsets_fit_reads_a_struct_nested_in_a_sliced_list() {
        let item = arrow::array::StructArray::from(vec![(
            Arc::new(Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )]);
        let list = list_of(Arc::new(item), vec![2, 1]);
        let tail = list.slice(1, 1);
        assert!(offsets_fit(&[Arc::clone(&tail), tail, list]));
    }

    #[test]
    fn nested_list_offsets_that_would_overflow_are_not_concatenated() -> Result<()> {
        let list =
            |len: usize| list_of(Arc::new(arrow::array::NullArray::new(len)), vec![len]);
        let nested = |len: usize| -> ArrayRef {
            let wrapped = arrow::array::StructArray::from(vec![(
                Arc::new(Field::new("l", list(len).data_type().clone(), true)),
                list(len),
            )]);
            list_of(Arc::new(wrapped), vec![1])
        };
        let small = nested(1);
        let big = nested(i32::MAX as usize - 1);
        assert!(offsets_fit(&[Arc::clone(&big), Arc::clone(&small)]));
        assert!(!offsets_fit(&[Arc::clone(&big), Arc::clone(&small), small]));
        assert!(!offsets_fit(&[Arc::clone(&big), big]));
        Ok(())
    }

    fn sliced_nested_batches(count: usize, rows: usize, seed: u64) -> Vec<RecordBatch> {
        use arrow::array::{MapArray, NullBufferBuilder, StructArray};
        let total = count * rows;
        let mut state = seed | 1;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut keys: Vec<i32> = (0..total as i32).collect();
        for i in (1..total).rev() {
            keys.swap(i, (next() % (i as u64 + 1)) as usize);
        }
        let (mut item_lengths, mut item_nulls) = (vec![], NullBufferBuilder::new(total));
        let (mut xs, mut tag_lengths, mut tags) = (vec![], vec![], vec![]);
        let (mut attr_lengths, mut attr_keys, mut attr_values) = (vec![], vec![], vec![]);
        let mut pad = vec![];
        for row in 0..total {
            let v = next();
            if v % 7 == 0 {
                item_lengths.push(0);
                item_nulls.append_null();
            } else {
                let items = (v % 4) as usize;
                item_lengths.push(items);
                item_nulls.append_non_null();
                for item in 0..items {
                    xs.push((v >> (item * 8)) as i32);
                    let n = ((v >> (item * 4)) % 3) as usize;
                    tag_lengths.push(n);
                    for t in 0..n {
                        tags.push(format!("tag-{row}-{item}-{t}"));
                    }
                }
            }
            let attrs = ((v >> 32) % 3) as usize;
            attr_lengths.push(attrs);
            for a in 0..attrs {
                attr_keys.push(format!("key{a}"));
                attr_values.push((v % 5 != 0).then(|| format!("value-{row}-{a}")));
            }
            pad.push(format!("{row:0>80}"));
        }
        let tags = list_of(Arc::new(StringArray::from(tags)), tag_lengths);
        let item = StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, true)),
                Arc::new(Int32Array::from(xs)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("tags", tags.data_type().clone(), true)),
                tags,
            ),
        ]);
        let items: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(item.data_type().clone(), true)),
            arrow::buffer::OffsetBuffer::from_lengths(item_lengths),
            Arc::new(item),
            item_nulls.finish(),
        ));
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(StringArray::from(attr_keys)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Utf8, true)),
                Arc::new(StringArray::from(attr_values)) as ArrayRef,
            ),
        ]);
        let attrs: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            arrow::buffer::OffsetBuffer::from_lengths(attr_lengths),
            entries,
            None,
            false,
        ));
        let batch = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int32Array::from(keys)) as ArrayRef),
            ("items", items),
            ("attrs", attrs),
            ("pad", Arc::new(StringArray::from(pad)) as ArrayRef),
        ])
        .unwrap();
        (0..count).map(|i| batch.slice(i * rows, rows)).collect()
    }

    #[tokio::test]
    async fn sliced_nested_rows_match_the_reference() -> Result<()> {
        let input = sliced_nested_batches(40, 50, 17);
        let schema = input[0].schema();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("k", &schema)?,
            SortOptions::default(),
        )])
        .unwrap();
        assert!(LateMaterialization::select(&input[0], &ordering)?.is_some());
        let all = concat_batches(&schema, &input)?;
        let indices = arrow::compute::sort_to_indices(all.column(0), None, None)?;
        let expected = take_record_batch(&all, &indices)?;
        let bytes: usize = input.iter().map(|b| b.get_sliced_size().unwrap()).sum();
        for pool in [None, Some(PeakPool::new(bytes / 8))] {
            let source = TestMemoryExec::try_new_exec(
                std::slice::from_ref(&input),
                Arc::clone(&schema),
                None,
            )?;
            let sort = Arc::new(SortExec::new(ordering.clone(), source));
            let output = collect(
                Arc::clone(&sort) as Arc<dyn ExecutionPlan>,
                context(pool.clone().map(|p| p as _), 32, 64 << 10),
            )
            .await?;
            assert_eq!(concat_batches(&schema, &output)?, expected);
            if let Some(pool) = pool {
                assert!(sort.metrics().unwrap().spill_count().unwrap() > 0);
                assert_eq!(pool.reserved(), 0);
            }
        }
        Ok(())
    }
}
