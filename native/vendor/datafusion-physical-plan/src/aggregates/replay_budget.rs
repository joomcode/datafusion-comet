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

//! Headroom genuinely granted before a spill merge chooses its fan-in.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{BufferSpec, layout};
use arrow::datatypes::{DataType, SchemaRef};

use datafusion_common::config::SpillCompression;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{DataFusionError, Result, internal_err, resources_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::{StreamExt, TryStreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::common::{
    EMIT_BYTES_PER_GROUP, column_group_key, emit_copies,
};
use super::group_values::GroupByMetrics;
use super::ordered_final_stream::OrderedFinalAggregateStream;
use crate::metrics::BaselineMetrics;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::{GetSlicedSize, SpillManager};
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Observable Arrow buffer footprint of a canonical IPC spill batch. The OS
/// backend reads 128 KiB chunks. StreamDecoder retains such a chunk on zero-copy
/// reads, or incrementally builds a MutableBuffer whose capacity can approach
/// twice its IPC body. The canonical writer rebases/slices variable arrays, but
/// logical serialized bytes alone do not cover the decoder's retained capacity.
///
/// Buffer padding is explicit. Compression and >8-byte alignment can retain a
/// body plus separately allocated decoded/aligned arrays. Views get a conservative
/// allowance for their variadic data-buffer padding. This bounds Arrow-visible
/// buffers for the pinned local spill backend, not allocator RSS or OS file cache.
pub(super) fn decoded_spill_memory_bound(
    logical_size: usize,
    file_size: Option<u64>,
    schema: &SchemaRef,
    compression: SpillCompression,
) -> Result<usize> {
    let (buffers, separate_alignment, views, dictionaries) = schema_layout_info(schema);
    let Some(padding) = buffers.checked_mul(64) else {
        return resources_err!("Aggregate IPC padding estimate overflow");
    };
    let Some(body) = logical_size
        .checked_mul(if views { 2 } else { 1 })
        .and_then(|bytes| bytes.checked_add(padding))
    else {
        return resources_err!("Aggregate IPC body estimate overflow");
    };
    let file_size = file_size.map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX));
    let body = file_size.map_or(body, |size| body.min(size));
    // Dictionary values can outlive the batch that introduced them. Distinct
    // dictionary bodies may therefore pin different I/O chunks in one output batch.
    let Some(io_limit) = dictionaries
        .checked_add(1)
        .and_then(|bodies| bodies.checked_mul(128 * 1024))
    else {
        return resources_err!("Aggregate IPC dictionary I/O estimate overflow");
    };
    let io_chunk = file_size.unwrap_or(io_limit).min(io_limit);
    let Some(mut bound) = body
        .checked_mul(2)
        .and_then(|scratch| scratch.checked_add(io_chunk))
    else {
        return resources_err!("Aggregate IPC decoder estimate overflow");
    };
    if compression != SpillCompression::Uncompressed || separate_alignment {
        let Some(with_arrays) = logical_size
            .checked_add(padding)
            .and_then(|bytes| bound.checked_add(bytes))
        else {
            return resources_err!("Aggregate IPC decoded arrays estimate overflow");
        };
        bound = with_arrays;
    }
    Ok(bound)
}

fn schema_layout_info(schema: &SchemaRef) -> (usize, bool, bool, usize) {
    fn visit(data_type: &DataType) -> (usize, bool, bool, usize) {
        let layout = layout(data_type);
        let mut buffers = layout.buffers.len().saturating_add(1);
        let mut aligned = layout.buffers.iter().any(|buffer| {
            matches!(buffer,
            BufferSpec::FixedWidth { alignment, .. } if *alignment > 8)
        });
        let mut views = matches!(
            data_type,
            DataType::Utf8View
                | DataType::BinaryView
                | DataType::ListView(_)
                | DataType::LargeListView(_)
        );
        let mut dictionaries =
            usize::from(matches!(data_type, DataType::Dictionary(_, _)));
        let mut child = |data_type: &DataType| {
            let (count, alignment, view, dictionary) = visit(data_type);
            buffers = buffers.saturating_add(count);
            aligned |= alignment;
            views |= view;
            dictionaries = dictionaries.saturating_add(dictionary);
        };
        match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => child(field.data_type()),
            DataType::Struct(fields) => {
                fields.iter().for_each(|field| child(field.data_type()))
            }
            DataType::Union(fields, _) => fields
                .iter()
                .for_each(|(_, field)| child(field.data_type())),
            DataType::Dictionary(_, values) => child(values),
            DataType::RunEndEncoded(ends, values) => {
                child(ends.data_type());
                child(values.data_type());
            }
            _ => {}
        }
        (buffers, aligned, views, dictionaries)
    }
    schema.fields().iter().fold(
        (0usize, false, false, 0usize),
        |(count, aligned, views, dictionaries), field| {
            let (extra, alignment, view, dictionary) = visit(field.data_type());
            (
                count.saturating_add(extra),
                aligned || alignment,
                views || view,
                dictionaries.saturating_add(dictionary),
            )
        },
    )
}

pub(super) struct PreparedReplay {
    pub(super) spills: Vec<SortedSpillFile>,
    pub(super) batch_size: usize,
    pub(super) replay_reservation: MemoryReservation,
    pub(super) merge_reservation: MemoryReservation,
}

impl PreparedReplay {
    /// Build the prepared input and return it with the replay's protected grant and
    /// row cap. A single run must stay unbuffered: the ordinary merge's single-file
    /// fast path prefetches two additional batches without a separate merge grant.
    pub(super) fn into_input(
        self,
        spill_manager: SpillManager,
        spill_expr: &LexOrdering,
        metrics: BaselineMetrics,
    ) -> Result<(SendableRecordBatchStream, MemoryReservation, usize)> {
        let Self {
            mut spills,
            batch_size,
            replay_reservation,
            merge_reservation,
        } = self;
        let input = if spills.len() == 1 {
            let spill = spills.pop().expect("one spill exists");
            let input = spill_manager.read_spill_as_stream_unbuffered(
                spill.file,
                Some(spill.max_record_batch_memory),
            )?;
            // No prefetch buffers exist on this path. Its starting merge grant is
            // zero; reject any future incompatible budget change rather than losing it.
            if merge_reservation.size() != 0 {
                return internal_err!(
                    "Single-run aggregate replay has an unexpected merge grant"
                );
            }
            Box::pin(ObservedStream::new(input, metrics, None))
                as SendableRecordBatchStream
        } else {
            StreamingMergeBuilder::new()
                .with_schema(Arc::clone(spill_manager.schema()))
                .with_spill_manager(spill_manager)
                .with_sorted_spill_files(spills)
                .with_expressions(spill_expr)
                .with_metrics(metrics)
                .with_batch_size(batch_size)
                .with_reservation(merge_reservation)
                .build()?
        };
        Ok((input, replay_reservation, batch_size))
    }
}

/// The common spill replay of hash, single and prefix-ordered final aggregation.
/// Budget preparation is lazy and may reshape oversized IPC batches before the
/// merge opens its readers. No future materialization uses infallible growth.
pub(super) struct ReplayInput {
    pub(super) aggregate: AggregateExec,
    pub(super) context: Arc<TaskContext>,
    pub(super) partition: usize,
    pub(super) batch_size: usize,
    pub(super) spill_expr: LexOrdering,
    pub(super) spill_manager: SpillManager,
    pub(super) spills: Vec<SortedSpillFile>,
}

impl ReplayInput {
    pub(super) fn into_stream(
        self,
        baseline_metrics: &BaselineMetrics,
        group_by_metrics: GroupByMetrics,
        reservation: MemoryReservation,
    ) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.aggregate.schema);
        let baseline_metrics = baseline_metrics.clone();
        let stream = futures::stream::once(async move {
            let prepared = reserve_replay_headroom(
                &self.aggregate,
                &self.spill_manager,
                self.spills,
                self.batch_size,
                reservation,
            )
            .await?;
            let (merged, replay_reservation, _) = prepared.into_input(
                self.spill_manager,
                &self.spill_expr,
                baseline_metrics.intermediate(),
            )?;
            let replay = OrderedFinalAggregateStream::new_with_input_and_metrics(
                &self.aggregate,
                &self.context,
                self.partition,
                merged,
                &InputOrderMode::Sorted,
                baseline_metrics,
                group_by_metrics,
                None,
                replay_reservation,
            )?;
            Ok::<SendableRecordBatchStream, DataFusionError>(Box::pin(replay))
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

/// Reserve replay state, copied output and scratch before the merge chooses its
/// buffers. Three largest spill-batch footprints cover these areas, with explicit
/// gather-index and owned variable-state header headroom. This is a conservative initial grant, not a bound on an
/// arbitrary state formed by merging many runs for one key; later growth remains
/// fallible. Primitive state/columnar keys need no materialization workspace.
///
/// Holding that grant must not starve even a minimal two-run merge. When their joint
/// grant is refused, release it before unbuffered re-spilling the largest IPC run in
/// row halves. Carry each run's smaller row cap into the final merge, which must not
/// reconstruct the original oversized batch. Unsplittable or genuinely unavailable
/// allocations fail with ResourcesExhausted, without future-memory overcommit.
pub(super) async fn reserve_replay_headroom(
    aggregate: &AggregateExec,
    spill_manager: &SpillManager,
    mut spills: Vec<SortedSpillFile>,
    batch_size: usize,
    mut reservation: MemoryReservation,
) -> Result<PreparedReplay> {
    let spill_schema = spill_manager.schema();
    let copying_keys = spill_schema
        .fields()
        .iter()
        .take(aggregate.group_by.num_group_exprs())
        .any(|field| !column_group_key(field.data_type()));
    let mut copying_states = false;
    let mut copying_state_columns = 0usize;
    for expression in aggregate.aggr_expr.iter() {
        let fields = expression.state_fields()?;
        let copying_columns = fields
            .iter()
            .filter(|field| emit_copies(field.data_type()))
            .count();
        copying_states |= copying_columns != 0
            || (!aggregate.struct_state_output
                && emit_copies(expression.field().data_type()));
        let Some(columns) = copying_state_columns.checked_add(copying_columns) else {
            return resources_err!("Aggregate variable-state column count overflow");
        };
        copying_state_columns = columns;
    }
    if !copying_keys && !copying_states {
        let merge_reservation = reservation.new_empty();
        return Ok(PreparedReplay {
            spills,
            batch_size,
            replay_reservation: reservation,
            merge_reservation,
        });
    }
    let mut logical_caps: Vec<_> = spills
        .iter()
        .map(|spill| spill.max_record_batch_memory)
        .collect();
    for (spill, &logical) in spills.iter_mut().zip(&logical_caps) {
        spill.max_record_batch_memory = decoded_spill_memory_bound(
            logical,
            spill.file.size(),
            spill_schema,
            spill_manager.compression,
        )?;
    }
    let (buffers, _, _, _) = schema_layout_info(spill_schema);
    let Some(encoding_padding) = buffers.checked_mul(64) else {
        return resources_err!("Aggregate IPC encoding padding estimate overflow");
    };
    let mut row_caps = vec![batch_size; spills.len()];
    loop {
        let Some((largest_index, largest)) = spills
            .iter()
            .enumerate()
            .max_by_key(|(_, spill)| spill.max_record_batch_memory)
            .map(|(index, spill)| (index, spill.max_record_batch_memory))
        else {
            return internal_err!("Aggregate spill replay requires at least one run");
        };
        let batch_size = row_caps.iter().copied().min().unwrap_or(batch_size);
        // IPC uses offsets for variable values; owned accumulators additionally
        // retain a Vec header per group/value (for example FIRST/LAST BytesStore
        // and per-group collection state). Protect those shallow slots before the
        // merge seats its readers, so many small values trigger smaller replay
        // batches rather than exhausting the output grant after aggregation.
        // This initial allowance does not bound a state formed by arbitrarily
        // many input rows for one key; subsequent growth remains fallible.
        let Some(headroom) = largest.checked_mul(3).and_then(|state_and_emit| {
            copying_state_columns
                .checked_mul(size_of::<Vec<u8>>())
                .and_then(|headers| headers.checked_add(EMIT_BYTES_PER_GROUP))
                .and_then(|per_row| batch_size.checked_mul(per_row))
                .and_then(|slots| state_and_emit.checked_add(slots))
        }) else {
            return resources_err!("Aggregate spill replay headroom estimate overflow");
        };
        // Plain StreamingMergeBuilder seats each run with one input and one output
        // batch at its smallest read-ahead. Use the two largest runs so the initial
        // grant protects a minimal merge regardless of run selection order.
        let second = spills
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != largest_index)
            .map(|(_, spill)| spill.max_record_batch_memory)
            .max()
            .unwrap_or(0);
        let Some(minimum_merge) = (if spills.len() > 1 {
            largest
                .checked_add(second)
                .and_then(|sum| sum.checked_mul(2))
        } else {
            Some(0)
        }) else {
            return resources_err!("Aggregate spill merge headroom estimate overflow");
        };
        let Some(joint) = headroom.checked_add(minimum_merge) else {
            return resources_err!("Aggregate spill replay joint budget overflow");
        };
        match reservation.try_resize(joint) {
            Ok(()) => {
                let replay_reservation = reservation.split(headroom);
                let merge_reservation = reservation.take();
                return Ok(PreparedReplay {
                    spills,
                    batch_size,
                    replay_reservation,
                    merge_reservation,
                });
            }
            Err(DataFusionError::ResourcesExhausted(_)) => {}
            Err(error) => return Err(error),
        }

        // A denied joint grant must not pin the output floor while reshaping: the
        // single unbuffered re-spill can fit where merge + replay cannot yet fit.
        reservation.free();
        let split_reservation = reservation.new_empty();
        // Grant the canonical logical read estimate first. Any larger, already
        // allocated decoded capacity is recorded below; a conservative planning bound
        // must not itself prevent opening a batch whose actual footprint can fit.
        // Future encoding is granted only after actual slice sizes are known.
        split_reservation.try_grow(logical_caps[largest_index])?;
        let target = spills.remove(largest_index);
        let old_row_cap = row_caps[largest_index];
        let mut source =
            spill_manager.read_spill_as_stream_unbuffered(target.file, Some(largest))?;
        let mut replacements = Vec::new();
        while let Some(batch) = source.next().await {
            let batch = batch?;
            let read_size = get_record_batch_memory_size(&batch);
            // IPC can retain an I/O chunk larger than its logical batch. This is
            // already allocated input, so record it. Every future encoding
            // allocation below still requires a successful fallible reservation.
            split_reservation.resize(split_reservation.size().max(read_size));
            let mut pieces = VecDeque::new();
            let nominal_half = (old_row_cap / 2).max(1);
            if batch.num_rows() > 1 {
                let mid = batch.num_rows() / 2;
                pieces.push_back((batch.slice(0, mid), nominal_half));
                pieces
                    .push_back((batch.slice(mid, batch.num_rows() - mid), nominal_half));
            } else {
                pieces.push_back((batch, nominal_half));
            }
            while let Some((piece, row_cap)) = pieces.pop_front() {
                let logical_size = piece.get_sliced_size()?;
                let Some(required) = logical_size
                    .checked_add(encoding_padding)
                    .and_then(|bytes| bytes.checked_mul(2))
                    .and_then(|encoding| {
                        piece
                            .num_rows()
                            .checked_mul(EMIT_BYTES_PER_GROUP)
                            .and_then(|scratch| encoding.checked_add(scratch))
                            .and_then(|future| read_size.checked_add(future))
                    })
                else {
                    return resources_err!(
                        "Aggregate replay split workspace estimate overflow"
                    );
                };
                match split_reservation.try_resize(required) {
                    Ok(()) => {}
                    Err(DataFusionError::ResourcesExhausted(_))
                        if piece.num_rows() > 1 =>
                    {
                        let mid = piece.num_rows() / 2;
                        let row_cap = (row_cap / 2).max(1);
                        pieces.push_front((
                            piece.slice(mid, piece.num_rows() - mid),
                            row_cap,
                        ));
                        pieces.push_front((piece.slice(0, mid), row_cap));
                        continue;
                    }
                    Err(DataFusionError::ResourcesExhausted(message)) => {
                        return resources_err!(
                            "{message}; aggregate replay reshape minimum: decoded_input={read_size}, logical_piece={logical_size}, piece_rows={}, encoding_padding={encoding_padding}, required={required}",
                            piece.num_rows()
                        );
                    }
                    Err(error) => return Err(error),
                }
                // Arrow's canonical IPC writer rebases Utf8/Binary offsets and
                // slices nested children. An extra take would only add an unnecessary
                // payload copy; the headroom above covers its encoding workspace.
                // Each logical chunk gets its own one-batch file to limit retained
                // zero-copy I/O chunks, in addition to the decoder-capacity bound.
                let result = spill_manager
                    .spill_record_batch_iter_and_return_max_batch_memory(
                        std::iter::once(Ok(piece)),
                        "AggregateReplay split IPC chunk before reserving emit workspace",
                    )?;
                let Some((file, max_record_batch_memory)) = result else {
                    return internal_err!(
                        "Re-spilling an aggregate replay chunk produced no rows"
                    );
                };
                // The canonical writer is finalized and its encode buffers have
                // dropped before another piece can resize this workspace.
                let logical = max_record_batch_memory;
                let max_record_batch_memory = decoded_spill_memory_bound(
                    logical,
                    file.size(),
                    spill_schema,
                    spill_manager.compression,
                )?;
                replacements.push((
                    SortedSpillFile {
                        file,
                        max_record_batch_memory,
                    },
                    row_cap,
                    logical,
                ));
            }
        }
        drop(source);
        split_reservation.free();
        let new_max = replacements
            .iter()
            .map(|(spill, _, _)| spill.max_record_batch_memory)
            .max()
            .unwrap_or(0);
        if replacements.is_empty() {
            return internal_err!("Re-spilling an aggregate replay run produced no rows");
        }
        if new_max >= largest {
            return resources_err!(
                "Aggregate spill replay cannot split a {largest}-byte batch further"
            );
        }
        // Restore the old run order, inserting its consecutive sorted chunks in
        // place. Keep nominal caps, rather than a last short remainder's row count.
        let mut new_spills = Vec::with_capacity(replacements.len());
        let mut new_caps = Vec::with_capacity(replacements.len());
        let mut new_logical = Vec::with_capacity(replacements.len());
        for (spill, cap, logical) in replacements {
            new_spills.push(spill);
            new_caps.push(cap);
            new_logical.push(logical);
        }
        spills.splice(largest_index..largest_index, new_spills);
        row_caps.splice(largest_index..=largest_index, new_caps);
        logical_caps.splice(largest_index..=largest_index, new_logical);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::SpillMetrics;
    use arrow::array::{
        ArrayRef, AsArray, BinaryArray, DictionaryArray, Int32Array, StringArray,
    };
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_physical_expr_common::metrics::ExecutionPlanMetricsSet;

    #[tokio::test]
    async fn dictionary_bodies_retained_across_batches_fit_the_physical_ipc_bound()
    -> Result<()> {
        const DICTIONARIES: usize = 4;
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut fields: Vec<_> = (0..DICTIONARIES)
            .map(|index| {
                Field::new(
                    format!("dictionary_{index}"),
                    dictionary_type.clone(),
                    false,
                )
            })
            .collect();
        fields.push(Field::new("payload", DataType::Binary, false));
        let schema = Arc::new(Schema::new(fields));
        let manager = SpillManager::new(
            Arc::new(RuntimeEnv::default()),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(&schema),
        );
        let mut dictionaries: Vec<ArrayRef> = (0..DICTIONARIES)
            .map(|index| {
                Arc::new(StringArray::from(vec![format!("initial-{index}")])) as ArrayRef
            })
            .collect();
        let payload = vec![0xa5; 256 * 1024];
        let mut batches = Vec::new();
        for step in 0..DICTIONARIES {
            // Update one dictionary between large record batches. The last batch
            // references dictionary bodies introduced in four distinct I/O regions.
            dictionaries[step] =
                Arc::new(StringArray::from(vec![format!("updated-{step}")]));
            let mut columns: Vec<ArrayRef> = dictionaries
                .iter()
                .map(|values| {
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(vec![0]),
                        Arc::clone(values),
                    )
                    .map(|array| Arc::new(array) as ArrayRef)
                })
                .collect::<std::result::Result<_, _>>()?;
            columns.push(Arc::new(BinaryArray::from(vec![payload.as_slice()])));
            batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
        }
        let (file, logical) = manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches.into_iter().map(Ok),
                "dictionary IPC body lifetime regression",
            )?
            .unwrap();
        let bound = decoded_spill_memory_bound(
            logical,
            file.size(),
            &schema,
            SpillCompression::Uncompressed,
        )?;
        let mut input = manager.read_spill_as_stream_unbuffered(file, Some(bound))?;
        let mut rows = 0;
        let mut largest_actual = 0;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            largest_actual = largest_actual.max(get_record_batch_memory_size(&batch));
            assert!(
                get_record_batch_memory_size(&batch) <= bound,
                "actual={} bound={bound} logical={logical}",
                get_record_batch_memory_size(&batch)
            );
            for index in 0..DICTIONARIES {
                let expected = if index <= rows {
                    format!("updated-{index}")
                } else {
                    format!("initial-{index}")
                };
                assert_eq!(
                    batch
                        .column(index)
                        .as_any_dictionary()
                        .values()
                        .as_string::<i32>()
                        .value(0),
                    expected
                );
            }
            assert_eq!(
                batch.column(DICTIONARIES).as_binary::<i32>().value(0),
                payload.as_slice()
            );
            rows += 1;
        }
        assert_eq!(rows, DICTIONARIES);
        // This fixture must exercise persistent dictionary allocations, not only
        // a large ordinary record-batch body which one I/O allowance already covers.
        let (buffers, _, _, _) = schema_layout_info(&schema);
        let single_body_bound = 2 * (logical + 64 * buffers) + 128 * 1024;
        assert!(
            largest_actual > single_body_bound,
            "fixture must retain multiple dictionary I/O chunks: actual={largest_actual}, one-body={single_body_bound}"
        );
        Ok(())
    }
}
