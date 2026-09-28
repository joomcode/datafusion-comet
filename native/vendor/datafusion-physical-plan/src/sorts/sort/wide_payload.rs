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

//! COMET PATCH: avoid copying wide binary payloads at every sort/merge boundary.
//! The public stream still contains Binary arrays. Views are local to one sort
//! partition, whose spill manager uses the same private schema and compacts view
//! buffers before writing. Existing sort reservations account for those buffers.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{BinaryArray, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::utils::collect_columns;

// View conversion costs more than copying tiny binary values. Select only columns
// with substantial payload per input row; do not make the user tune another flag.
const MIN_BYTES_PER_ROW: usize = 4096;

pub(super) struct WideBinaryPayload {
    original_schema: SchemaRef,
    view_schema: SchemaRef,
    columns: Vec<usize>,
}

impl WideBinaryPayload {
    pub(super) fn select(
        batch: &RecordBatch,
        ordering: &LexOrdering,
        original_schema: SchemaRef,
    ) -> Option<Self> {
        if batch.num_rows() == 0 {
            return None;
        }
        // Include references nested inside key expressions, not only plain Column keys.
        let key_columns: HashSet<_> = ordering
            .iter()
            .flat_map(|sort| collect_columns(&sort.expr))
            .map(|column| column.index())
            .collect();
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(index, array)| {
                if key_columns.contains(&index) {
                    return None;
                }
                // Dictionary, LargeBinary, nested and existing view columns retain
                // their existing path. Logical offsets handle sliced Binary correctly.
                let binary = array.as_any().downcast_ref::<BinaryArray>()?;
                let offsets = binary.value_offsets();
                let bytes = (offsets[offsets.len() - 1] - offsets[0]) as usize;
                (bytes / batch.num_rows() >= MIN_BYTES_PER_ROW).then_some(index)
            })
            .collect();
        if columns.is_empty() {
            return None;
        }
        let fields: Vec<_> = original_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if columns.contains(&index) {
                    Arc::new(
                        field.as_ref().clone().with_data_type(DataType::BinaryView),
                    )
                } else {
                    Arc::clone(field)
                }
            })
            .collect();
        let view_schema = Arc::new(Schema::new_with_metadata(
            fields,
            original_schema.metadata().clone(),
        ));
        Some(Self {
            original_schema,
            view_schema,
            columns,
        })
    }

    pub(super) fn view_schema(&self) -> &SchemaRef {
        &self.view_schema
    }

    pub(super) fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub(super) fn encode(
        mapping: &Option<Self>,
        batch: RecordBatch,
    ) -> Result<RecordBatch> {
        match mapping {
            None => Ok(batch),
            Some(mapping) => mapping.convert(batch, &mapping.view_schema),
        }
    }

    pub(super) fn decode(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.convert(batch, &self.original_schema)
    }

    fn convert(&self, batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
        let mut arrays = batch.columns().to_vec();
        for &index in &self.columns {
            arrays[index] = cast(&arrays[index], schema.field(index).data_type())?;
        }
        Ok(RecordBatch::try_new(Arc::clone(schema), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionPlan;
    use crate::sorts::sort::SortExec;
    use crate::test::TestMemoryExec;
    use arrow::array::{Array, Int32Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::Field;
    use datafusion_common::config::SpillCompression;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::{CastExpr, Column};
    use futures::TryStreamExt;

    fn batch(start: usize, rows: usize, width: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("payload", DataType::Binary, true),
            ],
            [("source".into(), "wide-sort-test".into())].into(),
        ));
        let keys =
            Int32Array::from_iter_values((start..start + rows).rev().map(|v| v as i32));
        let values: Vec<_> = (start..start + rows)
            .rev()
            .map(|value| {
                if value % 7 == 0 {
                    None
                } else {
                    let mut bytes = vec![(value % 251) as u8; width];
                    bytes[..4].copy_from_slice(&(value as i32).to_le_bytes());
                    Some(bytes)
                }
            })
            .collect();
        let payload = BinaryArray::from_iter(values.iter().map(|v| v.as_deref()));
        RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(payload)]).unwrap()
    }

    fn ordering(index: usize) -> LexOrdering {
        [PhysicalSortExpr::new_default(Arc::new(Column::new(
            if index == 0 { "key" } else { "payload" },
            index,
        )))]
        .into()
    }

    fn select(
        batch: &RecordBatch,
        ordering: &LexOrdering,
    ) -> Option<WideBinaryPayload> {
        WideBinaryPayload::select(batch, ordering, batch.schema())
    }

    #[test]
    fn wide_payload_selection_excludes_narrow_and_all_key_references() {
        assert!(select(&batch(0, 32, 32), &ordering(0)).is_none());
        let wide = batch(0, 32, 8192);
        assert!(select(&wide, &ordering(0)).is_some());
        assert!(select(&wide, &ordering(1)).is_none());
        let nested = [PhysicalSortExpr::new_default(Arc::new(CastExpr::new(
            Arc::new(Column::new("payload", 1)),
            DataType::Utf8,
            None,
        )))]
        .into();
        assert!(select(&wide, &nested).is_none());
        assert!(select(&wide.slice(0, 0), &ordering(0)).is_none());
    }

    #[test]
    fn wide_payload_roundtrip_preserves_slices_nulls_and_metadata() -> Result<()> {
        let original = batch(0, 32, 8192).slice(3, 21);
        let mapping = select(&original, &ordering(0)).unwrap();
        let encoded = mapping.convert(original.clone(), mapping.view_schema())?;
        assert_eq!(encoded.column(1).data_type(), &DataType::BinaryView);
        let decoded = mapping.decode(encoded)?;
        assert_eq!(decoded.schema(), original.schema());
        assert_eq!(decoded.column(1).to_data(), original.column(1).to_data());
        Ok(())
    }

    #[test]
    fn wide_payload_uses_declared_stream_schema_not_first_batch_metadata() -> Result<()>
    {
        let first = batch(0, 32, 8192);
        let declared = Arc::new(Schema::new_with_metadata(
            first.schema().fields().clone(),
            [("declared".into(), "stream-schema".into())].into(),
        ));
        let mapping =
            WideBinaryPayload::select(&first, &ordering(0), Arc::clone(&declared))
                .unwrap();
        let encoded = mapping.convert(first, mapping.view_schema())?;
        assert_eq!(mapping.decode(encoded)?.schema(), declared);
        Ok(())
    }

    #[tokio::test]
    async fn wide_payload_sort_preserves_values_and_releases_reservations() -> Result<()>
    {
        for (limit, compression) in [
            (2 * 1024 * 1024, SpillCompression::Uncompressed),
            (2 * 1024 * 1024, SpillCompression::Zstd),
            (128 * 1024 * 1024, SpillCompression::Uncompressed),
        ] {
            let batches: Vec<_> = (0..32).map(|i| batch(i * 32, 32, 8192)).collect();
            let schema = batches[0].schema();
            let source =
                TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
            let sort = SortExec::new(ordering(0), source);
            let pool = Arc::new(GreedyMemoryPool::new(limit));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
                .build_arc()?;
            let mut config = SessionConfig::new()
                .with_batch_size(16)
                .with_sort_in_place_threshold_bytes(1)
                .with_sort_spill_reservation_bytes(64 * 1024);
            config.options_mut().execution.spill_compression = compression;
            let context = Arc::new(
                TaskContext::default()
                    .with_runtime(runtime)
                    .with_session_config(config),
            );
            let output: Vec<_> = sort.execute(0, context)?.try_collect().await?;
            assert_eq!(
                pool.reserved(),
                0,
                "output must not retain sort reservations"
            );
            assert!(
                output
                    .iter()
                    .all(|b| b.schema() == schema && b.num_rows() <= 16)
            );
            let combined = concat_batches(&schema, &output)?;
            assert_eq!(combined.num_rows(), 1024);
            let keys = combined
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let payload = combined
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for row in 0..1024 {
                assert_eq!(keys.value(row), row as i32);
                assert_eq!(payload.is_null(row), row % 7 == 0);
                if row % 7 != 0 {
                    assert_eq!(payload.value(row).len(), 8192);
                    assert_eq!(&payload.value(row)[..4], &(row as i32).to_le_bytes());
                    assert!(
                        payload.value(row)[4..]
                            .iter()
                            .all(|v| *v == (row % 251) as u8)
                    );
                }
            }
            let spills = sort.metrics().unwrap().spill_count().unwrap_or(0);
            if limit == 2 * 1024 * 1024 {
                assert!(spills > 0, "low-memory case must exercise spill/merge");
            } else {
                assert_eq!(spills, 0);
            }
        }
        Ok(())
    }
}
