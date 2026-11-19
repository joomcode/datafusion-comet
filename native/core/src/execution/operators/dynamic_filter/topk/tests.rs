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

use super::*;

use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::ObjectStoreBackend;
use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::{cast, SortOptions};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

mod correctness;
mod eligibility;
mod lifecycle;
mod reader;
mod schema;
mod statistics;
mod timestamp;

#[test]
#[expect(
    deprecated,
    reason = "Verify the legacy child-replacement API as well as its supported replacement"
)]
fn fresh_sort_runtime_reset_and_child_replacement_preserve_spill_before_output() {
    let session = session(2);
    for spill_before_output in [false, true] {
        let template = sort(
            memory_input(vec![Some(3), Some(1), Some(2)], &DataType::Int32),
            2,
            SortOptions::default(),
        )
        .with_spill_before_output(spill_before_output);
        assert_eq!(template.clone().spill_before_output(), spill_before_output);
        let plan = Arc::new(wrapper(&template, &session));
        assert_eq!(plan.template.spill_before_output(), spill_before_output);
        assert_eq!(
            plan.build_runtime_sort()
                .unwrap()
                .sort
                .spill_before_output(),
            spill_before_output
        );

        let reset = Arc::clone(&plan).reset_state().unwrap();
        let reset = reset.downcast_ref::<TopKReaderFilterExec>().unwrap();
        assert_eq!(reset.template.spill_before_output(), spill_before_output);
        assert_eq!(
            reset
                .build_runtime_sort()
                .unwrap()
                .sort
                .spill_before_output(),
            spill_before_output
        );

        for key_type in [DataType::Int64, DataType::Float64] {
            let input = memory_input(vec![Some(103), Some(101), Some(102)], &key_type);
            for replace in [false, true] {
                let replaced = if replace {
                    Arc::clone(&plan).replace_children(
                        vec![Arc::clone(&input)],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )
                } else {
                    Arc::clone(&plan).with_new_children(vec![Arc::clone(&input)])
                }
                .unwrap();
                // Int64 keeps the wrapper; unsupported Float64 falls back to the original
                // native SortExec. Both rewrites must preserve the JVM-boundary contract.
                let actual = if let Some(wrapper) = replaced.downcast_ref::<TopKReaderFilterExec>()
                {
                    assert_eq!(key_type, DataType::Int64);
                    wrapper.template.spill_before_output()
                } else {
                    assert_eq!(key_type, DataType::Float64);
                    replaced
                        .downcast_ref::<SortExec>()
                        .unwrap()
                        .spill_before_output()
                };
                assert_eq!(
                    actual, spill_before_output,
                    "{key_type:?}, replace={replace}"
                );
            }
        }
    }
}

fn session(batch_size: usize) -> Arc<SessionContext> {
    let mut config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size)
        .with_parquet_page_index_pruning(false);
    // Isolate dynamic row-group pruning from row filters and page indexes.
    config.options_mut().execution.parquet.pushdown_filters = false;
    Arc::new(SessionContext::new_with_config(config))
}

fn batch(values: Vec<Option<i32>>, key_type: &DataType) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("key", key_type.clone(), true)]));
    RecordBatch::try_new(
        schema,
        vec![cast(&Int32Array::from(values), key_type).unwrap()],
    )
    .unwrap()
}

fn memory_input(values: Vec<Option<i32>>, key_type: &DataType) -> Arc<dyn ExecutionPlan> {
    let batch = batch(values, key_type);
    let batches = if batch.num_rows() == 0 {
        vec![batch.clone()]
    } else {
        (0..batch.num_rows())
            .step_by(2)
            .map(|offset| batch.slice(offset, 2.min(batch.num_rows() - offset)))
            .collect()
    };
    MemorySourceConfig::try_new_exec(&[batches], batch.schema(), None).unwrap()
}

fn sort(input: Arc<dyn ExecutionPlan>, k: usize, options: SortOptions) -> SortExec {
    SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("key", 0)),
            options,
        }])
        .unwrap(),
        input,
    )
    .with_preserve_partitioning(true)
    .with_fetch(Some(k))
}

fn wrapper(sort: &SortExec, session: &SessionContext) -> TopKReaderFilterExec {
    TopKReaderFilterExec::try_new(sort, session.copied_config().options())
        .unwrap()
        .expect("eligible TopK")
}

fn produced_filter(sort: &SortExec) -> Arc<DynamicFilterPhysicalExpr> {
    let mut expressions = sort.dynamic_expressions_produced();
    assert_eq!(expressions.len(), 1);
    (expressions.pop().unwrap() as Arc<dyn std::any::Any + Send + Sync>)
        .downcast::<DynamicFilterPhysicalExpr>()
        .unwrap()
}

fn keys(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = cast(batch.column(0), &DataType::Int32).unwrap();
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn count_metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
    plan.metrics()
        .and_then(|metrics| metrics.sum_by_name(name))
        .map_or(0, |metric| metric.as_usize())
}

/// Select key@1 in the physical file as key@0 in the scan output. Reader
/// attachment must retain live updates when it remaps the predicate.
fn parquet_input(
    values: Vec<Option<i32>>,
    key_type: &DataType,
    session: &Arc<SessionContext>,
    group_size: usize,
) -> (tempfile::NamedTempFile, Arc<DataSourceExec>) {
    let key = cast(&Int32Array::from(values), key_type).unwrap();
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Int32, false),
        Field::new("key", key_type.clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&file_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..key.len() as i32)) as ArrayRef,
            key,
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(group_size))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        file.reopen().unwrap(),
        Arc::clone(&file_schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(
        metadata.num_row_groups(),
        batch.num_rows().div_ceil(group_size)
    );
    let scan = parquet_scan(&[&file], file_schema, vec![1], None, false, session);
    (file, scan)
}

fn parquet_scan(
    files: &[&tempfile::NamedTempFile],
    file_schema: Arc<Schema>,
    projection: Vec<usize>,
    data_filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    allow_type_promotion: bool,
    session: &Arc<SessionContext>,
) -> Arc<DataSourceExec> {
    init_datasource_exec(
        Arc::new(file_schema.project(&projection).unwrap()),
        Some(file_schema),
        None,
        ObjectStoreUrl::local_filesystem(),
        ObjectStoreBackend::Local,
        vec![files
            .iter()
            .map(|file| {
                PartitionedFile::from_path(file.path().to_string_lossy().into_owned()).unwrap()
            })
            .collect()],
        Some(projection),
        data_filters,
        None,
        "UTC",
        true,
        false,
        allow_type_promotion,
        false,
        session,
        false,
        false,
        false,
        false,
        "CORRECTED",
        "CORRECTED",
        false,
    )
    .unwrap()
}
