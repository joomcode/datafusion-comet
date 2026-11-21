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

//! Aggregate replay leaves genuine headroom and shrinks oversized spill batches first.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{Field, Schema};
use datafusion_common::Result;
use datafusion_common::config::SpillCompression;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::AggregateUDF;
use datafusion_functions_aggregate::first_last::FirstValue;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use futures::StreamExt;

use super::group_values::GroupByMetrics;
use super::hash_stream::AggregateMemoryMetrics;
use super::hash_stream::memory_metrics_tests::ReservationOracle;
use super::ordered_final_stream::OrderedFinalAggregateStream;
use super::replay_budget::{decoded_spill_memory_bound, reserve_replay_headroom};
use super::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::common::collect;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::sorts::streaming_merge::SortedSpillFile;
use crate::spill::spill_manager::SpillManager;
use crate::test::TestMemoryExec;
use crate::{InputOrderMode, SendableRecordBatchStream};

fn value(key: i64) -> String {
    format!("value-{key:04}-{}", "x".repeat(4080))
}

/// A starting grant alone can starve the spill merge even when the query fits.
/// Here one spill batch occupies roughly a third of the finite pool. The naive
/// three-batch replay grant leaves no room for two readers. Reshaping the IPC
/// batches first must make the same query run under the original pool limit.
#[tokio::test]
async fn wide_replay_batches_are_reshaped_before_pinning_output_headroom() -> Result<()> {
    const ROWS: usize = 64;
    for (runs, roomy_single_file) in [(1usize, false), (1, true), (3, false)] {
        let raw_schema = Arc::new(Schema::new(vec![
            Field::new("key", arrow::datatypes::DataType::Int64, false),
            Field::new("value", arrow::datatypes::DataType::Utf8, true),
        ]));
        let expr = Arc::new(
            AggregateExprBuilder::new(
                Arc::new(AggregateUDF::new_from_impl(FirstValue::new())),
                vec![col("value", &raw_schema)?],
            )
            .schema(Arc::clone(&raw_schema))
            .alias("first_value")
            .build()?,
        );
        let mut state_fields = vec![Arc::clone(&raw_schema.fields()[0])];
        state_fields.extend(expr.state_fields()?);
        let spill_schema = Arc::new(Schema::new(state_fields));
        let batches = (0..3)
            .map(|run| {
                let start = (run * ROWS) as i64;
                RecordBatch::try_new(
                    Arc::clone(&spill_schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(start..start + ROWS as i64))
                            as ArrayRef,
                        Arc::new(StringArray::from_iter_values(
                            (start..start + ROWS as i64).map(value),
                        )),
                        Arc::new(BooleanArray::from(vec![true; ROWS])),
                    ],
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let logical_start = batches[0].get_array_memory_size();
        let pool_size = if roomy_single_file {
            // Additional coverage of the unchanged single-file fast path; this is
            // separate from the original tight-budget cases above and below.
            3 * decoded_spill_memory_bound(
                logical_start,
                None,
                &spill_schema,
                SpillCompression::default(),
            )? + ROWS * 24
        } else {
            3 * logical_start + ROWS * 24
        };
        let oracle = Arc::new(ReservationOracle::new(Arc::new(GreedyMemoryPool::new(
            pool_size,
        ))));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&oracle) as _;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()?;
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(Arc::clone(&runtime))
                .with_session_config(SessionConfig::new().with_batch_size(ROWS)),
        );
        let dummy = TestMemoryExec::try_new_exec(
            &[vec![RecordBatch::new_empty(Arc::clone(&spill_schema))]],
            Arc::clone(&spill_schema),
            None,
        )?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(vec![(col("key", &raw_schema)?, "key".into())]),
            vec![expr],
            vec![None],
            dummy,
            raw_schema,
        )?;
        let manager = SpillManager::new(
            runtime,
            SpillMetrics::new(&aggregate.metrics, 0),
            Arc::clone(&spill_schema),
        );
        let mut spills = Vec::new();
        if runs == 1 {
            // One file with several batches exercises the production single-run
            // reader, where read-ahead must not allocate two unreserved batches.
            let (file, max_record_batch_memory) = manager
                .spill_record_batch_iter_and_return_max_batch_memory(
                    batches.into_iter().map(Ok),
                    "single-file aggregate replay budget regression",
                )?
                .unwrap();
            spills.push(SortedSpillFile {
                file,
                max_record_batch_memory,
            });
        } else {
            for batch in batches {
                let (file, max_record_batch_memory) = manager
                    .spill_record_batch_iter_and_return_max_batch_memory(
                        std::iter::once(Ok(batch)),
                        "aggregate replay budget regression",
                    )?
                    .unwrap();
                spills.push(SortedSpillFile {
                    file,
                    max_record_batch_memory,
                });
            }
        }
        let largest = spills
            .iter()
            .map(|spill| spill.max_record_batch_memory)
            .max()
            .unwrap();
        if !roomy_single_file {
            assert!(
                3 * largest + ROWS * 24 + 4 * largest > pool_size,
                "the naive output grant and two readers must not fit together"
            );
        }
        let memory_metrics = AggregateMemoryMetrics::new(&aggregate.metrics, 0, &pool);
        let reservation = MemoryConsumer::new("FinalHashAggregateStream[0]/bounded-emit")
            .with_can_spill(true)
            .register(&memory_metrics.pool());
        let prepared =
            reserve_replay_headroom(&aggregate, &manager, spills, ROWS, reservation)
                .await?;
        if !roomy_single_file {
            assert!(
                prepared.batch_size < ROWS,
                "reshaped rows must cap the final merge output"
            );
            assert!(
                prepared
                    .spills
                    .iter()
                    .map(|spill| spill.max_record_batch_memory)
                    .max()
                    .unwrap()
                    < largest,
                "canonical IPC rewriting must reduce the largest physical batch"
            );
        } else {
            assert_eq!(prepared.spills.len(), 1);
        }
        // Check the rewritten bytes too: smaller metadata must correspond to smaller
        // decoded batches, rather than slices which still pin the original payload.
        for spill in &prepared.spills {
            let mut decoded = manager.read_spill_as_stream_unbuffered(
                Arc::clone(&spill.file),
                Some(spill.max_record_batch_memory),
            )?;
            while let Some(batch) = decoded.next().await {
                let batch = batch?;
                assert!(batch.num_rows() <= ROWS);
                if !roomy_single_file {
                    assert!(batch.num_rows() < ROWS);
                }
                let decoded_bytes = get_record_batch_memory_size(&batch);
                assert!(
                    decoded_bytes <= spill.max_record_batch_memory + 4096,
                    "decoded spill allocation {decoded_bytes} must match its smaller recorded footprint {} (runs={runs}, roomy_single_file={roomy_single_file}, rows={})",
                    spill.max_record_batch_memory,
                    batch.num_rows()
                );
            }
        }
        let metrics = BaselineMetrics::new(&aggregate.metrics, 0);
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "key",
            &spill_schema,
        )?)])
        .unwrap();
        let (merged, replay_reservation, _) =
            prepared.into_input(manager, &ordering, metrics.intermediate())?;
        let replay: SendableRecordBatchStream =
            Box::pin(OrderedFinalAggregateStream::new_with_input_and_metrics(
                &aggregate,
                &context,
                0,
                merged,
                &InputOrderMode::Sorted,
                metrics,
                GroupByMetrics::new(&aggregate.metrics, 0),
                None,
                memory_metrics.clone(),
                replay_reservation,
            )?);
        let output = collect(replay).await?;
        let mut actual = BTreeMap::new();
        for batch in output {
            let keys = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>();
            let values = batch.column(1).as_string::<i32>();
            for row in 0..batch.num_rows() {
                assert!(
                    actual
                        .insert(keys.value(row), values.value(row).to_owned())
                        .is_none(),
                    "each key is finalized once"
                );
            }
        }
        let expected = (0..(3 * ROWS) as i64)
            .map(|key| (key, value(key)))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            pool.reserved(),
            0,
            "replay, merge and reshaping release every grant"
        );
        let all_metrics = aggregate.metrics.clone_inner();
        let peaks: Vec<_> = all_metrics
            .iter()
            .filter(|metric| metric.value().name() == "peak_mem_used")
            .collect();
        assert_eq!(
            peaks.len(),
            1,
            "sequential spill replay must reuse one metric"
        );
        assert_eq!(
            peaks[0].value().as_usize(),
            oracle.peak(),
            "reported peak must cover the real simultaneous replay and merge grants"
        );
        assert!(oracle.peak() > 0);
        oracle.assert_balanced();
    }
    Ok(())
}

/// Legacy Single with a sorted prefix and grouping sets both still use their
/// existing merge/evaluate path. They must keep output headroom without changing
/// state-column bindings or the hidden grouping-ID key.
#[tokio::test]
async fn legacy_variable_single_and_grouping_sets_replay_with_a_tight_pool() -> Result<()>
{
    use super::StreamType;
    use crate::ExecutionPlan;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Int32Type, Int64Type};
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::lit;

    const ROWS: usize = 32;
    const BATCHES: usize = 16;
    for grouping_sets in [false, true] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("prefix", DataType::Int32, grouping_sets),
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batches = (0..BATCHES)
            .map(|batch| {
                let start = (batch * ROWS) as i64;
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from_iter((0..ROWS).map(|row| {
                            if grouping_sets && (batch * ROWS + row).is_multiple_of(5) {
                                None
                            } else {
                                Some((batch / 4) as i32)
                            }
                        }))) as ArrayRef,
                        Arc::new(Int64Array::from_iter_values(
                            start..start + ROWS as i64,
                        )),
                        Arc::new(StringArray::from_iter_values(
                            (start..start + ROWS as i64).map(value),
                        )),
                    ],
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()?;
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(SessionConfig::new().with_batch_size(ROWS)),
        );
        let prefix_order = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "prefix", &schema,
        )?)])
        .unwrap();
        let input = TestMemoryExec::try_new(&[batches], Arc::clone(&schema), None)?
            .try_with_sort_information(if grouping_sets {
                vec![]
            } else {
                vec![prefix_order]
            })?;
        let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
        let keys = vec![
            (col("prefix", &schema)?, "prefix".into()),
            (col("key", &schema)?, "key".into()),
        ];
        let group_by = if grouping_sets {
            PhysicalGroupBy::new(
                keys,
                vec![
                    (lit(ScalarValue::Int32(None)), "prefix".into()),
                    (lit(ScalarValue::Int64(None)), "key".into()),
                ],
                vec![vec![false, false], vec![true, false]],
                true,
            )
        } else {
            PhysicalGroupBy::new_single(keys)
        };
        let first = Arc::new(
            AggregateExprBuilder::new(
                Arc::new(AggregateUDF::new_from_impl(FirstValue::new())),
                vec![col("value", &schema)?],
            )
            .schema(Arc::clone(&schema))
            .alias("first_value")
            .build()?,
        );
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![first],
            vec![None],
            input,
            schema,
        )?;
        let probe = aggregate.execute_typed(0, &context)?;
        assert!(
            matches!(probe, StreamType::GroupedHash(_)),
            "this regression exercises the legacy route"
        );
        drop(probe);
        let output = collect(aggregate.execute(0, Arc::clone(&context))?).await?;
        assert!(
            aggregate.metrics().unwrap().spill_count().unwrap_or(0) > 0,
            "the tight pool must force actual legacy spilling"
        );
        let mut actual = BTreeMap::new();
        for batch in output {
            let prefixes = batch.column(0).as_primitive::<Int32Type>();
            let keys = batch.column(1).as_primitive::<Int64Type>();
            let values = batch
                .column(if grouping_sets { 3 } else { 2 })
                .as_string::<i32>();
            for row in 0..batch.num_rows() {
                let prefix = if arrow::array::Array::is_null(prefixes, row) {
                    None
                } else {
                    Some(prefixes.value(row))
                };
                let grouping_id = if grouping_sets {
                    match ScalarValue::try_from_array(batch.column(2), row)? {
                        ScalarValue::UInt8(Some(id @ (0 | 2))) => id,
                        other => panic!("unexpected grouping ID {other:?}"),
                    }
                } else {
                    0
                };
                if grouping_id == 2 {
                    assert_eq!(prefix, None);
                }
                assert!(
                    actual
                        .insert(
                            (prefix, keys.value(row), grouping_id),
                            values.value(row).to_owned()
                        )
                        .is_none(),
                    "each complete physical grouping key must appear once"
                );
            }
        }
        let expected = (0..(ROWS * BATCHES) as i64)
            .flat_map(|key| {
                let prefix = if grouping_sets && key % 5 == 0 {
                    None
                } else {
                    Some((key as usize / (ROWS * 4)) as i32)
                };
                let mut entries = vec![((prefix, key, 0u8), value(key))];
                if grouping_sets {
                    entries.push(((None, key, 2u8), value(key)));
                }
                entries
            })
            .collect();
        assert_eq!(actual, expected, "grouping_sets={grouping_sets}");
        assert_eq!(
            pool.reserved(),
            0,
            "legacy replay and preparation release every grant"
        );
    }
    Ok(())
}
