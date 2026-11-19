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

//! Sort regressions carried from the final Joom 1.1 tag.
//! Exercises the current Comet pool graph against changing Spark task grants.

use super::*;
use crate::execution::memory_pools::{fair_unified_pool_with_fake_spark, FakeSparkTask};
use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{JoinType, NullEquality};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_comet_proto::spark_operator::Operator;

const MB: usize = 1024 * 1024;

#[test]
fn sort_merge_reserve_scales_with_the_task_budget() {
    let mut config = SessionConfig::new();
    let default = config.options().execution.sort_spill_reservation_bytes;
    configure_sort_spill_reservation(&mut config, 64 * MB, 2, 1);
    assert_eq!(config.options().execution.sort_spill_reservation_bytes, MB);

    let mut config = SessionConfig::new();
    configure_sort_spill_reservation(&mut config, 8 * 1024 * MB, 4, 1);
    assert_eq!(
        config.options().execution.sort_spill_reservation_bytes,
        default
    );
}

#[test]
fn sort_merge_reserve_accounts_for_task_cpus_and_clamps_small_budgets() {
    for (limit, cores, task_cpus, expected) in [
        (64 * MB, 8, 2, 512 * 1024),
        (64 * MB, 1, 4, 2 * MB),
        (64 * MB, 0, 0, 2 * MB),
        (0, 8, 1, 1),
        (1, 8, 1, 1),
    ] {
        let mut config = SessionConfig::new();
        configure_sort_spill_reservation(&mut config, limit, cores, task_cpus);
        assert_eq!(
            config.options().execution.sort_spill_reservation_bytes,
            expected,
            "limit={limit} cores={cores} task_cpus={task_cpus}"
        );
    }
}

fn configured_sort_reserve(
    off_heap_limit: Option<usize>,
    overrides: HashMap<String, String>,
) -> usize {
    let context = prepare_datafusion_session_context(
        1024,
        Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
        vec![],
        u64::MAX,
        1,
        &overrides,
        &Operator::default(),
        off_heap_limit,
    )
    .unwrap();
    context
        .copied_config()
        .options()
        .execution
        .sort_spill_reservation_bytes
}

#[test]
fn explicit_sort_reserve_override_takes_precedence_over_the_cap() {
    let default = SessionConfig::new()
        .options()
        .execution
        .sort_spill_reservation_bytes;
    for explicit in [0, 1, 7 * MB, default * 2] {
        let settings = HashMap::from([
            (SPARK_EXECUTOR_CORES.to_string(), "2".to_string()),
            (
                "spark.comet.datafusion.execution.sort_spill_reservation_bytes".to_string(),
                explicit.to_string(),
            ),
        ]);
        assert_eq!(configured_sort_reserve(Some(64 * MB), settings), explicit);
    }
}

#[test]
fn on_heap_session_keeps_the_default_sort_reserve() {
    let default = SessionConfig::new()
        .options()
        .execution
        .sort_spill_reservation_bytes;
    let settings = HashMap::from([(SPARK_EXECUTOR_CORES.to_string(), "64".to_string())]);
    assert_eq!(configured_sort_reserve(None, settings), default);
    assert_eq!(
        configured_sort_reserve(Some(64 * MB), HashMap::new()),
        2 * MB
    );
}

#[derive(Clone, Debug)]
struct SortSpillCase {
    executor_cores: usize,
    task_share: usize,
    active_tasks_at_start: usize,
    active_tasks_later: usize,
    tasks_start_at_batch: usize,
    batch_size: usize,
    input_rows: usize,
    num_batches: usize,
    /// Average length of the `title` column; 0 leaves the column out.
    title_len: usize,
}

impl SortSpillCase {
    /// Six million product rows sorted by `product_variant_id` while the executor goes
    /// from two active tasks to eight.
    fn production() -> Self {
        Self {
            executor_cores: 8,
            task_share: 32 * MB,
            active_tasks_at_start: 2,
            active_tasks_later: 8,
            tasks_start_at_batch: 45,
            batch_size: 8192,
            input_rows: 8192,
            num_batches: 160,
            title_len: 60,
        }
    }

    fn four_to_eight_tasks_at(batch: usize) -> Self {
        Self {
            active_tasks_at_start: 4,
            tasks_start_at_batch: batch,
            ..Self::production()
        }
    }
}

struct ProductRows {
    schema: SchemaRef,
    case: SortSpillCase,
    spark: FakeSparkTask,
}

impl std::fmt::Debug for ProductRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProductRows")
            .field("case", &self.case)
            .finish()
    }
}

fn product_schema(case: &SortSpillCase) -> SchemaRef {
    let mut fields = vec![
        Field::new("product_variant_id", DataType::Utf8, true),
        Field::new("product_id", DataType::Utf8, true),
        Field::new("store_id", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("quantity", DataType::Int32, true),
    ];
    if case.title_len > 0 {
        fields.push(Field::new("title", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn product_batch(schema: &SchemaRef, case: &SortSpillCase, index: usize) -> RecordBatch {
    let start = (index * case.input_rows) as u64;
    let rows: Vec<u64> = (start..start + case.input_rows as u64).collect();
    let variant = StringArray::from_iter_values(
        rows.iter()
            .map(|&r| format!("{:016x}{:08x}", mix(r), mix(r ^ 7) as u32)),
    );
    let product = StringArray::from_iter_values(
        rows.iter()
            .map(|&r| format!("{:016x}{:08x}", mix(r / 4), r as u32)),
    );
    let store = Int64Array::from_iter_values(rows.iter().map(|&r| (mix(r) % 50_000) as i64));
    let price =
        Float64Array::from_iter_values(rows.iter().map(|&r| (mix(r) % 100_000) as f64 / 100.0));
    let quantity = Int32Array::from_iter_values(rows.iter().map(|&r| (r % 97) as i32));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(variant),
        Arc::new(product),
        Arc::new(store),
        Arc::new(price),
        Arc::new(quantity),
    ];
    if case.title_len > 0 {
        columns.push(Arc::new(StringArray::from_iter_values(rows.iter().map(
            |&r| {
                let len = case.title_len / 2 + (mix(r ^ 11) as usize % (case.title_len + 1));
                let mut s = format!("title {r} ");
                while s.len() < len {
                    s.push_str("lorem ipsum ");
                }
                s.truncate(len);
                s
            },
        ))));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

impl PartitionStream for ProductRows {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let case = self.case.clone();
        let spark = self.spark.clone();
        let off_heap_size = case.task_share * case.executor_cores;
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(0..case.num_batches).map(move |i| {
                if i == case.tasks_start_at_batch {
                    spark.set_limit(off_heap_size / case.active_tasks_later);
                }
                Ok(product_batch(&schema, &case, i))
            }),
        ))
    }
}

fn sort_plan(case: &SortSpillCase, spark: &FakeSparkTask) -> Arc<SortExec> {
    let schema = product_schema(case);
    let source = Arc::new(ProductRows {
        schema: Arc::clone(&schema),
        case: case.clone(),
        spark: spark.clone(),
    });
    let child = Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![source],
            None,
            Vec::<LexOrdering>::new(),
            false,
            None,
        )
        .unwrap(),
    );
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        col("product_variant_id", &schema).unwrap(),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    Arc::new(SortExec::new(ordering, child).with_fetch(None))
}

/// Reads a sort's output, checking that its keys come in order, and returns the rows.
async fn read_sorted(mut stream: SendableRecordBatchStream) -> DataFusionResult<usize> {
    let mut rows = 0;
    let mut last: Option<String> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for key in keys.iter() {
            let key = key.unwrap();
            if let Some(prev) = &last {
                assert!(prev.as_str() <= key, "output not sorted: {prev} > {key}");
            }
            last = Some(key.to_string());
        }
        rows += batch.num_rows();
    }
    Ok(rows)
}

/// Runs `plan` in one task's session, checks that its output is sorted on the first
/// column and that all memory is handed back, and returns the rows it produced.
async fn run_in_task(
    case: &SortSpillCase,
    plan: impl FnOnce(&FakeSparkTask) -> Arc<dyn ExecutionPlan>,
) -> DataFusionResult<(usize, Arc<dyn ExecutionPlan>)> {
    let off_heap_size = case.task_share * case.executor_cores;
    let (pool, spark) = fair_unified_pool_with_fake_spark(
        off_heap_size,
        off_heap_size / case.active_tasks_at_start,
    );
    let spill_dir = tempfile::tempdir().unwrap();
    let spark_config = HashMap::from([(
        SPARK_EXECUTOR_CORES.to_string(),
        case.executor_cores.to_string(),
    )]);
    let session = prepare_datafusion_session_context(
        case.batch_size,
        Arc::clone(&pool),
        vec![spill_dir.path().to_string_lossy().into_owned()],
        u64::MAX,
        1,
        &spark_config,
        &Operator::default(),
        Some(off_heap_size),
    )
    .unwrap();

    let plan = plan(&spark);
    let rows = read_sorted(plan.execute(0, session.task_ctx())?).await?;
    assert_eq!(
        pool.reserved(),
        0,
        "memory still reserved after the plan finished"
    );
    assert_eq!(spark.held(), 0, "memory not handed back to Spark");
    Ok((rows, plan))
}

fn spill_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0)
}

async fn assert_sort_spills(case: SortSpillCase) {
    match run_in_task(&case, |spark| {
        sort_plan(&case, spark) as Arc<dyn ExecutionPlan>
    })
    .await
    {
        Ok((rows, sort)) => {
            assert_eq!(rows, case.input_rows * case.num_batches, "{case:?}");
            assert!(spill_count(&sort) > 0, "sort did not spill: {case:?}");
        }
        Err(e) => panic!("native sort failed instead of spilling: {e}\n{case:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_spills_after_other_tasks_shrink_the_spark_share() {
    assert_sort_spills(SortSpillCase::production()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_spills_when_the_share_halves_early_or_late() {
    for batch in [20, 25, 50] {
        assert_sort_spills(SortSpillCase::four_to_eight_tasks_at(batch)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_spills_with_output_batches_smaller_than_its_runs() {
    assert_sort_spills(SortSpillCase {
        batch_size: 4096,
        ..SortSpillCase::four_to_eight_tasks_at(25)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_spills_many_small_input_batches() {
    assert_sort_spills(SortSpillCase {
        input_rows: 1024,
        num_batches: 1280,
        tasks_start_at_batch: 200,
        ..SortSpillCase::four_to_eight_tasks_at(25)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_spills_when_the_key_is_most_of_the_row() {
    assert_sort_spills(SortSpillCase {
        title_len: 0,
        num_batches: 240,
        ..SortSpillCase::four_to_eight_tasks_at(25)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_spills_with_a_fixed_share() {
    assert_sort_spills(SortSpillCase {
        active_tasks_at_start: 8,
        ..SortSpillCase::production()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_sorts_of_a_sort_merge_join_spill_after_the_share_shrinks() {
    for batch in [20, 80] {
        let case = SortSpillCase::four_to_eight_tasks_at(batch);
        let plan = |spark: &FakeSparkTask| -> Arc<dyn ExecutionPlan> {
            let left: Arc<dyn ExecutionPlan> = sort_plan(&case, spark);
            let right: Arc<dyn ExecutionPlan> = sort_plan(&case, spark);
            let on = vec![(
                col("product_variant_id", &left.schema()).unwrap(),
                col("product_variant_id", &right.schema()).unwrap(),
            )];
            Arc::new(
                SortMergeJoinExec::try_new(
                    left,
                    right,
                    on,
                    None,
                    JoinType::Inner,
                    vec![SortOptions {
                        descending: false,
                        nulls_first: true,
                    }],
                    NullEquality::NullEqualsNothing,
                )
                .unwrap(),
            )
        };
        match run_in_task(&case, plan).await {
            Ok((rows, join)) => {
                // Every key is unique and both sides read the same rows.
                assert_eq!(rows, case.input_rows * case.num_batches);
                for sort in join.children() {
                    assert!(spill_count(sort) > 0, "sort did not spill");
                }
            }
            Err(e) => panic!("sort-merge join failed instead of spilling: {e}\n{case:?}"),
        }
    }
}

/// Records the most the wrapped pool has had reserved.
#[derive(Debug)]
struct PeakPool {
    inner: Arc<dyn MemoryPool>,
    peak: std::sync::atomic::AtomicUsize,
}

impl PeakPool {
    fn record(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), std::sync::atomic::Ordering::Relaxed);
    }

    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::Relaxed)
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

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DataFusionResult<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Rows like the cube job's: a short key and a wide binary sketch.
#[derive(Debug)]
struct WideRows {
    schema: SchemaRef,
    rows_per_batch: usize,
    num_batches: usize,
    sketch_len: usize,
}

impl WideRows {
    fn batch(&self, index: usize) -> RecordBatch {
        let start = (index * self.rows_per_batch) as u64;
        let rows: Vec<u64> = (start..start + self.rows_per_batch as u64).collect();
        let key = StringArray::from_iter_values(
            rows.iter()
                .map(|&r| format!("{:016x}{:08x}", mix(r), mix(r ^ 7) as u32)),
        );
        let sketch = BinaryArray::from_iter_values(rows.iter().map(|&r| {
            (0..self.sketch_len as u64)
                .map(|i| mix(r.wrapping_mul(31).wrapping_add(i)) as u8)
                .collect::<Vec<u8>>()
        }));
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(key), Arc::new(sketch)],
        )
        .unwrap()
    }
}

impl PartitionStream for WideRows {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches: Vec<_> = (0..self.num_batches).map(|i| Ok(self.batch(i))).collect();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(batches),
        ))
    }
}

struct SortRun {
    rows: usize,
    first_output_bytes: usize,
    /// Bytes Spark had granted when the sort produced its first batch, that is while
    /// the final merge pass runs.
    held_during_final_merge: usize,
    peak_reserved: usize,
    spill_count: usize,
    spilled_rows: usize,
}

/// Sorts `source` by its first column in one task with a fixed Spark share, and checks
/// the output order and that all memory is handed back.
async fn sort_with_fixed_share(
    source: Arc<dyn PartitionStream>,
    share: usize,
    executor_cores: usize,
    batch_size: usize,
) -> SortRun {
    let off_heap_size = share * executor_cores;
    let (pool, spark) = fair_unified_pool_with_fake_spark(off_heap_size, share);
    let peak = Arc::new(PeakPool {
        inner: pool,
        peak: Default::default(),
    });
    let pool: Arc<dyn MemoryPool> = Arc::clone(&peak) as _;
    let spill_dir = tempfile::tempdir().unwrap();
    let spark_config =
        HashMap::from([(SPARK_EXECUTOR_CORES.to_string(), executor_cores.to_string())]);
    let session = prepare_datafusion_session_context(
        batch_size,
        Arc::clone(&pool),
        vec![spill_dir.path().to_string_lossy().into_owned()],
        u64::MAX,
        1,
        &spark_config,
        &Operator::default(),
        Some(off_heap_size),
    )
    .unwrap();
    let schema = Arc::clone(source.schema());
    let child = Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![source],
            None,
            Vec::<LexOrdering>::new(),
            false,
            None,
        )
        .unwrap(),
    );
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        col(schema.field(0).name(), &schema).unwrap(),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, child));

    let mut stream = sort.execute(0, session.task_ctx()).unwrap();
    let first = stream
        .next()
        .await
        .expect("sorted output")
        .unwrap_or_else(|e| panic!("native sort failed: {e}"));
    let held_during_final_merge = spark.held();
    let first_output_bytes =
        datafusion::common::utils::memory::RecordBatchMemoryCounter::new().count_batch(&first);
    let rest: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::once(async { Ok(first) }).chain(stream),
    ));
    let rows = read_sorted(rest)
        .await
        .unwrap_or_else(|e| panic!("native sort failed: {e}"));
    assert_eq!(pool.reserved(), 0, "memory still reserved after the sort");
    assert_eq!(spark.held(), 0, "memory not handed back to Spark");
    let metrics = sort.metrics().unwrap();
    SortRun {
        rows,
        first_output_bytes,
        held_during_final_merge,
        peak_reserved: peak.peak(),
        spill_count: metrics.spill_count().unwrap_or(0),
        spilled_rows: metrics.spilled_rows().unwrap_or(0),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_spill_merge_leaves_half_the_share_for_its_consumers() {
    let case = SortSpillCase {
        active_tasks_at_start: 8,
        tasks_start_at_batch: usize::MAX,
        num_batches: 240,
        ..SortSpillCase::production()
    };
    // The share stays fixed, so the source never changes it.
    let spark = fair_unified_pool_with_fake_spark(1, 1).1;
    let source = Arc::new(ProductRows {
        schema: product_schema(&case),
        case: case.clone(),
        spark,
    });
    let run = sort_with_fixed_share(
        source,
        case.task_share,
        case.executor_cores,
        case.batch_size,
    )
    .await;
    assert_eq!(run.rows, case.input_rows * case.num_batches);
    assert!(run.spill_count > 0, "sort did not spill");
    assert!(run.peak_reserved <= case.task_share, "overcommitted");
    assert!(
        run.held_during_final_merge * 2 <= case.task_share,
        "the final merge holds {} of a {} share",
        run.held_during_final_merge,
        case.task_share
    );
}

/// A returned batch is owned by the downstream consumer, not the sort's pool
/// reservation. The JVM row consumer does not reserve this Arrow memory. Bound
/// that handoff by batch size, and wide rows by bytes whatever the batch size,
/// rather than assuming the sort still accounts for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_sort_batches_bound_the_unreserved_jvm_handoff() {
    let source = Arc::new(WideRows {
        schema: Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("sketch", DataType::Binary, false),
        ])),
        rows_per_batch: 8192,
        num_batches: 1,
        sketch_len: 2048,
    });
    let large = sort_with_fixed_share(
        Arc::clone(&source) as Arc<dyn PartitionStream>,
        64 * MB,
        8,
        8192,
    )
    .await;
    let small = sort_with_fixed_share(source, 64 * MB, 8, 512).await;
    assert_eq!(large.rows, 8192);
    assert_eq!(small.rows, large.rows);
    assert_eq!(large.spill_count, 0);
    assert_eq!(small.spill_count, 0);
    assert!(large.first_output_bytes <= 5 * MB);
    assert!(small.first_output_bytes < 2 * MB);
    // The batches not yet returned remain reserved by the sorter.
    assert!(large.held_during_final_merge >= 11 * MB);
    assert!(small.held_during_final_merge >= 15 * MB);
    eprintln!(
        "sort handoff: large={}B reserved={}B; small={}B reserved={}B",
        large.first_output_bytes,
        large.held_during_final_merge,
        small.first_output_bytes,
        small.held_during_final_merge
    );
}

/// Scaled down from the cube job: ~4 KiB rows, so a full output batch of `batch_size`
/// rows is larger than the task's whole share, and every spill run is a single batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_of_rows_wider_than_the_share_per_batch_stays_accounted() {
    let share = 2 * MB;
    let batch_size = 512;
    let sketch_len = 4608;
    let (rows_per_batch, num_batches) = (96, 96);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("sketch", DataType::Binary, false),
    ]));
    let source = Arc::new(WideRows {
        schema,
        rows_per_batch,
        num_batches,
        sketch_len,
    });
    assert!(batch_size * sketch_len > share);
    let run = sort_with_fixed_share(source, share, 8, batch_size).await;
    assert_eq!(run.rows, rows_per_batch * num_batches);
    assert!(
        run.spill_count >= 8,
        "need many spills: {}",
        run.spill_count
    );
    assert!(
        run.spilled_rows > run.rows,
        "need a multi-pass merge: {} rows spilled",
        run.spilled_rows
    );
    assert!(
        run.peak_reserved <= share,
        "overcommitted: peak {} for a {share} share",
        run.peak_reserved
    );
    assert!(run.held_during_final_merge <= share);
}

#[derive(Clone, Copy, Debug)]
enum RowShape {
    KeyAndKibPayload,
    KibKeyAndId,
}

const KIB_ROW: usize = 1000;

struct KibRows {
    shape: RowShape,
    schema: SchemaRef,
    rows: usize,
    rows_per_batch: usize,
}

impl std::fmt::Debug for KibRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KibRows")
            .field("shape", &self.shape)
            .field("rows", &self.rows)
            .field("rows_per_batch", &self.rows_per_batch)
            .finish()
    }
}

fn kib_text(prefix: String) -> String {
    let mut s = prefix;
    while s.len() < KIB_ROW {
        s.push_str("lorem ipsum ");
    }
    s.truncate(KIB_ROW);
    s
}

impl KibRows {
    fn new(shape: RowShape, rows: usize, rows_per_batch: usize) -> Self {
        let schema = Arc::new(Schema::new(match shape {
            RowShape::KeyAndKibPayload => vec![
                Field::new("key", DataType::Int64, false),
                Field::new("id", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, false),
            ],
            RowShape::KibKeyAndId => vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
            ],
        }));
        Self {
            shape,
            schema,
            rows,
            rows_per_batch,
        }
    }

    fn batch(&self, start: usize) -> RecordBatch {
        let end = (start + self.rows_per_batch).min(self.rows);
        let ids: Vec<u64> = (start as u64..end as u64).collect();
        let id: ArrayRef = Arc::new(Int64Array::from_iter_values(ids.iter().map(|&i| i as i64)));
        let columns: Vec<ArrayRef> = match self.shape {
            RowShape::KeyAndKibPayload => vec![
                Arc::new(Int64Array::from_iter_values(
                    ids.iter().map(|&i| mix(i) as i64),
                )),
                id,
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|&i| kib_text(format!("{i:016x} "))),
                )),
            ],
            RowShape::KibKeyAndId => vec![
                Arc::new(StringArray::from_iter_values(
                    ids.iter()
                        .map(|&i| kib_text(format!("{:016x}{i:016x} ", mix(i)))),
                )),
                id,
            ],
        };
        RecordBatch::try_new(Arc::clone(&self.schema), columns).unwrap()
    }
}

impl PartitionStream for KibRows {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let source = KibRows::new(self.shape, self.rows, self.rows_per_batch);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter((0..self.rows).step_by(self.rows_per_batch))
                .map(move |start| Ok(source.batch(start))),
        ))
    }
}

struct OutputTrace {
    reserved: Vec<usize>,
    spill_counts: Vec<usize>,
    peak_reserved: usize,
}

impl OutputTrace {
    fn spill_count(&self) -> usize {
        *self.spill_counts.last().unwrap()
    }

    fn max_reserved_after(&self, fraction: f64) -> usize {
        let from = (self.reserved.len() as f64 * fraction) as usize;
        self.reserved[from..].iter().copied().max().unwrap_or(0)
    }

    fn reserved_at(&self, fraction: f64) -> usize {
        self.reserved[(self.reserved.len() as f64 * fraction) as usize]
    }
}

fn check_sorted_output(
    shape: RowShape,
    batch: &RecordBatch,
    last: &mut Option<Vec<u8>>,
    seen: &mut [bool],
) {
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row in 0..batch.num_rows() {
        let id = ids.value(row) as u64;
        let key = match shape {
            RowShape::KeyAndKibPayload => {
                let key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row);
                let payload = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row);
                assert_eq!(key, mix(id) as i64, "key of row {id}");
                assert_eq!(
                    payload,
                    kib_text(format!("{id:016x} ")),
                    "payload of row {id}"
                );
                ((key as u64) ^ (1 << 63)).to_be_bytes().to_vec()
            }
            RowShape::KibKeyAndId => {
                let key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row);
                assert_eq!(
                    key,
                    kib_text(format!("{:016x}{id:016x} ", mix(id))),
                    "key of row {id}"
                );
                key.as_bytes().to_vec()
            }
        };
        assert!(!seen[id as usize], "row {id} returned twice");
        seen[id as usize] = true;
        if let Some(prev) = last.as_ref() {
            assert!(*prev <= key, "output not sorted at row {id}");
        }
        *last = Some(key);
    }
}

async fn sort_and_trace(
    source: KibRows,
    share: usize,
    executor_cores: usize,
    spill_before_output: Option<usize>,
    jvm_consumer: bool,
) -> OutputTrace {
    let shape = source.shape;
    let rows = source.rows;
    let off_heap_size = share * executor_cores;
    let (pool, spark) = fair_unified_pool_with_fake_spark(off_heap_size, share);
    let peak = Arc::new(PeakPool {
        inner: pool,
        peak: Default::default(),
    });
    let pool: Arc<dyn MemoryPool> = Arc::clone(&peak) as _;
    let spill_dir = tempfile::tempdir().unwrap();
    let mut spark_config =
        HashMap::from([(SPARK_EXECUTOR_CORES.to_string(), executor_cores.to_string())]);
    if let Some(threshold) = spill_before_output {
        spark_config.insert(
            COMET_EXEC_SORT_SPILL_BEFORE_OUTPUT_THRESHOLD.to_string(),
            threshold.to_string(),
        );
    }
    let session = prepare_datafusion_session_context(
        8192,
        Arc::clone(&pool),
        vec![spill_dir.path().to_string_lossy().into_owned()],
        u64::MAX,
        1,
        &spark_config,
        &Operator::default(),
        Some(off_heap_size),
    )
    .unwrap();
    let schema = Arc::clone(&source.schema);
    let child = Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![Arc::new(source)],
            None,
            Vec::<LexOrdering>::new(),
            false,
            None,
        )
        .unwrap(),
    );
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        col("key", &schema).unwrap(),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )])
    .unwrap();
    let sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering, child).with_spill_before_output(jvm_consumer));
    let mut stream = sort.execute(0, session.task_ctx()).unwrap();
    let mut trace = OutputTrace {
        reserved: vec![],
        spill_counts: vec![],
        peak_reserved: 0,
    };
    let mut last = None;
    let mut seen = vec![false; rows];
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap_or_else(|e| panic!("native sort failed: {e}"));
        trace.reserved.push(pool.reserved());
        trace
            .spill_counts
            .push(sort.metrics().unwrap().spill_count().unwrap_or(0));
        check_sorted_output(shape, &batch, &mut last, &mut seen);
    }
    assert!(seen.iter().all(|&s| s), "rows missing from the output");
    drop(stream);
    assert_eq!(pool.reserved(), 0, "memory still reserved after the sort");
    assert_eq!(spark.held(), 0, "memory not handed back to Spark");
    trace.peak_reserved = peak.peak();
    trace
}

const SPILL_BEFORE_OUTPUT_SHARE: usize = 1536 * MB;
const SPILL_BEFORE_OUTPUT_ROWS: usize = 900_000;

fn input_bytes(rows: usize) -> usize {
    rows * KIB_ROW
}

async fn assert_spills_before_output(shape: RowShape, rows: usize) {
    let share = SPILL_BEFORE_OUTPUT_SHARE;
    for rows_per_batch in [8192, 3] {
        let case = format!("{shape:?} rows={rows} rows_per_batch={rows_per_batch}");
        let held = sort_and_trace(
            KibRows::new(shape, rows, rows_per_batch),
            share,
            8,
            None,
            true,
        )
        .await;
        eprintln!(
            "{case} off: spill_count={} peak={}MiB reserved at 0%={}MiB 50%={}MiB 90%={}MiB",
            held.spill_count(),
            held.peak_reserved / MB,
            held.reserved_at(0.0) / MB,
            held.reserved_at(0.5) / MB,
            held.reserved_at(0.9) / MB
        );
        assert_eq!(held.spill_count(), 0, "{case}");
        assert!(held.reserved_at(0.9) >= input_bytes(rows) / 2, "{case}");

        let spilled = sort_and_trace(
            KibRows::new(shape, rows, rows_per_batch),
            share,
            8,
            Some(share / 4),
            true,
        )
        .await;
        eprintln!(
            "{case} on: spill_count={} peak={}MiB max reserved during output={}MiB",
            spilled.spill_count(),
            spilled.peak_reserved / MB,
            spilled.max_reserved_after(0.0) / MB
        );
        assert!(spilled.spill_counts.iter().all(|&c| c >= 1), "{case}");
        assert!(spilled.max_reserved_after(0.0) <= 64 * MB, "{case}");
        assert!(
            spilled.peak_reserved <= held.peak_reserved + held.peak_reserved / 10,
            "{case}"
        );

        let below = sort_and_trace(
            KibRows::new(shape, rows, rows_per_batch),
            share,
            8,
            Some(held.peak_reserved),
            true,
        )
        .await;
        assert_eq!(below.spill_count(), 0, "{case}");
        assert_eq!(below.reserved.len(), held.reserved.len(), "{case}");
        assert!(below.reserved_at(0.9) >= input_bytes(rows) / 2, "{case}");

        let native_consumer = sort_and_trace(
            KibRows::new(shape, rows, rows_per_batch),
            share,
            8,
            Some(share / 4),
            false,
        )
        .await;
        assert_eq!(native_consumer.spill_count(), 0, "{case}");
        assert_eq!(
            native_consumer.reserved.len(),
            held.reserved.len(),
            "{case}"
        );
        assert!(
            native_consumer.reserved_at(0.9) >= input_bytes(rows) / 2,
            "{case}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_materialized_sort_spills_before_output_above_the_threshold() {
    assert_spills_before_output(RowShape::KeyAndKibPayload, SPILL_BEFORE_OUTPUT_ROWS).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sort_spills_before_output_above_the_threshold() {
    assert_spills_before_output(RowShape::KibKeyAndId, SPILL_BEFORE_OUTPUT_ROWS / 2).await;
}

/// Runs `plan` in one task whose Spark share stays at `share`, reads its output, checks
/// that all memory is handed back, and returns the rows and the most the pool reserved.
async fn run_with_fixed_share(
    share: usize,
    executor_cores: usize,
    plan: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<(usize, usize)> {
    let off_heap_size = share * executor_cores;
    let (pool, spark) = fair_unified_pool_with_fake_spark(off_heap_size, share);
    let peak = Arc::new(PeakPool {
        inner: pool,
        peak: Default::default(),
    });
    let pool: Arc<dyn MemoryPool> = Arc::clone(&peak) as _;
    let spill_dir = tempfile::tempdir().unwrap();
    // As in production, where the merge headroom is 10 MiB of a 4.25 GiB share.
    let spark_config = HashMap::from([
        (SPARK_EXECUTOR_CORES.to_string(), executor_cores.to_string()),
        (
            "spark.comet.datafusion.execution.sort_spill_reservation_bytes".to_string(),
            (share / 400).to_string(),
        ),
    ]);
    let session = prepare_datafusion_session_context(
        8192,
        Arc::clone(&pool),
        vec![spill_dir.path().to_string_lossy().into_owned()],
        u64::MAX,
        1,
        &spark_config,
        &Operator::default(),
        Some(off_heap_size),
    )
    .unwrap();
    let mut stream = plan.execute(0, session.task_ctx())?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    drop(stream);
    assert_eq!(pool.reserved(), 0, "memory still reserved after the plan");
    assert_eq!(spark.held(), 0, "memory not handed back to Spark");
    Ok((rows, peak.peak()))
}

fn streaming(source: Arc<dyn PartitionStream>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        StreamingTableExec::try_new(
            Arc::clone(source.schema()),
            vec![source],
            None,
            Vec::<LexOrdering>::new(),
            false,
            None,
        )
        .unwrap(),
    )
}

/// Rows whose sort key, two 24-character ids and a day, is nearly all of the row.
#[derive(Debug)]
struct KeyedRows {
    schema: SchemaRef,
    rows_per_batch: usize,
    num_batches: usize,
}

impl KeyedRows {
    fn new(rows_per_batch: usize, num_batches: usize) -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![
                Field::new("merchant_id", DataType::Utf8, false),
                Field::new("created_date", DataType::Int32, false),
                Field::new("product_id", DataType::Utf8, false),
                Field::new("row", DataType::Int64, false),
            ])),
            rows_per_batch,
            num_batches,
        }
    }

    fn ordering(&self) -> LexOrdering {
        LexOrdering::new(
            ["merchant_id", "created_date", "product_id"]
                .map(|name| PhysicalSortExpr::new_default(col(name, &self.schema).unwrap())),
        )
        .unwrap()
    }

    fn batch(&self, index: usize) -> RecordBatch {
        let start = (index * self.rows_per_batch) as u64;
        let rows: Vec<u64> = (start..start + self.rows_per_batch as u64).collect();
        let id = |r: u64| format!("{:016x}{:08x}", mix(r), mix(r ^ 7) as u32);
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|&r| id(r / 50)),
                )),
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|&r| (mix(r ^ 3) % 3650) as i32),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|&r| id(r ^ 11)),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|&r| r as i64))),
            ],
        )
        .unwrap()
    }
}

impl PartitionStream for KeyedRows {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches: Vec<_> = (0..self.num_batches).map(|i| Ok(self.batch(i))).collect();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(batches),
        ))
    }
}

/// A spill merges the batches the sort buffered inside the memory it holds for them.
/// When the sort key is most of the row, the merge's batches and the rows it encodes
/// their keys into need a little more than that, and the merge asked Spark for it. With
/// the task's share used up the sort failed with "Additional allocation failed for
/// ExternalSorter[0] ... Failed to acquire N bytes plus 0 bytes overcommitted, only got N
/// bytes", as merchants_lifecycle did on every attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_sort_by_a_key_that_is_most_of_the_row_spills_in_a_fixed_share() {
    for share in [24, 28, 32, 36, 40].map(|mb| mb * MB) {
        let source = Arc::new(KeyedRows::new(1024, 1200));
        let ordering = source.ordering();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, streaming(source)));
        match run_with_fixed_share(share, 8, Arc::clone(&sort)).await {
            Ok((rows, peak)) => {
                assert_eq!(rows, 1024 * 1200);
                assert!(spill_count(&sort) > 0, "sort did not spill");
                assert!(peak <= share + share / 50, "{peak} reserved of {share}");
            }
            Err(e) => panic!("native sort failed instead of spilling at {share}: {e}"),
        }
    }
}

/// Stop while a real final spill merge still owns files and pool reservations.
/// Its run readers execute on Tokio tasks, so dropping the root stream only aborts
/// them; the plan pool must wait for their eventual reservation/file cleanup.
async fn stop_during_final_spill_merge(cancel: bool) {
    use crate::execution::operators::CancellableExec;

    fn contains_spill_file(dir: &std::path::Path) -> bool {
        std::fs::read_dir(dir).unwrap().any(|entry| {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            kind.is_file() || (kind.is_dir() && contains_spill_file(&entry.path()))
        })
    }

    let share = 2 * MB;
    let off_heap_size = share * 8;
    let rows = 16_384;
    let (pool, spark) = fair_unified_pool_with_fake_spark(off_heap_size, share);
    let spill_dir = tempfile::tempdir().unwrap();
    let session = prepare_datafusion_session_context(
        128,
        Arc::clone(&pool),
        vec![spill_dir.path().to_string_lossy().into_owned()],
        u64::MAX,
        1,
        &HashMap::from([(SPARK_EXECUTOR_CORES.to_string(), "8".to_string())]),
        &Operator::default(),
        Some(off_heap_size),
    )
    .unwrap();
    let context = session.task_ctx();
    let cancellation = PlanCancellation::of(&context).expect("session cancellation token");
    let source = Arc::new(KibRows::new(RowShape::KeyAndKibPayload, rows, 256));
    let schema = Arc::clone(source.schema());
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("key", &schema).unwrap(),
    )])
    .unwrap();
    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, streaming(source)));
    let plan = CancellableExec::wrap(Arc::clone(&sort));
    let mut stream = plan.execute(0, Arc::clone(&context)).unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("sort must reach its final spill merge within ten seconds")
        .expect("first sorted batch")
        .expect("sort must spill rather than fail under its task grant");
    assert!(first.num_rows() > 0 && first.num_rows() < rows);
    check_sorted_output(
        RowShape::KeyAndKibPayload,
        &first,
        &mut None,
        &mut vec![false; rows],
    );
    assert!(spill_count(&sort) > 0, "must exercise real spill readers");
    assert!(
        pool.reserved() > 0,
        "final merge must still hold reservations"
    );
    assert!(spark.held() > 0, "reservations must be charged to Spark");
    assert!(
        contains_spill_file(spill_dir.path()),
        "must stop while final-merge spill files are still live"
    );
    drop(first);

    if cancel {
        cancellation.cancel();
        let next = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("cancellation must not wait for another spill batch");
        match next {
            Some(Err(error)) => assert_eq!(error.to_string(), cancelled_error().to_string()),
            other => panic!("cancelled spill merge returned success or EOF: {other:?}"),
        }
    }

    drop(stream);
    drop(plan);
    drop(sort);
    drop(context);
    drop(session);
    let held = tokio::task::block_in_place(|| {
        pool.downcast_ref::<PlanMemoryPool>()
            .expect("fixture must include production per-plan accounting")
            .wait_until_released(Instant::now() + Duration::from_secs(2))
    });
    assert_eq!(held, 0, "aborted spill readers still hold reservations");
    assert_eq!(pool.reserved(), 0, "sort reservations leaked after stop");
    assert_eq!(spark.held(), 0, "sort did not return its Spark grant");

    // Check the isolated test root, not platform-specific directory/file names.
    // Reservation release and the last spill-file drop can occur in one task's
    // destructor at slightly different instants, so allow that task to finish.
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::fs::read_dir(spill_dir.path())
            .unwrap()
            .next()
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stopped spill readers retained files or their managed directory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_final_spill_merge_releases_reservations_and_files() {
    stop_during_final_spill_merge(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_dropped_final_spill_merge_releases_reservations_and_files() {
    stop_during_final_spill_merge(false).await;
}
