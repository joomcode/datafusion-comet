<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Maintained Joom feature map

This is the current maintenance contract for the Joom Comet 1.2 line, not an
implementation backlog. It retains all 59 registered IDs: 58 scope entries and
H01 (deliberately absent reverted history). Aliases J19=I04, J20=I06 and J28=I07
are alternate names, not extra features. C01 (ANSI) and C02 (evaluation order)
are cross-cutting correctness obligations.

Joom origin: `joom-1.1-13`, commit
`981e26dff6b64110a8b3949a0ed065523d9012e4`, based on upstream
`499ee08e1b0b47ebf17ef29ed5b7b0eb91906741`. New upstream base: Apache
`branch-1.2` commit `93cc6f9d71136bd0d87f77dd901989144d8c09fb`.
Old Joom lineage below identifies the original commits; G01-G10 lines identify
retained implementation commits in this branch.

Each entry links the contract/reason to current code and regression anchors.
Anchors identify where to investigate and retest, not proof of every possible
combination. The final qualified executable source
`7e3b6bcf87389f9cdcbe27db6f74562b5e792c05`, actual coverage/limits and exact
AMD64/ARM64 JAR SHA256 are recorded in [QUALIFICATION.md](QUALIFICATION.md).
Use [VALIDATION.md](VALIDATION.md) when modifying a contract,
[RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md) for deployment constraints and
[DATAFUSION.md](DATAFUSION.md) for the vendor boundary.

## Implementation policy

Use upstream implementations when they replace Joom behavior; add missing
residuals and retain regressions. Equivalent rows are not claims of unique Joom
invention. Do not mechanically copy historical modules over newer upstream
architecture, resurrect reverted rules, include post-tag13 work or add PR #5889.
Cost-table recalibration requires separate review.

## Scope and integration

G01: 57cc4cb62, 5a9cbf856, 132291957, 0710508c4 and correctness follow-ups through c04d2d03c.

### H01 — Reverted stage-wide heuristics: retain history without restoring them

RevertIsolatedNativeOperators and UnifyStageEngines were introduced and then removed before joom-1.1-13. Their absence is the state of the source tag; active BoundaryFormats/cost-model and wide-row fallback features are tracked separately.

**Implementation choice:** Excluded: these rules were reverted before tag13; leaving them absent loses no active source-tag feature.

**Old Joom lineage:** `2eda5358d`, `edaf6cae3`, `0a92f716c`, `4f92985f5`.

**Code:** [spark/src/main/scala/org/apache/comet/rules/CometRule.scala](../../spark/src/main/scala/org/apache/comet/rules/CometRule.scala).

**Regression anchors:** [CostBasedEngineChoiceSuite](../../spark/src/test/scala/org/apache/comet/rules/CostBasedEngineChoiceSuite.scala).

### P01 — Exact Spark 4.2.1 / Scala 2.13 profile

Pin official Spark 4.2.1 and Scala 2.13.18 consistently in the POM, test patch and CI. Runtime qualification uses exact Temurin 25.0.5.1+2; release compilation/bytecode retain Java 17. A declared dependency alone is not execution evidence.

**Code:** [pom.xml](../../pom.xml), [dev/diffs/4.2.1.diff](../../dev/diffs/4.2.1.diff), [dev/ci/java25-runtime.json](../../dev/ci/java25-runtime.json).

**Regression anchors:** [CometExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala).

### I10 — Scala/Rust compatibility and inclusion of Joom tests in CI

Preserve Scala/Rust compatibility and actual CI registration of every migrated regression, including Delta and a separate vendor target. Spark 4.2/Temurin 25 is the primary gate; ordinary upstream default-only PR success is insufficient.

**Old Joom lineage:** `1a52c8ca6`, `4f150e140`, `bb535748a`, `035ef5c91`, `092d70af2`, `c8b47b466`, `03c33a5ff`.

**Code:** [.github/workflows/ci.yml](../../.github/workflows/ci.yml), [dev/ci/check-suites.py](../../dev/ci/check-suites.py), [.github/actions/rust-test/action.yaml](../../.github/actions/rust-test/action.yaml).

**Regression anchors:** [dev/ci/check-suites.py](../../dev/ci/check-suites.py), [dev/ci/check-ci-config.py](../../dev/ci/check-ci-config.py).

### P03 — Verify semantics and fallback on Spark 4.2.1

Preserve Spark 4.2.1 values and mandatory ANSI errors with ANSI=true and independent Comet-off baselines. Keep mixed SUM/AVG overflow and lazy-evaluation guards. Struct scalar-subquery PR #5889 remains excluded. Known semantic exceptions are recorded in RUNTIME-REQUIREMENTS.md; do not claim universal equivalence.

**Code:** [spark/src/main/scala/org/apache/comet/serde/aggregates.scala](../../spark/src/main/scala/org/apache/comet/serde/aggregates.scala), [spark/src/main/scala/org/apache/comet/serde/QueryPlanSerde.scala](../../spark/src/main/scala/org/apache/comet/serde/QueryPlanSerde.scala), [spark/src/main/scala/org/apache/spark/sql/comet/CometWindowExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/CometWindowExec.scala).

**Regression anchors:** [CometExpressionSuite](../../spark/src/test/scala/org/apache/comet/CometExpressionSuite.scala), [CometMathExpressionSuite](../../spark/src/test/scala/org/apache/comet/CometMathExpressionSuite.scala), [CometConditionalEvaluationOrderSuite](../../spark/src/test/scala/org/apache/comet/exec/CometConditionalEvaluationOrderSuite.scala).

### D01 — Standalone DataFusion55.2.0 vendor and a consistent dependency graph

Keep a locally modifiable physical-plan inside Comet; other DataFusion55.2.0 and Arrow/Parquet59.3.2 crates remain at the target lock. No separate fork of the entire DataFusion project is required.

**Old Joom lineage:** `6eaf12e80`.

**Code:** [native/Cargo.toml](../../native/Cargo.toml), [native/Cargo.lock](../../native/Cargo.lock), [native/vendor/datafusion-physical-plan/Cargo.toml](../../native/vendor/datafusion-physical-plan/Cargo.toml).

**Regression anchors:** [.github/actions/rust-test/action.yaml](../../.github/actions/rust-test/action.yaml).

### P02 — Align Delta4.4.1 with Spark4.2

Native contrib and runtime use the Spark4.2-compatible io.delta:delta-spark_4.2_2.13:4.4.1 artifact. Do not mix it with the old kernel-backed contrib-delta profile.

**Code:** [pom.xml](../../pom.xml), [contrib/delta-spark/pom.xml](../../contrib/delta-spark/pom.xml), [.github/workflows/delta_contrib_test.yml](../../.github/workflows/delta_contrib_test.yml).

**Regression anchors:** [CometDeltaNativeScanSuite](../../contrib/delta-spark/src/test/scala/org/apache/comet/contrib/delta/CometDeltaNativeScanSuite.scala).

### I01 — Native Delta scan: Spark planning + native Parquet/DV/rebase execution

Preserve native Delta reads through Spark file planning: deletion vectors, column mapping/schema adaptation, datetime rebase, partition/data/runtime filters and supported scan shapes; safely fall back for unsupported shapes. Do not replace this scanner with the old kernel-backed contrib-delta.

**Implementation choice:** Use upstream final PR #5365 scanner, preserving split-scoped DV selection, rebase and DPP fallback improvements; add missing ADC, serialization and planning-deadlock protection.

**Old Joom lineage:** `c69bd9c39`.

**Code:** [contrib/delta-spark/src/main/scala/org/apache/spark/sql/comet/CometDeltaNativeScanExec.scala](../../contrib/delta-spark/src/main/scala/org/apache/spark/sql/comet/CometDeltaNativeScanExec.scala), [native/core/src/execution/planner/delta_spark_scan.rs](../../native/core/src/execution/planner/delta_spark_scan.rs).

**Regression anchors:** [CometDeltaNativeScanSuite](../../contrib/delta-spark/src/test/scala/org/apache/comet/contrib/delta/CometDeltaNativeScanSuite.scala), [DeltaPlanDataInjectorSuite](../../contrib/delta-spark/src/test/scala/org/apache/spark/sql/comet/DeltaPlanDataInjectorSuite.scala), [CometDeltaDmlReproSuite](../../contrib/delta-spark/src/test/scala/org/apache/comet/contrib/delta/CometDeltaDmlReproSuite.scala).

### I02 — GCS/GKE ADC-equivalent Hadoop auth flags

For gs://, do not reject native Delta solely because fs.gs.auth.service.account.enable=true / google.cloud.auth.service.account.enable=true or fs.gs.auth.type=COMPUTE_ENGINE/APPLICATION_DEFAULT. Keyfiles, emails/private keys/client ids, false/non-ADC auth types must still decline.

**Old Joom lineage:** `a43523692`.

**Code:** [contrib/delta-spark/src/main/scala/org/apache/comet/contrib/delta/DeltaScanSupport.scala](../../contrib/delta-spark/src/main/scala/org/apache/comet/contrib/delta/DeltaScanSupport.scala).

**Regression anchors:** [DeltaScanContribSuite](../../contrib/delta-spark/src/test/scala/org/apache/comet/contrib/delta/DeltaScanContribSuite.scala).

### I03 — Delta outputPartitioning: prevent planning deadlock

Reading outputPartitioning during planning/cost-boundary analysis must not start file listing/subqueries under the QueryExecution monitor and deadlock; execution of a fused native parent must obtain the correct partition count.

**Implementation choice:** Keep upstream lifecycle with safe planning metadata and correct execution partition count. Copying the old unconditional UnknownPartitioning(0) would break fused-parent sizing.

**Old Joom lineage:** `0e17ce3e5`.

**Code:** [contrib/delta-spark/src/main/scala/org/apache/spark/sql/comet/CometDeltaNativeScanExec.scala](../../contrib/delta-spark/src/main/scala/org/apache/spark/sql/comet/CometDeltaNativeScanExec.scala).

**Regression anchors:** [CometDeltaNativeScanSuite](../../contrib/delta-spark/src/test/scala/org/apache/comet/contrib/delta/CometDeltaNativeScanSuite.scala).

### I04 — Native scan: DPP lifecycle and zero partitions after pruning

Retain native scan for eligible DPP; planning metadata must not prematurely run unresolved subqueries. A fully pruned scan creates zero partitions without attempting to read an absent source-key or inventing a partition.

**Implementation choice:** Use upstream #6268 DPP lifecycle and ordinary zero-partition handling. Empty scans already existed upstream 1.1; the extra old Joom fix addressed its later lookup path.

**Old Joom lineage:** `65be8b751`, `45fad9eea`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/CometNativeScanExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/CometNativeScanExec.scala), [spark/src/main/scala/org/apache/spark/sql/comet/operators.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/operators.scala).

**Regression anchors:** [CometDppFallbackRepro3949Suite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometDppFallbackRepro3949Suite.scala).

### I08 — Joom release builder for two architectures and libcomet packaging

From a clean verified HEAD, produce Linux amd64 and arm64 libraries, core and Delta JARs. Preserve commit and checksums; verify ELF, GLIBC and the bytes of both libraries in the shaded JAR.

**Old Joom lineage:** `e6b7d5eb4`.

**Code:** [dev/release/joom/Dockerfile](../../dev/release/joom/Dockerfile), [dev/release/joom/build-release.sh](../../dev/release/joom/build-release.sh).

**Regression anchors:** [dev/release/joom/test-native-build-provenance.py](../../dev/release/joom/test-native-build-provenance.py), [dev/release/joom/test-packaged-runtime.py](../../dev/release/joom/test-packaged-runtime.py), [dev/release/joom/test-rc-result.py](../../dev/release/joom/test-rc-result.py).

### I09 — Immutable versions when publishing to JFrog

The Maven version matches the Git tag; commit.txt matches the tagged commit; checksums are valid. An existing version is never overwritten. A JAR downloaded after upload is byte-identical to the built JAR.

**Old Joom lineage:** `e6b7d5eb4`.

**Code:** [dev/release/joom/publish-jfrog.sh](../../dev/release/joom/publish-jfrog.sh).

**Regression anchors:** [dev/release/joom/verify-release.py](../../dev/release/joom/verify-release.py).

### J18 — Load the native library once when NativeBase is reinitialized

Do not extract/load a second libcomet when loaded is reset while bundledLibrary is preserved.

**Implementation choice:** Use upstream #6493 per-classloader bundledLibrary state rather than duplicating the old fix.

**Old Joom lineage:** `1b63d8be6`.

**Code:** [spark/src/main/java/org/apache/comet/NativeBase.java](../../spark/src/main/java/org/apache/comet/NativeBase.java).

**Regression anchors:** [CometSparkSessionExtensionsSuite](../../spark/src/test/scala/org/apache/comet/CometSparkSessionExtensionsSuite.scala).

## Native runtime

G02: 912ef9155, deefac49a, 8d0b4fc83.

### I05 — Nested sliced boolean FFI offsets

Boolean bitmap offsets are normalized recursively at every list/struct level before JNI export; values/nulls in sliced arrays remain unchanged.

**Implementation choice:** Upstream #6339/#6449 recursive FFI repair already existed in official 1.1 and remains unchanged; retain the additional deep sliced-boolean Joom regression.

**Old Joom lineage:** `5618dec71`, `64e096c91`.

**Code:** [native/common/src/ffi_offsets.rs](../../native/common/src/ffi_offsets.rs), [native/core/src/execution/jni_api.rs](../../native/core/src/execution/jni_api.rs).

**Regression anchors:** [CometCodegenSuite](../../spark/src/test/scala/org/apache/comet/CometCodegenSuite.scala), [CometExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala).

### I06 — Timestamp timezone labels: producer repairs and comparisons across producers

Retain upstream timestamp producer, CASE/coalesce and date_trunc label fixes, Spark 4.2 overflow behavior and Joom mixed-producer regressions. The old broad comparison-side cast was unnecessary for the verified target cases and is not reintroduced. Check UTC aliases, non-UTC/DST, NULLs, NTZ and truncation/seconds boundaries.

**Implementation choice:** Use upstream #6341/#6345/#6347 producer/branch repairs and retained Joom regressions instead of restoring obsolete blanket relabeling.

**Old Joom lineage:** `65be8b751`, `c9574d243`, `d78393122`.

**Code:** [native/spark-expr/src/datetime_funcs/seconds_to_timestamp.rs](../../native/spark-expr/src/datetime_funcs/seconds_to_timestamp.rs), [native/spark-expr/src/datetime_funcs/timestamp_trunc.rs](../../native/spark-expr/src/datetime_funcs/timestamp_trunc.rs), [native/core/src/execution/planner.rs](../../native/core/src/execution/planner.rs).

**Regression anchors:** [CometTimestampComparisonSuite](../../spark/src/test/scala/org/apache/comet/CometTimestampComparisonSuite.scala), [CometDateTimeUtilsSuite](../../spark/src/test/scala/org/apache/comet/CometDateTimeUtilsSuite.scala).

### J14 — Shared32-bit offset budget and splitting explode/concat before overflow

OffsetBudget/offset_extents account for repeated nested variable-width data; explode can split the result of one input row across batches; coalescer/window concat do not overflow offsets.

**Old Joom lineage:** `dfa6439e5`.

**Code:** [native/common/src/offset_extents.rs](../../native/common/src/offset_extents.rs), [native/operators/src/explode.rs](../../native/operators/src/explode.rs).

**Regression anchors:** [CometExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala), [CometWindowSemanticsSuite](../../spark/src/test/scala/org/apache/comet/exec/CometWindowSemanticsSuite.scala), [CometShuffleReadCoalesceSuite](../../spark/src/test/scala/org/apache/comet/exec/CometShuffleReadCoalesceSuite.scala), [native/common/src/offset_extents.rs](../../native/common/src/offset_extents.rs), [native/operators/src/explode.rs](../../native/operators/src/explode.rs).

### J16 — Capture TaskContext before results are read on another thread

CometExecIterator captures task context at creation and uses it when the output iterator is read outside the task thread; preserve task metrics/cancellation/lifecycle.

**Old Joom lineage:** `042ab7d3f`.

**Code:** [spark/src/main/scala/org/apache/comet/CometExecIterator.scala](../../spark/src/main/scala/org/apache/comet/CometExecIterator.scala), [spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java](../../spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java).

**Regression anchors:** [CometOffTaskThreadSuite](../../spark/src/test/scala/org/apache/comet/exec/CometOffTaskThreadSuite.scala).

### J22 — Safe ownership of fetched shuffle blocks and early close

FetchedBlocksInputStream closes every received block exactly once before next; close does not fetch remaining blocks; kill checks occur between blocks; task completion must not double-release Netty buffers.

**Old Joom lineage:** `705fc113f`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala).

**Regression anchors:** [CometBlockStoreShuffleReaderSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReaderSuite.scala).

### J15 — Stop killed tasks inside the native plan

The task-kill watcher calls cancelPlan; cancellable native streams/row loops/shuffle writer/JVM codegen checks stop work and safely release resources.

**Old Joom lineage:** `55c5b6fe8`.

**Code:** [native/common/src/cancellation.rs](../../native/common/src/cancellation.rs), [native/core/src/execution/jni_api.rs](../../native/core/src/execution/jni_api.rs), [spark/src/main/scala/org/apache/comet/CometExecIterator.scala](../../spark/src/main/scala/org/apache/comet/CometExecIterator.scala), [spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java](../../spark/src/main/java/org/apache/spark/CometTaskMemoryManager.java).

**Regression anchors:** [CometNativeTaskKillSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometNativeTaskKillSuite.scala), [CometTaskMemoryManagerSuite](../../spark/src/test/scala/org/apache/spark/CometTaskMemoryManagerSuite.scala).

## Sort and spill budgets

G03: d35817534, af05c94e7.

### D11 — Cap initial sort merge reserve at1/32 of task budget

With off-heap execution, initial datafusion.execution.sort_spill_reservation_bytes is capped at min(upstream_default, max(off_heap_limit / max(executor_cores / max(task_cpus,1),1) /32,1)). Large tasks retain the upstream default. Explicit spark.comet.datafusion.* overrides apply AFTER the cap and take precedence. This protection is separate from D03 spillBeforeOutputThreshold.

**Old Joom lineage:** `65be8b751`, `d153b3a85`, `4ff37081a`.

**Code:** [native/core/src/execution/jni_api.rs](../../native/core/src/execution/jni_api.rs).

**Regression anchors:** [CometNativeTaskKillSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometNativeTaskKillSuite.scala), [native/core/src/execution/jni_api/sort_memory_tests.rs](../../native/core/src/execution/jni_api/sort_memory_tests.rs).

### D02 — Sort memory, SpillWorkspace and multipass merge

Preserve reservations when Spark shrinks the grant, across all merge passes; count encoded Rows and shared buffers once; leave downstream headroom; reduce read-ahead/split overly wide spill batches.

**Old Joom lineage:** `943936fed`, `880575b8d`, `b3cd74ad9`, `d7d47f3bb`, `d82e3a387`, `69e638984`, `a4dc994ad`, `426ca8e0b`, `1b8d480ab`, `6ae0323ab`, `5b75a38e7`, `399e6c9fe`, `b2771dd97`, `29b0bb02b`.

**Code:** [native/vendor/datafusion-physical-plan/src/sorts/spill_workspace.rs](../../native/vendor/datafusion-physical-plan/src/sorts/spill_workspace.rs), [native/vendor/datafusion-physical-plan/src/sorts/multi_level_merge.rs](../../native/vendor/datafusion-physical-plan/src/sorts/multi_level_merge.rs), [native/vendor/datafusion-physical-plan/src/sorts/streaming_merge.rs](../../native/vendor/datafusion-physical-plan/src/sorts/streaming_merge.rs).

**Regression anchors:** [CometExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala), [CometNativeTaskKillSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometNativeTaskKillSuite.scala), [native/vendor/datafusion-physical-plan/src/sorts/sort/comet_memory_tests.rs](../../native/vendor/datafusion-physical-plan/src/sorts/sort/comet_memory_tests.rs).

### D04 — Wide-payload sort: keys first, BinaryView, prefix and gather

Sort narrow keys, gather payload once from genuinely referenced batches; use BinaryView for wide binary columns, an eight-byte key prefix with full comparison on collisions; coalesce small batches; account for concat pool budget and sliced nested offsets.

**Old Joom lineage:** `39f49a033`, `be99cc1bd`, `5da77919c`, `3b37b4a12`, `d796f0903`, `f66011d6d`, `f7a13eb6a`, `6975ebae0`, `5e4ef4aeb`, `bbcc30785`, `e9a487872`.

**Code:** [native/vendor/datafusion-physical-plan/src/sorts/sort/late_materialize.rs](../../native/vendor/datafusion-physical-plan/src/sorts/sort/late_materialize.rs), [native/vendor/datafusion-physical-plan/src/sorts/sort/wide_payload.rs](../../native/vendor/datafusion-physical-plan/src/sorts/sort/wide_payload.rs).

**Regression anchors:** [CometExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala), [native/vendor/datafusion-physical-plan/src/sorts/sort/late_materialize.rs](../../native/vendor/datafusion-physical-plan/src/sorts/sort/late_materialize.rs).

### D03 — Output merge and handing native sort results to the JVM

Allow spill-before-output when retained native input could starve a JVM consumer; do not impose it on a native-only chain. When output-merge budget is insufficient, spill in groups, preserve the precheck grant and bounded overcommit. Keep separate from D11 initial reserve: spillBeforeOutputThreshold defaults to1/4 of the task off-heap share, is disabled on-heap, and explicit0 disables the mechanism.

**Old Joom lineage:** `65be8b751`, `d153b3a85`, `4ff37081a`, `5f6cbb871`, `568955d21`, `6b11f2e69`, `2b28fed53`, `a675e5489`, `542d344eb`, `238f2db33`.

**Code:** [native/core/src/execution/planner.rs](../../native/core/src/execution/planner.rs), [native/vendor/datafusion-physical-plan/src/sorts/sort.rs](../../native/vendor/datafusion-physical-plan/src/sorts/sort.rs).

**Regression anchors:** [CometExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala), [CometNativeTaskKillSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometNativeTaskKillSuite.scala), [native/core/src/execution/jni_api/sort_memory_tests.rs](../../native/core/src/execution/jni_api/sort_memory_tests.rs).

## Aggregates

G04: ba3aa5480, 4b109026e, 18f7550f1, 5133f0ab7, a89fea10e.

### D10 — PartialMerge/COUNT(DISTINCT) spill: upstream replacement decision

Retain upstream Single/Final plus MergeAsPartial and Spark binary-state codecs, including native collect_list/set and DISTINCT. Adapt Joom memory/offset/ordered-drain guards to that architecture; do not restore old PartialReduce routing. Spilled multistage aggregation must emit every key exactly once.

**Implementation choice:** Upstream state-codec/MergeAsPartial architecture replaces old PartialReduce mapping; Joom memory/emit/drain residuals remain on top.

**Old Joom lineage:** `6c8193e76`.

**Code:** [native/core/src/execution/planner/aggregate.rs](../../native/core/src/execution/planner/aggregate.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [CometFirstLastBoolAggFuzzSuite](../../spark/src/test/scala/org/apache/comet/exec/CometFirstLastBoolAggFuzzSuite.scala).

### D06 — Aggregate initial reservation and fully ordered accounting

Do not raise a false ResourcesExhausted when the first reservation is denied for a spill-capable aggregate; account for already allocated fully ordered final memory without a mistaken repeated fallible refusal.

**Old Joom lineage:** `85bef53f6`, `c6178ee56`.

**Code:** [native/vendor/datafusion-physical-plan/src/aggregates/hash_stream.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/hash_stream.rs), [native/vendor/datafusion-physical-plan/src/aggregates/ordered_final_stream.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/ordered_final_stream.rs), [native/vendor/datafusion-physical-plan/src/aggregates/starved_spill_tests.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/starved_spill_tests.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [native/vendor/datafusion-physical-plan/src/aggregates/starved_spill_tests.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/starved_spill_tests.rs).

### D07 — Aggregate group-key 32-bit offset budget

Guard variable/nested group-key 32-bit offsets across every layout: emit Partial states or spill Final/Single. Resume evaluated input/grouping sets exactly once. Include the hidden grouping ID in spilled/replayed physical keys, distinguishing source NULLs from rollup NULLs.

**Old Joom lineage:** `6450a99f5`.

**Code:** [native/vendor/datafusion-physical-plan/src/aggregates/group_values/mod.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/group_values/mod.rs), [native/vendor/datafusion-physical-plan/src/aggregates/group_values/single_group_by/bytes.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/group_values/single_group_by/bytes.rs), [native/vendor/datafusion-physical-plan/src/aggregates/group_values/multi_group_by/bytes.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/group_values/multi_group_by/bytes.rs), [native/vendor/datafusion-physical-plan/src/aggregates/group_values/row.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/group_values/row.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [native/vendor/datafusion-physical-plan/src/aggregates/starved_spill_tests.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/starved_spill_tests.rs).

### D08 — Aggregate emit pre-reservation and output lifetime

Reserve future key/state/index emission fallibly; retain materialized output and workspace grants for their real lifetime. Replay prepares output plus minimal merge headroom jointly, budgets owned state headers/decoder scratch and reshapes oversized IPC runs without silently raising the pool.

**Old Joom lineage:** `056ff7097`.

**Code:** [native/vendor/datafusion-physical-plan/src/aggregates/replay_budget.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/replay_budget.rs), [native/vendor/datafusion-physical-plan/src/aggregates/ordered_final_stream.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/ordered_final_stream.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [native/core/tests/aggregate_emit_memory.rs](../../native/core/tests/aggregate_emit_memory.rs), [native/vendor/datafusion-physical-plan/src/aggregates/replay_budget_tests.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/replay_budget_tests.rs).

### N09 — collect_list low-copy materialization

When gathering runs, use copy_runs/MutableArrayData instead of a slice object per run; support variable-size capacities/struct recursion and preserve result-array order/schema.

**Old Joom lineage:** `056ff7097`.

**Code:** [native/spark-expr/src/agg_funcs/collect.rs](../../native/spark-expr/src/agg_funcs/collect.rs), [native/core/tests/aggregate_emit_memory.rs](../../native/core/tests/aggregate_emit_memory.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [native/spark-expr/src/agg_funcs/collect.rs](../../native/spark-expr/src/agg_funcs/collect.rs), [native/core/tests/aggregate_emit_memory.rs](../../native/core/tests/aggregate_emit_memory.rs).

### D09 — Ordered aggregate drain once

Materialize a completed ordered group range once, then slice it before consuming more input. Retain pending backing storage/workspace. MergeAsPartial keeps raw states and packs output structs shallowly.

**Old Joom lineage:** `ee5d84128`, `3ef370e25`.

**Code:** [native/vendor/datafusion-physical-plan/src/aggregates/aggregate_hash_table/common.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/aggregate_hash_table/common.rs), [native/vendor/datafusion-physical-plan/src/aggregates/aggregate_hash_table/common_ordered.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/aggregate_hash_table/common_ordered.rs), [native/vendor/datafusion-physical-plan/src/aggregates/ordered_drain_tests.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/ordered_drain_tests.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [native/vendor/datafusion-physical-plan/src/aggregates/ordered_drain_tests.rs](../../native/vendor/datafusion-physical-plan/src/aggregates/ordered_drain_tests.rs).

### N04 — Unordered FIRST/LAST GroupsAccumulator

Preserve the fast grouped accumulator for Spark FIRST/LAST without ordering, including ignore-null behavior and supported scalar/string/binary/boolean types.

**Old Joom lineage:** `2fa930bc6`, `25f52bfce`.

**Code:** [native/spark-expr/src/agg_funcs/first_last.rs](../../native/spark-expr/src/agg_funcs/first_last.rs), [native/core/src/execution/planner.rs](../../native/core/src/execution/planner.rs).

**Regression anchors:** [CometFirstLastBoolAggFuzzSuite](../../spark/src/test/scala/org/apache/comet/exec/CometFirstLastBoolAggFuzzSuite.scala).

### N05 — Boolean MIN/MAX through the native groups path

Use grouped bool_and/bool_or for Boolean MIN/MAX with Spark NULL behavior, while preserving the upstream retract-capable MIN/MAX selector for sliding windows.

**Old Joom lineage:** `2fa930bc6`, `25f52bfce`.

**Code:** [native/core/src/execution/planner.rs](../../native/core/src/execution/planner.rs).

**Regression anchors:** [CometFirstLastBoolAggFuzzSuite](../../spark/src/test/scala/org/apache/comet/exec/CometFirstLastBoolAggFuzzSuite.scala), [CometWindowSemanticsSuite](../../spark/src/test/scala/org/apache/comet/exec/CometWindowSemanticsSuite.scala).

### N06 — Native string MIN/MAX: upstream equivalent

Preserve native MIN/MAX on string values with default collation and correct fallback for unsupported collations.

**Implementation choice:** Use upstream #6927 native string MIN/MAX with default collation; retain unsupported-collation fallback.

**Old Joom lineage:** `6f7a970eb`.

**Code:** [spark/src/main/scala/org/apache/comet/serde/aggregates.scala](../../spark/src/main/scala/org/apache/comet/serde/aggregates.scala).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala).

### N08 — Native SortAggregate conversion, streaming and order-sensitive guard

Use upstream SortAggregate/distribution/restore-sort architecture. Omit grouping sorts only for COUNT and nonfloating MIN/MAX below a proven unordered shuffle boundary through deterministic Project/Filter, with deterministic arguments/filters/results. Preserve root order and ordering for FIRST/LAST, MIN_BY/MAX_BY, SUM/AVG, floating extrema and collections; reconstruct grouping sorts on every Spark restoration path.

**Implementation choice:** Keep upstream CometSortAggregateExec, add only the proven-safe residual streaming/order optimization.

**Old Joom lineage:** `6f7a970eb`, `4d7eec2d2`, `e930327f4`, `9153d794f`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/operators.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/operators.scala), [spark/src/main/scala/org/apache/comet/serde/aggregates.scala](../../spark/src/main/scala/org/apache/comet/serde/aggregates.scala).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [CostBasedEngineCorrectnessSuite](../../spark/src/test/scala/org/apache/comet/rules/CostBasedEngineCorrectnessSuite.scala).

### N07 — Native MIN_BY/MAX_BY with a string value

Preserve native MIN_BY/MAX_BY when the returned value is string; do not lose ordering and null/tie/PartialMerge contracts from original Joom code.

**Old Joom lineage:** `92f4b7297`, `9153d794f`.

**Code:** [spark/src/main/scala/org/apache/comet/serde/aggregates.scala](../../spark/src/main/scala/org/apache/comet/serde/aggregates.scala), [spark/src/main/scala/org/apache/comet/rules/CometExecRule.scala](../../spark/src/main/scala/org/apache/comet/rules/CometExecRule.scala), [native/spark-expr/src/agg_funcs/max_min_by.rs](../../native/spark-expr/src/agg_funcs/max_min_by.rs).

**Regression anchors:** [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala).

## Filtered sort-merge joins

G05: 0ef152e47, 72d40726e.

### D05 — Sort-merge join: filter before payload and hoisting

Do not materialize payload for rejected pairs; evaluate buffered-only expressions once per buffered row and streamed-only expressions once per run. Preserve streamed ordering, FULL/LEFT/RIGHT null/duplicate correctness and volatile-expression restrictions.

**Old Joom lineage:** `f761b31c3`, `eedede1a6`, `93dc75be8`, `600f88ee6`, `2692c3edc`, `fb34a5fbd`, `4bc36982c`, `d531476cc`, `4575593c7`, `f5eab1948`, `780cb99a8`, `5bf9f6e10`.

**Code:** [native/vendor/datafusion-physical-plan/src/joins/sort_merge_join/filter.rs](../../native/vendor/datafusion-physical-plan/src/joins/sort_merge_join/filter.rs), [native/vendor/datafusion-physical-plan/src/joins/sort_merge_join/materializing_stream.rs](../../native/vendor/datafusion-physical-plan/src/joins/sort_merge_join/materializing_stream.rs).

**Regression anchors:** [CometSmjJoinFilterAnsiSuite](../../spark/src/test/scala/org/apache/comet/exec/CometSmjJoinFilterAnsiSuite.scala), [CometSmjJoinFilterFuzzSuite](../../spark/src/test/scala/org/apache/comet/exec/CometSmjJoinFilterFuzzSuite.scala), [native/vendor/datafusion-physical-plan/src/joins/sort_merge_join/tests.rs](../../native/vendor/datafusion-physical-plan/src/joins/sort_merge_join/tests.rs).

## Windows

G06: fff076ab8, 0028887b3.

### N01 — Spillable non-streaming partition windows

Spill eligible whole-partition aggregates/values, partition-size functions, suffix frames and mixed nodes. Whole floating SUM/AVG folds in input order. Suffix SUM/AVG, suffix floating MIN/MAX (also nested floating values) and LEAD/LAG IGNORE NULLS retain Spark fallback. Keep FILTER/DISTINCT/frame guards.

**Old Joom lineage:** `65be8b751`, `e90fcec24`, `e43546613`, `bc64dfceb`.

**Code:** [native/operators/src/partition_aggregate_window.rs](../../native/operators/src/partition_aggregate_window.rs), [spark/src/main/scala/org/apache/spark/sql/comet/CometWindowExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/CometWindowExec.scala).

**Regression anchors:** [CometPartitionAggregateWindowSuite](../../spark/src/test/scala/org/apache/comet/exec/CometPartitionAggregateWindowSuite.scala), [CometWindowSemanticsSuite](../../spark/src/test/scala/org/apache/comet/exec/CometWindowSemanticsSuite.scala).

### N02 — Batch tiny partition windows and coalesce output

Batch tiny complete partitions and coalesce output with shared nested-offset limits; preserve cross-batch boundaries and spill. Repeated PERCENT_RANK projections share one per-batch peer result. Reject partition size >Int.MAX with a structured Spark error.

**Old Joom lineage:** `95dc9ae9b`, `d60929066`.

**Code:** [native/operators/src/partition_aggregate_window.rs](../../native/operators/src/partition_aggregate_window.rs), [native/common/src/offset_extents.rs](../../native/common/src/offset_extents.rs).

**Regression anchors:** [CometWindowSemanticsSuite](../../spark/src/test/scala/org/apache/comet/exec/CometWindowSemanticsSuite.scala).

### N03 — SortedWindowExec for simple streaming windows

Stream ROW_NUMBER/RANK/DENSE_RANK and eligible LEAD/LAG across sorted batches. Shifts require literal offset/default, abs(offset)<=1024 and no IGNORE NULLS. Preserve peers, NULL/default semantics and cross-batch partitions. Unsupported custom shapes use the safe standard/Spark path.

**Old Joom lineage:** `47ce539e4`.

**Code:** [native/operators/src/sorted_window.rs](../../native/operators/src/sorted_window.rs), [native/core/src/execution/planner/window.rs](../../native/core/src/execution/planner/window.rs).

**Regression anchors:** [CometWindowExecSuite](../../spark/src/test/scala/org/apache/comet/exec/CometWindowExecSuite.scala), [CometWindowSemanticsSuite](../../spark/src/test/scala/org/apache/comet/exec/CometWindowSemanticsSuite.scala).

## Shuffle and columnar-to-row

G07: c894f535f, 3774cc51f, 1a2734992.

### J10 — Avoid copying unique dictionary values during JVM-shuffle row conversion

When nondictionary fallback is needed and nonnull keys reference unique values in insertion order, reuse the values buffer instead of casting; repeated keys/null values use safe ordinary fallback.

**Old Joom lineage:** `d9ea39240`.

**Code:** [native/shuffle/src/spark_unsafe/row.rs](../../native/shuffle/src/spark_unsafe/row.rs).

**Regression anchors:** [CometShuffleWriterSemanticsSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleWriterSemanticsSuite.scala), [native/shuffle/src/spark_unsafe/row.rs](../../native/shuffle/src/spark_unsafe/row.rs).

### J11 — Other Spark memory consumers can trigger JVM shuffle-writer spill

Owner-thread idle spill callback responds to sibling Spark memory consumers. Preserve busy/reentrant/closed guards, portable reset and pending-row pointer restoration; poison a failed spill and remove its partial file so failure cannot resume as success.

**Old Joom lineage:** `b65fdbe01`.

**Code:** [spark/src/main/java/org/apache/spark/shuffle/comet/CometShuffleMemoryAllocatorTrait.java](../../spark/src/main/java/org/apache/spark/shuffle/comet/CometShuffleMemoryAllocatorTrait.java), [spark/src/main/java/org/apache/spark/shuffle/comet/CometUnifiedShuffleMemoryAllocator.java](../../spark/src/main/java/org/apache/spark/shuffle/comet/CometUnifiedShuffleMemoryAllocator.java), [spark/src/main/java/org/apache/spark/shuffle/sort/CometShuffleExternalSorter.java](../../spark/src/main/java/org/apache/spark/shuffle/sort/CometShuffleExternalSorter.java).

**Regression anchors:** [CometShuffleExternalSorterSpillSuite](../../spark/src/test/scala/org/apache/spark/shuffle/sort/CometShuffleExternalSorterSpillSuite.scala), [CometUnifiedShuffleMemoryAllocatorSuite](../../spark/src/test/scala/org/apache/spark/shuffle/comet/CometUnifiedShuffleMemoryAllocatorSuite.scala).

### J13 — Gather wide shuffle rows only from used batches plus shared type alignment

A partition chunk gathers payload from the referenced batch subset; align nested type instances to the writer schema without copying values; preserve row-order/null/metadata guards.

**Old Joom lineage:** `94abcc0fc`.

**Code:** [native/shuffle/src/partitioners/partitioned_batch_iterator.rs](../../native/shuffle/src/partitioners/partitioned_batch_iterator.rs), [native/shuffle/src/shuffle_writer.rs](../../native/shuffle/src/shuffle_writer.rs), [native/shuffle/src/type_align.rs](../../native/shuffle/src/type_align.rs).

**Regression anchors:** [CometShuffleWriterSemanticsSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleWriterSemanticsSuite.scala), [native/shuffle/src/type_align.rs](../../native/shuffle/src/type_align.rs).

### J23 — Select direct shuffle reads during initial planning

Select ShuffleScan proto during initial conversion, not only late AQE; a native parent must not retain ordinary Scan with an expensive JVM round trip; JVM dispatch must match serialized proto.

**Old Joom lineage:** `4956d831b`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala), [spark/src/main/scala/org/apache/comet/serde/operator/CometSink.scala](../../spark/src/main/scala/org/apache/comet/serde/operator/CometSink.scala).

**Regression anchors:** [CometNativeShuffleSuite](../../spark/src/test/scala/org/apache/comet/exec/CometNativeShuffleSuite.scala), [CometShuffleReadCoalesceSuite](../../spark/src/test/scala/org/apache/comet/exec/CometShuffleReadCoalesceSuite.scala).

### J12 — Shuffle-read coalescing with row and byte limits

Coalesce JVM/native direct shuffle blocks subject to row/byte/nested-offset limits; a byte-oversized block passes unchanged. Reject impossible row counts and malformed headers, propagate errors instead of EOF and release prefetched/delivered batches once. Preserve multirow/sliced Bloom merge over coalesced buffers.

**Old Joom lineage:** `2d30872c8`, `192bad884`.

**Code:** [native/shuffle/src/read_coalescer.rs](../../native/shuffle/src/read_coalescer.rs), [spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/NativeBatchDecoderIterator.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/NativeBatchDecoderIterator.scala), [spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometBlockStoreShuffleReader.scala).

**Regression anchors:** [CometShuffleReadCoalesceSuite](../../spark/src/test/scala/org/apache/comet/exec/CometShuffleReadCoalesceSuite.scala), [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala), [native/shuffle/src/read_coalescer.rs](../../native/shuffle/src/read_coalescer.rs).

### J21 — Normalize offsets in sliced broadcast batches

When coalescing broadcast sliced string/binary/list values, preserve exact boundaries and results; verify ordinary and reused/AQE broadcast.

**Implementation choice:** Use upstream offset normalization before IPC serialization; retain sliced/nested/reused-broadcast Joom regressions.

**Old Joom lineage:** `d913126f8`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/util/Utils.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/util/Utils.scala).

**Regression anchors:** [UtilsSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/util/UtilsSuite.scala), [CometJoinSuite](../../spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala).

### J24 — Small batchSize and capping JVM shuffle batch size at its use site

A positive small batchSize does not fail during CometConf initialization; effectiveJVMshuffleBatch=min(configuredJVM,batchSize).

**Implementation choice:** Use upstream effective JVM shuffle cap at its use sites instead of the old initialization-time restriction.

**Old Joom lineage:** `ea38aad51`.

**Code:** [spark/src/main/scala/org/apache/comet/CometConf.scala](../../spark/src/main/scala/org/apache/comet/CometConf.scala), [spark/src/main/java/org/apache/spark/sql/comet/execution/shuffle/CometDiskBlockWriter.java](../../spark/src/main/java/org/apache/spark/sql/comet/execution/shuffle/CometDiskBlockWriter.java), [spark/src/main/java/org/apache/spark/sql/comet/execution/shuffle/SpillWriter.java](../../spark/src/main/java/org/apache/spark/sql/comet/execution/shuffle/SpillWriter.java).

**Regression anchors:** [CometConfSuite](../../spark/src/test/scala/org/apache/comet/CometConfSuite.scala), [CometShuffleWriterSemanticsSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleWriterSemanticsSuite.scala).

### J26 — Large-batch memory diagnostics at decode/FFI boundaries

Optional COMET_DEBUG_BATCH_MEMORY=1 logs rows/shared-buffer bytes/native allocated/reserved for batch>=16MiB; inactive in normal mode.

**Old Joom lineage:** `ca6b963f0`.

**Code:** [native/core/src/execution/jni_api.rs](../../native/core/src/execution/jni_api.rs), [native/core/src/execution/operators/shuffle_scan.rs](../../native/core/src/execution/operators/shuffle_scan.rs).

**Regression anchors:** [CometShuffleReadCoalesceSuite](../../spark/src/test/scala/org/apache/comet/exec/CometShuffleReadCoalesceSuite.scala), [native/core/src/execution/jni_api.rs](../../native/core/src/execution/jni_api.rs).

### J08 — Binary borrowing in non-codegen ColumnarToRow

Physically copy BinaryType into UnsafeRow through a borrowed UTF8String span, avoiding an intermediate byte[]; plans/data remain Binary; unsupported vector/nested/dictionary paths retain ordinary fallback.

**Old Joom lineage:** `c0b0448c7`, `3d99265f0`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/CometColumnarToRowExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/CometColumnarToRowExec.scala).

**Regression anchors:** [CometBatchRowProjectionSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometBatchRowProjectionSuite.scala).

### J09 — Reuse C2R projection instances

Bound the projection-instance pool by schema/count/buffer bytes; return instances only after all task readers finish so a reused UnsafeRow buffer cannot corrupt data still being read.

**Implementation choice:** Keep upstream generated-class cache (#6858) plus the bounded instance pool; class caching alone does not define safe instance lifetime.

**Old Joom lineage:** `9560d4a8e`, `8e065270a`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/CometColumnarToRowExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/CometColumnarToRowExec.scala).

**Regression anchors:** [CometBatchRowProjectionSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometBatchRowProjectionSuite.scala).

### J25 — Disable whole-stage codegen for C2R above Comet broadcast

CometColumnarToRowExec.supportCodegen=false above CometBroadcastExchange through BroadcastQueryStage/ReusedExchange so broadcast is not executed as an ordinary RDD.

**Old Joom lineage:** `3adfb1ecc`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/CometColumnarToRowExec.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/CometColumnarToRowExec.scala).

**Regression anchors:** [CometJoinSuite](../../spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala), [CometBroadcastKryoPayloadSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometBroadcastKryoPayloadSuite.scala).

### J27 — Default Kryo broadcast-payload coverage

Preserve original default-Kryo broadcast-payload regression scenarios; this is validation, not a separate algorithmic feature.

**Old Joom lineage:** `a52c1685f`.

**Regression anchors:** [CometBroadcastKryoPayloadSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometBroadcastKryoPayloadSuite.scala).

## JVM codegen and serialization

G08: 2dc13d5bd.

### J05 — Scalar subqueries as runtime arguments to the JVM codegen dispatcher

Lower ScalarSubquery→BoundReference; pass the resolved result as a native argument; broadcast length-one Arrow vectors through rowMask, including nested/null checks. A cached kernel must not capture the old result.

**Old Joom lineage:** `65be8b751`.

**Code:** [spark/src/main/scala/org/apache/comet/serde/CometScalaUDF.scala](../../spark/src/main/scala/org/apache/comet/serde/CometScalaUDF.scala), [spark/src/main/scala/org/apache/comet/codegen/CometBatchKernelCodegenInput.scala](../../spark/src/main/scala/org/apache/comet/codegen/CometBatchKernelCodegenInput.scala), [native/spark-expr/src/jvm_udf/mod.rs](../../native/spark-expr/src/jvm_udf/mod.rs).

**Regression anchors:** [CometCodegenRuntimeScalarSuite](../../spark/src/test/scala/org/apache/comet/CometCodegenRuntimeScalarSuite.scala), [CometCodegenSourceSuite](../../spark/src/test/scala/org/apache/comet/CometCodegenSourceSuite.scala).

### J06 — Short-circuit JVM-dispatched conjuncts in filters

Apply deterministic JVM-dispatch filter masks only above a proven eager-safe prefix; keep long AND segments balanced. Mandatory NULL/evaluation-order/CSE guards remain when the optional optimization is disabled. Ambiguous physical evaluation order uses Spark.

**Old Joom lineage:** `764adf614`, `092d70af2`.

**Code:** [spark/src/main/scala/org/apache/spark/sql/comet/operators.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/operators.scala), [native/core/src/execution/expressions/logical.rs](../../native/core/src/execution/expressions/logical.rs).

**Regression anchors:** [CometFilterShortCircuitSuite](../../spark/src/test/scala/org/apache/comet/exec/CometFilterShortCircuitSuite.scala), [CometConditionalEvaluationOrderSuite](../../spark/src/test/scala/org/apache/comet/exec/CometConditionalEvaluationOrderSuite.scala).

### J07 — ScalaUDF function pools: send closures once per native plan

Share live UDF functions/captures once in Spark task serialization with task/retry isolation, typed references and current classloader/occurrence contracts. Fresh actions snapshot current driver state; do not restore the old global serialized-byte cache, which can freeze captured values across actions. Preserve shared identity/size savings and shaded-JAR coverage.

**Implementation choice:** Preserve pool intent through current live task transport instead of stale serialized-byte caching.

**Old Joom lineage:** `df0b1b9ff`, `ab179fa2e`.

**Code:** [spark/src/main/scala/org/apache/comet/udf/codegen/CometUdfFunctionPool.scala](../../spark/src/main/scala/org/apache/comet/udf/codegen/CometUdfFunctionPool.scala), [spark/src/main/scala/org/apache/comet/udf/codegen/CometSharedFunctionRef.scala](../../spark/src/main/scala/org/apache/comet/udf/codegen/CometSharedFunctionRef.scala).

**Regression anchors:** [CometUdfFunctionPoolSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/CometUdfFunctionPoolSuite.scala), [CometScalaUDFClassLoaderSuite](../../spark/src/test/scala/org/apache/comet/CometScalaUDFClassLoaderSuite.scala).

### J17 — Task-binary size: exclude heavy plans from Java serialization

Keep heavy NativeShuffleSpec/operator protobufs in explicit map-side/task carriers and transient driver caches; downstream dependencies retain small admission configuration. Intern SQL text in root/scan/Delta/shuffle envelopes without changing keys or ANSI positions. Finalized native-shuffle UDF keys also require task transport.

**Implementation choice:** Extend the upstream root QueryContextInterner into missing envelopes without changing error context.

**Old Joom lineage:** `a7170c287`, `d0c3e79d2`, `af29142a3`, `61590a84a`.

**Code:** [spark/src/main/scala/org/apache/comet/serde/QueryContextInterner.scala](../../spark/src/main/scala/org/apache/comet/serde/QueryContextInterner.scala), [spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleDependency.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleDependency.scala), [spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleInputRDD.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleInputRDD.scala).

**Regression anchors:** [CometTaskBinarySizeSuite](../../spark/src/test/scala/org/apache/comet/exec/CometTaskBinarySizeSuite.scala), [QueryContextInternerSuite](../../spark/src/test/scala/org/apache/comet/QueryContextInternerSuite.scala), [CometNativeShuffleInputRDDSuite](../../spark/src/test/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleInputRDDSuite.scala).

## Engine choice and boundary formats

G09: d961ec434, 4467d0337.

### I07 — Wide-decimal hash shuffle correctness

Use upstream native Decimal128 hashing including precision >18; retain exact Spark partition assignments and legal mixed Spark/native boundaries. Do not restore the superseded blanket precision>18 restriction.

**Implementation choice:** Upstream #6548 native hashing supersedes the old #6005-style restriction.

**Old Joom lineage:** `88cfb2929`.

**Code:** [native/spark-expr/src/hash_funcs/utils.rs](../../native/spark-expr/src/hash_funcs/utils.rs), [spark/src/main/scala/org/apache/comet/rules/BoundaryFormats.scala](../../spark/src/main/scala/org/apache/comet/rules/BoundaryFormats.scala).

**Regression anchors:** [CostBasedEngineCorrectnessSuite](../../spark/src/test/scala/org/apache/comet/rules/CostBasedEngineCorrectnessSuite.scala), [CometNativeShuffleSuite](../../spark/src/test/scala/org/apache/comet/exec/CometNativeShuffleSuite.scala).

### J02 — Choose shuffle/broadcast formats by producer and consumer

Choose boundary formats from both producer and consumer; Spark-to-Spark remains Spark and co-partitioned hash functions remain compatible. Preserve reused/materialized exchanges, root shuffles/subqueries, direct inputs, CapturedSQLConf and AQE tags. Restore SortAggregate grouping sorts and TopK offset/projection exactly once.

**Old Joom lineage:** `01df400a3`, `b5fdf8668`, `34a841191`.

**Code:** [spark/src/main/scala/org/apache/comet/rules/BoundaryFormats.scala](../../spark/src/main/scala/org/apache/comet/rules/BoundaryFormats.scala), [spark/src/main/scala/org/apache/comet/rules/ChooseBoundaryFormats.scala](../../spark/src/main/scala/org/apache/comet/rules/ChooseBoundaryFormats.scala), [spark/src/main/scala/org/apache/comet/rules/CometRule.scala](../../spark/src/main/scala/org/apache/comet/rules/CometRule.scala).

**Regression anchors:** [ChooseBoundaryFormatsSuite](../../spark/src/test/scala/org/apache/comet/rules/ChooseBoundaryFormatsSuite.scala), [CostBasedEngineCorrectnessSuite](../../spark/src/test/scala/org/apache/comet/rules/CostBasedEngineCorrectnessSuite.scala).

### J03 — Separate wide-row sort fallback heuristic

The public feature remains in tag13: Spark consumer plus wide payload measured by leaf-column count returns sort to Spark; stable initial/AQE decision; ignored when costBasedEngines is active.

**Old Joom lineage:** `2d3cc8914`, `909335e3f`, `9ff373647`, `b83e9172b`.

**Code:** [spark/src/main/scala/org/apache/comet/rules/WideRowSortFallback.scala](../../spark/src/main/scala/org/apache/comet/rules/WideRowSortFallback.scala), [spark/src/main/scala/org/apache/comet/rules/LeafColumns.scala](../../spark/src/main/scala/org/apache/comet/rules/LeafColumns.scala), [spark/src/main/scala/org/apache/comet/rules/CometExecRule.scala](../../spark/src/main/scala/org/apache/comet/rules/CometExecRule.scala).

**Regression anchors:** [WideRowSortFallbackSuite](../../spark/src/test/scala/org/apache/comet/rules/WideRowSortFallbackSuite.scala).

### J04 — Separate wide-row shuffle fallback heuristic

At the configured threshold, keep wide-payload shuffle in Spark; leaf counting excludes the partition key; ignore when the cost model is active.

**Old Joom lineage:** `c2562fb89`, `b83e9172b`.

**Code:** [spark/src/main/scala/org/apache/comet/rules/WideRowShuffleFallback.scala](../../spark/src/main/scala/org/apache/comet/rules/WideRowShuffleFallback.scala), [spark/src/main/scala/org/apache/comet/rules/LeafColumns.scala](../../spark/src/main/scala/org/apache/comet/rules/LeafColumns.scala).

**Regression anchors:** [WideRowShuffleFallbackSuite](../../spark/src/test/scala/org/apache/comet/rules/WideRowShuffleFallbackSuite.scala).

### J01 — Calibrated per-operator engine selection by whole-plan cost

Retain whole-plan cost selection, initial/AQE decisions, schema-width/nested/shuffle features, overrides/explanations and final tag13 calibration (calib29 plus final adjustments). EngineCostTable and LeafColumns retain tag13 behavior. Recalibration is a separate decision; price never legalizes unsafe state/expression conversion.

**Old Joom lineage:** `fd190a66f`, `d1148b208`, `cea0ae513`, `f9793a790`, `1f7b89bbe`, `71a7c0723`, `3dbd273cd`, `bef0b9f6a`, `250bfd2e1`, `88c3edfb2`, `c7df21932`, `3023e0b02`, `c90d450ad`, `8b4fc571d`, `60bbf7614`, `34a841191`, `3fa650292`.

**Code:** [spark/src/main/scala/org/apache/comet/rules/CostBasedEngineChoice.scala](../../spark/src/main/scala/org/apache/comet/rules/CostBasedEngineChoice.scala), [spark/src/main/scala/org/apache/comet/rules/EngineCostTable.scala](../../spark/src/main/scala/org/apache/comet/rules/EngineCostTable.scala), [spark/src/main/scala/org/apache/comet/rules/LeafColumns.scala](../../spark/src/main/scala/org/apache/comet/rules/LeafColumns.scala), [spark/src/main/scala/org/apache/comet/rules/CometRule.scala](../../spark/src/main/scala/org/apache/comet/rules/CometRule.scala).

**Regression anchors:** [CostBasedEngineChoiceSuite](../../spark/src/test/scala/org/apache/comet/rules/CostBasedEngineChoiceSuite.scala), [CostBasedEngineCorrectnessSuite](../../spark/src/test/scala/org/apache/comet/rules/CostBasedEngineCorrectnessSuite.scala).

## Corrections and boundaries that must survive later changes

- **G01:** retain tiny-decimal rescaling (`f8223f0f3`), typed duplicate Parquet
  projection errors (`3c1e8b71e`, `6277197a7`), UNION layouts/AQE bindings
  (`99ed2f61a`, `43af9beaf`) and associated Spark comparisons.
- **G02:** set cancellation before waking producer/I/O and interrupting only
  registered JNI memory-acquisition threads. Registration/interrupt/removal share
  a lock which is never held while entering Spark. Preserve entry interrupts,
  clear Comet-issued interrupts before worker reuse, reject batch/EOF races and
  drop streams synchronously. The watcher/cancel registry uses tokens rather
  than dereferencing the native execution pointer on the watcher thread.
- **G03:** drop retired ArrayRefs before returning their memory charge. Preserve
  weak-reference retirement checks in late_materialize.rs, TopK reconstruction
  of spill-before-output flags, and final-merge cancellation/early-drop cleanup.
- **G04:** include hidden grouping IDs in spill keys and preserve only valid
  ordering/uniqueness properties. Future output reservations are fallible.
  An independent SORT BY control returned wrong order with a guard removed;
  keep exact Spark ordering regressions. Native Single-vs-Partial parity is
  supplementary to an independent Spark oracle. Initial variable-state owned
  headers need reservation; logical accounting still does not bound RSS.
- **Upstream distribution contract:** preserve `originalPlan.requiredChildDistribution`
  through `CometDelegatesRequiredChildDistribution` in
  [operators.scala](../../spark/src/main/scala/org/apache/spark/sql/comet/operators.scala)
  (upstream `89b6ad738`). Aggregate/window/join wrappers must retain the original
  clustering requirement: AQE skewed REBALANCE can otherwise split one key into
  several output groups. Keep the skew/AQE regression coverage when changing
  wrappers or planner restoration.
- **Version-scoped floating collect_set:** retain upstream `1a10bc99e` behavior in
  [aggregates.scala](../../spark/src/main/scala/org/apache/comet/serde/aggregates.scala).
  The scalar FLOAT/DOUBLE PartialMerge restriction applies only before Spark 4.2.
  Spark 4.2's SPARK-57298 normalizes NaN and signed zero inside its buffer
  conversion, so Comet reapplies the recursive normalizer to raw Partial/Complete
  input. Do not broaden the older fallback to Spark 4.2 or remove normalization.
  Nested arrays use JVM dispatch, and fall back to Spark when that dispatcher is
  disabled; scalar/struct cases remain native. Keep the Spark-4.2-only float-set
  regression in
  [CometAggregateSuite](../../spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala).
- **G05:** constant filters require an explicit row count. Filter/hoist only real
  matching pairs, never synthetic outer null-pairs sharing a chunk. Hoist only
  positively deterministic/nonthrowing work. Unproved casts/arithmetic and
  unknown/stateful UDFs stay at their original location; lacking hoisting alone
  need not cause fallback. Unsafe nested AND/OR RHS join conditions retain a
  targeted Spark join fallback. Preserve streamed ordering and duplicate counts.
- **G06:** floating whole-partition accumulation follows input order. Preserve
  accepted suffix SUM/AVG, suffix nested floating-extrema and IGNORE NULLS
  fallback. FILTER/DISTINCT must be rejected before native construction loses
  their meaning. Partition windows default false; SortedWindow defaults true.
  Some scratch/coalescing/suffix scalar memory remains outside pool accounting.
- **G07:** failed owner spill is terminal; fetched/prefetched/delivered batches
  have one owner. Bloom merge processes every sliced/coalesced input row.
  Coalescing's 64 MiB byte target is neither a reservation nor an RSS cap;
  pending and concatenated arrays may coexist and oversized blocks pass through.
  Celeborn JVM decoding stays blockwise; eligible direct-native decoding coalesces.
- **G08:** projection AND/OR evaluates RHS on NULL LHS, while eligible whole-stage
  Filter may reject NULL before later conjuncts. Use actual physical eligibility
  (including maxFields/factory mode). Ambiguous inferred null checks, stateful
  prefixes and harmful shared CSE use Spark. Stateful COALESCE/multiple
  state-bearing IF/CASE use one JVM kernel inside Comet. Arbitrary side effects
  in falsely deterministic UDFs have no universal first-error-order guarantee.
- **G09:** range keys equal to a bound stay in the lower partition (`d961ec434`).
  Spark restoration reconstructs SortAggregate grouping sorts and current AQE
  LogicalQueryStage tags/links, and removes inserted local TopK so offset and
  projection run once. Preserve guarded aggregate codecs and shuffle admission.
- **G10:** retained all earlier features while fixing aggregate peak-memory metrics
  over replay (`73cf036b7`), dictionary writer safety under Miri (`f700491ad`),
  native-shuffle UDF task transport (`4e0f73119`) and unconsumed prefetch release
  (`81474a889`). Keep the restored complete CI and release-support ABI checks.
  Profile/fixture adaptations do not authorize weakening rows/errors or replacing
  official Spark.

## Configuration contracts

Authoritative definitions are in
[CometConf.scala](../../spark/src/main/scala/org/apache/comet/CometConf.scala).
These are library defaults, not claims about external job/image overrides.

| Key                                                                             | Retained default or meaning                                                          |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `spark.comet.exec.window.partitionAggregate.enabled`                            | false                                                                                |
| `spark.comet.exec.window.sorted.enabled`                                        | true                                                                                 |
| `spark.comet.exec.filter.shortCircuitJvmDispatch.enabled`                       | true; mandatory guards survive false                                                 |
| `spark.comet.shuffle.read.coalesce.enabled`                                     | true                                                                                 |
| `spark.comet.shuffle.read.coalesce.maxBytes`                                    | 64 MiB; 0 disables the byte target                                                   |
| `spark.comet.exec.sort.spillBeforeOutputThreshold`                              | unset: 1/4 off-heap task share; on-heap or explicit 0 disables; JVM-output path only |
| `spark.comet.exec.boundaryFormats.enabled`                                      | false; cost mode already chooses formats                                             |
| `spark.comet.exec.costBasedEngines.enabled`                                     | false; true takes precedence over standalone wide-sort/shuffle heuristics            |
| `spark.comet.exec.costBasedEngines.costTable`                                   | empty overrides; preserve final tag13 calibration                                    |
| `spark.comet.exec.costBasedEngines.log.enabled`                                 | false; fallback explain also exposes decisions                                       |
| `spark.comet.exec.costBasedEngines.cometOperatorWeight` / `sparkOperatorWeight` | -1 / 0 for unpriced operators                                                        |
| `spark.comet.exec.costBasedEngines.cometOperatorWeights`                        | empty per-operator overrides                                                         |
| `spark.comet.exec.sort.wideRowFallback.enabled` / `.minLeafColumns`             | false / 50 payload leaves outside sort keys                                          |
| `spark.comet.shuffle.wideRowFallback.minLeafColumns`                            | 0 disabled; excludes partition-key leaves                                            |
| `COMET_DEBUG_BATCH_MEMORY=1`                                                    | opt-in >=16 MiB decode/FFI diagnostics                                               |

Explicit `spark.comet.datafusion.*` overrides take precedence after D11's initial
1/32 sort-reserve cap. That cap and D03's 1/4 spill-before-output threshold solve
different problems. Projection-instance pools retain schema/count/buffer caps;
returning a pool entry requires all readers to have finished.

## Later upstream upgrades and historical detail

The equivalence audit concerned the pinned 1.2/55.2 base. DF56 workspace lending
and clustered aggregation, larger-offset proposals, partition-window accounting
and upstream lazy evaluation are partial/alternative overlaps, not permission to
drop residuals or silently upgrade dependencies. Recheck exact code and semantics
before replacement; a similarly named open PR is not a qualified substitute.

For original per-stage commands or the complete initial audit, Git object
`586bbbf614823d70535a9ec1a384ea63aff7f4e0` retains the former paths
`docs/joom-migration/feature-register.json`, G01-results.json through
G10-results.json and vendor-provenance.json. Those are historical snapshots,
not current instructions; no runtime code depends on those removed documents.
