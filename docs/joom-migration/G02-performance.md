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

# G02 memory-acquisition bookkeeping: before/after performance

Date: 2026-11-19. Feature source: `e0388e3bc0e5415ea2b85664b1c8cf8e3cbd7574`, `joom/1.2-native-runtime`. The performance investigation itself did not change production source, Git refs/index or published artifacts. Subsequent G02 integration is separately user-approved and recorded in decisions.json and the integration completion receipt. G03 remains unauthorized.

## Result

The new acquisition registration has a measurable Java-bridge cost. The zero-balance single-thread case consistently slows down; saturated two-worker use of one manager also slows down. The tested SQL workloads do not establish a reproducible SQL-level regression, but their noise is too large to establish equivalence or a small upper bound. Do not translate the bridge percentage directly into job runtime.

| Direct bridge case                      | Before, ns/pair | After, ns/pair | Change in time/pair | Exploratory paired 95% interval |
| --------------------------------------- | --------------: | -------------: | ------------------: | ------------------------------- |
| acquire16/release16; persistent32       |           270.4 |          283.7 |               +4.9% | -0.3% to +10.2%                 |
| acquire32/release32; zero balance       |           231.6 |          249.8 |               +7.9% | +2.3% to +13.5%                 |
| 2 workers; shared manager; persistent32 |           445.9 |          545.7 |              +22.4% | +5.9% to +39.1%                 |
| 4 workers; shared manager; persistent32 |           454.4 |          513.6 |              +13.0% | -3.3% to +30.7%                 |

Each unit is an acquire/release pair. The release implementation is unchanged, so the difference measures the manager rewrite around acquisition, including kill checks, coordination and bookkeeping. Single-thread results exclude JNI, constructor/setup and cleanup. For multiple workers, ns/pair is wall time amortized across all workers, a throughput measure rather than individual call latency. The two-worker +22.4% time corresponds to about18.3% less aggregate throughput. Persistent-balance and four-worker intervals include zero; retain them as point estimates, not separately established effects.

The direct case is a small auxiliary extension using the project's existing Spark `Benchmark`/`addTimerCase` and `TaskMemoryTestUtils` real memory fixture. It was not an already-existing Comet benchmark and was not added to production/project sources. It introduces no alternative timing engine. The manager is published outside timing to prevent JIT lock elimination as a purely local object; concurrent workers genuinely share the captured manager without task thread-locals.

## Accepted practical interpretation and stage decision

The user accepted the following assessment and authorized G02 integration on 2026-11-19. For ordinary batch-oriented jobs, fractional-percent influence, generally well below1%, is an engineering expectation, not a measured production result, upper bound or guarantee. Each Spark task owns its lock; unrelated tasks do not contend for it. The default fair_unified pool already serializes native acquisitions in Rust. For scale only,1000 calls times roughly18ns extra is about0.018ms, or about0.05% of40ms processing; this arithmetic is not a job forecast.

The microbenchmark percentages must not be interpreted as job slowdown. Tiny frequent reservations and parallel native workers inside one task, particularly with greedy_unified, remain relevant conditions. Do not block G02 integration or complicate the cancellation mechanism solely for the currently observed microbenchmark cost. Retain the correctness fix; no additional optimization is required for this stage on the current evidence. Measured bridge costs, inconclusive SQL comparisons and all platform/measurement limitations above and below remain unchanged. Final correctness qualification remains required, and real GCP/release performance is not claimed.

## Existing SQL cases

| Existing selected case       | Before, ms/query | After, ms/query | Change in means | Exploratory paired 95% interval |
| ---------------------------- | ---------------: | --------------: | --------------: | ------------------------------- |
| join_fair_unified_batch4096  |           273.25 |          283.75 |           +3.8% | -5.3% to +13.0%                 |
| sort_fair_unified_batch256   |            41.75 |           40.25 |           -3.6% | -16.3% to +10.3%                |
| sort_fair_unified_batch4096  |            36.25 |           37.75 |           +4.1% | -7.3% to +15.5%                 |
| sort_greedy_unified_batch256 |            40.25 |           40.50 |           +0.6% | -9.4% to +10.8%                 |

Sort uses the unchanged public `CometExecBenchmark.sortExecBenchmark` and stock deterministic mix64 fixture, scaled to262144 rows. Join selects the existing `CometHashJoinBenchmark` inner-projected query through the unchanged `runExpressionBenchmark` method, with the exact stock1,048,576 probe /100,000 build fixture and force-SHJ settings. Selection/orchestration lives only in an auxiliary launcher. No new SQL workload or timing engine was invented.

Off-heap512MiB is explicit before SparkContext creation, with local[5], Java17.0.19-tem and Spark4.2.1. Stock benchmarks otherwise default to on-heap/unbounded mode, which would bypass the changed bridge. Batch4096 is the normal case;256 increases reservation frequency. fair_unified and greedy_unified are verified. Stock fixture ANSI=false is retained; these runs do not replace existing ANSI=true correctness tests.

Separate untimed diagnostics capture the actual executed noop-write plan, require CometSortExec/CometHashJoinExec, and compare all collected Spark/Comet rows. Java memory-manager TRACE proves nonzero actual JNI requests. Counts and requested bytes match exactly before/after for every selected case. Each diagnostic block includes one noop write and one native collection, so its request total is not presented as a per-timed-query count.

## Comparison controls

- Before: exact manager source from pre-follow-up9dd53f988, plus a binary-compatibility `cancelPendingAcquires` stub that throws if called. Only healthy/no-cancellation workloads are measured; such a call invalidates a run.
- After: exact manager source from final e0388e3bc. Both are compiled by the same Java17 compiler into separate auxiliary class directories, including nested classes. Each fresh JVM puts the selected directory first on its otherwise identical classpath and prints the loaded code origin.
- All other final Comet/Spark classes are identical. Native library is the unchanged local dev-profile macOSARM64 dylib, SHA256 `8c14dc78b946e0048d9da14993cb200821e272c13556c4ab0d547995fe278938`. This is not an optimized Linux release benchmark. Other follow-up changes, including the iterator's post-JNI kill check, are present in both arms and are not isolated by this comparison.
- Eight fresh JVMs per experiment, ordered before/after/after/before/after/before/before/after. Four adjacent balanced pairs. No concurrent builds/tests/other benchmark runs. Host was on AC power; macOS offers no controlled P/E-core placement here.
- Direct cases: standard Spark Benchmark,5s warm-up, at least5 iterations and3s measured time,1M pairs per single-thread iteration. Contention:250k total pairs across2/4 prestarted workers,32B persistent reservation plus16B per worker in a real100B pool; no refusal/spill is expected. Grant checks, setup and exact accounting/cleanup remain outside timing.
- Existing SQL cases retain their stock2s warm-up and at least2 iterations/2s measurement. Measurement TRACE/DEBUG logging is disabled. Raw direct iterations and SQL mean/stdev tables are retained.

## Limits and interpretation

Every SQL JVM emitted exactly two background driver/executor-heartbeat exceptions because sandbox denies ProcessHandle/sysctl process-tree collection. Queries and benchmark accounting checks succeeded, but these exceptions add SQL timing noise. Bridge-only JVMs have no such errors. The unchanged Spark SQL control cases also drifted by roughly1.5% to9.8%, reinforcing that the SQL point estimates cannot be attributed to manager overhead with confidence.

Sandbox also denies the CPU-model probe in Spark Benchmark's report header. An auxiliary copy of exact Spark v4.2.1 Benchmark replaces only getProcessorName with an explicit unavailable-metadata label. Timing, warm-up, iteration and statistics code are textually unchanged; the metadata-only patch is saved. No denied metadata was read through another mechanism.

Per-JVM means, not individual inner iterations, are the statistical blocks. Reported percentage points are ratios of overall means; intervals are on the mean of four paired percentage changes and therefore need not be centered exactly on that ratio. They use Student t(df=3), are exploratory, and are not adjusted for multiple comparisons. SQL stock output rounds mean timings to1ms. The intervals cannot justify a less-than5% SQL impact claim; upper ends are roughly11–16%.

Default fair_unified currently holds its Rust state mutex across grow/try_grow calls into Spark, so the saturated multiworker Java stress does not directly model that native path. greedy_unified allows parallel manager calls. The stress result is evidence about shared-manager coordination cost, not a22% query slowdown. Spilling, cancellation latency, real GCP jobs, optimized native AMD64/ARM64 artifacts and future feature combinations are not qualified by this report.

The implementation contains extra per-acquire coordination and record/map allocation. The experiment measures their combined cost; it does not apportion the delta between locks, allocation, kill checks or changed JIT inlining. A focused reuse/per-thread-coordination optimization is a possible follow-up, preserving the cancellation/grant/FFI regression coverage. No such optimization or cancellation-policy change was made here. Production performance remains a later qualification; the existing correctness fix should not be removed merely to recover the old unsafe behavior.

## Reproduction and saved files

All source copies, compiled class hashes, commands, stdout, GC logs, diagnostic proofs, parsers and machine-readable summaries are under `${MIGRATION_AUX_ROOT}/20261119_comet12-g02-performance`:

- `start.json`, `compiled-class-hashes.json`, `compiled-class-hashes-final.json`, `benchmark-metadata-only.patch`.
- `run_benchmarks.py`, `run_concurrent.py`, `analyze.py`, `classpath.txt`, `jvm-opens.json`.
- `diagnostic-proof.json`, `measurement-receipts.json`, `analysis.json`, `concurrent-receipts.json`, `concurrent-analysis.json`, `summary.json`, `completion.json`.
- Standard-framework auxiliary cases: `MemoryAcquireBenchmark.scala`, `MemoryAcquireConcurrentBenchmark.scala`; selected existing SQL launcher: `SqlBenchmarkSelection.scala`.
- Dated per-run directories under `20261119_results` contain exact commands, output/GC logs and Spark-managed temporary files.

Do not rerun the metadata probe to work around sandbox, use the saved documented label. Reusing the script requires the same local classpath/build and Java installation. Standard build caches/native targets are shared and must not be cleared wholesale. Sizes and cleanup scope are recorded in completion.json and CLEANUP.md. During the performance investigation no Git mutation, merge, G03, artifact publication or production operation occurred. Subsequent approved consolidation/integration is recorded separately.
