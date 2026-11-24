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

# Validating changes to Joom Comet 1.2

The recorded qualification evidence is in [QUALIFICATION.md](QUALIFICATION.md).
[FEATURES.md](FEATURES.md) maps the original Joom improvements to current code,
regression tests and upstream replacements. Preserve that chain when fixing a
production issue: record the input/trigger, independent reference, changed code,
regression identity, source SHA and any newly qualified JARs.

## Correctness contract

Use official Apache Spark 4.2.1 (revision
`617618396087429b5627a86ce933bb0f5d6645cb`) **without Comet** as the independent
reference, then run the same query/input snapshot with Comet enabled and identical
significant settings. Use the Java 25 runtime and SQL defaults from
[RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md).

- Compare complete values, types/schema, NULLs, duplicate rows, grouping keys and
  every order guaranteed or observed by the query. Counts/checksums alone are
  insufficient. Do not use arbitrary tolerances or broad normalization to hide
  a difference.
- Compare **must succeed / must fail**, including mandatory ANSI overflow/cast/
  division errors and relevant error class/SQLSTATE/parameters. Successful Comet
  output in place of a mandatory Spark error is a correctness failure.
- Prove which path actually ran: initial/executed native plans, operator metrics,
  actual forced spill, JNI callbacks or dispatcher/fallback, as appropriate.
  A matching result produced entirely by fallback does not test a native fix.
- FIRST/LAST and tied MIN_BY/MAX_BY may select different valid candidates across
  genuinely unordered layouts. Construct deterministic ordered fixtures for
  endpoint checks; validate actual extremal candidates and unique group rows for
  ties. Preserve multiplicities when normalizing only a genuinely unordered result.
- Test mixed Spark partial/native final and the reverse for SUM/AVG/Bloom buffers,
  decimal overflow flags, PartialMerge/DISTINCT spill, NULL/grouping sets and LIMIT.
  Include AQE, cost/boundary modes, forced spill, sliced/nested arrays and resource
  cleanup whenever the changed path interacts with them.
- Keep lazy AND/OR/CASE/UDF and SMJ evaluation order, native windows and their
  fallback guards. Errors may be avoided only when original Spark would
  avoid evaluating that expression. `ansi=true` does not make
  `allowIncompatible=true` implementations safe.
- A negative control deliberately runs a bounded regression without the proposed
  fix to demonstrate that it detects the defect; restore and verify the final
  source afterward. A deliberately failing control is not an unfixed final test.
- Fix a confirmed unsafe path or select safe targeted fallback **before execution**.
  Post-write retries or late fallback cannot protect against silent incorrect data.
  Preserve known exceptions honestly; this document does not claim they were fixed.

The G04 independent review found four silent-result counterexamples in six new
Spark differential tests: nondeterministic projection/filter/aggregate-result
expressions observed sort removal before a shuffle. The final proof crosses only
deterministic wrappers, arguments, filters and result expressions. It preserves
native ordered aggregation when that proof fails. Regression oracles were also
strengthened to check every group/key/count/NULL/duplicate, require PartialMerge
itself to spill, and compare exact Bloom bytes in all three engine combinations.
Native Partial-to-Final versus Single shares implementation and is complementary
evidence, **not** a fully independent Spark oracle. Original Spark runtime classes
were resolved from official Maven JARs, without Comet shadow copies.

## Local builds and targeted regressions

Run from `${COMET_ROOT}`. Follow
[the development guide](../source/contributor-guide/development.md): build native
before JVM tests, use the Maven reactor from the repository root and **do not use
`-pl`**. Check that the loaded library was rebuilt from the current source; stale
`target` libraries cannot qualify a fix. Debug builds suffice for normal local
correctness work. Select the affected suites from FEATURES rather than starting
an uncontrolled full build.

Example target-profile commands (select the actual pinned JDK first):

```bash
python3 -B dev/ci/java-runtime.py --java "${JAVA_HOME:?}/bin/java"
./mvnw -B -Pspark-4.2 compile -DskipTests
cargo build --locked --manifest-path native/Cargo.toml
./mvnw -B -Pspark-4.2 test -Dtest=none \
  -Dsuites=org.apache.comet.exec.CometWindowSemanticsSuite
```

For Delta-dependent changes include `-Pspark-4.2,delta`. The native JNI tests need
`libjvm` on the platform library path; on Linux use
`LD_LIBRARY_PATH="${JAVA_HOME:?}/lib/server${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"`.
Build `comet-test-udfs` explicitly before native tests that dlopen it. Use the
shared workspace `native/Cargo.lock`, not an independently resolved vendor crate.
For example, the existing `make PROFILES=-Pspark-4.2 test-vendor` target compiles
JVM prerequisites and tests Comet plus vendor with the unified graph, including
vendor `test_utils` statistics. See [DATAFUSION.md](DATAFUSION.md).

The strict CI commands remain in `.github/actions/rust-test/action.yaml`:

```bash
cargo clippy --locked --manifest-path native/Cargo.toml --color=never \
  --all-targets --workspace --no-deps -- -D warnings
cargo nextest run --locked --manifest-path native/Cargo.toml \
  -p datafusion-comet -p datafusion-physical-plan --lib \
  -E 'package(datafusion-physical-plan)'
cargo nextest run --locked --manifest-path native/Cargo.toml \
  -p datafusion-comet -p datafusion-physical-plan --lib \
  --features datafusion-physical-plan/test_utils \
  -E 'package(datafusion-physical-plan) & test(statistics::)'
```

The deprecated `fetch_update` allowance is **statement-local** inside
vendored DataFusion. Keep that algorithm and all other strict warnings; do not
exclude the vendor from Clippy or add a global `-A deprecated` acceptance path.
Full CI separately covers Python UDF and jemalloc feature variants and native
benches/dependency checks; a selected vendor invocation is not their substitute.

The target exec cohort opts into the complete fuzz matrices through forked-JVM
`JAVA_TOOL_OPTIONS` flags `-Dcomet.test.aggFuzz.full=true` and
`-Dcomet.test.smjFuzz.full=true`. Confirm all 31
`org.apache.comet.exec.CometFirstLastBoolAggFuzzSuite` and 33
`org.apache.comet.exec.CometSmjJoinFilterFuzzSuite` registrations actually ran.
For windows preserve the merged log and run the maintained verifier:

```bash
python3 -B dev/ci/verify-native-windows.py target/qualification-exec.log \
  --output target/qualification-windows.json
```

The verifier independently expects 88 action identities and native/spill/fallback
requirements; it supplements exact Scala result/schema/error assertions. Source
or verifier changes require a reviewed mapping update, not deletion of a failing
case. The old migration-directory copy is not the maintained verifier.

Spark SQL source-test patches under `dev/diffs` must be regenerated using
[the Spark SQL guide](../source/contributor-guide/spark-sql-tests.md); never
hand-edit the diff. TPC plan-stability acceptance uses ordinary execution with
golden generation disabled. Regenerate a snapshot only after independent review
of its semantic/path change, not to turn an unexpected plan into a pass.

## Full CI and release qualification

Routine fixes use local builds and focused regressions first. Record each
unverified applicable case with its feature, owner, settings, intended command,
source SHA and evidence. Focused native Linux AMD64 GHA may resolve a platform
issue. No rule requires full GHA or a multiarch release after every small fix;
final coverage is unchanged.

Current routing is defined by `.github/workflows/ci.yml`,
`dev/ci/compute-changes.py` and `dev/ci/linux-test-profiles.py`:

- Primary PR runtime: Spark 4.2 / exact Temurin 25; normal quality/native checks
  and path routing remain enabled. PR-to-default-branch coverage is intentionally
  narrower than the full matrix.
- Merge queue adds the routed macOS, benchmark compile, target Spark SQL/Delta,
  PyArrow and current Iceberg gates. Older profiles run in the nightly/label/
  manual tiers. Spark 3.4 SQL is opt-in/manual.
- A PR targeting `branch-N.M` requests all three tiers, still through path
  filters. A direct push to `branch-1.2` does **not** start all 138 jobs. Manual
  **Comet CI** dispatch on the selected ref requests the complete matrix.
- Main-only push/cache refresh and default-branch scheduled runs are separate
  inherited triggers.
- `Required Checks`, suite-inventory guards, the native-window verifier, lint,
  vendor, compatibility, Spark/Delta/SQL and other quality gates remain
  required where applicable. Audit actual outcomes rather than relying on a green
  aggregate or assuming all version-return/ignored bodies executed.

Use `dev/local-ci.sh spark 4.2 sql_core-1` for an affected stock SQL shard when
appropriate; `COMET_LOCAL_CI_HOME` selects its working location.
Inspect the runner first: preparation downloads sources and sweeps the
shared Maven repository. `SKIP_PREPARE=1` also skips installing Comet and must not
be used after a source change. Prefer focused GHA when a local full job would add
hours or duplicate the same CI check.

The target SQL workflow installs only CI-contained, wheel/hash-locked NumPy
2.3.5, pandas 2.3.3, PyArrow 22.0.0 and their four pinned dependencies via
`dev/ci/setup-spark42-python.sh`. `verify-spark42-python.py` checks Spark's own
pandas/PyArrow minimums and PySpark from the exact Spark 4.2.1 checkout, not a
second PyPI `pyspark`. Missing prerequisites must fail before discovery, rather
than silently canceling the 58 required pandas registrations. Local Python
installation is not needed for this workflow.

For a release candidate after executable/runtime changes:

1. Freeze the final source/dependencies/configuration, remove temporary CI
   bypasses and restore normal automatic coverage before the final run.
2. Execute full Comet CI plus Miri and Actions CodeQL on that exact source.
   Inspect individual passed/failed/canceled/ignored/pending counts and required
   named selectors/cohorts. Resolve every applicable deferred check.
3. Run separate manual **RC Build & Smoke** with `expected_commit` equal to the
   selected ref's full SHA. Verify the same finished JAR bytes on native AMD64
   and ARM64; record actual Java vendor/build, source, lock, ELF/ABI, loaded class
   origins and bundled native SHA256. See [RC-BUILD-SMOKE.md](RC-BUILD-SMOKE.md).
4. Download and preserve the qualified archive/JARs/receipts together, verify API
   archive digest and internal checksums, and report scope/limits before release.
   Neither a new filename nor a green earlier SHA qualifies replacement bytes.

Rerun only failed infrastructure jobs on the same SHA when possible. Preserve
actual producer attempt, runner, timestamps, steps, source and artifact identities;
retry status aliases never create positive test credit. If source changes, qualify
the resulting executable state and JARs rather than borrowing earlier results.
All canceled/ignored/unrun tests receive zero credit. Four HDFS tests and three
manual native benchmarks remain unexecuted, and
Iceberg is outside target production workload, without removing legacy CI.

## Java 25 test-oracle decisions

`dev/ci/java25-runtime.json` and `java-runtime.py` verify the exact vendor/build
and add `--enable-native-access=ALL-UNNAMED` plus
`--sun-misc-unsafe-memory-access=allow`. Keep the pin and real-runtime evidence
when updating `actions/setup-java`; v6 handles the four-component build string.
Spotless 3.0.0 (google-java-format 1.28.0) is required because the earlier formatter
invoked a removed javac API. All license/import/Scala/Java formatting checks remain.

`CometTemporalExpressionSuite` takes EST/MST/HST expectations from the JDK's
public `ZoneId.SHORT_IDS`: on the selected Java 25, original Spark resolves them
to America/Panama, America/Phoenix and Pacific/Honolulu, whereas Java 17 uses fixed
offsets. This is an independently checked Java-version oracle, not a Comet
production behavior change.

The expressions CI cohort alone sets `-XX:-OmitStackTraceInFastThrow` so hot
ArithmeticException paths retain the strict existing `long overflow` text and
stack assertions. Original Spark without Comet reproduced message elision after
warmup: all 350,000 calls still failed as required. This diagnostic flag is not a
production requirement or operator fallback. Final-JAR smoke keeps ordinary
fast-throw behavior and mandatory-error checks. Preserve original SQL inputs and
error/native assertions; do not silently accept success instead of error.

## Performance practice and retained findings

Use the existing Scala Spark Benchmark suites, Rust Criterion and TPC-H/DS
runners; see [the benchmarking guide](../source/contributor-guide/benchmarking.md).
Do not introduce a special framework, uncontrolled generator or publish step.
Check the actual plan and changed path; record before/after source, toolchain,
parameters, allocation/ownership checks and measurement limits. Existing
`CometBenchmarkBase` uses ANSI=false for its fixtures; performance runs do not
replace ANSI=true correctness. Select bounded data sizes and account for generated
files. `make benchmark-*` builds a release first; inspect targets before running.
`benchmarks/micro/run.py` may install/clone/build/publish, so do not run those side
effects without inspecting the runner.

| Area / existing tool                                                                          | Retained measurement and interpretation                                                                                                                                                                                                                                                                                                                                                               |
| --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runtime cancellation bookkeeping; Spark Benchmark and stock sort/hash-join selections         | Acquire/release bridge zero-balance 231.6→249.8 ns/pair (+7.9%, exploratory 95% interval +2.3…+13.5%); persistent balance 270.4→283.7 (+4.9%, interval crosses zero). Shared-manager 2-worker stress 445.9→545.7 (+22.4%, +5.9…+39.1%); 4-worker +13.0% interval crosses zero. SQL means −3.6…+4.1%, wide noisy intervals: neither a stable query regression nor a small upper bound was established. |
| Wide sort payload; `native/core/benches/sort_payload.rs`, retained vendor `sort_wide_payload` | Existing 4 KiB pipeline: Binary copies 411.38 μs versus BinaryView then materialization 343.89 μs, about 16.4% less time; dev/quick copy-kernel result, not full SortExec/spill/job speedup.                                                                                                                                                                                                          |
| Filtered SMJ; vendor `benches/sort_merge_join.rs`                                             | Existing `interval_Inner/4400000`: same 4.4 million candidate pairs, 98 payload columns and 400 asserted output rows; 21.609 s → 321.77 ms (about 67.16×). Quick dev direct-operator diagnostic, not a job forecast; custom temporal casts remain unhoisted.                                                                                                                                          |
| Native windows; `native/operators/benches/window.rs` and `sorted_window.rs`                   | Paired DataFusion/custom quick dev measurements: ROW_NUMBER 206.27→3.7279 ms, LEAD 348.13→6.1067 ms; whole SUM/COUNT at 1/10/1000 rows per key 3.1344 s→247.69 ms / 329.61→38 ms / 21.394→16.533 ms; suffix FIRST_VALUE IGNORE NULLS 7.9966 s→681.92 ms. Custom path made 1,347 pool calls for 2 million partitions; baseline zero calls meant absent accounting, not zero memory use.                |
| Dictionary fallback; Criterion `row_columnar`                                                 | Same 256-row/64 KiB fixture: plain 9.0696→8.6473 ms and dictionary 41.625→40.071 ms, p=0.08; no established change. Separate regressions prove buffer reuse/lifetime.                                                                                                                                                                                                                                 |
| Binary C2R copy; `CometBinaryRowCopyBenchmark`                                                | Ordinary→borrowed ns/row: 32 B 11.977→10.988; 4 KiB 203.609→69.457; 192 KiB 8102.885→3190.165. Large intermediate arrays avoided (about 3,663/175,119→45 allocated B/row); 32 B allocation slightly higher (43.5→45). This tests borrowing, not the separate instance-pool benefit or whole-query speed.                                                                                              |
| Wide shuffle read; `CometWideShuffleReadBenchmark`                                            | Four modes completed measured iterations; CPU-metadata collection through `sysctl` failed, so the complete benchmark command is **not passed**. Raw medians 307/267/307/247 ms for direct/direct+coalescing/JVM/JVM+coalescing are diagnostic only.                                                                                                                                                   |
| Conditional evaluation; `native/spark-expr/benches/conditional.rs`                            | 56 registrations/28 pairs; final ordinary strings retain the exact upstream vector kernel (quick differences −0.85…+0.80%). Correct ANSI masking has operator differences −50.36…+181.52% across cast/selectivity/NULL/AND/OR fixtures. An early 20–30× string regression was fixed by concrete implementation/type/alignment checks; guards must not be relaxed to regain unsafe eager evaluation.   |

All these local measurements used macOS ARM64 and mostly debug/dev native code,
Java 17, quick/bounded fixtures, without controlled P/E-core or thermal placement.
SQL heartbeat/process-metadata failures and drift added noise to runtime timing;
bridge-only measurements did not have that noise. The G02 bridge comparison
isolated before/after manager classes (pre-fix `9dd53f988` versus `e0388e3bc`)
with identical other code, eight fresh JVMs/four balanced pairs; concurrent
ns/pair is throughput amortized over workers, not per-call latency. The auxiliary
bridge extension used Spark's existing Benchmark/TaskMemoryTestUtils and was not
added as a new project performance framework.

Each Spark task owns its manager lock and fair_unified already serializes native
acquisition in Rust; frequent tiny reservations and greedy_unified parallel
workers remain conditions to watch. These microbenchmarks do not establish a
bound on whole-job impact. Retain the cancellation safety fix.
No optimized dualarch production GCP speedup, slowdown percentage or universal
no-regression claim has been established. Real workload measurements remain useful
when production issues are investigated.

Aggregate `aggregate_misc_pair/max_by` had only three debug diagnostic timings
(40.653/40.829/41.534 ms), not a before/after comparison; the original FIRST/LAST
benchmark was restored and compiles. Cost-model coefficients/calibration were
preserved without recalibration; its new standard benchmark platform query was
not completed because its `sysctl` platform query failed, so no successful G09
full benchmark is claimed.
Keep those limits when interpreting the cost model or planning future measurements.
