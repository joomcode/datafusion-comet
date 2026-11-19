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

# G02: native runtime migration — accepted and integrated

Branch: `joom/1.2-native-runtime`, integrated into `branch-1.2`. Previous base: `c04d2d03c2f572f99e3767f3c8a65f5294ca5328` (`joom-1.2-1-rc1`). Source contracts: `joom-1.1-13`. The user accepted the performance interpretation and authorized integration on 2026-11-19. The final three-commit identity, local/remote refs and post-merge proof are recorded in `${MIGRATION_AUX_ROOT}/20261119_comet12-g02-integration/completion.json`. This source snapshot records stage acceptance; integration execution is a post-commit gate. G03 has not started.

## Delivered scope

| Card           | Result                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| J14            | Ported shared OffsetBudget/offset extents and adapted explode to the new operators crate. One input row can span output batches; nested repeated variable-width values stay within 32-bit offsets. Small synthetic ceilings test the boundary without multi-GB allocations. Windows/coalescer consumers remain assigned to G06/G07.                                                                                                                                  |
| J16            | Captured TaskContext at iterator construction and reused it for off-task-thread reads, metrics, task-memory and completion callbacks. The 1.2 lifecycle/classloader behavior is preserved.                                                                                                                                                                                                                                                                           |
| J22            | Explicit ownership of the current fetched block: close once before advancing, never fetch unread blocks on close, retain per-read interruption checks and terminal error safety. Tests use actual Spark/Netty refcounts.                                                                                                                                                                                                                                             |
| J15            | Added a kill watcher, Arc-only native cancellation registry, cancellation-aware operator wrappers, interruptible I/O/producer waits, shuffle write checks and periodic JVM/UDF/row-reader checks. Cancellation is an error, never successful EOF. Preserve upstream synchronous stream drop and memory-release accounting. Scoped interruption now unblocks killed-task Spark JNI memory waits; a post-JNI kill check rejects batch/EOF races with safe FFI cleanup. |
| I05            | Retained upstream recursive FFI normalization, byte-identical to the old Joom tag. Restored the missing two-level nullable-struct sliced-boolean regression and exercised the existing sliced-UDF path.                                                                                                                                                                                                                                                              |
| I06            | Retained upstream UTC producer, CASE/coalesce and date_trunc implementations with Spark 4.2 overflow behavior. Preserved Joom mixed-producer comparisons, timezone aliases, nulls, NTZ, DST and overflow regressions. These tests do not require the old broad comparison-side cast; safe default dispatch remains intact.                                                                                                                                           |
| C01 (G02 part) | Executed focused ANSI arithmetic and numeric-cast boundaries. Removed the stale unary-negation Spark 4.2 cancellation, required MIN negation to fail on integral scalar/array paths and MAX scalar negation to succeed; retained ANSI=false array parity. Spark 4.2 canonicalizes both errors; assert exact ARITHMETIC_OVERFLOW, SQLSTATE 22003 and message parameter `overflow`. SUM/AVG and evaluation-order scope remain G04/G08.                                 |

The cancellation wrapper forwards DataFusion 55.2 properties, downcasts, statistics and optimizer hooks. The shuffle-offset lookup is delegate-aware, and a native test checks both bare and wrapped writers. DataFusion 55.2.0, Arrow/Parquet 59.3.2, Cargo.lock and vendor production code are unchanged.

## Local validation

Java 17 / Spark 4.2.1 / macOS ARM64. ANSI=true is explicit in new value/lifecycle fixtures; timezone and boundary suites also compare ANSI=false deliberately.

| Check                                                               | Final result                                                                       |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Native build                                                        | Passed (dev library)                                                               |
| Common / offsets / FFI / cancellation                               | 81 passed                                                                          |
| Explode                                                             | 22 passed                                                                          |
| JNI lifecycle, cancellation, producer stop and reservations         | 43 passed                                                                          |
| Cancellable wrapper behavior and rewrites                           | 4 passed                                                                           |
| Shuffle partition batches                                           | 7 passed                                                                           |
| Native total                                                        | **157 passed, 0 failed/ignored**                                                   |
| Unified Maven reactor, focused JVM/SQL selection                    | **93 passed, 0 failed/canceled/ignored**                                           |
| Strict Clippy                                                       | Passed for all four modified crates, all targets, no dependencies, warnings denied |
| Cargo fmt / Maven Spotless / CI configuration and suite inventories | Passed                                                                             |

The JVM selection includes all new suites, four actual non-interrupting task-kill scenarios, AQE/native/JVM off-thread shuffle, values/metrics/thread-local checks, real block refcounts, sliced nested FFI/UDF export, timezone/ANSI SQL dimensions, Spark 4.2 truncation overflow, arithmetic errors and eight numeric-cast boundaries. The actual test-resource library SHA matches the freshly built dylib. New suites are registered in normal Linux and macOS CI inventories; automatic triggers and checks are not disabled.

Initial failures were retained in the evidence: a wrapper-unaware shuffle downcast (fixed with a targeted regression), an off-thread metrics fixture inspecting Dataset.rdd's sibling QueryExecution (fixed to execute the inspected plan, retaining positive assertions), and obsolete unary-overflow text expectations (replaced by stronger version-correct structured checks). The final complete selected batch passed all 93 cases. No test was ignored or canceled to obtain this result.

## Spark JNI memory-wait cancellation: closed locally

The original validation gap exposed a real hang: a native poll waiting in Spark acquireExecutionMemory cannot be preempted by a Tokio abort/token alone. A bounded negative control disables only the new scoped interruption and reproduces stuck cancellation/close in both JNI paths. The production method is automatically restored and its hash verified afterwards. No control switch is committed.

The fix registers only threads actively acquiring memory for the task and serializes their interruption with registration/outermost exit. Native cancellation is set first. The lock is never held while entering Spark; killed-task close also cancels before synchronously stopping the producer. Successful racing grants remain accounted, and partial grants stranded by an interrupted sibling spill are left to normal Spark task cleanup, without unsafe task-wide delta refunds. Worker reuse and pre-existing interrupt state are tested.

Four real-JNI tests use a real isolated 100-byte Spark pool, verify native-producer/JVM-fed execution and actual fair/greedy configuration, keep a competitor holding 90 bytes, and require TaskKilledException plus completed close within two seconds and zero subject reservations/live plans. Two Scan-only tests kill inside the JNI input callback and reject successful batch/EOF returns while checking Arrow/FFI cleanup. All 16 manager tests, including six added regressions, pass. The combined final selection is 93/93, with zero failed/canceled/ignored cases. Final Linux/release qualification remains separately required.

## Explicitly unverified / later work

- **G02-LINUX-AMD64-RELEASE:** native Linux AMD64 and the CI/release feature graph remain for the full agreed final GHA and final exact-JAR AMD64/ARM64 smoke. Local ARM64 debug success does not replace them.
- **G02-OFFSET-CONSUMERS:** window concat and shuffle-coalescer adoption and combined tests follow in their explicitly authorized G06/G07 ports.

These entries, owners and required evidence are in STATE.json#validation_backlog. The subsequent user-authorized bookkeeping performance comparison is recorded in G02-performance.md. It uses the existing benchmark framework and reports a measured bridge cost with limited SQL conclusions; production performance is not qualified. Before production, complete all agreed feature work, clear applicable validation gaps, remove every temporary CI mechanism, restore automatic full CI and qualify the final state/artifacts. Production use remains unauthorized.

## Evidence, environment and history

Machine-readable results: `G02-results.json`. Raw commands/logs, source hashes, reviews and earlier failed reports: `${MIGRATION_AUX_ROOT}/20261119_comet12-g02`. Final follow-up test reports, negative-control proof, source hashes, reviewed diff and push receipt: `${MIGRATION_AUX_ROOT}/20261119_comet12-g02/20261119_memory-wait`. Final commit identities are recorded in the follow-up directory's `completion.json` after coherent commits are created; no extra service commit is required merely to quote its own SHA.

Use the installed direct stable Rust toolchain in PATH, RUSTUP_TOOLCHAIN=stable and both DYLD library variables pointing at Java17 lib/server for native tests. The installed Homebrew protoc has a missing dynamic dependency; per-command PROTOC selects the existing official G01 ProtocBridge-cache 33.5 binary. No host tool/configuration changes, unsandboxed commands or replacement caches were needed. The existing pristine-vendor deprecation warning remains documented and unchanged.

All migration execution documentation stays under docs/joom-migration. The earlier 2026-11-19 naming, repository-boundary and validation-policy decisions accompany the substantive G02 history. No full per-group GHA, new release tag, main change, artifact publication, production operation or datasci access was performed. Only the user-approved G02 branch integration/push is added at closeout; actual refs are recorded in the integration receipt. Cleanup locations and shared-cache exclusions are maintained in CLEANUP.md and cleanup-ledger.json.

## Accepted performance and integration closeout

The user accepted the ordinary-batch fractional-percent/generally-below1% expectation as engineering judgement, not a measured production guarantee. Current microbenchmark overhead alone does not require optimization or block G02 integration. All measured bridge costs and inconclusive SQL/platform limitations remain in G02-performance.md and decisions.json#g02_performance_acceptance.

Service/performance/handoff documentation is folded into the third substantive commit. Runtime/test sources and native library bytes match the candidate that passed157 native and93 JVM/SQL cases. Consequently, documentation-only consolidation and a tree-identical fast-forward do not require repeating those builds/tests. The final source tarball still receives Apache RAT and documentation/integrity checks. Exact results and final commit names are in the post-commit integration receipt. RC1 is preserved, no G02 tag or intermediate release is created, and G03 requires separate explicit authorization.
