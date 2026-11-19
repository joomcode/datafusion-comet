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

> **Final source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

> **G01 finalization authorized:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

> **Latest completed result, 2026-11-18:** [RC Build & Smoke 44388252264](https://github.com/joomcode/datafusion-comet/actions/runs/44388252264) on `b93d553856a0339af7468bcff2070e735a1a3c71` — **SUCCESS,7/7jobs;12/12packaged smoke cases on AMD64 and ARM64**. Artifacts were downloaded and locally verified. The reports below preserve historical stages; see “Manual RC Build & Smoke” and `G01-results.json#rc_build_smoke` for this completed qualification. Earlier merge/tag holds are historical and are superseded by the current conditional finalization authorization.

# G01: results and review proposal

Status: **G01 release JARs built, checked on both architectures and downloaded; final English documentation/history cleanup and exact-SHA RC qualification in progress**.
The pre-integration `branch-1.2` is `9691385f35e4c5bd29514dca78d2044dd20e4bed`; G02 has not started.
Planned candidate: `joom-1.2-1-rc1` (tag not yet created; finalization must bind it to the qualified final source). Historical artifacts were built from `2b8f80b3f253559bc999f73769b05c88ebcd5fc2` and are not presented as the final RC of the new PR.
Apache base: `93cc6f9d71136bd0d87f77dd901989144d8c09fb`.

## Proposed contents for branch-1.2

1. Exact Spark4.2.1 / Scala2.13.18 / Java17 and Delta4.4.1 profile; CI and `dev/diffs/4.2.1.diff` generated from the Spark fixture.
2. Native Delta using the upstream scanner: GCP ADC whitelist, planning partitioning without scans/scalar-subquery execution, transient driver payloads during serialization, DPP/empty-scan regression. Hadoop encryption-context admission guards preserve fallback and do not change ordinary GCS behavior.
3. Pristine published `datafusion-physical-plan55.2.0` in a separate commit, root patch, shared Cargo.lock/workspace, explicit vendor test target. Runtime DataFusion55.2 / Arrow-Parquet59.3.2.
4. Test-only corrections to four vendor IPC statistics assertions for actual Arrow59.3.2 sizes; no DataFusion algorithms changed in G01. Correct test-UDF library lookup for the new Cargo1.100 layout.
5. Release Dockerfile/build/publish/validator for Linux ARM64 and AMD64 x86-64-v3, GLIBC<=2.31; checks for version/graph/source-and-lock cleanliness, Git SHA, ELF, two bundled `.so` files, Delta SPI and SHA256. Optional Maven flags now work with Bash3.2.
6. CI inventory excludes generated Spark fixtures in target; explicit vendor tests and exact Spark4.2.1 are included in CI.
7. Documentation and decisions only in `docs/joom-migration`.

## Substantive commits at the earlier qualification stage

```text
ea4b3228e spark: establish Joom Spark 4.2.1 and Delta baseline
0a76d68a3 native: vendor pristine datafusion-physical-plan 55.2.0
15a466a09 native: test vendored DataFusion in the Comet workspace
857d2de1f ci: exclude generated Spark trees from suite inventory
73efe84b4 release: build and validate Joom Comet for Spark 4.2.1
13cfe918c docs: preserve approved migration plan and G01 execution record
d725b298e test: import shuffle exchange in Spark 4.2.1 fixture
2b8f80b3f release: handle optional Maven flags in macOS Bash
```

Service planning/checkpoint commits were combined into one documentation commit before that build. Full original commits are retained in local backup ref `refs/joom-migration/checkpoints/20261117-before-service-cleanup`; old/new SHA mapping is in `G01-results.json`. The pristine vendor import remained separate: `0a76d68a3`; workspace integration: `15a466a09`.

## Checks at the earlier local qualification stage

| Check                                 | Result                                                                                                                         |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Native workspace                      | 4326 passed,0 failed,5 ignored; includes1695 vendor tests                                                                      |
| Vendor statistics with test_utils     | 42 passed                                                                                                                      |
| Full Delta                            | 291 passed                                                                                                                     |
| ANSI/runtime                          | 75 passed,3 previous upstream cancellations (#4967)                                                                            |
| Spark4.2.1 SQL                        | MathFunctions62 + DPP AQE on2/off1 =65 passed                                                                                  |
| Packaged Linux JARs                   | ARM64 12/12; AMD64:2 Delta cases passed, then stalled. FORK also did not finish within300s. Remaining AMD64 cases were not run |
| Release archive/provenance            | ELF,GLIBC<=2.31,Git SHA,lock,versions,two `.so` files,Delta SPI,SHA256 passed                                                  |
| Negative validator/publisher fixtures | 19 passed; no real publication                                                                                                 |
| Bash3.2 regression                    | Online/offline flags passed; original failure reproduced                                                                       |
| Formatting/lint                       | Cargo fmt and strict Comet Clippy passed; pristine-vendor deprecation warnings documented                                      |

Five ignored Rust tests:4 require an HDFS cluster,1 is an existing benchmark. Unit/Delta/ANSI checks ran before the history squash; their runtime source was preserved. Subsequent changes concerned only the test-fixture import and release Bash helper. A new full-classpath Spark compile and65 SQL tests passed after the fixture fix.

The SHA of our installed Joom JAR was checked on the Spark SQL classpath. Packaged smoke puts the final JARs first, excludes other Comet/main classes, checks NativeBase/Delta-scan class origins and loads bundled `.so`; network none, only local fixtures/loopback. It checks complete results, expected ANSI errors and actual native execution.

## Historical artifacts

Directory: `target/joom-release/joom-1.2-1-rc1`.

| File                                        | SHA256                                                             |
| ------------------------------------------- | ------------------------------------------------------------------ |
| comet-spark-spark4.2_2.13-1.2.0.jar         | `40182026a96ccbf4958d2f741d19268e518173ff2960f937f1d3b700172defff` |
| comet-contrib-delta-spark4.2_2.13-1.2.0.jar | `0c50187c4624b46b31a47326b7b281d6cde96d1b458697845d74debc0b07ae8c` |

Alongside them: `sha256.txt`, `commit.txt`, `build-manifest.json`, `effective-pom.xml`, both `.so` files, actual toolchain/Cargo metadata/ELF imports and `smoke/` evidence. The internal JAR base version is1.2.0; the Joom Maven publication version corresponds to the tag. No push/JFrog/production operation occurred at this stage.

Measured native build: ARM64 5m31s; AMD64 12m26s. After the Bash-only fix: ARM cache hit1.39s, AMD partial rebuild9m26s. These are Docker build measurements on this Mac, not Spark-job performance.

## Boundaries of the historical result

**Blocker to full qualification at that stage:** standard AMD64 smoke under Rosetta stopped after2 Delta tests. Almost all JVM threads froze in Rosetta rt_mutex_schedule; child jspawnhelper awaited a handshake; jcmd/SIGQUIT produced no dump. FORK also failed to finish. A simple Java-only probe with100 spawns passed; that did not prove absence of Comet bugs. The user reported that only a Mac was available and requested the result with that limitation. No additional workarounds were attempted; reliable closure required a native AMD64 runner. Details/logs are in G01-results.json and raw evidence. RC1 was not tagged, and actual publisher validate-only using that tag did not run.

The candidate contains G01 scope. Remaining agreed capabilities belong to G02–G09, including full ANSI qualification P03/I10. Do not treat RC1 as the completed replacement for `joom-1.1-13`.

Actual GCS/GKE ADC reads and production performance were not checked. Full BoundaryFormats deadlock reproduction remains G09; query interning/task-binary regression belongs to G08. The three upstream generic ANSI cancellations were disclosed; typed mandatory-overflow checks passed, including ARM64 packaged tests; AMD64 ANSI smoke had not run at that stage. Spark teardown reported possible daemon-thread-leak warnings; selected tests passed and later-group lifecycle checks remain required.

## Preview and integration sequence at that stage

Full diff and separate implementation/vendor previews are in `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-final-qualification`. Tree comparison: `git diff branch-1.2 joom/1.2-dev`; vendor residual: `git diff 0a76d68a3 joom/1.2-dev -- native/vendor/datafusion-physical-plan`.

The feature branch resolved add/add conflicts introduced by history cleanup: branch-1.2 became its ancestor without changing branch-1.2 itself. The entire file tree and24 documents matched the pre-alignment candidate; seven substantive patches and a separate vendor import were preserved. The PR could then be prepared without moving or overwriting the base ref. **The historical integration gate required GHA/review and direct user confirmation; the user has now authorized integration after finalization checks.**

The user first reviews the composition and diff. G02 still requires a separate direct authorization even after G01 integration.

Full machine-readable results: `G01-results.json`. Raw logs and auxiliary scripts: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-final-qualification` and previous `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-resumed`. Cleanup: `CLEANUP.md`; new-file list: `created-files.txt`.

## First native AMD64 GHA result for PR28

[Run44137489889, attempt2](https://github.com/joomcode/datafusion-comet/actions/runs/44137489889) ended in **FAILURE** on `db2bd16584c1381853cc170abab7cd87f289c85c`. 117 jobs passed,9 independently failed, Required Checks also failed,17 skipped. No jobs remained running; five-minute monitoring ended when the run completed.

Logs confirmed2639 passed Comet native tests (5 previous skips),1695 passed vendor tests without skips,42 additional passed statistics tests; Delta/Spark4.2.1 —294 passed without failures/cancellations. Comet scans/shuffle/exec and both macOS suites passed. The previously canceled ANSI add/subtract/multiply **actually passed** on AMD64. Comet expressions:2284 passed,2 failed,2 canceled,12 ignored. SparkSQL4.2.1:6 shards passed,3 failed.

Groups unresolved at that stage:

- **Correctness priority — SPARK-60262:** two Parquet tests with ambiguous fieldID/name mapping failed to produce mandatory `PARQUET_FIELD_REQUESTED_MORE_THAN_ONCE`. Spark added the guard in commit `59f657f2b059b28b0c5df2e5c6e28a35ebd25369`; no equivalent was found in Comet's independent field resolution. Returned rows were not checked in those negative tests, so absence of silent incorrect results was not established. Protection/fallback and both regressions were required, not test exclusion.
- **ANSI casts:** Spark4.2.1/SPARK-60119 (`eaed2f091c8dd4f86e92a7a87a6a28f424110897`) rounds extremely small `1e-2147483647` to zero. One test expected an obsolete literal exception; native string→decimal with negative scale still returned an error instead of successful zero. Result/error parity and adaptation were required, not cancellation.
- **SparkSQL core-1:**33 operator/fusion/partitioning assertions expected Spark-specific plans. Some preceding data checks passed;12 Union cases and1 DataFrame case failed before subsequent result checks. Simply disabling33 tests was unacceptable.
- **Other test-compatibility cases:**3 corrupted-Parquet diagnostic mismatches (a read error did occur); one synthetic DV-fixture failure each on Spark3.5/4.0 (Delta serializer assertion; target Spark4.2.1 passed).
- **Dependency infrastructure:**3 jobs failed on Maven Central HTTP429 before tests; SparkSQL core-2 `SPARK-33084: Add jar support Ivy URI in SQL` could not resolve `spring-core:6.1.6` through Ivy. The Ivy log did not state an HTTP code.

Full diagnostics and exact tests: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-runner-fix/gha-final-summary.json` and JSON/log files listed there. About43MiB were saved in the task directory and recorded in the cleanup ledger. Sources and Git refs did not change during monitoring. Final documents were saved locally without another commit/push to avoid starting CI again. `branch-1.2`/RC1/G02 were unchanged. CI-profile execution did not replace final release-JAR smoke at RC checkpoints.

## Corrections after the first full GHA

Corrected source package: `6ae961dcbae949db09e349b05c91fc1cd6464d87`. At this historical stage, a full repeat GHA had not started. The nine independent failures of the preceding run were analyzed; changes were matched to exact Spark code and independently reviewed.

- Decimal casts match SPARK-60119 through a behavioral serde flag with exact patch-version boundaries; older Spark retains its old behavior.
- Native Parquet rejects ambiguous projected-field mapping before reading data. Pruning, exact physical-name keys, nested-error order and Spark error envelope/cause/parameters are preserved.
- UNION with a known layout can no longer silently use concat when partition counts are wrong; an empty layout preserves partitions and a zero result for COUNT-like consumers.
- Spark-specific assertions remain on the Spark path; separate native counterparts check complete results, actual native operators, layout and extension observers. Hooks account for an already converted native projection. Corrupt Parquet checks both reader paths and requires an error.
- Only the synthetic Delta DV fixture changed: an explicit bitmap offset. Maven parent-POM resolution retries are bounded and occur only before compilation/tests; checks themselves are neither retried nor weakened. ADD JAR uses a local Ivy fixture preserving transitive/exclude and class-loading assertions.

| Targeted check                                                   | Result                                                                             |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Rust parser / tiny values / field resolver / error serialization | 5 + 2 + 10 + 1 passed                                                              |
| Comet JVM                                                        | 30 passed; after envelope-converter fix,19/19 rerun                                |
| Spark SQL                                                        | 223 passed,0 failed;3 previous ignored Union cases                                 |
| Delta changed fixture                                            | 1/1 passed                                                                         |
| Maven retry fault injection                                      | 4/4 passed                                                                         |
| Native build, Maven install, format, CI config                   | passed                                                                             |
| Spark patch                                                      | regenerated from v4.2.1; forward/reverse apply and equality of all121 files passed |

Early local compilation/assertion failures are preserved in logs; the table reflects the final successful checks of those scenarios. No correctness checks were disabled. Full Rust/vendor/Comet/Delta/Spark SQL suites were delegated to the upcoming GHA. Local results do not prove the complete absence of bugs.

No new release build/smoke or RC1 tag occurred at that stage. Earlier JARs referred to the old source SHA and were not final artifacts of the corrected PR. `branch-1.2` remained unchanged; G02 had not started. Raw evidence: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes`; machine summary: `G01-results.json#ci_corrections`.

## Follow-up after run44180095887

[Full GHA on ac3694504](https://github.com/joomcode/datafusion-comet/actions/runs/44180095887) ended in **FAILURE:124success/3failure/17skipped**. All nine original failed jobs passed, including9/9 Spark4.2.1 SQL shards. Selected lines of full logs confirmed the original33 SQL failures,21 native Union and9 native-extension counterparts,3 SPARK-60262 and6 SPARK-59832, both SPARK-33084. The expression artifact confirmed3 ANSI arithmetic and2 decimal-cast cases; total2287passed,2 previous cancellations,12ignored. Canceled/ignored cases were not reported as passes. Native2649passed/5skipped; vendor1695passed; statistics42; Delta4.2 —294passed without cancellations.

The run discovered two additional issues:10 reader assertions expecting the old exception class after early footer validation and **incorrect Union/AQE results on Spark4.1** when the configuration changed:10 rows `(k,4)` instead of5 `(k,8)`. The third failure was the Required Checks aggregator. Those verdicts apply only to the old SHA, not to the following correction.

New source commit `30b6c4d7f531fad7b6550c5d3f1c92cefd096e58` contains:

- Immutable policy in CometUnion constructor state; AQE/case-class copy preserves the planning choice. Layout is derived from the children of the specific copy; per-instance laziness and early evaluation before starting children are retained, and incompatible RDD counts still fail fast. Snapshot-capable Spark4.2.1 continues to manage policy itself; legacy capability is detected by presence of `UnionConfSnapshot`, accounting for4.2.0 and backports.
- Correct partitioning namespace after child reordering/replacement: remap Spark layout to fixed Comet output. A new strict test found this case; `references.subsetOf(outputSet)` remains asserted.
- Reader checks require the exact structured file-read error, actual file URI and direct native cause. Native-plan checks, mandatory rejection, diagnostics and absence of INTERNAL_ERROR remain. Older Spark retains original CometNativeException; production code was not changed merely for this assertion. `ignoreCorruptFiles`/`ignoreMissingFiles` still exclude full native scan; no new silent-skipping path was added.

The local negative control reproduced wrong groups before the fix. Afterwards: Spark4.1 union12passed; Spark4.2 union12passed; Spark3.5/Scala2.12 legacy controls35passed; reader42 —27passed; converter19passed; SparkSQL with the newly installed JAR —21 native Union+9 extension+3 field guards, all passed without cancellations/ignored. Coverage includes both directions of configuration change, AQE on/off, Hash/Single, copy/withNewChildren, live child-layout replacement, late conversion, complete data and RDD counts. Maven install/format and independent review passed. Rust/vendor did not change in this follow-up.

A new full GHA was pending a single follow-up push at that stage. Source-commit files matched the locally tested worktree byte for byte (`followup-source-commit-verification.json`). Evidence of the first corrected run is frozen in `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes`; the next run is stored separately in `20261117_followup-gha`. Old Linux release JARs had not been updated and RC1 had not been created; merge/base/G02 were then subject to the direct user gate.

## Final full GHA on the qualified runtime-source PR HEAD

[Run44205897075,attempt3](https://github.com/joomcode/datafusion-comet/actions/runs/44205897075) completed **SUCCESS** on `26590264b878fb8eae2ca439a8bcc0dfb513cb36`: **127successful/0failed/17conditional-skipped jobs**, Required Checks passed. The verdict applies to those sources, not the historical release JARs.

The first attempt had only8 Maven parent-POM HTTP429 failures before compilation/tests,2 dependent coverage failures and the aggregator. A selective rerun preserved116successful executions; after the second attempt, only Spark3.4 expressions HTTP429 and the aggregator remained. The third selective rerun preserved125successful executions and completed the remaining2. Timestamp equality was confirmed; no full-pipeline rerun, new commits or weaker checks were involved. A temporary502 jobs API error was handled using the standard read-only check-runs API; records from other workflows were excluded from the count.

On that SHA, confirmed:9/9 Spark4.2.1 SQL shards,2649native tests (5 previous ignored),1695vendor tests,42statistics tests and294Delta tests without cancellations. The expression artifact contains2287passed,0failed,2 previous canceled,12ignored; **the three ANSI add/subtract/multiply and both decimal-cast cases actually passed**. All10 duplicate-reader cases passed; both Union regression tests actually passed on4.1 and4.2. Canceled/ignored cases are not represented as successful tests; full ANSI qualification in later groups remains planned.

This closes the native AMD64 source/runtime GHA gap for G01. **Smoke of the exact bytes of final release JARs remains separate RC acceptance.** Previous dualarch JARs from source2b8f80b3f lack the latest corrections; no new release build/RC1 tag had occurred at this stage. GCS/GKE production reads and production performance are not qualified.

PR28 was updated with the result without pushing another commit; local progress documents were saved without another GHA. `branch-1.2` remained `9691385f35e4c5bd29514dca78d2044dd20e4bed`; no merge/RC1/G02 occurred. The next step then was user review.

Evidence: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes/20261117_followup-gha`, including immutable attempt1/2/3 snapshots, failure classifications and `java-artifact-*-verification.json`. The cleanup ledger records final size (~21.4MiB auxiliary;39.7GiB total existing native target;829MiB Spark fixture). Nothing was automatically removed.

## Manual RC Build & Smoke

Qualified source before finalization: `b93d553856a0339af7468bcff2070e735a1a3c71`. [Run44388252264](https://github.com/joomcode/datafusion-comet/actions/runs/44388252264) completed SUCCESS: **7/7 jobs**, zero failed/skipped.

Native AMD64 and ARM64 were built on native runners with the release profile. Final Comet and Delta JARs were packaged once. Each architecture actually passed **12/12 cases**: Delta4, DPP1, reload1, ANSI6. Checks verified Spark4.2.1, Java17, final-JAR class origins and SHA256 of the bundled `.so` actually loaded. The final job matched both receipts to identical source/lock/JAR/package/support/native hashes.

Artifacts were downloaded to `target/joom-release/gha-44388252264`. ZIP SHA256 matched GitHub: `1c633ecec930ce8a2e4f95a2fe859ddefcd89f3a2898fc77105775448ad6035b`. Local checks verified checksums, embedded Git revision, versions, ELF/dependency graph, provenance and both receipts.

| Runtime JAR                                 | SHA256                                                             |
| ------------------------------------------- | ------------------------------------------------------------------ |
| comet-spark-spark4.2_2.13-1.2.0.jar         | `9f7d57bfb88efceeecce15d96ca320a2d38f81aacc95f4bbf67d46915bfc0686` |
| comet-contrib-delta-spark4.2_2.13-1.2.0.jar | `5581281ff5bf826f04a038318d802be13d9f66cb8084e3d8a40846b5175bd632` |

Workflow preparation corrected three observed failures: an ambiguous negative fixture, a separate Maven classpath invocation without reactor artifacts on clean M2, and Git2.25.1 ignoring command-scope safe.directory. The latter was reproduced in the same builder with UID1001 and fixed using configuration trusting only the checkout inside the container. No failure was hidden by weakened checks or blind retry.

The workflow was registered in main by adding only its YAML file under separate user authorization. Branch-1.2 did not change. Automatic Comet CI runs were stopped as instructed; one delayed run finished on two missing license headers before it could be stopped. Both were fixed; Apache RAT for exact source b93 passed. Runtime code matches previously qualified 26590264b. The new workflow adds release-profile and packaged-JAR checks to the prior full CI qualification.

Old JARs from2b8f80b3f remain historical. The native AMD64 packaged-smoke gap is closed for the newer JARs. RC1 tagging, JFrog publication, merge and G02 had not occurred at the end of that run. This is a G01 candidate; remaining Joom capabilities belong to G02–G09. Guide: `RC-BUILD-SMOKE.md`; cleanup: `CLEANUP.md`. The user has subsequently authorized G01 finalization and conditional integration/RC1 tagging; publication and G02 remain excluded.
