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

> **Current G10 authorization, 2026-11-21:** The user explicitly started `joom/1.2-release-qualification` from accepted G09 `4467d0337`. Restore all temporary CI bypasses and full normal automatic quality/test coverage, close applicable remaining gaps, run full agreed GHA and exact-SHA RC Build & Smoke on native AMD64/ARM64, and download/verify the final JARs. Report before merge or stable tag. Feature publication and workflow control are authorized; branch-1.2/main/tag changes, external artifact publication, production and datasci remain excluded. Earlier G10 holds below are historical. See STATE.json and decisions.json#g10_authorization.

> **Current G09 acceptance, 2026-11-21:** The user approved publishing G09 and integrating it into `branch-1.2`. The two-commit fast-forward and exact local/remote/source/document verification are recorded after commit in `${MIGRATION_AUX_ROOT}/20261121_comet12-g09-integration/completion.json`. All29 qualified non-migration files remain unchanged from `00f5c17bc`. Earlier G09 review holds below are historical. G10, full per-group GHA, main/tag/release changes, production and datasci remain unauthorized; final full agreed CI and exact final-JAR AMD64/ARM64 qualification remain mandatory.

> **Current G09 review gate, 2026-11-21:** I07/J01/J02/J03/J04 are locally qualified on `joom/1.2-cost-based-engines` from `2dc13d5bd`. See G09-report.md/G09-results.json:230 native and495unique JVM/SQL/Delta cases passed, with all selected failures resolved. Await explicit push/integration approval; branch-1.2/main/RC1 are unchanged. G10, full per-group GHA, release/publication, production and datasci remain unauthorized. Final full agreed CI and exact final-JAR AMD64/ARM64 qualification remain mandatory. Earlier stage banners are historical.

> **Current G09 authorization, 2026-11-21:** G01-G08 are integrated. The user explicitly started G09 on `joom/1.2-cost-based-engines` from `2dc13d5bd`: I07/J01/J02/J03/J04 as one coherent block, preserving the final tag13 costs and accepted correctness fallbacks. Use local builds and targeted Spark4.2.1 result/error/native-coverage tests; report before merge. G10, push/merge before review, full per-group GHA, main/tag/release changes, production and datasci remain unauthorized. Earlier G09 holds below are historical. See STATE.json and decisions.json#g09_authorization.

> **Current G08 acceptance, 2026-11-21:** The user approved retaining the safe fallbacks, publishing G08 and integrating it into `branch-1.2`. The one-commit fast-forward and exact local/remote/source/document verification are recorded after commit in `${MIGRATION_AUX_ROOT}/20261121_comet12-g08-integration/completion.json`. All58 qualified non-migration files remain unchanged from `2625e2c8e`. Earlier G08 review holds below are historical. G09-G10, full per-group GHA, main/tag/release changes, production and datasci remain unauthorized; final agreed qualification remains mandatory.

> **Current G08 review gate, 2026-11-20:** J05/J06/J07/J17 and necessary C02 changes are locally qualified on `joom/1.2-jvm-codegen` from accepted G07 `1a2734992`. See G08-report.md/G08-results.json and STATE.json. Await explicit push/integration approval; branch-1.2, main and RC1 are unchanged. G09, full per-group GHA, production and datasci remain unauthorized. Earlier authorization banners below are historical; final full CI/release qualification remains mandatory.

> **Current G08 authorization, 2026-11-20:** The user explicitly started approved G08 on `joom/1.2-jvm-codegen` from accepted G07 `1a2734992`. Implement J05/J06/J07/J17 and necessary C02 evaluation-order fixes with local builds and targeted original-Spark correctness checks. Report before push/integration. G09-G10, full per-group GHA, main/tag/release changes, production and datasci access remain unauthorized. Earlier G08 holds below are historical. See STATE.json and decisions.json#g08_authorization.

> **Current G07 acceptance, 2026-11-20:** The user explicitly approved G07 push/integration into `branch-1.2`. The three-commit fast-forward and exact local/remote/tree verification are recorded after commit in `${MIGRATION_AUX_ROOT}/20261120_comet12-g07-integration/completion.json`. Qualified non-migration source remains unchanged from `62e9ffd7a`. Earlier G07 review holds below are historical. G08-G10, full per-group GHA, main/tag/release changes, production and datasci access remain unauthorized; final agreed qualification/backlogs remain mandatory.

> **Current G07 review gate, 2026-11-20:** G07 is locally implemented and qualified on `joom/1.2-shuffle-and-c2r` from accepted G06 `0028887b3`. See G07-report.md, G07-results.json and STATE.json. Await user review before push/integration; `branch-1.2`, main and RC1 are unchanged. G08-G10, full per-group GHA, release/publication, production and datasci work remain unauthorized. Final Linux/release/JAR and G09 cost-model integration coverage remain required. Earlier G07 authorization below is historical.

> **Current G07 authorization, 2026-11-20:** G01-G06 are integrated. The user explicitly started the approved G07 cards on `joom/1.2-shuffle-and-c2r` from `0028887b3`. Use local builds and targeted original-Spark correctness checks; report before push/integration. G08-G10, full per-group GHA, main/tag changes, release/publication, production and datasci work remain unauthorized. Earlier G07 holds below are historical. See STATE.json and decisions.json#g07_authorization.

> **Current G06 acceptance, 2026-11-20:** The user accepted keeping the current correctness fallbacks and requested completion after review. G06 is complete; its two-commit fast-forward and actual local/remote/tree preservation proof are recorded after commit in `${MIGRATION_AUX_ROOT}/20261120_comet12-g06-integration/completion.json`. Qualified non-migration source is unchanged from `2aeca4df3`. Earlier G06 push/integration holds below are historical. G07-G10, full per-group GHA, main/tag updates, artifact publication, production and datasci work remain unauthorized. Final full Linux/release/JAR qualification remains mandatory.

> **Current G06 review gate, 2026-11-20:** N01/N02/N03 are implemented and locally qualified in `joom/1.2-native-windows` from accepted G05 `72d40726e`. See G06-report.md/G06-results.json and STATE.json. Await user review before push/integration; `branch-1.2`, main and RC1 are unchanged. G07-G10, full per-group GHA, main/tag changes, publication, production and datasci work remain unauthorized. Final full Linux/release/JAR qualification is mandatory. Earlier G06 implementation banners below are historical.

> **Current G06 authorization, 2026-11-20:** G01-G05 are complete and integrated. The user explicitly started N01/N02/N03 on `joom/1.2-native-windows` from `72d40726e`. Use local builds/targeted correctness checks and report before push/integration. G07-G10, full per-group GHA, main/tag changes, publication, production and datasci access remain unauthorized. Earlier G06 holds below are historical. See STATE.json and decisions.json#g06_authorization.

> **Current G05 acceptance, 2026-11-20:** The user approved publishing G05 and integrating it into `branch-1.2`. Actual fast-forward/local/remote/tree verification is recorded after commit in `${MIGRATION_AUX_ROOT}/20261120_comet12-g05-integration/completion.json`. The qualified non-migration source is unchanged from `5448f369a`; earlier G05 push/integration holds below are historical. G06-G10, full per-group GHA, main/tag changes, artifact publication, production and datasci work remain unauthorized. Final full-fuzz/Linux/release/JAR qualification remains mandatory.

> **Historical G05 review gate, 2026-11-20:** D05 is implemented and locally qualified in `joom/1.2-smj-filter` from accepted G04 `a89fea10e`. See G05-report.md/G05-results.json and STATE.json. Report before push/integration; protected refs remain unchanged. G06-G10, full per-group GHA, main/tag changes, publication, production and datasci work remain unauthorized. Final full-fuzz/Linux/release/JAR qualification remains mandatory. Earlier G05 authorization below is historical.

> **Historical G05 implementation authorization, 2026-11-20:** G01-G04 are complete and integrated. The user explicitly started G05 on `joom/1.2-smj-filter` from `a89fea10e`. Implement D05 with local builds/targeted regressions and report before push/integration. G06-G10, full per-group GHA, main/tag changes, publication, production and datasci access remain unauthorized. Earlier G05 holds below are historical. See decisions.json#g05_authorization and STATE.json.

> **Current G04 acceptance, 2026-11-20:** G04 is complete and accepted for publication/integration into branch-1.2. Exact final local/remote refs, five commits and qualified-source preservation are recorded after integration in `${MIGRATION_AUX_ROOT}/20261120_comet12-g04-integration/completion.json`. G05 requires separate explicit authorization. Earlier G04 push/merge holds below are historical; final qualification remains required.

> **Historical G04 continuation checkpoint, 2026-11-19:** Implementation and local targeted validation are complete; see G04-report.md/G04-results.json and `${MIGRATION_AUX_ROOT}/20261119_comet12-g04/completion.json`. Memory, Partial metadata and ordering failures were corrected; regressions pass without higher budgets or weaker checks. Await user review: no push, merge, full GHA, G05 or protected-ref changes. Preserve historical checkpoints below.

> **Final source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

> **G01 finalization authorized:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

# G01 continuation checkpoint

Latest completed qualification: **RC Build & Smoke 44388252264 on b93d553856a0339af7468bcff2070e735a1a3c71 succeeded. Final JARs were downloaded and locally verified.** All 7 jobs and 12/12 packaged tests on AMD64 and ARM64 passed. Directory: `target/joom-release/gha-44388252264`. Apache RAT for source b93 passed; runtime code has not changed since full CI on 26590264b. Monitoring and builds ended. Final progress documents were saved locally without another push. The user has now authorized G01 finalization, English documentation, service-history cleanup, an exact-SHA RC rerun as needed, integration and RC1 tagging after successful checks. Earlier merge/tag holds below are historical. Publication, G02 and Java 21 work remain excluded.

- Feature `joom/1.2-dev`; PR https://github.com/joomcode/datafusion-comet/pull/28. The abbreviated historical base was 9691385f. The pre-integration base `branch-1.2` is `9691385f35e4c5bd29514dca78d2044dd20e4bed`. Verify actual refs and the current finalization state before resuming.
- Earlier run44180095887/ac3694504 ended in FAILURE:124success/3failure/17skipped. All nine original failed jobs passed; two additional cases were discovered and have since been fixed in the source above. Do not attribute an old verdict to a new source.
- Source35/Scala2.12 controls35passed;41union12passed;42union12passed; reader27passed/converter19passed; fresh-JAR SparkSQL21+9+3passed. The pre-fix negative control on41 did return incorrect groups. All source files after commit matched the tested worktree byte for byte.
- Run44205897075 completed successfully on its exact HEAD; polling ended. New auxiliary directory: `${MIGRATION_AUX_ROOT}/20261117_comet12-g01-ci-fixes/20261117_followup-gha`; check-pr-gha.py/fetch-job-log.py are available there. Helper g01_gha_monitor_five_minutes finished and all final receipts were saved; do not overwrite immutable receipts of the previous run.
- Older Linux release JARs use source2b8f80b3f and **do not contain the fixes**. Do not treat them as a final RC or production-ready. Host/M2 JARs with the new Scala code were used only for local tests. Dualarch packaged smoke remains an RC gate; it passed for the newer b93 artifact set.
- Explicitly select Java17; use direct stable-toolchain Cargo bin and JNI-library PATH when needed. Working official protoc33.5 is already in the ProtocBridge cache. Global SDK/PATH/host tools were not changed.
- All execution documentation is here; preserve caches/user checkouts. New test-only/network outputs are in the cleanup ledger. Git feature push/PR28 updates are authorized; artifact publication/production operations are excluded.

## G10 frozen-source checkpoint (2026-11-21)

Read G10-report.md, G10-results.json and STATE.json#g10. All local preparatory
checks are complete; full remote qualification is a mandatory post-commit gate.
No merge, main/tag change, external publication, production or datasci action is
authorized. Preserve the exact frozen candidate SHA and all prior evidence.

Actual remote progress, run IDs and failures are retained under
`${MIGRATION_AUX_ROOT}/20261121_comet12-g10`. Use monitor.py for a bounded GitHub
snapshot every five minutes; inspect new failures promptly. If a source correction
requires a new SHA, cancel superseded builds rather than waiting unnecessarily.
Never treat a generation run, ignored/canceled test or skipped applicable job as
a successful correctness check. Reconcile every backlog item with actual evidence.

After full exact-source Comet CI, CodeQL, Miri and native AMD64/ARM64 RC Build &
Smoke pass, download and independently verify the final archive, JAR checksums,
source/lock/build provenance and both architecture smoke receipts. Save completion.json
externally and report before merge and stable tag. Do not amend the tested source
merely to record post-commit results; a real source change requires requalification.

## G10 replacement frozen-source checkpoint

The initial `68133883e` candidate remains preserved with its failed/canceled
receipts. Exact strict Spark 3.5 + Delta and Spark 4.2 + Delta package/semantic
Scalafix now pass after the bounded quality fixes. Dollar-placeholder literals
have compiler-proven identical runtime bytes; no SQL or assertion is changed.
See G10-results.json, STATE.json#g10, the replacement source manifest and the
external candidate-v2.json for the final frozen source. No old workflow is active.

Publish and qualify the replacement SHA with complete Comet CI, CodeQL, Miri and
RC Build & Smoke on native AMD64/ARM64. Poll every five minutes; analyze new
failures promptly and cancel superseded source runs if necessary. Preserve every
candidate, failed/positive control and immutable job/artifact receipt. Verify the
actual final JARs and reconcile every validation-backlog item before reporting.
Do not amend the qualified source merely to record post-commit outcomes.
Report before merge/stable tag; main, branch-1.2, release tags, external artifact
publication, production and datasci remain outside the authorized write scope.

## G10 final frozen-source checkpoint

The final candidate retains the original 12 packaged cases and adds 11 existing
registrations for G08/G09 linkage and actual default Kryo: 23 cases in 11 groups
per native AMD64/ARM64 architecture. Origin proof requires 17 Core and two Delta
classes from the final JARs; checkout production classes and unexpected duplicates
are rejected. Previous `040f83bac` runs were stopped for this coverage completion
with no observed test failure; their CodeQL success belongs only to that SHA.

Local prechecks passed: 11 additional real shaded cases, one new 19-origin Delta
probe, 11 runtime fault controls, 18 receipt/contract controls, and the full source
and compiled secondary-suite inventory. The previously omitted concrete default
Kryo suite is now listed in both Linux/macOS workflows. All 15 backlog entries
map to actual acceptance evidence; none is closed from planned execution alone.
Production source is unchanged from `040f83bac`.

Read `STATE.json#g10`, `G10-results.json` and the external `candidate-v3.json` and
source manifest. All local processes are done. Publish and qualify the exact SHA
with full Comet CI, CodeQL, Miri and the 23-case RC. Poll every five minutes;
inspect failures promptly and cancel superseded runs if a correction is needed.
Preserve old manifests, controls and receipts. Download and independently verify
final JARs, then reconcile every backlog item before reporting. Do not amend tested
source merely for status updates. Report before merge/stable tag; main,
branch-1.2, release tags, external publication, production and datasci are excluded.

## G10 final Miri-first checkpoint

One existing dictionary roundtrip test now retains both supported Rust codecs
under Miri; ordinary native tests still run uncompressed, LZ4 and Zstd. All data,
writer/checksum/decoded-row assertions and 19 test registrations are unchanged.
Native19/fmt/strictClippy pass; production code is unchanged. All previous source
manifests and interrupted-run receipts remain preserved.

Local official nightly installation succeeded (about 520 MiB; stable default
unchanged). Miri sysroot preparation could not create its standard cache inside
sandbox, and the user rejected escalated setup. Do not repeat escalation or
relocate caches. No local Miri test executed or is counted as passed.

Read STATE.json#g10/G10-results.json and external candidate-v4.json. All local
processes are done. Publish the candidate and run ONLY Miri GHA first, with
CodeQL automatic. After Miri succeeds, dispatch full Comet CI and the23-case
nativeAMD64/ARM64 RC on that SAME frozen SHA. Inspect statuses every five minutes,
classify actual failures, preserve receipts and require all15backlog items and
actual final JAR/origin proofs. Report before merge/stabletag; no main/branch-1.2,
release tag, external publication, production or datasci changes are authorized.

## G10 scan and SQL contract correction checkpoint

Read G10-results.json#local_scan_SQL_contract_qualification and STATE.json#g10.
The previous frozen-source note is historical. All planned local gates must have
actual completed receipts before this update is applied, including current-source
replay and both restored readers on Spark 4.1. Keep any bounded cohort composition
and genuine version-N/A registration explicit; a failed invocation stays failed. Retain every failed/intermediate
receipt. Do not count cancellations, ignores or censored SQL prefixes as passes.
Freeze the new source manifest, then complete same-SHA full CI and dualarch
23-case packaged qualification, verify final artifacts and report before merge
or stable tag. All merge, stable-tag and production permissions remain false.

## G10 shuffle UDF key transport checkpoint

Read G10-results.json#shuffle_UDF_key_transport_followup and STATE.json#g10.
The cf86a3b4 full CI found real missing-pool runtime failures; its successful
CodeQL/Miri107/RC23/artifact checks are historical. Final local receipts must bind
the complete key-proto capture/interner correction and compatibility annotation.
Then freeze a new source and run full same-SHA CI/CodeQL/Miri plus RC25 on both
native architectures. Download and verify the corrected final JARs. No canceled,
ignored, version-N/A or negative-control failure receives executed-pass credit.
Report before merge or stable tag; production remains unauthorized.
