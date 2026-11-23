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

> **Current G10 acceptance, 2026-11-23:** The user accepted final G10 qualification and explicitly authorized committing the final documentation, pushing `joom/1.2-release-qualification`, fast-forward integration into `branch-1.2` and post-integration verification. The qualified non-migration source remains `81474a889489da4a4a4aa8e49330e6398c1937e3`; final JARs retain that build provenance. Actual final SHA and local/remote proof are recorded after commit in `${MIGRATION_AUX_ROOT}/20261123_comet12-g10-integration/completion.json`. A missing receipt is not proof that integration succeeded. Stable tagging, JFrog/external artifact publication, main changes, production and datasci remain excluded. Older merge holds below are historical for G10. See decisions.json#g10_integration_authorization.

## Current continuation: approved Spark runtime contract, 2026-11-22

The user explicitly approved official Spark4.2.1 with
`spark.sql.columnVector.offheap.enabled=false`, preserving ANSI=true, and asked
to continue full G10 GHA and RC Build & Smoke on native AMD64/ARM64. The earlier
accidental selection/withdrawal is superseded by this later direct instruction.
Read RUNTIME-REQUIREMENTS.md and decisions.json#g10_spark421_column_vector_contract.
The older pending-decision checkpoints below are historical.

The v13 working source includes the independently qualified prefetched-output
cleanup fix and three regressions, target Spark4.2 test defaults and RC proof of
actual session defaults. Finish/verify local gates, source/document manifests,
then commit/push only the feature and bind actual identities externally. All old
v12 and diagnostic receipts remain historical. Preserve explicit off-heap=true
and ANSI=false regression cases, full original test/oracle/fuzz coverage and all
quality gates. Do not merge the separate diagnostic branch or change Spark.

Follow STATE.json#g10.runtime_contract_v13_followup and external LIVE-v13.json.
Require full exact-SHA GHA/CodeQL/Miri and RC27 on each native architecture,
verified final JAR downloads and all15 backlog reconciliations. Poll every five
minutes and diagnose failures promptly. No branch-1.2/main/tag changes, merge,
stable release, production, external publication or datasci access is allowed.

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

> **Current G04 acceptance, 2026-11-20:** The user approved publishing G04 and integrating it into `branch-1.2`. G04 is complete; the five-commit fast-forward, exact local/remote refs and unchanged qualified non-migration tree are recorded after commit in `${MIGRATION_AUX_ROOT}/20261120_comet12-g04-integration/completion.json`. Earlier G04 push/merge holds below are historical. G05-G10, full per-group GHA, main/tag changes, artifact publication and production remain unauthorized. Final qualification/backlogs remain required.

> **Historical G04 review gate, 2026-11-19:** G04 is implemented and locally qualified in `joom/1.2-native-aggregates` on accepted G03 `af05c94e7`. See G04-report.md/G04-results.json and STATE.json. Await review before push or integration; `branch-1.2`, main and RC1 are unchanged. G05-G10, full per-group GHA, publication and production remain unauthorized. Final qualification/backlogs remain required.

> **Historical G03 acceptance, 2026-11-19:** The user approved pushing and integrating G03 into branch-1.2 after local validation. G03 is complete; actual final refs/tree and post-merge proof are recorded after commit in `${MIGRATION_AUX_ROOT}/20261119_comet12-g03-integration/completion.json`. Preserve two substantive commits and the original implementation receipt. G04-G10, main/tag changes, full per-group GHA, artifact publication and production remain unauthorized. Final Linux/release and the known baseline workspace lint/toolchain checks remain required. See decisions.json#g03_integration_authorization and STATE.json.

> **Historical pre-integration G03 result, 2026-11-19:** G03 is implemented and locally validated, ready for integration review. See G03-report.md/G03-results.json and STATE.json. The documentation push is verified; branch-1.2 stays at672988395 until further approval. G04-G10 remain unauthorized. Final Linux/release qualification and the known pristine-vendor/toolchain lint issue remain tracked; no production use.

> **Historical G03 implementation authorization, 2026-11-19:** The user explicitly started G03 after the documentation push verified origin/branch-1.2 at672988395. Implement D11/D02/D04/D03 in joom/1.2-sort-spill-budget, using local builds/targeted regressions. Earlier G03 holds below are historical. Report before integration; G04-G10, main/tag changes, full per-group GHA, publication and production remain unauthorized. See decisions.json#g03_authorization and STATE.json.

> **Historical post-G02 rules, 2026-11-19:** G01 is complete at `joom-1.2-1-rc1` (`c04d2d03c`); G02 is complete and accepted for integration into branch-1.2, including the closed JNI memory-wait cancellation case and accepted performance assessment; actual integration/remote refs are recorded in the post-commit receipt; G03-G10 remain gated. Do not touch `${DATASCI_ROOT}/`. For each remaining group use local builds/targeted regressions and track unverified cases; focused Linux AMD64 GHA is available when needed. Do not run full GHA after every group, and G06 intermediate release qualification is removed. Before final acceptance remove all temporary CI mechanisms, restore normal automatic full CI, then run the full agreed GHA and exact final-JAR smoke on native AMD64/ARM64. No additional main commits or production use. Correctness requirements remain unchanged. See `decisions.json#future_migration_validation_policy`.

> **Historical G01 source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

> **Historical G01 source-freeze status:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

> **Historical pre-rewrite G01 qualification:** RC Build & Smoke passed on `b93d55385`. All7 jobs and12 packaged smoke cases on each architecture passed. Final JARs were downloaded to `target/joom-release/gha-44388252264`; SHA256 and provenance were verified. Full runtime CI on 26590264b is retained; runtime code has not changed. Results: `RC-BUILD-SMOKE.md`, `G01-report.md`, `G01-results.json`, `STATE.json`. The subsequent finalization instruction authorizes history cleanup, English documentation, matching final RC qualification, integration and RC1 tagging after successful checks. Publication and G02 remain excluded.

# Joom Comet 1.2: plan and handoff

This directory is the durable entry point for implementing the Joom Comet migration. Chat history is not the only source of decisions. At the historical preparation checkpoint on 2026-11-17, the plan, Java/Rust, Docker and conditional network scope were approved; only G01 implementation was authorized then. Later explicit instructions authorized and completed G02 and G03. Current acceptance and integration are recorded above and in STATE.json; G05-G10 require separate explicit authorization.

## Portable path notation

Read [PATHS.md](PATHS.md) before resolving paths in this directory. Comet file
paths are repository-relative; external evidence uses documented root variables.
Local bindings and original receipts stay outside Git. Historical path-bearing
output and translated quotations use normalized notation, preserving their
meaning rather than claiming byte-identical raw output.

## Read before working

1. `decisions.json`: current user decisions and permissions.
2. `STATE.json`: current group, branches, checks, limitations and next step.
3. `approved-plan.json`: approved nine groups and tenth-group reserve, dependencies, RCs.
4. `feature-register.json`: full register of58 active items plus historical H01; select every card of your group from `approved-plan.json`, excluding none silently.
5. `correctness-test-plan.txt`, `vendor-provenance.json`, `target-native-dependencies.json`: correctness requirements and vendor provenance.
6. `NETWORK.md`: approved network scope with safety condition. Read the active authorization and its translated user statement in `decisions.json`.
7. `HANDOFF.md`: worker-session task and reporting rules.
8. `PERFORMANCE.md`, `cleanup-ledger.json`, `CLEANUP.md`: established benchmarks and disk accounting.

## Fixed decisions

- Source: `joom-1.1-13`, commit `981e26dff6b64110a8b3949a0ed065523d9012e4`.
- Apache `branch-1.2` pinned to `93cc6f9d71136bd0d87f77dd901989144d8c09fb`. Do not replace the base with newer upstream without separate discussion.
- Spark4.2.1 / Scala2.13.18; DataFusion55.2.0, Arrow/Parquet59.3.2; target Delta integration4.4.1. Exact compatibility is checked during implementation.
- Maximize use of agreed upstream implementations while retaining all missing Joom capabilities. Similar upstream code does not prove equivalence.
- Official Spark4.2.1 requires `spark.sql.columnVector.offheap.enabled=false` in the target deployment and qualification; see [RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md). Explicit regression variants remain.
- ANSI=true. Compare Spark4.2.1 Comet-off/on: results AND mandatory errors; retain mixed SUM/AVG overflow guards. Fix or restrict a known unsafe path before execution. A successful build alone does not prove correctness.
- Do not touch/include PR5889. Do not automatically add changes after source tag13.
- Delivery: local Comet/Delta JARs for Linux amd64+arm64 and Spark4.2 image, SHA256, Git SHA, build parameters and test results. Scoped Git permissions for G01/G02/G03 and the completed G01 integration/RC1 tagging are recorded in `decisions.json`; they do not authorize a later stage or a new PR/CI launch. JFrog publication, datasci image changes and production rollout remain excluded.
- Check performance using established Comet tools: Scala microbenchmarks, Rust Criterion and available TPC-H/DS runners; do not create special infrastructure. The user will later run full production GCP performance checks; do not claim this migration has passed them.

## Work organization

The main chat implements and coordinates G01–G09 sequentially in approved feature branches (G10 is a reserve). The coordinator independently delegates bounded research and independent review to internal helpers, passes context and verifies results. Nine branches do not require nine chats. There is one implementation writer in the shared checkout; do not run all heavy builds concurrently. Bounded disjoint documentation/review assignments may be delegated explicitly by the coordinator.

Create a separate user-visible session at the coordinator's discretion when a long isolated task/separate context or continuation handoff offers a concrete benefit. A new PR, large diff or compaction alone is not a reason. First explain why and preserve HEAD/tree/tests/next step, then pass the task and verify the result. Session-management permissions have already been granted. Leave the existing G01 preparation chat idle; do not automatically start implementation there.

After each completed stage, record commits, checks and next step in `STATE.json` and the report inside `docs/joom-migration`. Save one-off raw logs/tool outputs in dated temp directories and account for them in the cleanup ledger. Repository migration instructions belong only here. Before the next group, verify actual Git HEAD/cleanliness and dependencies; state files do not replace this check. The coordinator owns integration. Do not commit known unrelated changes from another executor.

RC1 after G01 is complete and immutable. The planned intermediate G06 qualification/RC2 milestone is canceled. The previously planned final RC3 remains the final candidate after all agreed work and required integration fixes, removal of temporary CI mechanisms and restoration of normal automatic full CI; qualify with full agreed GHA plus exact final-JAR packaged smoke on native AMD64/ARM64. This policy change does not renumber existing or planned final tags. Additional candidates use unused immutable names; stable `joom-1.2-1` uses the last qualified RC. Final qualification remains mandatory whether or not G10 is used.

## Transferring permissions

`decisions.json` records explicit user authorization for necessary local operations in this checkout, including `.git`. The previous Git prohibition was superseded for this work. Network operations in `NETWORK.md` are authorized for the entire migration with an explicit condition: downloaded/executed artifacts must be known and widely used, with no reasonable safety concerns. Seek separate agreement if an unusual tool/source is needed or safety is uncertain. That dependency-download authorization alone does not include push, external publication or production operations; later explicit feature-push/PR and G01 integration/tag permissions are recorded separately in `decisions.json`.

The user added the local Comet project and explicitly authorized the coordinator to create/manage working sessions, send tasks/clarifications and verify results. The coordinator handles inter-session transfer without requiring the user to copy context manually. The original implementation approval covered G01 only; G02 and G03 were subsequently authorized and completed. G04-G10 remain gated, and G03 integration is explicitly approved in decisions.json#g03_integration_authorization.

Keep all one-off scripts, raw logs and tool outputs only in subdirectories of `${MIGRATION_AUX_ROOT}`; each new directory there is named `YYYYMMDD_description`. Do not use `/tmp` for executor-selected output/temp files. Standard tool-managed caches retain their usual locations. Do not automatically clear the shared Maven/SBT cache through `dev/local-ci.sh`; inspect its behavior first and use the existing Spark checkout instead of downloading source without control. Ask the user to download/checkout additional research sources; do not obtain source through UI. Cargo Git dependencies are a separate approved network scope, not permission to clone arbitrary projects.

## Preservation and original audit

This package is intended for Git storage. `MANIFEST.json` records file SHA256 at handoff; update its checksum when intentionally changing a file. Previous materials are preserved in:

- `${MIGRATION_AUX_ROOT}/20261116_comet12-migration-register`
- `${MIGRATION_AUX_ROOT}/20261116_comet12-consolidated-plan`
- `${MIGRATION_AUX_ROOT}/20261116_comet12-execution-setup`

`approved-plan.json`/`feature-register.json` snapshots preserve approved pre-implementation scope; track progress in `STATE.json`. Do not run the old register generator to update progress: it deliberately sets NOT_STARTED/NOT_RUN and is not an execution tracker. Record new confirmed user decisions primarily in `decisions.json` and reflect them in current tasks; old temp reports may be historical snapshots. Do not rely on automatic synchronization of chat histories.

All newly committed documentation and commit messages must be English. Original Russian evidence and pre-translation content are retained in `${MIGRATION_AUX_ROOT}/20261118_comet12-g01-finalization/documentation-before.zip`. Translated user statements in the documentation preserve the decisions rather than claiming to be original English quotations.

## Environment and blockers

Java17.0.19-tem is suitable and verified with javac; the global selection at the preparation checkpoint was Java21. The coordinator sets JAVA_HOME to17.0.19-tem per build command without changing global Java. Java21 qualification is currently on hold at the user's request.
Rust/Cargo1.100.0 are installed and checked; Homebrew proxies are in `${RUSTUP_PACKAGE_ROOT}/bin`. Add that path to PATH for individual commands if needed; editing the user's `~/.zshrc` is unnecessary.
If a required tool/access/capability is actually missing, report the concrete blocker to the user and resolve it together; do not spend time on long workarounds.
