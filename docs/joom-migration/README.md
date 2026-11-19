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

> **Current rules, 2026-11-19:** G01 is complete at `joom-1.2-1-rc1` (`c04d2d03c`); G02 is complete and accepted for integration into branch-1.2, including the closed JNI memory-wait cancellation case and accepted performance assessment; actual integration/remote refs are recorded in the post-commit receipt; G03-G10 remain gated. Do not touch `${DATASCI_ROOT}/`. For each remaining group use local builds/targeted regressions and track unverified cases; focused Linux AMD64 GHA is available when needed. Do not run full GHA after every group, and G06 intermediate release qualification is removed. Before final acceptance remove all temporary CI mechanisms, restore normal automatic full CI, then run the full agreed GHA and exact final-JAR smoke on native AMD64/ARM64. No additional main commits or production use. Correctness requirements remain unchanged. See `decisions.json#future_migration_validation_policy`.

> **Historical G01 source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

> **Historical G01 source-freeze status:** documentation and history cleanup, followed by exact-SHA RC qualification, are in progress. Integration into `branch-1.2` and `joom-1.2-1-rc1` are conditional on successful checks. G02 and Java 21 work remain on hold.

> **Historical pre-rewrite G01 qualification:** RC Build & Smoke passed on `b93d55385`. All7 jobs and12 packaged smoke cases on each architecture passed. Final JARs were downloaded to `target/joom-release/gha-44388252264`; SHA256 and provenance were verified. Full runtime CI on 26590264b is retained; runtime code has not changed. Results: `RC-BUILD-SMOKE.md`, `G01-report.md`, `G01-results.json`, `STATE.json`. The subsequent finalization instruction authorizes history cleanup, English documentation, matching final RC qualification, integration and RC1 tagging after successful checks. Publication and G02 remain excluded.

# Joom Comet 1.2: plan and handoff

This directory is the durable entry point for implementing the Joom Comet migration. Chat history is not the only source of decisions. Initial environment/preparation state on2026-11-17: plan approved, Java/Rust and Docker checked, network approved subject to safety conditions. **G01 implementation is authorized.** The user separately authorizes each subsequent group after evaluating the previous one; do not start G02–G10 now.

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
- ANSI=true. Compare Spark4.2.1 Comet-off/on: results AND mandatory errors; retain mixed SUM/AVG overflow guards. Fix or restrict a known unsafe path before execution. A successful build alone does not prove correctness.
- Do not touch/include PR5889. Do not automatically add changes after source tag13.
- Delivery: local Comet/Delta JARs for Linux amd64+arm64 and Spark4.2 image, SHA256, Git SHA, build parameters and test results. Feature Git push/PR updates and conditional G01 integration/RC1 tagging are now authorized as recorded in `decisions.json`; JFrog publication, datasci image changes and production rollout remain excluded.
- Check performance using established Comet tools: Scala microbenchmarks, Rust Criterion and available TPC-H/DS runners; do not create special infrastructure. The user will later run full production GCP performance checks; do not claim this migration has passed them.

## Work organization

The main chat implements and coordinates G01–G09 sequentially in approved feature branches (G10 is a reserve). The coordinator independently delegates bounded research and independent review to internal helpers, passes context and verifies results. Nine branches do not require nine chats. There is one implementation writer in the shared checkout; do not run all heavy builds concurrently. Bounded disjoint documentation/review assignments may be delegated explicitly by the coordinator.

Create a separate user-visible session at the coordinator's discretion when a long isolated task/separate context or continuation handoff offers a concrete benefit. A new PR, large diff or compaction alone is not a reason. First explain why and preserve HEAD/tree/tests/next step, then pass the task and verify the result. Session-management permissions have already been granted. Leave the existing G01 preparation chat idle; do not automatically start implementation there.

After each completed stage, record commits, checks and next step in `STATE.json` and the report inside `docs/joom-migration`. Save one-off raw logs/tool outputs in dated temp directories and account for them in the cleanup ledger. Repository migration instructions belong only here. Before the next group, verify actual Git HEAD/cleanliness and dependencies; state files do not replace this check. The coordinator owns integration. Do not commit known unrelated changes from another executor.

RC1 after G01 is complete and immutable. The planned intermediate G06 qualification/RC2 milestone is canceled. The previously planned final RC3 remains the final candidate after all agreed work and required integration fixes, removal of temporary CI mechanisms and restoration of normal automatic full CI; qualify with full agreed GHA plus exact final-JAR packaged smoke on native AMD64/ARM64. This policy change does not renumber existing or planned final tags. Additional candidates use unused immutable names; stable `joom-1.2-1` uses the last qualified RC. Final qualification remains mandatory whether or not G10 is used.

## Transferring permissions

`decisions.json` records explicit user authorization for necessary local operations in this checkout, including `.git`. The previous Git prohibition was superseded for this work. Network operations in `NETWORK.md` are authorized for the entire migration with an explicit condition: downloaded/executed artifacts must be known and widely used, with no reasonable safety concerns. Seek separate agreement if an unusual tool/source is needed or safety is uncertain. That dependency-download authorization alone does not include push, external publication or production operations; later explicit feature-push/PR and G01 integration/tag permissions are recorded separately in `decisions.json`.

The user added the local Comet project and explicitly authorized the coordinator to create/manage working sessions, send tasks/clarifications and verify results. The coordinator handles inter-session transfer without requiring the user to copy context manually. Current implementation authorization covers only G01; later groups remain gated.

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
