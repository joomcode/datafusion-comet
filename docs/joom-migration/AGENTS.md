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

# Instructions for migration documentation

Keep migration execution guidance, decisions, handoffs, progress and cleanup
instructions inside this directory. Do not add migration-specific guidance to
the repository root AGENTS.md or other project documentation.

Before work, read README.md, PATHS.md, decisions.json, STATE.json and approved-plan.json.
G01 is complete at joom-1.2-1-rc1; G02 and G03 are integrated. G04 is complete and accepted for integration; see G04-report.md/G04-results.json, STATE.json and the post-commit integration receipt above. Start G05 or later groups only after separate explicit user authorization. Session orchestration, local Git and the approved network scope
remain authorized; publishing/production actions remain excluded.

If a required tool, permission or environment capability is missing, report the
specific blocker to the coordinator/user rather than spend time building a long
or complicated workaround. Normal per-command PATH/JAVA_HOME selection is fine.

Use the project's established performance tools and conventions; see PERFORMANCE.md.
Keep ANSI=true correctness coverage separate from benchmark fixture settings.

Maintain cleanup-ledger.json before/after downloads, builds and benchmarks, and
finish CLEANUP.md with exact resource identities, sizes and targeted cleanup
commands. Treat baseline/shared caches and user-provided checkouts as shared/user
resources, never automatically disposable migration outputs. Do not delete them.

Use HANDOFF.md for cross-chat transfer. Each real worker prompt must explicitly
point here because there is deliberately no migration pointer in root AGENTS.md.
Record progress and evidence without silently changing the approved 58-card scope.

The main chat is the default implementation/integration owner; internal helpers
perform bounded research and independent review. The coordinator chooses delegation
and occasional user-visible sessions autonomously within the authorized scope.
Nine feature branches do not require nine separate chats. See HANDOFF.md and
decisions.json#execution_model. G04 integration is approved and recorded in the receipt above; the start gate remains active for G05-G10.

## G01 finalization and committed language

The user authorized final English documentation, service-history consolidation,
exact-SHA RC qualification, and conditional G01 integration/tagging on 2026-11-18.
See decisions.json#g01_finalization_authorization and G01-CLOSEOUT.md. Earlier
no-merge/no-tag statements are historical for G01. That historical hold was superseded by later G02/G03 integration and G04 local implementation authorizations. G04 is accepted for integration; G05-G10 and Java 21 work remain closed. Commit messages and committed documentation must be English; user quotes
originally written in Russian are translated, with originals preserved in the
pre-finalization archive. A final post-build source commit would change provenance;
record release acceptance in the immutable annotated tag and artifact receipts.

## Naming and commit granularity

Before creating a commit, PR or Git branch, check the repository's established
naming practice, including the Joom work on branch-1.1. Follow that practice and
the approved migration branch names rather than inventing a new convention.
Keep all committed text and PR text in English.

Prefer a small number of coherent commits. Create a separate commit only when
there is a clear reason, such as an independently reviewable change, a required
dependency boundary or the separately auditable pristine vendor import. Fold
related fixes and routine documentation/checkpoints into their relevant change
before integration; do not create small commits merely to record progress.
Preserve correctness, reviewability and immutable release tags.

These rules were explicitly requested on 2026-11-19. The local documentation
update should accompany the next authorized substantive change rather than
create a standalone service commit. G02/G03 are integrated; G04 is complete and accepted for integration; G05-G10 remain gated.

## Repository boundary and validation policy (2026-11-19)

Do not access, modify, build, test or perform Git operations in
`${DATASCI_ROOT}/`; another chat is working there in parallel.
Keep all migration work in Comet and the approved auxiliary/cache locations.

Do not run full GHA after every G. Use local builds and targeted regression
tests; record each deferred or unverified case in STATE.json#validation_backlog
with its owner, intended verification and actual evidence. Use focused Linux
AMD64 GHA when necessary. Final coverage is unchanged.

There is no intermediate release qualification after G06. CI changes belong in
authorized feature branches and branch-1.2 only; do not add commits in main.
Track all temporary CI disablements, switches and workarounds in
STATE.json#temporary_ci_mechanisms. Before final acceptance remove all of them
and restore full CI with normal automatic triggers and complete agreed coverage.
After cleanup, run the full agreed GHA and packaged smoke of the final JAR bytes
on native AMD64 and ARM64. Further fixes may require further qualification runs.

Keep ANSI=true, Spark/Comet result and mandatory-error comparison, and actual
native-execution verification. Never weaken checks; fix incompatibilities or
select a safe targeted fallback before execution. Do not use the migration in
production until all agreed features and final qualification succeed. That
condition does not itself grant permission to deploy or use production.
See decisions.json#future_migration_validation_policy. Every next G remains
subject to separate user authorization.

## Historical G02 follow-up authorization (2026-11-19)

The user approved closing the JNI memory-wait cancellation case, rerunning affected local regressions, folding the fix into its substantive G02 commit, pushing `joom/1.2-native-runtime`, and showing the final diff for merge approval. At that stage the approval superseded the earlier remote-action hold for the feature push only. The subsequent G02 integration authorization below now supersedes its merge hold. G03, full per-group GHA, new release tags, main changes and artifact publication remain excluded. The memory-wait case is now closed by local evidence in G02-report.md/G02-results.json; platform/release qualification remains required.

## Historical G02 accepted integration (2026-11-19)

The user explicitly approved the practical performance conclusions, service-change consolidation, integration of G02 into branch-1.2, post-merge verification and reporting the final merged commit names. See decisions.json#g02_performance_acceptance and #g02_integration_authorization. This supersedes prior G02 merge holds and the feature-push-only limit for this integration. Keep three substantive commits, fold documentation into the existing third commit and fast-forward without an extra merge commit. Synchronize only the feature/integration refs using guarded Git operations; main and release tags remain unchanged. Final local/remote/tree proof is recorded after commit in `${MIGRATION_AUX_ROOT}/20261119_comet12-g02-integration/completion.json`. G02 is complete; G03-G10 require separate explicit authorization. No new full per-group GHA or release qualification is requested. Production remains unauthorized.

## Portable committed paths (2026-11-19)

The user explicitly requested replacement of existing absolute local paths and
one coherent documentation commit. Follow PATHS.md in every subsequent committed
project file: use Comet-root-relative paths, documented root placeholders for
external evidence/checkouts, and HOME for standard user locations. Keep exact
machine bindings and raw evidence outside Git. JSON placeholders are literal;
resolve them explicitly without eval. Preserve suffixes, hashes, permissions,
feature scope and stage gates; validate cleanup roots and quote required variables.
Normalize historical output/translated statements transparently; do not claim
they are byte-identical raw records. Update MANIFEST.json after intentional edits.
Do not access the datasci repository, regardless of its local checkout location.
That earlier documentation-only authorization did not start G03 or authorize
push, CI, release, production, source/history rewriting or artifact publication.
G03 integration and G04 local implementation were later authorized separately; G04 integration is approved; G05-G10 remain closed.
