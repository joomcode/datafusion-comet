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

# Instructions for migration documentation

Keep migration execution guidance, decisions, handoffs, progress and cleanup
instructions inside this directory. Do not add migration-specific guidance to
the repository root AGENTS.md or other project documentation.

Before work, read README.md, decisions.json, STATE.json and approved-plan.json.
G01 is complete at joom-1.2-1-rc1. The user explicitly authorized G02 on
2026-11-19; see decisions.json#g02_authorization. Start G03 or any later group
only after separate explicit user authorization. Session orchestration, local Git and the approved network scope
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
decisions.json#execution_model. G02 is authorized; the stage gate remains active for G03-G10.

## G01 finalization and committed language

The user authorized final English documentation, service-history consolidation,
exact-SHA RC qualification, and conditional G01 integration/tagging on 2026-11-18.
See decisions.json#g01_finalization_authorization and G01-CLOSEOUT.md. Earlier
no-merge/no-tag statements are historical for G01. That historical hold was superseded for G02 on 2026-11-19; G03-G10 and
Java 21 work remain closed. Commit messages and committed documentation must be English; user quotes
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
create a standalone service commit. G02 was explicitly authorized on 2026-11-19; later groups remain gated.

## Repository boundary and validation policy (2026-11-19)

Do not access, modify, build, test or perform Git operations in
`/Users/skotlov/d/projects/datasci/`; another chat is working there in parallel.
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

## G02 follow-up authorization (2026-11-19)

The user approved closing the JNI memory-wait cancellation case, rerunning affected local regressions, folding the fix into its substantive G02 commit, pushing `joom/1.2-native-runtime`, and showing the final diff for merge approval. At that stage the approval superseded the earlier remote-action hold for the feature push only. The subsequent G02 integration authorization below now supersedes its merge hold. G03, full per-group GHA, new release tags, main changes and artifact publication remain excluded. The memory-wait case is now closed by local evidence in G02-report.md/G02-results.json; platform/release qualification remains required.

## G02 accepted integration (2026-11-19)

The user explicitly approved the practical performance conclusions, service-change consolidation, integration of G02 into branch-1.2, post-merge verification and reporting the final merged commit names. See decisions.json#g02_performance_acceptance and #g02_integration_authorization. This supersedes prior G02 merge holds and the feature-push-only limit for this integration. Keep three substantive commits, fold documentation into the existing third commit and fast-forward without an extra merge commit. Synchronize only the feature/integration refs using guarded Git operations; main and release tags remain unchanged. Final local/remote/tree proof is recorded after commit in `/Users/skotlov/d/temp/20261119_comet12-g02-integration/completion.json`. G02 is complete; G03-G10 require separate explicit authorization. No new full per-group GHA or release qualification is requested. Production remains unauthorized.
