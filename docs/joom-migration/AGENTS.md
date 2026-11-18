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
The latest direct user instruction authorizes implementation of **G01 only**.
Do not start G02 or any later group without a new explicit user instruction after
review of G01. Session orchestration, local Git and the approved network scope
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
decisions.json#execution_model. The stage gate remains active for G02–G10; only G01 is authorized.

## G01 finalization and committed language

The user authorized final English documentation, service-history consolidation,
exact-SHA RC qualification, and conditional G01 integration/tagging on 2026-11-18.
See decisions.json#g01_finalization_authorization and G01-CLOSEOUT.md. Earlier
no-merge/no-tag statements are historical for G01. G02-G10 and Java 21 work remain
closed. Commit messages and committed documentation must be English; user quotes
originally written in Russian are translated, with originals preserved in the
pre-finalization archive. A final post-build source commit would change provenance;
record release acceptance in the immutable annotated tag and artifact receipts.
