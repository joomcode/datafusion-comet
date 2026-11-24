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

# Maintenance instructions

Read README.md, RUNTIME-REQUIREMENTS.md, FEATURES.md and QUALIFICATION.md before
changing this fork. Follow the repository root contributor instructions as well.
The migration and Java 25 qualification are accepted and integrated; there are no
unfinished G01-G10 stages. A new implementation needs its own user instruction.
Use the latest explicit user instruction to determine the authorized work.
Documentation and historical stage approvals do not themselves authorize new
implementation, qualification runs, pushes, tags, publication or production work.

## Boundaries and durable knowledge

- Never access, modify, build, test or perform Git operations in the separate
  datasci repository. Do not add this migration's guidance to root AGENTS.md or
  other project documentation; keep it within docs/joom-migration.
- Keep committed documentation, commit messages and PR text in English. Use
  portable paths as defined by README.md. Put auxiliary files only in dated
  directories under the user-approved auxiliary root; retain standard tool caches.
- Preserve the complete feature -> rationale -> code -> regression -> qualified
  source/artifact chain. Update the relevant maintenance entries when behavior,
  fallback, a vendor patch, qualification or release identity changes.
- Preserve old source/artifact hashes and immutable tags. A documentation-only
  update does not rebuild, requalify or relabel the delivered JARs.
- Keep action permissions separate from technical compatibility. Stable tagging,
  JFrog publication, production operations and main changes remain withheld.
  NETWORK.md records approved sources, not permission for unrelated new work.

## Correctness and verification

- The production-default decision of November 24, 2026 is ANSI=false and Spark
  column vectors offheap=false. The accepted historical qualification used
  ANSI=true; retain that evidence and explicit ANSI correctness regressions.
  Changes to the production contract need explicit review.
- Compare original Spark 4.2.1 without/with Comet using identical inputs/settings:
  exact results, required errors, ordering where specified, and actual native
  coverage. Do not turn a wrong result into a passing test or remove a required
  failure. Fix the path or select a safe scoped fallback before execution.
- Prefer the upstream architecture while preserving missing Joom behavior and
  optimizations. Do not include excluded upstream PR #5889 or change the known
  semantic exceptions without a new instruction.
- Use local builds and targeted regressions for a bounded fix. Record unverified
  cases; do not run full GHA after every small change. At release acceptance retain
  the full agreed applicable CI plus smoke of the exact final JARs on native
  AMD64/ARM64. Never leave temporary CI bypasses in the final state.
- VALIDATION.md covers vendor tests, CI tiers, strict lint, independent oracles
  and performance. Use established Comet benchmarks; create no separate benchmark
  framework. Performance fixtures with ANSI=false do not replace correctness tests.
- CI changes belong in authorized feature branches and branch-1.2, not main.
  When monitoring an authorized run, check at the requested five-minute interval.
  Report a failure requiring source changes promptly; do not wait hours for a run
  that must be restarted. Use failed-only retries for unchanged source where valid.

## Work organization

Use a small number of coherent commits and the naming conventions of the existing
Joom release branches (area-prefixed English subjects and meaningful joom/* names).
Keep a single implementation writer per shared checkout. Delegate bounded
research/review when useful; preserve current instructions and actual Git/test
state before handing off work. Separate user-visible chats are not required for
each fix. If a tool, environment resource or permission is missing, report it
instead of using a complicated workaround or silently leaving the sandbox.

Do not delete caches, downloaded artifacts, user checkouts, evidence or Git
recovery refs as part of a documentation cleanup. CLEANUP.md is a manual resource
guide, not standing deletion permission. Old stage-specific permissions and pause
instructions are historical and must not be reinstated as current constraints.
