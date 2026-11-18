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

> **Current authorization (2026-11-18):** finish G01, finalize English documentation, consolidate service/checkpoint history, repeat exact-source RC Build & Smoke if needed, then integrate into branch-1.2 and create joom-1.2-1-rc1 after successful verification. Check completeness afterwards. Do not start G02 or do further Java 21 work. STATE.json records actual progress; decisions.json records the latest instruction.

> **Historical G01 PR preparation:** history was aligned without changing the tree/24 documents or branch-1.2; three ANSI guards were removed for actual native AMD64 GHA execution. Only fast checks locally, full coverage in GHA. The user creates the PR using G01-PR.md. At that point: no merge/RC1/G02. See decisions.json for newer decisions.

> **Historical status before native RC qualification:** G01 implemented; AMD64 runtime qualification NOT complete. branch-1.2 NOT changed; user preview/confirmation first. G02 not authorized. Results: G01-report.md / G01-results.json. These limitations describe that earlier checkpoint, not the current RC result.

> **G01 resumed by the user on 2026-11-17:** ProtocBridge cache writes, loopback and Docker were verified inside the sandbox. The historical pause below is retained for traceability; current status is in STATE.json. Only G01 was authorized.

> **Historical pause, 2026-11-17:** all our builds/tests were stopped at the user's request. Code saved in Git; G01 unfinished. PAUSED.md was the authoritative continuation point; no restart before explicit resume. G02 not authorized.

**G01 was resumed by the user on 2026-11-17. STATE.json is the current status; PAUSED.md records the continuation point. G02 is not authorized.**

# Handoff between work sessions

The local datafusion-comet project is registered as
`79ec4996-572a-4070-96f7-2344b555eb2c`, with primary directory
`/Users/skotlov/d/projects/datafusion-comet`.
The user explicitly authorized the coordinator to create/manage sessions, pass tasks
and clarifications, and verify results. The user does not need to copy context manually.

## Selected execution model

One main chat executes and coordinates. The nine feature branches remain;
a separate user-visible chat per group is unnecessary. The coordinator chooses
bounded source/compatibility research and independent review tasks for internal
helpers, and when to delegate them. Implementation and integration stay in the main
chat by default, with a single writer per checkout.

The coordinator creates a new user-visible session when a lengthy isolated task,
a bounded separate context or a continuation handoff offers concrete benefit.
A new PR, large diff or compaction is not an automatic reason. Explain the reason
to the user and transfer the checkpoint/context independently; repeated consent is
not required within the existing scope. The user does not manually allocate tasks.

## Current phase

**The user has authorized G01, implemented by the main chat in joom/1.2-dev.**
The G01 preparation chat remains idle. Do not proceed to G02-G10 without separate
explicit user authorization after the previous group's report. The latest G01
finalization instruction includes integration and RC1 after successful checks.

## Coordinator responsibilities

1. Execute in the main chat with internal helpers by default. Create separate
   user-visible sessions only under the criteria above.
2. Explicitly include the repo and docs/joom-migration/README.md, decisions.json,
   STATE.json, approved-plan.json, PERFORMANCE.md, cleanup ledger and group cards in
   each task prompt. The migration section was removed from root AGENTS.md at the user's request.
3. Pass the exact group/branch/base SHA, allowed actions and latest decisions.
   A current explicit code hold takes precedence over general Git permission.
4. Read the worker response, verify evidence and save the report here.
   Separate chats do not automatically synchronize later messages.
5. Once implementation is authorized, keep one writer per checkout; review and
   research may run in parallel. The coordinator owns integration/shared STATE/decisions.
   Any explicit assignment of disjoint documentation files must remain nonoverlapping.
6. Forward clarifications independently. Report real environment blockers or a new
   required decision to the user.

## Required task contents

- Group and all its register cards; goal/boundaries, branch and exact base SHA.
- Latest code hold/authorization, network safety condition and publication prohibition.
- Approved-plan sources and user-provided checkouts.
- ANSI=true correctness/mandatory errors; do not include PR5889.
- Performance using established Comet conventions, without special infrastructure.
- Exact write scope; process documentation only in docs/joom-migration.
- Report format; do not automatically begin the next group.

## Worker report

For preparation, a chat response is sufficient: the coordinator reads and records it here.
Task-to-task communication is authorized only for this work; do not message people.
During implementation, save reports in this directory and raw logs in dated temp directories.

Record group/branch/base/head/cleanliness, completed and remaining cards,
upstream replacements, changed shared APIs, test commands/exit codes/log paths,
native coverage, performance results and limitations, created paths/Docker IDs/
volumes/sizes for cleanup, artifacts/checksums, blockers and exact next step.
Do not claim unperformed checks passed.

Changing sessions/compaction is not a reason to repeat completed stages.
STATE and decisions reflect actual progress and direct user authorization;
old snapshots must not overwrite the current state.

Translation note: historical Russian statements above have been translated into
English. The original pre-translation documents are archived at
`/Users/skotlov/d/temp/20261118_comet12-g01-finalization/documentation-before.zip`.
