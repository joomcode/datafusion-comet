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

# G10: final integration and release qualification

## Current replacement checkpoint (2026-11-22)

This section supersedes the historical candidate notes below. The replacement
SHA is intentionally not predicted; final run IDs and outcomes belong in external
receipts after commit. Merge, stable tag and production remain unauthorized.

The bounded follow-up changes 13 test/resource/release-validation files. It does
not change production eligibility, native algorithms, CI workflows, dependencies
or lockfiles. All 244 original SQL records, data/configuration/error statements
and statement order across the three resources are preserved. Precise fallback
expectations now account for actual Spark 3.4/3.5 plans while retaining native
expression proof on the other profiles. Every listed Spark operator and reason
is required; all other computation remains subject to the strict native checks.
Typed native scans retain source multiplicity. An unlisted AQE read is structural
only if columnar and backed by the typed Comet exchange; all inner plans are
still checked. A real row-shuffle negative control proves rejection.

One formerly ignored DateAdd case is active again. Its original 10,000-row input,
two dictionary modes and DATE output remain; explicit ANSI true/false, real NULL
and overflow witnesses, an independent wide-integer wrap oracle and actual
native date_add/unix_date assertions check Spark rows or mandatory errors. One
timestamp-cast regression checks seven native-unsupported targets across both
ANSI modes and dispatcher on/off: 28 internal variants, exact Spark rows/errors
and actual dispatched/native-scan or precise Project/native-scan paths. Analysis
rejections earn no native execution credit. Eleven inherited native-expression
ignores retain explicit source-guard/scope dispositions; they are not passes.

| Final local profile cohort | Runner successes | Executed positive | Version N/A |
| -------------------------- | ---------------: | ----------------: | ----------: |
| Spark 3.4                  |               60 |                60 |           0 |
| Spark 3.5                  |               60 |                60 |           0 |
| Spark 4.0                  |               60 |                60 |           0 |
| Spark 4.1                  |               60 |                60 |           0 |
| Spark 4.2.1                |               60 |                58 |           2 |

These are separate per-profile cohorts, not 300 unique tests. The two Spark4.2
Iceberg registrations return through their logged version guards and receive no
executed-pass credit; all older-profile Iceberg jobs remain required. Final
Spark4.2 + Delta semantic/package checks passed in 138.94 seconds; final strict
Spark3.5 + Delta semantic/package checks passed in 226.88 seconds. The initial
offline3.5 plugin-availability failure remains preserved as failed evidence.

RC acceptance grows additively from 25 to 27 cases in 13 groups per architecture,
with 24 Core and two Delta origin classes. All prior selectors and origin guards
remain. The 41 Python contract controls and five source-guard controls passed;
they do not replace native Linux packaged execution. Core98 runtime cases,
88 native-window actions, the separate58 corrective cases and the original five
Spark3.4 UDF SQL fixtures require actual final source/run/job-bound evidence.
All15 backlog entries remain open until reconciled with that evidence.

Historical v8 source `4e0f731190ae350e13fe00186676493b33306e29` passed RC25
(`44963792674`), Miri107 (`44963790704`) and Actions CodeQL (`44963769288`),
and its archive/JARs were downloaded and verified. Its full CI (`44963788842`)
was canceled after real older-profile test-contract failures. These source-bound
receipts are retained without qualification credit for the replacement source.

Read `G10-results.json#profile_expression_correctness_followup`, `STATE.json#g10`,
and the final local matrix/static review under
`${MIGRATION_AUX_ROOT}/20261121_comet12-g10/20261122_final-independent-review`.
The unarmed v9 backlog map defines requirements, not closure. After freezing the
replacement SHA, full agreed CI, Actions CodeQL, Miri, native AMD64/ARM64 RC27,
verified downloaded final JARs and all15 backlog dispositions remain mandatory.
Record outcomes externally without amending the tested source for status alone.

## Historical preparation and source-bound attempts

**Frozen-source qualification candidate.** G10 is prepared on
`joom/1.2-release-qualification`, based on accepted G09
`4467d0337089036d3f01d28375260ce3dbcaaca5`. Local checks below passed. Full GHA,
CodeQL/Miri and exact-SHA RC Build & Smoke are post-commit acceptance gates;
their actual verdicts are recorded externally, not asserted by this source
checkpoint. See `G10-results.json` and the final completion receipt. Merge and
stable-tag creation still require explicit user approval.

## Bounded changes

- **Restore complete CI quality coverage.** Remove the stale exclusion of
  `datafusion-physical-plan` from strict workspace Clippy. Retain the approved
  statement-local deprecated-API allowance and every other warning check.
  Preserve normal PR/release-branch/queue/nightly routing, all five Linux Spark
  profiles, existing suites, shared vendor lock/feature graph and durable
  runner/cache/Maven-retry fixes. CodeQL now runs automatically on Joom feature
  and release branches using Ubuntu 24.04, with unchanged permissions.
- **Execute previously deferred runtime checks.** The Linux Spark 4.2 `exec` row
  enables the full aggregate and SMJ fuzz matrices in the forked JVM. Merged
  Maven output retains the original exit status. The existing window verifier,
  promoted byte-for-byte to `dev/ci/verify-native-windows.py`, requires all 88
  native/fallback/spill actions. Logs, verifier output and test XML are retained.
- **Restore two aggregate-metric tests and fix their real observability gap.**
  New grouped streams omitted peak-memory registration/publication, so Final
  falsely reported zero. A recorder observes only each aggregate consumer and
  its sibling reservations, sharing one Gauge through sequential spill replay.
  Original reservation amounts, grants, consumer identity, task pools, errors
  and cancellation policy remain unchanged. Metric tests now also compare
  complete results with original Spark 4.2.1 under `ANSI=true`. The metric is
  declared reservation peak, not RSS. Fixed metadata and atomic bookkeeping
  add some overhead; no zero-overhead or performance claim is made.
- **Reactivate six Bloom-filter fixtures on Spark 4.2.** Register the real Spark
  aggregate/expression with its `system.builtin` registry key and clean it up
  correctly. Remove obsolete Spark 4.2 cancellations without weakening their
  original SQL/native assertions. This is fixture compatibility, not a new
  Bloom-filter implementation.
- **Correct flattened-AND coverage reporting.** Tag native AND control only
  after filter conversion succeeds; accepted Spark fallbacks gain no false
  native coverage. Differential tests cover native conjunction, a single
  predicate and the shared-ANSI-cast fallback. TPC plan snapshots are generated
  by actual executions and reviewed against approved fallback behavior, never
  changed merely to hide a new incompatibility. All five profiles have passed generated-plan and ordinary verification. These
  assertions validate plans, not execution of the TPC query results.
- **Keep old-profile adapters neutral on Spark 4.2.** Reuse the existing
  expression shim and SQLConf entry where older Spark lacks the newer API.
  A test helper preserves values returned inside `withSQLConf` on Spark 3.4,
  including nulls, exceptions and configuration restoration. Empty scan fixtures
  use Spark's planner with explicit type/attribute/empty-row assertions. Original
  oracles, inputs and broadcast assertions remain intact; no reflection or
  version-specific bypass is introduced for that fixture.

## Local evidence at this checkpoint

| Check                                | Actual recorded outcome                                                                                             |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| Native metric/helper/replay tests    | 4 distinct registrations passed; no failed, canceled or ignored final selected case                                 |
| Spark 4.2.1 SQL/coverage/Bloom tests | 11 passed: two restored metric cases, six Bloom fixtures and three coverage cases                                   |
| Original TPC plan assertions         | q2, q3 and q77a passed after the AND counter correction; these are plan assertions, not query-result execution      |
| Strict workspace Clippy              | Passed with vendor included and `-D warnings`                                                                       |
| CI wiring controls                   | Eight injected Maven success/failure cases preserve exit status; empty/truncated native-window evidence is rejected |

The former zero-peak failure and unsuccessful intermediate compilation/plan
attempts remain recorded. They are not counted as passes. All-profile compilation and ordinary generated-plan verification passed. Final
remote qualification remains a mandatory post-commit gate.
Do not add overlapping cohorts or helper-control executions to runtime totals.

Evidence is under `${MIGRATION_AUX_ROOT}/20261121_comet12-g10`, including
`20261121_metric-audit`, `20261121_ci-audit` and `20261121_coverage-audit`.
The changed-source manifest is bound by SHA256 in `G10-results.json`; the final
external receipt binds these bytes and complete per-run inventory to the exact Git SHA.

## Final source freeze and acceptance

Prepare one coherent candidate with the finalized source, tests, workflows,
documentation and explicit pending post-commit gates. Freeze its Git SHA before
launching the full agreed GHA, CodeQL/Miri and RC Build & Smoke. Qualify that same
SHA on native Linux AMD64/ARM64 and download the resulting Core/Delta JARs.

CI and RC verdicts are **post-commit facts**. Record them in immutable external
receipts and the delivered `release-qualification.json`, with run IDs, SHA256,
source/build provenance and actual loaded-artifact smoke evidence. Do not amend
the candidate merely to change this report from pending to passed: that creates
another SHA and invalidates exact-source provenance. Any subsequent source change,
including a source amendment, requires renewed qualification of the resulting SHA.

Before reporting completion, reconcile every validation-backlog item with actual
passing evidence or an already approved applicability exclusion, such as the
unavailable HDFS cluster. Confirm full agreed tests and quality checks actually
ran, including full fuzz and native-window evidence. A green aggregate alone,
skipped jobs or successful artifact upload is insufficient.

Report the final diff, remaining limitations and verified JAR locations **before
merge and stable tag creation**. `branch-1.2`, main, release tags, external artifact
publication and production remain outside this stage's write scope. Datasci access
remains prohibited.
Preserve cleanup identities and shared-cache ownership in the ledger; do not
delete source checkouts, evidence, build outputs or shared caches automatically.

The release manifest now describes build provenance and points to final qualification receipts instead of the obsolete G01-incomplete status.

The Scala compiler maximum heap is raised from1GiB to2GiB after a real Spark4.1 test-source compilation OOM. This only changes the forked build compiler; no Spark job/runtime memory settings or dependencies change.

## All-profile local plan verification

| Spark profile | Ordinary cases passed | Generation cases passed |
| ------------- | --------------------: | ----------------------: |
| 3.4           |                   129 |                     135 |
| 3.5           |                   129 |                     135 |
| 4.0           |                   129 |                     135 |
| 4.1           |                   129 |                     135 |
| 4.2           |                   129 |                     135 |

Generation uses the existing documented procedure, oldest profile first. Ordinary
runs then compare actual plans to the generated snapshots. The independent review
attributes changed eligibility to previously approved evaluation-order and mixed
aggregate-buffer fallbacks. No new unsafe path is admitted and PR #5889 remains
excluded. Original q2/q3/q77a plan oracles also passed before regeneration.
The focused final C2R/pooling and conditional-evaluation regressions passed on
Spark 4.2.1, retaining original Spark result/error and native/fallback assertions.

## Early remote quality feedback

The first candidate `68133883ea32f6ab8c758f2b9d87f4aa7a36e642` exposed syntactic
and semantic Scalafix findings and strict Scala 2.12 warnings. The affected CI,
Miri and RC runs were stopped promptly; no canceled run counts as qualification.
CodeQL passed that earlier SHA and must run again for the final candidate.

The corrections remove redundant string-interpolation prefixes and unused
imports, explicitly discard three already ignored return values, and make
existing numeric and tuple adaptations explicit, including test fixtures.
Thirty-seven Delta dollar-placeholder literals use constant concatenation: both
Scala compilers confirm identical original UTF-8 bytes and strict-lint success.
The older Scalafix automatic rewrite would change those values, so its unsafe
suggestion is rejected and retained as a negative control.
Two Bloom fixture identifiers retain their inferred type explicitly for Scalafix.
Two unused default SPI
parameters retain their required signatures with parameter-local annotations.
Existing shuffle-manager annotations address Scala 2.13 private-type warnings;
the inherited unused-annotation exception now selects the two exact source files,
because this compiler diagnostic has an empty method site. Compiler controls
confirm an unrelated source file still fails. Remaining warning categories and
all SQL/result/error/native assertions stay strict. No execution algorithm,
configuration default, calibrated cost or public method signature changes.

The exact strict Spark 3.5 + Delta reactor and Spark 4.2 + Delta semantic Scalafix
are checked locally before publishing the replacement candidate. Actual command
receipts and source hashes are retained externally. The replacement SHA still
requires complete CI and exact-SHA dualarch RC qualification.

## Complete final-JAR integration coverage

The first two candidates remain historical; stopped CI/RC/Miri runs never count
as successful acceptance. The final backlog audit required direct packaged linkage
checks beyond the original 12 G01 cases. The RC workflow now selects 23 existing
test registrations per architecture: the original 12, UDF pool (1), opaque Delta
serialization (2), cost/mixed/ANSI (4), wide sort (1), wide shuffle (1), wide decimal
range (1), and real default Kryo broadcast (1). Full functional CI runs all agreed
suites; packaged smoke checks representative linkage of the final bytes.

Each group proves 17 unique classes originate in the final Core JAR. Both Delta
groups also prove two final Delta classes and the actual bundled native hashes.
Support archives cannot shadow production classes. Receipt validation rejects
incomplete groups, pending/skipped cases, wrong/duplicate origins and duplicate
JSON fields, while preserving the original source/lock/native/JAR guards. The
obsolete G01-only export scope is replaced with truthful targeted final-feature
coverage and a separate full-CI gate.

All 11 additional registrations passed locally from the same Darwin shaded JARs;
one real native Delta case also passed the new 19-class probe. These are prechecks,
not Linux or final-SHA qualification. Runtime fault controls (11/11) and receipt
contract controls (18/18) passed. Python 3.8 grammar/API review passed; actual Focal
execution remains a remote gate. Production code and dependency pins remain
unchanged from `040f83bac`.

The existing `CometBroadcastDefaultKryoSuite` was a second class omitted from
filename-based workflow lists. Both Linux/macOS lists now run its two AQE cases.
Independent source and compiled inventories found all 17 concrete secondary suites
listed and no other unexpected runnable omission. All 15 backlog entries have
explicit evidence requirements in the external acceptance map. Actual final gates
and source/JAR receipts must pass before merge or stable-tag approval is requested.

## Miri codec compatibility and final execution order

A new dictionary writer regression reached `ZSTD_createCCtx`, which Miri cannot
interpret. The diagnostic was an unsupported C call, not undefined behavior.
Only that test's codec fixture is adapted: ordinary native testing retains the
original uncompressed/LZ4/Zstd matrix; Miri retains uncompressed and pure Rust LZ4.
Both writer entry points, all row/dictionary/checksum/byte assertions and all 19
selected test registrations remain. No ignored test or Miri validation flag is
added. Exact reverse reconstruction preserves the production prefix and all other
source bytes. Native tests (19/19), formatting and strict shuffle Clippy passed.

An official dated nightly/Miri installation added about 520 MiB while leaving
stable as default. Local sysroot setup was blocked by sandbox cache permissions;
the user rejected a one-off escalated setup. No local Miri test executed and no
cache was relocated. The F700 candidate therefore ran Miri GHA first, followed
by full Comet CI and the 23-case dualarch RC after Miri succeeds, all on the same
frozen SHA. CodeQL runs automatically. Previous interrupted runs do not qualify
the final source. All actual remote verdicts remain mandatory post-commit facts.

## Packaged test-support ABI correction

Candidate `f700491ad` passed CodeQL and both actual Linux Miri jobs
(19 unsafe-row tests and 88 hash tests, zero failed or ignored). Its RC passed
the original 12 cases on both architectures, then the new UDF fixture aborted:
the production Core JAR relocates Protobuf, while automatic support preparation
copied unrelocated test bytecode. The earlier local precheck had explicitly shaded
that support copy and therefore missed the automatic preparation gap. The running
full CI was canceled immediately; neither unfinished CI nor failed RC qualifies
this source. Their receipts and the raw failing fixture remain preserved.

Automatic preparation now uses the existing offline Maven Jar/Shade plugins to
relocate only Core and Delta test support, using the exact resolved release POM
policy, including the Arrow C exclusion and service transformer. Production JAR
hashes are checked before and after preparation. Normal developer classpaths,
production code, dependency pins, all 23 selectors and all result/error/native
and 17 Core plus 2 Delta class-origin assertions remain unchanged.

The actual new helper reproduced the raw-support failure, then prepared both
support JARs and passed all 23 unchanged cases across 11 groups on the shaded
Darwin JARs, with zero failures/cancellations/ignored/pending cases. Production and
dependency bytes remained identical. The 14 support contracts and 18 export
contracts passed, as did Python 3.8 grammar and source-scope checks. This is a local
ABI precheck, not final Linux acceptance. Evidence is under
`${MIGRATION_AUX_ROOT}/20261121_comet12-g10/20261121_packaged-protobuf-fix`.

The replacement candidate repeats every final gate on its own frozen SHA.
Because this fix changes only test-support packaging and its contracts, and
Miri has now actually passed the identical native sources, Miri, full Comet CI
and dualarch RC may run in parallel, with automatic CodeQL. All actual final
verdicts, backlog closure and downloaded-artifact verification remain mandatory
post-commit gates. Report before merge or stable tag; protected refs stay unchanged.

## Existing scan and SQL contract qualification

The first full local Spark 4.2 correction cohort registered 1,781 cases:
1,393 passed, 14 failed, 373 were canceled and one was ignored. The scan portion
had 422 passes plus explicitly classified nonpasses; all 203 aggregate cases
passed. SQL
failures exposed stale plan expectations under the accepted G06/G08 safety
fallbacks; the reported failures did not show a Spark row-result mismatch.
This is finite evidence, not a claim of universal equivalence.

Per-query SQL expectations now require the observed Spark boundary and reason,
retained native file scans, and native execution for all other operators. Actual
AQE stages are inspected so wrappers cannot hide an unexpected Spark operator.
Negative controls reject missing scans, wrong reasons and unlisted operators.
Every original SQL statement, input, configuration and error expectation is
preserved; only assertion directives change. Already supported empty-array
ARRAYS_ZIP cases retain their original native assertions. ANSI-specific routes
retain the original fully native requirement when ANSI is enabled.

The aggregate regression keeps the original shared-negation query as an explicit
accepted fallback check. An equivalent query over a materialized negative input
independently matches original Spark and requires native Partial/Final aggregates.
Its input values, exact variance expectations and numerical tolerance are preserved.
The only production change in this correction series copies fallback diagnostics
from a synthesized Divide guard after conversion has already returned None; it
changes neither eligibility nor execution.

Two inherited Parquet coverage gaps were restored. The missing optional parent
with a required child passes both dictionary variants. The NullType case passes
all four off-heap/legacy combinations. Both keep the original Spark oracle, add
explicit expected rows and prove full native DataFusion scans; no production
reader change was needed. The stale NullType cancellation is replaced by the
actual Spark 4.1+ Parquet writer capability guard. Both restored cases also pass the complete Spark 4.1 local profile.

Later targeted cohorts overlap the first run and must not be added together.
The completed correction reconciliation records 1,782 unique case keys across
cohorts, including one new AQE control: 1,410 framework-success registrations and 372
canceled cases, with no unresolved case. Twenty framework successes are explicit
version-N/A SQL fixtures: their unchanged guard logs and returns before executing
the SQL body. Exact case/configuration keys and 20 actual guard log lines refine
the union to 1,390 executed positive outcomes, 20 version-N/A registrations and
372 separately classified cancellations. No N/A case receives executed-pass credit. All 17 modified SQL file/configuration
registrations passed their complete current resource; posexplode passed only on
its final full-file retry. Failed earlier query prefixes receive no acceptance
credit. This is not one full 1,782-case run. The remaining profile/strict and current-source replay gates must pass before
this checkpoint is applied; final same-SHA remote receipts control qualification.

The user confirmed Parquet and Delta workloads and no Iceberg workloads. The
expected remaining target-scan cancellations are 358 Iceberg cases outside that
workload and 14 genuine older-Spark version cases; they are not passes. All
32 agreed older-profile Iceberg jobs remain mandatory. Rust's four HDFS tests
and three manual benchmarks are a separate applicability policy. The 58-card
migration scope is unchanged.

The completed local gate index records actual profile, TPC plan, strict-quality
and current-source replay results, preserving every failed invocation and any
bounded source-equivalent cohort composition. A logged Spark-version guard is
not executed coverage even if its framework registration reports success. Full GHA and final artifact gates remain open
until their own exact-source receipts pass. All final CI and 23-case AMD64/ARM64
packaged smoke results must bind the same newly frozen source and final JARs.
The 7b9b135e candidate's runs or artifacts remain historical. Final artifact
download/verification, backlog closure and reporting before merge or stable tag
are still required; production remains unauthorized.

Iceberg setup now receives a root-model-only preflight with a bounded retry for
the observed, known immutable Apache parent POM lookup failure. Install and tests
remain single-shot; compiler and test failures are not retried. The existing Lint
retry budget is unchanged. No mirror, dependency or cache policy changes were
introduced. The earlier log proves an artifact lookup failure, not its exact HTTP
status or recovery; actual remote success must still be observed.

The new native-scan assertion helper uses explicit Seq[Class[_]] element types and
immutable .toSeq normalization for Scala 2.12/2.13 compatibility; its class set, routing requirements and row assertions
are unchanged. Existing TPC plan baselines are refreshed only with the official
generator for newly propagated fallback diagnostics on already Spark operators.
Their exact profile/case disposition and ordinary non-generation verification
remain bound to the completed local gate receipts. No plan eligibility or SQL
semantics is changed by these test-only compiler and diagnostic-baseline fixes.

The official plan generator passed 135 cases on each of Spark 3.4, 3.5, 4.0,
4.1 and 4.2; ordinary verification then passed 129 on each profile. The six-case
difference is the existing generator-only coverage policy. Only shared extended
plan diagnostics changed for q49/q66 on Spark 3.4/3.5; a Spark 4.0 q66 override
preserves the prior effective Spark 4.x plan. Three shared base files and that
one override changed. These are plan checks, not SQL row-result counts.

## Shuffle UDF key transport correctness follow-up

The cf86a3b4 full CI exposed five Spark 3.4 UDF SQL runtime failures. Hash/range
key serialization happened after the task snapshot, leaving key functions outside
the transported pool. Two Spark 4.2 old-source regressions reproduce missing-pool
failures. This is a runtime correctness defect in the incomplete G08 migration,
not an assertion-only adjustment.

A separate controlled test uses two isolated classloader copies of the process-local
pool to model independent ID spaces; it does not launch two operating-system
processes. Mixed IDs can resolve the wrong compatible function (111 instead of 222). This
is evidence of a real source-derived hazard; the audit did not observe an actual
production job silently writing incorrect results. The old Joom tag attached pools
to the complete writer plan, so this finding does not establish the same old
production defect.

The correction prepares hash/range key protos on the driver, captures every
child/common/key function together and reuses those exact protos on executors.
Global fallback remains disabled. Hash seeds, partition counts, range boundaries,
shared evaluation context, SQL configuration, live per-action snapshots and task
cleanup retain their contracts. Key contexts also reuse the existing interner.
Tests compare exact Spark rows/partition IDs and mutable-action snapshots.

The first fixed Spark 4.2 six-suite run passed 130 cases, before final interner
and Scala 2.12 annotation updates. Its passing outcome is preserved with that
source limit; the first Spark 3.4 strict attempt failed compilation and ran no
tests. Actual final source-bound local gates are recorded separately.

Packaged acceptance grows additively from 23 to 25 cases per architecture: two
existing hash/range UDF shuffle tests and five additional Core origin classes.
All old cases/origin checks remain, giving 12 groups, 22 Core and two Delta
classes. The seven-job RC workflow structure is unchanged. All 36 project Python
controls passed; they do not replace actual packaged JVM runs.

The cf86a3b4 CodeQL, selected Miri107 and RC23/download checks passed, but remain
historical old-source evidence; its full CI was canceled after the runtime
failures. Fresh same-SHA full CI, CodeQL, Miri, RC25 on AMD64/ARM64 and downloaded
final-JAR verification are mandatory. No old JAR is a final corrected deliverable.
All 58 active cards, historical H01, prior decisions and 15 backlog requirements
remain preserved. Merge, stable tag and production remain unauthorized.

The original five Spark 3.4 CI SQL fixtures remain a final remote gate, not a
claimed local external-SBT run. Each unchanged registration must actually pass
in the next same-SHA full GHA. Local direct hash/range reproductions and pool
suites cover the corresponding runtime path. A later context-test-only failure
expected a full query string where original Spark legitimately reports cropped
context; the corrected oracle compares actual Spark structured context while
retaining error-class and SQLSTATE checks. Its failed attempt stays preserved.

Runtime qualification grows additively from 94 to 98 required cases: the three
pool/key regressions and the new key-context interner test. All previous cases,
88 native window actions and 15 backlog obligations remain required. Final
source-bound local proof records the seven Spark 4.2 suites and the explicitly
composed Spark 3.4 result; a failed 33-case invocation is never relabeled green.
