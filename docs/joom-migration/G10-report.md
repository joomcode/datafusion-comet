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
