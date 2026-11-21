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
