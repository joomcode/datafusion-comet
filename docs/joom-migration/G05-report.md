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

# G05: filtered sort-merge joins

G05 is implemented and locally qualified in `joom/1.2-smj-filter` from accepted
G04 `a89fea10ed661a9e76ba2cd3bdbd3a78b5b3aec8`. Review is required before push/integration. `branch-1.2`, main and
RC1 remain unchanged. No full GHA, G06 or production operation was started.

## Preserved behavior and safety adaptations

D05 carries the complete final `joom-1.1-13` filter-before-payload block: narrow
filter projections, contiguous gathering/projection reuse, streamed ordering,
LEFT/RIGHT/FULL null/duplicate/cross-freeze corrections, buffered per-key-range
caching and streamed per-index-run evaluation. The pristine 55.1 and 55.2 SMJ files
are byte-identical, so this does not discard intervening 55.2 SMJ changes.
The 12 registered old commits and retained tests/benchmarks are mapped in
`${MIGRATION_AUX_ROOT}/20261120_comet12-g05/g05-source-parity-final.json`. The three post-tag commits on the old lazy-filter
branch are outside the pinned migration scope and were not automatically adopted.

Hoisting now requires a positive deterministic/nonthrowing proof. The native
whitelist covers known primitives; the Comet bridge reuses the existing CaseWhen
proof for custom expressions, with a transparent marker inserted immediately
before SMJ construction. Rewriting drops that marker rather than trusting a
changed subtree. Unknown/UDF/random and potentially failing casts/arithmetic
stay unhoisted. An unhoisted expression can still execute natively at its original
location; unsafe eager evaluation is the restriction. Date/timestamp parsing and
other unproved fallible work are not eagerly cached, even in Legacy mode.

DataFusion 55.2 AND/OR is only heuristically lazy. Until G08 establishes full row-level
lazy evaluation, a filtered SMJ whose nested AND/OR RHS is not positively eager-safe
falls back to Spark. This is a narrow correctness guard, not a temporary CI bypass.
Native CASE/IF and safe TRY_CAST remain tested. G08 must preserve the guard until
mixed masks, NULL semantics and mandatory errors prove it can be relaxed.

Two additional hazards were corrected without copying expected results from Comet:

- A zero-column filter is evaluated with an explicit batch row count; FALSE/NULL
  cannot silently accept pairs. Direct tests compare complete typed tuples and
  duplicate multiplicities for TRUE/FALSE/NULL and four join types.
- Synthetic outer null-pairs can share a live buffered chunk. Residuals now see
  only actual equal-key candidate positions, and synthetic rows retain their
  already-null payload and order. Streamed-only hoisting obeys the same guard.
  Before correction, original Spark succeeded while native ANSI cast failed;
  after correction the exact query succeeds and rows match. Real malformed matches
  must still raise `CAST_INVALID_INPUT`.

Upstream `requiredChildDistribution` delegation is preserved and checked under
AQE/skewed REBALANCE. Existing tag13 memory accounting is retained: cache results
are allocated before accounting with infallible `grow`, and scratch/shared/FFI
buffers can exceed reservations. Drop/error paths balance declared charge; this
is not an RSS or OOM safety guarantee. No pool/fairness redesign was introduced.

## Local evidence

- **88 unique native tests passed**, zero failed/ignored, using the root shared
  lockfile and combined Comet/vendor/expression graph. SMJ spill/replay/accounting,
  streamed ordering, all outer joins, constant filters and safe-hoist/marker guards
  are covered. Overlapping test selections are deduplicated in the source manifest.
- **39 JVM/SQL tests passed**, zero remaining failed/ignored in qualified
  selections: 10 new ANSI tests, 10 default fuzz tests and 19 inherited SMJ tests.
  ANSI tests perform 193 Spark comparisons including 60 required errors; default fuzz
  performs 291 comparisons. Complete rows and multiplicities, observable ordering,
  actual native/Spark engine and real spill are asserted.
- The combined run retained passing fuzz/JoinSuite evidence separately from four
  invalid old NULL-LHS test expectations. The corrected ANSI suite was rerun in full
  and passed. Catalyst had legitimately pushed a one-side nullable conjunct into
  a RIGHT join scan, so both engines succeeded. A mixed-side CASE prevents scan pushdown. Default ON optimization
  is still checked; the additional NULL-expression check excludes only
  ReplaceNullWithFalseInPredicate identically for both engines. It tests the
  retained runtime NULL contract, not a CI exception. No expected row, required
  error or correctness assertion was weakened to accommodate native output.
- Native build, fresh bundled dylib SHA256 match, Maven compile, strict workspace
  Clippy (`-D warnings` with only the accepted local deprecated-API exception),
  Rust/Scala formatting and CI suite/config checks pass. Source-license and final
  content/manifest checks are recorded in the completion receipt.
- Restored existing Criterion `interval_Inner/4400000` benchmark measured 21.609 s
  before and 321.77 ms after (~67.16x) with identical 4.4M-pair/98-payload-column input and
  400 asserted rows. This quick dev-profile local measurement is diagnostic, not a
  real-job speed forecast. The two original custom-cast SMJ benchmarks also compile. Their temporal casts
  intentionally remain unhoisted; those direct-operator benchmarks measure the
  filter-before-payload path under the new safe eligibility rule.

The oracle is fresh original Apache Spark 4.2.1 execution without Comet, using the
same inputs/settings. It compares successful results and required structured error
class/type/SQLSTATE. Engine assertions follow semantic comparisons. Independent
source and test review reports are retained in the evidence directory. The earlier
G04 original-Spark runtime provenance remains applicable; local Maven tests use
Spark 4.2.1 runtime artifacts, not the modified Spark checkout.

## Remaining qualification and handoff

23 full fuzz cases are intentionally opt-in and canceled in the default run; they
are **not passes**. They add 4,578 comparisons. Final qualification must deliver
`comet.test.smjFuzz.full=true` to the forked test JVM (for example through
`JAVA_TOOL_OPTIONS`) and verify all 23 full cases actually execute. The default
workflow suite registration alone does not enable them.

Evidence is local macOS ARM64/Java17/dev-profile evidence. The final full agreed
Linux CI/release-feature qualification and exact final-JAR AMD64/ARM64 smoke remain
mandatory after all migration groups, with all applicable backlogs closed and
temporary CI mechanisms removed. Tests were registered permanently on both CI
platforms; no full GHA or new release artifact was produced here.

`G05-results.json` records commands, source/library hashes, retained limitations
and explicit deferred work. Raw logs, failed experiments, source proofs and cleanup
identities stay under `${MIGRATION_AUX_ROOT}/20261120_comet12-g05`. No evidence was deleted. `completion.json` records
actual local commit IDs/tree after committing. STATE, decisions and manifests retain
all earlier group acceptance and final-gate requirements. G06 requires separate user
permission. No push or merge is authorized by this completion report.

## Accepted integration (2026-11-20)

The user approved pushing G05 and integrating it into branch-1.2, with G06 still
closed. Acceptance documentation is folded into the final substantive commit;
the first commit and every qualified non-migration file are preserved exactly.
The two-commit fast-forward adds no merge/service commit. Historical qualification
counts, source hashes, failed experiments and remaining final gates stay intact.
Actual final local/remote refs, tree and preservation proof are recorded after
commit in `${MIGRATION_AUX_ROOT}/20261120_comet12-g05-integration/completion.json`.
Earlier push/integration holds in this report describe the review checkpoint and
are superseded only by this explicit approval. Full per-group GHA was not requested.
