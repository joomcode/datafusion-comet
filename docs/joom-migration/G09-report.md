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

# G09: cost-based engines and boundary formats

G09 is implemented on `joom/1.2-cost-based-engines` from accepted G08
`2dc13d5bd8697d32d8439e970fa1644cd0984d0e`. Local targeted qualification is complete.
The user approved publication and integration into `branch-1.2` on 2026-11-21.
The two substantive commits preserve the exact qualified non-migration source
from `00f5c17bcc8d5615f631bee7e3e9ebb0c1958fa0`. Actual local/remote refs, the final tree and preservation
proof are recorded after commit in `${MIGRATION_AUX_ROOT}/20261121_comet12-g09-integration/completion.json`.
The original implementation receipt and initial review gate remain historical.
G10, full per-group GHA, main/tag/release changes, production and datasci work
remain unauthorized; final complete qualification is still mandatory.

## Preserved contracts

| Card | Result                                                                                                                                                                                                                       |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| J01  | Whole-plan engine selection with initial/AQE decisions, table overrides, per-operator weights, explanations and the final tag13 calibration. `EngineCostTable.scala` and `LeafColumns.scala` remain byte-identical to tag13. |
| J02  | Shared producer/consumer boundary selection, Spark shuffle between Spark operators, native/JVM-columnar alternatives, compatible co-partitioning, reused and materialized exchanges, root shuffles and subqueries.           |
| J03  | Separate schema-based wide-sort fallback, payload leaf threshold and persistent AQE decision; inactive when cost selection is enabled.                                                                                       |
| J04  | Separate wide-shuffle fallback, excluding partition-key leaves, for both native and JVM-columnar paths; inactive when cost selection is enabled.                                                                             |
| I07  | Use upstream native wide-decimal hashing, including precision above 18, without restoring the old restriction. Verify mixed writers, exact partition assignments, NULLs and signed-byte boundaries.                          |

Library defaults are unchanged from tag13: cost/boundary/wide-sort modes are off;
wide-sort threshold 50, wide-shuffle threshold 0, native fallback weight -1 and Spark
weight 0. No recalibration or automatic change to analytics configuration is made.
The approved feature register and plan remain immutable scope snapshots.

## Necessary adaptations and correctness fixes

- Preserve the current split planner, cache/plan-only rules, `queryStagePrep`
  behavior, distribution delegation, AQE coalescing and native-shuffle specs.
  Whole-plan conversion is an additional context; it does not replace those APIs.
- Use the current `CometBaseAggregateExec` for mixed-buffer safety and partial
  preservation, including the separate upstream SortAggregate wrapper. Typed,
  decimal, TRY and other unsafe states cannot be relabelled merely by price.
- Restore operators with `sparkFallback`, including hypothetical producers used
  for pricing. Restoring a SortAggregate must put back its grouping sort; an
  inserted local TopK must disappear instead of applying the global offset and
  projection again.
- Preserve a current direct `LogicalQueryStage` link during actual restoration.
  Do not promote inherited links or copy unrelated current tags. When adding the
  restored SortAggregate's input sort, retain the original aggregate's tags: a
  case-class copy alone loses them and makes AQE map the aggregate incorrectly.
- Retain G05/G06/G08 correctness eligibility and fallbacks. The legacy cost tests
  now distinguish supported nonthrowing conditions from accepted ANSI/nullable
  guards. Nonthrowing cross-input CASE still proves native execution, price-based
  Spark selection and native restoration through a targeted price override.
- Keep native-shuffle conversion/sticky-fallback/invalid-UTF8/Celeborn admission,
  `CapturedSQLConf`, task UDF transport and writer block boundaries unchanged.
  The columnar-format eligibility helper is pure and honors those constraints.
- Evaluate native and JVM-columnar hash/range eligibility in the same complete
  key-projection CSE context as the actual writer. This admits supported nested
  expressions without bypassing G08's operator-wide evaluation-order guard.
- Correct native range equality: a key equal to a range bound stays in the lower
  partition, matching original Spark 4.2.1's strict bound-before-key comparison.
  The former non-strict comparison reproducibly shifted partition IDs in a
  fully sampled three-row fixture. Source inspection finds that comparison in
  both the pinned upstream and tag13; no historical production incident or
  ordinary join-result corruption is inferred from this finding.

These are adaptations of the tag13 features to the current architecture.
A demonstrated failure of a naive port is not evidence of an incident in the old
production runtime.

## Independent checks

`CostBasedEngineCorrectnessSuite` uses original Spark without Comet for rows,
duplicates, schema, required errors and SQLSTATE. Data includes decimal 18/19/38,
NULLs, positive/negative extremes and 127/128/-128/-129, nested decimal keys and
both AQE settings. It exercises all four cost/boundary flag combinations and
asserts actual writer/consumer paths, rather than counting fallback as native.
Six hash/range key-context cases cover native/auto/JVM modes, safe lazy IF keys
and mandatory shared-CAST errors. The tiny range fixture fully samples its input;
exact partition IDs are therefore a deterministic Spark oracle. Native writer
tests independently check equality, repeated bounds, NULL placement, descending
order and the more-than-128-bound branch. Floating range coverage retains NaN and
signed-zero cases with independently derived Spark expectations.

The suite also covers restored partial/final SortAggregate ordering, fused TopK
offset/projection, retained G08 mandatory CAST errors and the G06 suffix-window
fallback. A real planned SUM/AVG/Bloom aggregate is relabelled so Spark Partial
feeds an already serialized native Final. The Final's bytes remain identical,
its results match Spark including an all-NULL group, and COUNT is asserted to
remain outside the two-way relabelling contract.

G04 mixed SUM overflow is tested with cost disabled, default cost selection and
forced legal mixed execution. G07 skewed/staged broadcast is tested with both
cost settings and codegen.maxFields 3/100. Five G09 suites are registered in both
existing CI matrices; workflow triggers and earlier temporary CI mechanisms are
unchanged.

Five stale-scan planner fixtures explicitly construct the stale state they claim
to test; G07 correctly produces `SHUFFLE_SCAN` in the ordinary starting plan.
The post-AQE shuffle fixture checks both an all-native alias projection and the
accepted Spark ANSI-filter fallback with a native sort above the stage. It uses
enough rows to prevent Spark's legitimate one-row sort elimination. Original
row/error assertions remain in place.

## Local qualification

- 493 core JVM/SQL tests and two distinct Delta tests passed.
- 227 native shuffle tests and three core floating range/window tests passed.
- No selected test failed, was canceled, ignored or left pending.
- Core and Delta developer JARs built; bundled ARM64 native bytes match the
  newly built library. Strict touched-package Clippy and format checks passed.
- Sixteen focused repetitions and overlapping earlier cohorts are not added to
  the unique counts. Failed intermediate runs and deliberate controls are
  retained with their dispositions.

## Negative controls

The assertions and Spark oracle remain unchanged while only production
restoration statements are temporarily reverted:

1. Restoring the two old direct-copy sites successfully compiles. The no-AQE
   SortAggregate query completes with 92 rows instead of Spark's 7; the AQE case
   violates required input ordering. Both TopK cases fail attribute binding.
   This combined experiment does not isolate the hypothetical pricing site as a
   cause of incorrect SQL results.
2. Removing only restored SortAggregate tag preservation reproduces AQE's extra
   Partial/attribute-binding failure.

All mutated sources are restored byte-for-byte from hashed positive backups
before qualification. Failed controls are expected evidence, not passing tests or
remaining defects. See the separate raw receipts and restoration proof.

## Performance and remaining gates

The final tag13 calibration is preserved, rather than asserted optimal for the
new runtime. The standard Spark Benchmark platform query `sysctl` is still
blocked inside the sandbox. No replacement benchmark framework, outside-sandbox
run or unsupported job-level speed estimate is introduced. Real GCP performance
validation remains with the user under the existing policy.

Local evidence is macOS ARM64/Java17/Spark4.2.1 and the existing debug native
library. Developer JARs are not final Linux release artifacts. Final full agreed
CI/fuzz/profile coverage, removal of all earlier temporary CI mechanisms and
packaged smoke of the exact final Linux AMD64/ARM64 JARs remain mandatory.
No full GHA, new release tag or G10 is started by this stage.

Detailed commands, counts, hashes, failed attempts, source reviews and cleanup
records are in G09-results.json and
`${MIGRATION_AUX_ROOT}/20261121_comet12-g09`.
