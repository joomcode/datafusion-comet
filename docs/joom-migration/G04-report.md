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

# G04: native aggregate states, memory and ordering

Status: implementation and local targeted qualification are complete in
`joom/1.2-native-aggregates`, based on accepted G03 `af05c94e7`.
The user requires a report before integration. No G04 merge, push, full GHA,
release tag, publication, production use or G05 work is authorized by this stage.

## Scope and retained upstream behavior

| Card | Implementation                                                                                                                                                                                                                                                                                                                                                                                          |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D10  | Keep upstream Single/Final + MergeAsPartial and Spark aggregate-state codecs. Never restore the old PartialReduce routing; distinct merging must emit each key once under spill.                                                                                                                                                                                                                        |
| D06  | Recover initial spill reservation refusal and account already allocated fully ordered state without treating future materialization as an existing allocation.                                                                                                                                                                                                                                          |
| D07  | Guard 32-bit group-key offsets across layouts and stream routes, including legacy grouping sets; resume evaluated input exactly once. Include the hidden grouping ID in spilled/replayed physical keys.                                                                                                                                                                                                 |
| D08  | Estimate variable-state/key/index emission, retain pending output/workspace grants, and request genuinely new memory fallibly. Adaptive spill replay reserves output and minimal merge jointly.                                                                                                                                                                                                         |
| N09  | Copy collect_list runs without per-run array slices; preserve upstream DISTINCT/codecs, nested fields, nulls, ordering and empty structs.                                                                                                                                                                                                                                                               |
| D09  | Materialize a completed ordered range once and slice it before reading more input. Final MergeAsPartial keeps raw pending states and packs returned state structs shallowly.                                                                                                                                                                                                                            |
| N04  | Use grouped unordered FIRST/LAST with ignore-null semantics and supported primitive/string/binary types; retain upstream fallback implementations for other types.                                                                                                                                                                                                                                      |
| N05  | Use grouped bool_and/bool_or for Boolean MIN/MAX. Keep the original MIN/MAX selector for windows, including sliding retraction.                                                                                                                                                                                                                                                                         |
| N06  | Retain upstream native string MIN/MAX and default-collation rules; validate Joom cases and unsupported-collation fallback.                                                                                                                                                                                                                                                                              |
| N07  | Support default-collation string values for MIN_BY/MAX_BY; retain ordering restrictions and newer float normalization. Cache retained-row sizes instead of scanning all groups per input batch.                                                                                                                                                                                                         |
| N08  | Keep upstream SortAggregate and required distribution/restore-sort contracts. Preserve ordering by default. Omit grouping sorts only for COUNT and nonfloating MIN/MAX below a proven unordered shuffle boundary, crossing only Project/Filter. Keep ordering for FIRST/LAST, MIN_BY/MAX_BY, SUM/AVG, floating extrema and collect aggregates. Reconstruct required sorts on every Spark fallback path. |

The narrower unused-sort optimization is intentional: avoiding changes to
floating rounding, intermediate ANSI overflow and order-sensitive selection has
priority over removing every sort from the older implementation.

Upstream mixed SUM/AVG protections remain in place. The source audit confirms
that the SUM serde and native decimal-SUM protections already retain the old
Joom behavior; newer upstream AVG safety checks are preserved. Regression tests
compare both results and mandatory errors across native/Spark boundaries with
ANSI enabled.

## Correctness findings closed locally

- Ordered Partial emits intermediate states on reservation refusal rather than failing a recoverable tiny-pool query. Final merges repeated active keys. FIRST/LAST NULL boundaries and IGNORE NULLS are checked against Single without raising the pool.
- Partial/PartialReduce no longer advertise false group-key uniqueness. Final/Single uniqueness includes the hidden grouping-set ID. Grouping-set NULL expansion discards invalid inherited ordering, constants, equivalences, constraints and raw-key partitioning. Normal grouping retains valid projected ordering; true global aggregates still emit at most one row per partition.
- Root ordering remains observable when Spark has already removed its SQL sort. A disabled-guard control successfully returned keys `[9,3,7,1,5,NULL]` for `SORT BY`, while Spark returned `[NULL,1,3,5,7,9]`. Five other cases detected preservation-property violations, not wrong rows. Fixed bytes were restored; all six ORDER BY/SORT BY cases pass with AQE on/off.
- A 7 MiB/16384-group short-string test proves the initial owned-state-header allowance is necessary: the positive passes, while disabling only that allowance fails during Final replay at 4096 groups (retained2229064,emit2147064,floor2997840; total6308536+1378288 exceeds7340032). Fixed source was restored. This is distinct from the wide-data one-row encoding minimum below.
- Boolean grouped specialization retains retract-capable upstream MIN/MAX for sliding windows.
- Spill replay reserves state/output and minimal merge jointly, reshapes oversized runs under the unchanged pool, and keeps single-file reads unbuffered. Its initial grant also covers per-group owned variable-state Vec headers absent from IPC offsets; physical estimates cover decoder scratch growth, retained I/O chunks, padding and codec/alignment buffers. The writer already rebases supported arrays; an unnecessary write-side copy was removed.
- Legacy replay includes the hidden grouping ID, keeping source NULLs distinct from rolled-up NULLs. Offset guards resume evaluated grouping sets exactly once. Existing allocations may be accounted infallibly; future materialization remains fallible. A new input quantum and allocator growth can exceed logical reservation estimates; these do not bound RSS.

## Local validation

- **559 unique native checks passed** (558 unit tests plus the original memory fixture), zero failed/ignored, using the root shared lockfile and combined Comet/vendor/expression graph.
- **330 JVM/SQL tests passed**, zero failed or aborted; 27 canceled and 2 ignored cases are tracked separately. Spark4.2.1/native comparisons cover results and mandatory ANSI errors, mixed SUM/AVG stages, strings, ordering/fallback, DISTINCT spill, first/last/bool differential queries, and G02 task cancellation/memory waits.
- The original memory fixture checks 524288 values/65536 groups, exact keys/payloads, actual spill and cleanup at its unchanged 130.8 MiB pool. See `emit-memory-final.log` for allocation/reservation peaks; they do not measure container RSS.
- Native build and Maven test compilation pass; dylib SHA256 matches the Maven runtime resource. Formatting, CI-config/suite registration, strict Comet Clippy and source-license checks have receipts. Workspace Clippy with only deprecated APIs diagnostically exempted passes. Full strict workspace Clippy retains the known pristine-vendor Rust1.100 deprecation; no CI exemption or production-vendor warning-only change was made.
- The original 61-expression pressure query keeps its 107374-byte pool and batch64. Added variable FIRST/LAST14 plus sorted collect_list IDs select spill-capable ObjectHash/native hash; this new family uses 1 MiB. Both require actual aggregate spill, native execution and exact results. Diagnostics show why the added wide workload cannot use the old tier with the current bounded algorithm: decoded input98069 + even one-row encoding10324 =108393 >107374; a 256 KiB probe also cannot seat an unsplittable75281-byte decoded run. These are reservation minima, not RSS impossibility claims. The dedicated native regression still uses107374 bytes. Mixed75 functional queries remain; only grouped pressure shapes receive the collection sentinel. No original test limit, production fairness rule or correctness assertion was weakened. Details and failed probes: `${MIGRATION_AUX_ROOT}/20261119_comet12-g04/variable-pressure-tier-rationale.json`.

- Existing Criterion `aggregate_misc_pair/max_by` quick dev run measured 40.653–41.534 ms (estimate40.829 ms). This is diagnostic, not A/B or a production-job estimate. The original FIRST/LAST benchmark is restored; no separate performance framework was introduced.

## Remaining qualification and review gate

Local evidence is macOS ARM64/Java17/dev-profile evidence. Final full agreed Linux CI and release-feature qualification plus packaged smoke of exact final JAR bytes on native AMD64/ARM64 remain mandatory after all groups.

Six default fuzz cases pass;25 opt-in cases remain for explicit final execution. Ensure `comet.test.aggFuzz.full=true` reaches the forked JVM, for example via `JAVA_TOOL_OPTIONS=-Dcomet.test.aggFuzz.full=true`. Verify every full case executed: default full GHA alone does not enable them.

Two unchanged upstream mixed BloomFilter planning tests cancel on Spark4.2 with issue#4968; these guards do not prove current serde incompatibility. Two pre-existing metric tests are ignored. Final qualification must execute or obtain an explicit applicability disposition for these four cases; none counts as a pass or is silently excluded.

The existing strict-workspace toolchain backlog remains open. G04 added no temporary CI disablement: its workflow changes permanently register the fuzz suite on Linux/macOS. Audit actual final CI as agreed before acceptance.

Raw commands, source hashes, failed intermediate attempts, reviews and provenance are under `${MIGRATION_AUX_ROOT}/20261119_comet12-g04`. Historical receipt log paths are resolved by `historical-log-relocations.json`; raw receipts are preserved unchanged. See `G04-results.json`, `STATE.json` and post-commit `completion.json`. Earlier failures are historical evidence, not passing results for final code. Approved feature-register/plan files remain planning snapshots; STATE and this report record execution.

The three local content commits separate grouped kernels, native emission/replay with collect_list, and JVM ordering/string support plus evidence. Exact commit IDs and tree are in the external completion receipt.

`branch-1.2`, main and RC1 are unchanged. The feature is local and awaits review before integration. No push, full GHA, new tag/release artifact, publication, production use or G05 work is included.
