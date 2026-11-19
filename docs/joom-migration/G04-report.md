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

Status: G04 implementation/local qualification are complete and the user approved
publishing `joom/1.2-native-aggregates` and integrating it into `branch-1.2` on
2026-11-20. The base is accepted G03 `af05c94e7`. Acceptance documentation is
folded into the final existing G04 commit; all qualified non-migration bytes are
preserved. Actual final commits/tree and successful local/remote integration are
recorded after commit in `${MIGRATION_AUX_ROOT}/20261120_comet12-g04-integration/completion.json`.
G05, full per-group GHA, main/tag changes, publication and production use remain
unauthorized. Earlier review holds below are historical; final qualification
requirements are unchanged.

## Scope and retained upstream behavior

| Card | Implementation                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D10  | Keep upstream Single/Final + MergeAsPartial and Spark aggregate-state codecs. Never restore the old PartialReduce routing; distinct merging must emit each key once under spill.                                                                                                                                                                                                                                                                                              |
| D06  | Recover initial spill reservation refusal and account already allocated fully ordered state without treating future materialization as an existing allocation.                                                                                                                                                                                                                                                                                                                |
| D07  | Guard 32-bit group-key offsets across layouts and stream routes, including legacy grouping sets; resume evaluated input exactly once. Include the hidden grouping ID in spilled/replayed physical keys.                                                                                                                                                                                                                                                                       |
| D08  | Estimate variable-state/key/index emission, retain pending output/workspace grants, and request genuinely new memory fallibly. Adaptive spill replay reserves output and minimal merge jointly.                                                                                                                                                                                                                                                                               |
| N09  | Copy collect_list runs without per-run array slices; preserve upstream DISTINCT/codecs, nested fields, nulls, ordering and empty structs.                                                                                                                                                                                                                                                                                                                                     |
| D09  | Materialize a completed ordered range once and slice it before reading more input. Final MergeAsPartial keeps raw pending states and packs returned state structs shallowly.                                                                                                                                                                                                                                                                                                  |
| N04  | Use grouped unordered FIRST/LAST with ignore-null semantics and supported primitive/string/binary types; retain upstream fallback implementations for other types.                                                                                                                                                                                                                                                                                                            |
| N05  | Use grouped bool_and/bool_or for Boolean MIN/MAX. Keep the original MIN/MAX selector for windows, including sliding retraction.                                                                                                                                                                                                                                                                                                                                               |
| N06  | Retain upstream native string MIN/MAX and default-collation rules; validate Joom cases and unsupported-collation fallback.                                                                                                                                                                                                                                                                                                                                                    |
| N07  | Support default-collation string values for MIN_BY/MAX_BY; retain ordering restrictions and newer float normalization. Cache retained-row sizes instead of scanning all groups per input batch.                                                                                                                                                                                                                                                                               |
| N08  | Keep upstream SortAggregate and required distribution/restore-sort contracts. Preserve ordering by default. Omit grouping sorts only for COUNT and nonfloating MIN/MAX below a proven unordered shuffle boundary, crossing only deterministic Project/Filter, with deterministic aggregate arguments/filters/results. Keep ordering for FIRST/LAST, MIN_BY/MAX_BY, SUM/AVG, floating extrema and collect aggregates. Reconstruct required sorts on every Spark fallback path. |

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

## Initial local validation, before the independent review

- **559 unique native checks passed** (558 unit tests plus the original memory fixture), zero failed/ignored, using the root shared lockfile and combined Comet/vendor/expression graph.
- **330 JVM/SQL tests passed**, zero failed or aborted; 27 canceled and 2 ignored cases are tracked separately. Spark4.2.1/native comparisons cover results and mandatory ANSI errors, mixed SUM/AVG stages, strings, ordering/fallback, DISTINCT spill, first/last/bool differential queries, and G02 task cancellation/memory waits.
- The original memory fixture checks 524288 values/65536 groups, exact keys/payloads, actual spill and cleanup at its unchanged 130.8 MiB pool. See `emit-memory-final.log` for allocation/reservation peaks; they do not measure container RSS.
- Native build and Maven test compilation pass; dylib SHA256 matches the Maven runtime resource. Formatting, CI-config/suite registration, strict Comet Clippy and source-license checks have receipts. Strict workspace/all-targets Clippy now passes with unchanged `-D warnings` flags and no command-line lint allowances after the user-approved statement-local `fetch_update` exception. The old diagnostic and failed strict receipts remain historical; see the follow-up below.
- The original 61-expression pressure query keeps its 107374-byte pool and batch64. Added variable FIRST/LAST14 plus sorted collect_list IDs select spill-capable ObjectHash/native hash; this new family uses 1 MiB. Both require actual aggregate spill, native execution and exact results. Diagnostics show why the added wide workload cannot use the old tier with the current bounded algorithm: decoded input98069 + even one-row encoding10324 =108393 >107374; a 256 KiB probe also cannot seat an unsplittable75281-byte decoded run. These are reservation minima, not RSS impossibility claims. The dedicated native regression still uses107374 bytes. Mixed75 functional queries remain; only grouped pressure shapes receive the collection sentinel. No original test limit, production fairness rule or correctness assertion was weakened. Details and failed probes: `${MIGRATION_AUX_ROOT}/20261119_comet12-g04/variable-pressure-tier-rationale.json`.

- Existing Criterion `aggregate_misc_pair/max_by` quick dev run measured 40.653–41.534 ms (estimate40.829 ms). This is diagnostic, not A/B or a production-job estimate. The original FIRST/LAST benchmark is restored; no separate performance framework was introduced.

## Remaining qualification and review gate

Local evidence is macOS ARM64/Java17/dev-profile evidence. Final full agreed Linux CI and release-feature qualification plus packaged smoke of exact final JAR bytes on native AMD64/ARM64 remain mandatory after all groups.

Six default fuzz cases pass;25 opt-in cases remain for explicit final execution. Ensure `comet.test.aggFuzz.full=true` reaches the forked JVM, for example via `JAVA_TOOL_OPTIONS=-Dcomet.test.aggFuzz.full=true`. Verify every full case executed: default full GHA alone does not enable them.

At the initial checkpoint, two inherited mixed BloomFilter planning tests canceled on Spark4.2. The subsequent independent audit adapted their function identifiers to the required Spark4.2 namespaces; both now pass. A new exact-byte Spark comparison also passes for fully native and both mixed directions. Two pre-existing metric tests remain ignored and require final execution or an explicit applicability disposition; neither counts as a pass.

The prior local strict-workspace deprecation blocker is resolved by the approved source-local exception; final Linux strict Clippy remains required. G04 added no temporary CI disablement: its workflow changes permanently register the fuzz suite on Linux/macOS. Audit actual final CI as agreed before acceptance.

Raw commands, source hashes, failed intermediate attempts, reviews and provenance are under `${MIGRATION_AUX_ROOT}/20261119_comet12-g04`. Historical receipt log paths are resolved by `historical-log-relocations.json`; raw receipts are preserved unchanged. See `G04-results.json`, `STATE.json` and post-commit `completion.json`. Earlier failures are historical evidence, not passing results for final code. Approved feature-register/plan files remain planning snapshots; STATE and this report record execution.

The three local content commits separate grouped kernels, native emission/replay with collect_list, and JVM ordering/string support plus evidence. Exact commit IDs and tree are in the external completion receipt.

`branch-1.2`, main and RC1 are unchanged. The feature is local and awaits review before integration. No push, full GHA, new tag/release artifact, publication, production use or G05 work is included.

## Approved Clippy exception follow-up, 2026-11-19

The user approved retaining the DataFusion atomic update and allowing its renamed-API deprecation only on that statement. An explanatory `allow(deprecated)` is paired with a statement-local `expect(clippy::allow_attributes)` because supported older Rust versions do not report the deprecation. No algorithm, atomic ordering, dependency, toolchain or CI flags changed. This is a permanent approved exception, not a temporary CI workaround. All other strict checks remain.

Local strict workspace/all-targets Clippy passed in 57.29 seconds; formatting passed. Receipts and a byte-for-byte proof that removing only the attributes restores the original source are in `${MIGRATION_AUX_ROOT}/20261119_comet12-g04-clippy-exception`. The original runtime/test/library receipts precede this lint-only annotation and are preserved; no fresh runtime or release-JAR qualification is claimed. Final Linux CI and all other validation backlogs remain required. G04 still awaits integration approval; branch-1.2, main and RC1 remain unchanged.

## Independent Spark-semantics review follow-up

The user requested an independent adversarial review before integration. It found a real successful-but-wrong-result case in the new ordering optimization, plus weaknesses in several new test oracles. The correctness fix and strengthened checks pass locally: 53 JVM/SQL and 239 native tests, with 25 opt-in full fuzz cases still deferred. The two BloomFilter planning cancellations are closed. Original qualification receipts remain historical. See [G04-independent-semantics.md](G04-independent-semantics.md) and `G04-results.json#independent_semantics_review` for the exact source/provenance, negative controls, changes and limits. G04 remains unmerged; G05 and production use remain unauthorized.

## G04 acceptance and integration, 2026-11-20

The user approved the feature push and integration into branch-1.2, followed by verification and the final commit list. Five substantive commits are retained: grouped kernels; aggregate emit/replay memory; SortAggregate/string selection; the approved narrow Clippy exception; and the independent-review ordering fix. Acceptance/state documentation is folded into the last commit without changing any qualified non-migration file. No new merge or service commit is added. The original qualified HEAD `36a8e7b79` remains reachable through the local backup ref `refs/joom-migration/backups/g04-before-integration-20261120`; previous receipts remain intact.

The separate mixed BloomFilter recheck passed 3/3 on the qualified source, with no cancellations. Its exact-byte Spark comparison and both mixed directions are preserved in `${MIGRATION_AUX_ROOT}/20261119_comet12-g04-independent-semantics/20261120-mixed-bloom-completion.json`. Initial and follow-up test cohorts overlap and must not be summed. Twenty-five opt-in fuzz cases, the two inherited metric-test dispositions, final Linux/full agreed CI and exact final-JAR AMD64/ARM64 qualification remain required. G05 starts only after separate user authorization. Integration proof is in `${MIGRATION_AUX_ROOT}/20261120_comet12-g04-integration/completion.json`.
