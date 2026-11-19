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

# G03: sort spill budgets and JVM output handoff

Status: G03 is complete and accepted for integration into `branch-1.2` under
explicit user approval on 2026-11-19. The feature is `joom/1.2-sort-spill-budget`,
based on `6729883958442e865707d692f32a84e9554e4090`. The tested runtime/test sources
are unchanged. This source snapshot records acceptance; actual merge/push execution,
final local/remote refs and the tree are proved in
`${MIGRATION_AUX_ROOT}/20261119_comet12-g03-integration/completion.json`.
G04 is not authorized.

## Implemented scope

| Card | Final behavior                                                                                                                                                                                                                                                     |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| D11  | Cap the initial merge reserve at 1/32 of the effective task budget, before explicit DataFusion overrides; preserve the large-task default and on-heap behavior. Use the original memory_limit even for greedy_unified, whose pool config stores zero.              |
| D02  | Retain charged workspace across Spark grant shrink and merge passes; account for reused encoded rows/shared buffers once; preserve read-ahead, wide spill-batch splitting and downstream headroom.                                                                 |
| D04  | Sort narrow keys and gather referenced payloads once, private BinaryView conversion, 8-byte prefix with full collision comparison, tiny-batch coalescing and nested/sliced offset guards. Restore the declared output schema and Binary values.                    |
| D03  | Default spill-before-output threshold to 1/4 of the off-heap task share; default on-heap zero and explicit zero disable it. Mark only sorts reaching the JVM through streaming ancestors. Preserve grouped output spilling, precheck grants and bounded shortfall. |

The affected original vendor sort/support files match pristine 55.1 and 55.2, so the
final tag 13 residuals could be carried without overwriting different55.2 fixes.
DataFusion stays at 55.2.0, Arrow 59.3.2; lockfiles/dependencies are unchanged. Unrelated
aggregate/window/join implementations are not ported in G03.

Comet 1.2's TopK wrapper reconstructs SortExec: it now preserves the output-spill
flag through template/runtime creation, reset and child replacement. Planner tests
cover streaming ancestors, nested sorts and buffering barriers, including actual
native plans. Existing G02 cancellation and pool decorators remain in place.

## Additional ownership correction

The old late-materialized column concatenation returned source-buffer charges
while its local old.arrays still retained all original ArrayRefs. Successful
concatenation groups now drop those source references before releasing charges;
fallback pieces and shared-buffer ownership remain intact. The regression samples
Weak<Array> ownership inside MemoryPool::shrink, checks payload values and final
zero reservation. The negative control (only the fix disabled) reproduces live
counts [4,4]; the corrected source passes the expected retirement sequence.

WideBinaryPayload consumes its owned column vector rather than cloning headers.
The replaced upstream ReservationStream remains only as a test fixture, preserving
its two original regressions without an unused production wrapper. No warning
suppression or correctness-check relaxation was added to CI.

## Validation

- **371 unique Rust unit cases passed**, zero failed/ignored: the combined sort
  selection is 51 Comet + 275 vendor; additional spill-manager, retained-stream,
  JNI-lifecycle and cancellation-wrapper selections bring unique coverage to 371.
  Final targeted wide-payload cases were repeated after the ownership cleanup.
- **41 selected Spark 4.2.1 JVM/SQL cases passed**, zero failed/canceled/ignored:
  sort/child-sort parity and metrics plus G02 JNI task kill, blocked memory waits
  and TaskMemoryManager regressions. Execution cases retain ANSI=true and actual
  native-operator assertions. Final native bytes match the Maven test resource.
- The 19 native memory cases include grant shrink, multipass merging, very small
  input batches, wide rows and JVM/native-only output boundaries. Added cancel and
  early-drop cases stop an active final spill merge with real files/reservations,
  reject successful batch/EOF after cancellation, and verify bounded cleanup,
  zero Spark grants and removal of managed spill files.
- Strict Comet Clippy --all-targets/--no-deps passes. Full workspace strict Clippy
  still fails the previously known pristine DF 55.2 Atomic::fetch_update deprecation
  under Rust 1.100. The separate -Adeprecated diagnostic passes all other workspace
  lints, but **is not strict CI acceptance**. CI flags are unchanged; the unrelated
  vendor API is not changed solely for this warning, as the user requested.
- Existing Criterion evidence and its limits: G03-performance.md. Formatting,
  CI config/suite inventory, documentation manifest, source/ref audit and exact
  source RAT are recorded in the post-commit completion receipt.

Two coherent commits separate the vendor implementation from Comet/JVM wiring,
regressions and documentation; no service/checkpoint commit is added. Exact final
commit/tree identities and the tested-source comparison are recorded after commit
in `${MIGRATION_AUX_ROOT}/20261119_comet12-g03-integration/completion.json`.
The original implementation receipt for `2241a53d2` remains unchanged in
`${MIGRATION_AUX_ROOT}/20261119_comet12-g03/completion.json`.

## Remaining qualification and evidence

Native Linux/release coverage and exact final-JAR smoke on AMD64/ARM64 remain
scheduled for the final migration qualification. The full-workspace Rust/toolchain
lint issue must also be resolved there without silently weakening CI; it is tracked
explicitly in STATE.json. No per-group full GHA, RC tag, release JAR publication,
main change, datasci access or production operation occurred. The user subsequently approved G03 integration; G04 still requires a separate
explicit instruction.

Commands, logs, native baseline copy, port/review manifests, positive/negative
proof and final receipt: `${MIGRATION_AUX_ROOT}/20261119_comet12-g03`.
Cleanup guidance and measured resource sizes: CLEANUP.md and cleanup-ledger.json.
