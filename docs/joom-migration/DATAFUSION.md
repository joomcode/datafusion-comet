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

# DataFusion vendor maintenance

## Exact origin and integration boundary

Comet supplies Spark planning, Spark-compatible expression/aggregate semantics,
JNI/Arrow interchange, shuffle and native execution integration. DataFusion
supplies the Rust execution-plan framework and physical operators used by Comet.
Only **datafusion-physical-plan** is vendored here; a separate fork or copy of the
entire DataFusion repository is unnecessary for the current patch set.

| Item                                                       | Maintained identity                                                                                                                                      |
| ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Published crate                                            | `datafusion-physical-plan` **55.2.0**, crates.io                                                                                                         |
| Published archive SHA256 / original registry checksum      | `b1451d6817e5ac0dd21819b19949d1c420ab51d748f55dd9a96ecb5418a24c97`                                                                                       |
| Corresponding DataFusion tag/commit                        | `55.2.0` / `500744d5c25499055e3b2ca46b022bc425989203`                                                                                                    |
| Pristine published-crate import in retained branch history | `5a9cbf8569721ce18733cf0cf18c73bc57abd48f`                                                                                                               |
| Shared-workspace/test integration                          | `1322919571ef70a4f454ea41b545a6efc9d3d301`                                                                                                               |
| Current path                                               | `native/vendor/datafusion-physical-plan`                                                                                                                 |
| Authoritative lock                                         | `native/Cargo.lock`                                                                                                                                      |
| Resolved native Arrow / Parquet                            | **59.3.2** / **59.3.2**                                                                                                                                  |
| Older Joom vendor origin                                   | Published physical-plan 55.1.0, imported at `6eaf12e80b5fbaa551f6838910da5da598b2ea2d`; DataFusion tag commit `7d3835c71f30cbd3c3ae4041732267f1f453097a` |

The 55.2.0 source was imported from the **normalized published crate**, not by
copying a Git workspace directory. The latter inherits DataFusion workspace
fields and is not a standalone manifest under Comet. Keep package identity
55.2.0, `Cargo.toml.orig`, LICENSE.txt/NOTICE.txt and published source provenance.
Do not make the vendor inherit Comet's package version 1.2.0.

[Native Cargo.toml](../../native/Cargo.toml) includes the vendor as a workspace
member and applies `[patch.crates-io] datafusion-physical-plan = { path = ... }`.
The member is intentionally absent from `default-members`: ordinary default
Comet tests do not imply its internal tests executed. Other DataFusion crates
stay at the target locked 55.2 graph. Arrow/Parquet manifests still allow 59.2
compatible versions; **59.3.2 comes from the shared lock**, not an exact 59.3.2
manifest requirement. The vendor's nested published Cargo.lock is provenance,
not a second runtime/test resolution.

## Patch families and dependencies

Read [FEATURES.md](FEATURES.md) for each card's full contract, origin and tests.
The current vendor retains upstream Single/Final plus **MergeAsPartial** and
Spark state codecs. It does not restore the old Joom PartialReduce routing.

| Cards               | Vendor behavior and principal modules                                                                                                                                                                                                                                                          |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D02 / D03           | Retain charged sort workspaces across grant shrink, multipass/final merge and output handoff: `sorts/spill_workspace.rs`, `multi_level_merge.rs`, `stream.rs`, `streaming_merge.rs`, `spill/spill_manager.rs`, `stream.rs`                                                                     |
| D04                 | Narrow-key sort, BinaryView/prefix comparison and one-time referenced-payload gather: `sorts/sort.rs`, `sort/late_materialize.rs`, `sort/wide_payload.rs`, `sorts/cursor.rs`; retire source ArrayRefs before releasing charges                                                                 |
| D06 / D07           | Initial refusal recovery, offset budget, grouping-set hidden IDs and truthful output properties: `aggregates/hash_stream.rs`, `grouped_hash_stream.rs`, `ordered_*_stream.rs`, `single_stream.rs`, `partial_reduce_stream.rs`, `group_values/*`, `aggregate_hash_table/*`, `aggregates/mod.rs` |
| D08 / D09 / D10     | Fallible future emit reservation, retained ordered output and joint replay/output/merge headroom: `aggregates/replay_budget.rs`, `aggregate_hash_table/common.rs`, `common_ordered.rs` and the stream/table consumers                                                                          |
| N09 integration     | Comet low-copy collect_list lives in `native/spark-expr/src/agg_funcs/collect.rs`; vendor emit/replay guards must account its variable states and nested/empty-struct layout                                                                                                                   |
| D05                 | Filter candidates before wide payload, buffered-range/streamed-run reuse and safe hoisting: `joins/sort_merge_join/filter.rs`, `materializing_stream.rs`, `mod.rs`, `joins/mod.rs`                                                                                                             |
| G10 metrics         | Preserve one peak recorder across spill replay/delegated memory pools, without resetting aggregate metrics after a spill; Comet-side replay wiring is part of the same contract                                                                                                                |
| Lint-only allowance | Statement-local deprecated Atomic::fetch_update allowance in `src/topk/mod.rs`; original algorithm and atomic ordering remain unchanged                                                                                                                                                        |

Sort reservation scaling and marking a sort's JVM-output path live in Comet,
not solely this crate. They depend on task context/memory/cancellation and preserve
the TopK wrapper's reconstruction behavior. Aggregate kernels/state codecs in
Comet must remain compatible with vendor emit/replay state layouts. SMJ safe-hoist
markers and expression eligibility are also coordinated with Comet's planner.
Windows are in `native/operators`, not new DataFusion vendor production patches.

Logical reservation balancing is not a bound on allocator RSS. Selected scratch,
shared buffers, coalescing and suffix scalar memory are not completely reserved.
Do not remove output-lifetime or safety guards merely because a pool counter
returns to zero.

The final retained implementation commits are:

- Sort: `d35817534` plus Comet wiring `af05c94e7`.
- Aggregates: `4b109026e`, coordinated with kernels `ba3aa5480`, JVM ordering
  `18f7550f1` and the independent correctness fix `a89fea10e`.
- SMJ: `0ef152e47` plus Comet/outer-pair safety `72d40726e`.
- Narrow lint exception: `5133f0ab7`.
- Final aggregate metrics: `73cf036b7`.

## Test integration and baseline adaptations

The pristine archive is unchanged in its import commit. Integration deliberately
adapted two spill metric tests in `src/spill/mod.rs` and `spill_pool.rs` to compare
against actual flushed file size: Arrow 59.3.2 omits all-valid IPC bitmap buffers
compared with the older pristine test graph. This does not relax cleanup/lifetime
assertions or alter production spill bytes. DataFusion's rustfmt.toml supplies
its edition-2024/width-90 formatting. New tests/benchmarks accompany patch families.

Current CI explicitly uses Comet's feature-unified root graph:

```shell
cd native
cargo nextest run --locked -p datafusion-comet -p datafusion-physical-plan \
  --lib -E 'package(datafusion-physical-plan)'
cargo nextest run --locked -p datafusion-comet -p datafusion-physical-plan \
  --lib --features datafusion-physical-plan/test_utils \
  -E 'package(datafusion-physical-plan) & test(statistics::)'
cargo clippy --locked --all-targets --workspace --no-deps -- -D warnings
```

These are the calls from
[the Rust CI action](../../.github/actions/rust-test/action.yaml). Use the existing
project setup/JVM-library environment and [VALIDATION.md](VALIDATION.md) when
running locally.
Selecting only the vendor from an isolated manifest or resolving its nested lock
can test a different feature graph. Test-only dev dependencies may add entries to
the root lock; inspect runtime graph changes separately.

In particular retain:

- `sorts/sort/comet_memory_tests.rs`: grant shrink, wide spill/multipass/output,
  source retirement and cleanup; plus Comet `jni_api/sort_memory_tests.rs`.
- `aggregates/starved_spill_tests.rs`, `ordered_drain_tests.rs`,
  `replay_budget_tests.rs`: empty initial refusal, offset/grouping-set boundaries,
  output ownership, ordered pending states and tight replay budgets.
- `native/core/tests/aggregate_emit_memory.rs`: actual key/payload preservation,
  spill and cleanup under the unchanged memory fixture.
- `joins/sort_merge_join/tests.rs` and the Comet SMJ ANSI/fuzz suites: all outer
  joins, duplicates/NULLs, constant filters and eager-evaluation hazards.

The statement-local deprecated allowance preserves the existing atomic algorithm.
Rust 1.100 renamed this API to try_update; do not change production atomic code
solely to silence it. All other warnings remain errors, including vendor tests
and benches. A command-line `-A deprecated` does not satisfy strict CI.

## Updating the vendor safely

1. Start from the exact published crate and preserve an independently identifiable
   pristine import, archive digest and corresponding upstream commit. A new
   DataFusion version is a coordinated dependency upgrade, not a local file swap.
2. Compare each current patch family against the new upstream implementation.
   Preserve behavior and regressions when upstream supplies an equivalent;
   explicitly review partial alternatives. DF56 workspace lending/clustered
   aggregation were outside the selected base and are not silently included.
3. Keep one physical-plan package identity and the root patch/lock; inspect the
   resolved Cargo metadata/tree, feature union, native Arrow/Parquet versions and
   optional contrib graphs. Check actual path-source rather than just version.
4. Run affected vendor and Comet tests together, including result/mandatory-error
   comparisons with original Spark and assertions that the intended native route
   executed. Native Single-vs-Partial parity alone is not an independent Spark
   oracle. Preserve sort/window/SMJ/shuffle cross-feature ownership regressions.
5. Follow full final-source CI and exact final-JAR AMD64/ARM64 qualification rules
   when producing a new release. Current proof belongs to source
   `7e3b6bcf87389f9cdcbe27db6f74562b5e792c05`, not arbitrary later vendor bytes.

For a complete reproducible file-level patch inventory, from the repository root:

```shell
git diff --stat 5a9cbf8569721ce18733cf0cf18c73bc57abd48f -- native/vendor/datafusion-physical-plan
git diff 5a9cbf8569721ce18733cf0cf18c73bc57abd48f -- native/vendor/datafusion-physical-plan
```

This compares the pristine import with the current working state, including
manifest/test/style integration. Restrict a historical comparison to a second
explicit SHA when diagnosing an incident. The import/integration commits above
identify the retained branch history; the archive checksum pins the published
crate independently of later patch commits.
