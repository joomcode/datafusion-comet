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

# G03 performance evidence

The existing Joom Criterion benchmark `native/core/benches/sort_payload.rs` is
carried from `joom-1.1-13`, with its original Binary materialization and full-value
assertions. `sort_wide_payload` is also retained in the vendor crate; no new
benchmark framework or production workload was introduced.

## Executed comparison

Built using the existing workspace/lockfile and `cargo bench --profile dev
--locked --offline -j4 --manifest-path native/Cargo.toml -p datafusion-comet
--bench sort_payload -- sort_payload_4096b/pipeline --quick`.
After the other test/build commands ended, ran that same existing benchmark binary
with `--bench sort_payload_4096b/pipeline --quick`. Explicit `--bench` is required
when launching a Criterion binary directly; the earlier direct test-mode run is
only a smoke check, not a timing measurement.

| Existing pipeline, 4 KiB payload | Quiet dev timing estimate | Criterion interval         |
| -------------------------------- | ------------------------- | -------------------------- |
| Binary copies                    | 411.38 microseconds       | 408.20-424.10 microseconds |
| BinaryView then final Binary     | 343.89 microseconds       | 342.55-349.24 microseconds |

This is about 16.4% less time for this copy-pipeline case. Both paths return the
same materialized Binary values. It is **not** a comparison of the complete old
and new SortExec, SQL jobs, reservations, spilling or JVM conversion. Dictionary
figures printed by the fixture are not proof of a new dictionary-to-sort path.

Limitations: macOS ARM64, unoptimized dev native code, Criterion quick mode,
a single quiet process without controlled P/E-core placement or thermal state.
The initial timing run overlapped other work and is retained as diagnostic only.
These measurements do not prove production speedup, equivalence, an upper bound
on regression, or a bound on RSS. No release-profile dependency graph was built
solely to obtain a local performance number.

## Correctness and allocation behavior

The full targeted vendor sort regressions compare sorted values against reference
sorts, including prefix collisions, nested/sliced arrays, sparse referenced batches,
dictionaries and schema restoration. Native task-share tests check actual spill,
reservation peaks, grant shrink and output handoff; these counters are not RSS.
The additional physical-source-lifetime regression observes ownership inside
MemoryPool::shrink and detects the premature uncharging missed by a pool-peak test.

Evidence: `${MIGRATION_AUX_ROOT}/20261119_comet12-g03`, especially
`payload-kernel-benchmark-quiet-measured.log`, `native-test-cases.json`,
`native-sort-final-fixed.log`, `concat-source-positive.log`,
`concat-source-negative-control.log` and `negative-control-restoration.json`.
Full agreed GHA and exact final-JAR AMD64/ARM64 qualification remain mandatory.
