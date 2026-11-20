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

# G07 performance observations

Use the project's existing benchmarks only. These measurements use local macOS
ARM64, Java17 and a debug native library; they are diagnostics, not a forecast
for production GCP jobs. Correctness evidence is separate and uses ANSI=true.

## Existing dictionary fallback benchmark

Restore the old wide-Binary group in `row_columnar` and run the same256-row,
64KiB/distinct fixture before and after J10, with Criterion `--profile dev --quick`.

| Case                   |   Before |    After |
| ---------------------- | -------: | -------: |
| Plain encoding control | 9.0696ms | 8.6473ms |
| Dictionary fallback    | 41.625ms | 40.071ms |

Criterion reports p=0.08 and no statistically significant change for either case.
Do not claim a proven speedup or absence of every regression from these quick runs.
Separate tests prove actual buffer reuse and exact String/Binary contents/lifetime.

## Existing binary row-copy benchmark

`CometBinaryRowCopyBenchmark.java` is byte-identical to `joom-1.1-13` and compares
getBinary+write with borrowing raw bytes for the same UnsafeRow copy. It validates
arbitrary invalid-UTF8 binary contents before timing, then alternates order for
seven rounds and reports medians.

| Payload | Ordinary ns/row | Borrowed ns/row | Ordinary allocated B/row | Borrowed allocated B/row |
| ------- | --------------: | --------------: | -----------------------: | -----------------------: |
| 32B     |          11.977 |          10.988 |                     43.5 |                       45 |
| 4096B   |         203.609 |          69.457 |                     3663 |                       45 |
| 196608B |        8102.885 |        3190.165 |                   175119 |                       45 |

The large-payload copy mechanism is faster and avoids intermediate payload arrays
in this fixture. This does not measure the separate instance-pool benefit over
upstream's class cache, and it does not predict whole-query/job speedup. Small
payload allocation is slightly higher in the borrowed variant in this measurement.

## Existing wide-shuffle read benchmark

`CometWideShuffleReadBenchmark.scala` is also byte-identical to the old tag.
Bounded parameters:64leaves,1048576total leaf values =16384rows,8maps,32partitions,
shufproj, four existing direct/JVM/coalescing modes,2GiBheap. The benchmark's
original on-heap/unbounded-memory fixture settings are retained.

All four modes completed their measured iterations. The standard summary then
failed because sandbox denies `/usr/sbin/sysctl -n machdep.cpu.brand_string`.
No code or runner workaround was added, and the command is explicitly **not**
counted as passed. Medians derived from the final reported measured iterations
(the log's Stopped-after counts) are307ms/direct,267ms/direct+coalescing,
307ms/JVM and247ms/JVM+coalescing. These raw rounded values are diagnostics only:
there is no completed standard summary, native release comparison or production
performance claim.

Commands, exact outputs, per-round data, exit status and parser output are retained
under `${MIGRATION_AUX_ROOT}/20261120_comet12-g07`: `dictionary-before.*`,
`dictionary-after.*`, `binary-copy-benchmark.*`, `wide-shuffle-read-benchmark.*`
and `performance-results.json`. Nothing was downloaded for these benchmarks.
