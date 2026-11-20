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

# G07: shuffle and columnar-to-row local qualification

G07 is locally implemented on `joom/1.2-shuffle-and-c2r` from accepted G06 `0028887b3`.
Await user review before push or integration. No full GHA, G08, main/tag/release,
production or datasci operation was performed.

## Scope and retained upstream implementations

| Cards       | Result                                                                                                                                                                                                                                 |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| J08/J09     | Raw Binary borrowing into owned UnsafeRows and bounded projection-instance pooling; keep upstream class cache and direct converters. Task completion ordering prevents reuse while a reader still owns a projection.                   |
| J10/J13     | Unique dictionary fallback reuses its values buffer; native writer gathers only referenced batches and aligns compatible nested schema instances. Existing run/round-robin paths remain.                                               |
| J11         | Idle owner-thread spill callback, busy/reentrancy/closed guards, allocation-free portable reset, pending-row pointer restoration and terminal failed-spill poisoning/partial-file cleanup. G02 TaskContext/retry behavior is retained. |
| J12/J23/J26 | Shared read coalescer, initial and AQE direct read, nested offset/row/JNI bounds, EOF/error/ownership protections and opt-in batch diagnostics. Full old J12 commit audit also restored the required multirow/sliced Bloom merge.      |
| J21/J24     | Keep upstream sliced-broadcast offset normalization and small/effective JVM batch cap. Restore old regressions and add nested/raw binary, AQE and reuse cases.                                                                         |
| J25/J27     | Ordinary C2R avoids codegen on Comet broadcast wrappers; default Kryo tests include the actual broadcast payload. Cost-model matrix is explicitly deferred to G09; final shaded-JAR qualification remains due.                         |

The source is `joom-1.1-13^{commit}` = `981e26dff6b64110a8b3949a0ed065523d9012e4`.
Upstream base remains `93cc6f9d71136bd0d87f77dd901989144d8c09fb`.
DataFusion55.2 vendor and the shared lockfile are unchanged by G07. Full per-card
old commits, adaptations, source hashes and receipts are in [G07-results.json](G07-results.json).

## Correctness evidence

- **292 unique native cases passed:** full shuffle crate223, targeted core35,
  diagnostic1 and Bloom33, with the Comet shared dependency graph.
- **266 unique JVM registrations passed** across retained cohorts. This includes
  48 C2R/pool/broadcast/Utils cases,100 unique read/AQE/DPP/reuse/lifecycle cases
  (108 executions because eight filters overlap),62 writer/allocator/context/C2R
  cases, configuration and additional broadcast/SQL regressions. Repeated runs
  and repaired cases are not added to the unique count.
- The final sorted JVM writer matrix exercises32 combinations of AQE, actual
  direct/JVM input, coalescing, codec and1/17-row batch limits. It compares every
  row, duplicate, raw byte, nested NULL, schema and floating value with original
  Spark4.2.1 without Comet, under ANSI=true. Native-sort, serialized input flags,
  sorted-writer handle and records-written assertions prevent incidental fallback.
- The24-combination Bloom matrix compares complete serialized bytes against
  original Spark, including both mixed aggregate directions and an all-NULL map.
  Tests assert the actual native aggregate modes, writer and reader representation.
- Targeted cases cover owner/thread memory accounting, spill refusal/resumption,
  invalid headers/buffer lengths/FFI width, prefetched and delivered batch release,
  close racing blocked reads/native imports, and failure propagation instead of EOF.
- Strict workspace/all-targets Clippy, Rust format, Maven compile/style checks and
  both CI suite/config inventory checks passed. Five suites are added to each
  existing CI platform matrix; triggers/profiles/coverage are otherwise unchanged.

Negative controls restore the exact old Bloom merge body or remove only the
writer's terminal failure check. They reproduce wrong sliced-filter output or
unsafe continuation respectively; sources are restored exactly afterwards.
The pre-fix native dylib also fails the same SQL with eight partial states in
both JVM-decoded and direct-native coalescing. The final dylib passes. No negative
control is counted as a product failure or a positive test.

Review exposed two test setup gaps: native-only shuffle could not admit a Spark
Partial boundary, and global ORDER BY inserted a Spark range exchange before the
intended native consumer. Both fixtures now assert the actual intended paths.
A generic native-plan checker also needed the explicitly expected ReusedExchange
wrapper; exact result and reused-Comet-broadcast assertions remain. Intermediate
failures and subsequent passing evidence are retained in the results inventory.
No selected applicable case remains failed, canceled or ignored locally.

## Limits and remaining qualification

The coalescer's64MiB default is a concatenation target, **not** a memory-pool
reservation or RSS cap. Pending and concatenated arrays can coexist; one
byte-oversized block passes unchanged. Individually oversized row counts fail.
Celeborn JVM decoding remains blockwise; validated native direct decoding can coalesce.
Projection pooling keeps the original schema/instance/buffer caps.

[Performance measurements](G07-performance.md) use the existing project benchmarks;
they do not establish whole-job speedups. Optional wide-read measurements completed,
but the benchmark command's final CPU-info report is blocked by sandbox `sysctl`.
This is recorded as an incomplete performance command, without changing benchmark
or production code or counting it as correctness validation.

Final agreed Linux/CI/release coverage and exact final-JAR AMD64/ARM64 smoke remain
mandatory, including default-Kryo shaded packaging. Spark4.0.4 reset compatibility
was independently checked in exact source; its runtime remains for final CI because
that JAR is not cached locally. The original J25 cost-model toggle matrix must run
in G09 after the cost-model implementation exists. Earlier qualification backlogs
and temporary-CI cleanup obligations are preserved.

All raw evidence, failed attempts, proposals, full-source reviews, source manifests
and completion receipt are retained at
`${MIGRATION_AUX_ROOT}/20261120_comet12-g07`. See [CLEANUP.md](CLEANUP.md).

## Accepted integration (2026-11-20)

The user explicitly approved G07 publication and integration into `branch-1.2`.
Keep all three substantive commits and fold acceptance documentation into the
third; use fast-forward integration with no additional merge/service commit.
All46 qualified non-migration source/test/CI/runtime-reference paths remain
byte-identical to `62e9ffd7a`; the original local qualification and performance
limitations are unchanged. Initial review holds above are historical.

Actual final local/remote SHA, tree, source/document preservation and publication
evidence are recorded after commit in
`${MIGRATION_AUX_ROOT}/20261120_comet12-g07-integration/completion.json`.
G08 remains unauthorized. Final agreed qualification and earlier backlogs persist.
