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

# G06: native partition and sorted windows

N01/N02/N03 are implemented and locally qualified in `joom/1.2-native-windows`
from accepted G05 `72d40726eee5fadbf94c5ddd0537d05854766d88`.
Review is required before push or integration. `branch-1.2`, main and RC1 remain
unchanged. No full GHA, release, production operation or G07 work was started.

## Ported scope

- **N01:** the final tag13 spilling partition executor, including whole-partition
  aggregates and FIRST/LAST/NTH_VALUE, partition-size functions, supported suffix
  ROWS/RANGE frames, reverse-buffer spill/replay and mixed bounded/unbounded nodes.
- **N02:** evaluate complete tiny partitions together, preserve partitions crossing
  batch boundaries, coalesce output subject to row and nested offset limits, and
  avoid memory-pool calls per tiny partition. The actual output consumer uses the
  G02 `OffsetBudget`; tests cover sliced Boolean and nested List/Map/Struct values.
- **N03:** stream ROW_NUMBER/RANK/DENSE_RANK and eligible LEAD/LAG over sorted
  batches without per-partition slicing, retaining cross-batch peers and shifts.
  Literal offset/default, absolute offset at most 1024 and no IGNORE NULLS remain
  eligibility requirements. Unsupported custom cases retain the standard path.

The executors move into the Comet 1.2 operators crate; selection and Spark/JNI
configuration use the current planner. The partition executor stays **disabled
by default**, and SortedWindow stays **enabled by default**, matching tag13.
The keys remain `spark.comet.exec.window.partitionAggregate.enabled` and
`spark.comet.exec.window.sorted.enabled`. No external deployment configuration
was changed. Upstream FILTER, decimal, frame-bound and RANGE guards, result-type
casts and original Spark child distribution/ordering requirements are retained.
No vendor DataFusion production file is changed by G06.

All base window test registrations and the ten G06-specific old Scala test names
are retained. Old native tests and the existing paired benchmark are retained;
unsafe old native-plan expectations now require the explicitly safe Spark path
and still compare complete results. Source mapping and precise adaptations are in
`G06-results.json` and the auxiliary source-parity review.

## Correctness adaptations

This is a semantic port, not a byte-for-byte restoration of unsafe execution paths.
Fresh original Apache Spark 4.2.1 execution is the independent SQL oracle.

| Case                    | Final behavior and reason                                                                                                                                                                                                                                            |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Whole floating SUM/AVG  | The partition executor folds rows in input order. Arrow's batch reduction produced 0 for `[1e16, -1e16, 1]`, where Spark produces 1. With the partition executor disabled, these windows fall back to Spark. This also covers integral AVG whose result is Double.   |
| Suffix SUM/AVG          | Spark recomputes each suffix forward. Reverse accumulation can change floating results or suppress mandatory intermediate ANSI overflow. All such suffix aggregates use Spark, including integral and decimal cases.                                                 |
| Suffix floating MIN/MAX | Spark retains the first equal value, including signed zero. Reverse evaluation can pick another equal value. Floating values nested inside arrays/structs are included in the targeted Spark fallback. Nonfloating extrema remain eligible.                          |
| LEAD/LAG IGNORE NULLS   | The current upstream bounded executor returned defaults too early across small batches. These expressions use Spark; ordinary eligible shifts still use SortedWindow.                                                                                                |
| Repeated PERCENT_RANK   | Both projections reuse one per-batch peer-state result. The tag13 logic could advance state twice and return 0.5 instead of 0.25 for the second projection across a peer boundary.                                                                                   |
| Oversized partition     | Both new executors reject the first row beyond Int.MAX, including a LEAD-only plan. The structured error is converted to Spark 4.2's original condition, parameters, exception class and SQLSTATE. Synthetic boundary-state tests avoid allocating billions of rows. |
| FILTER / DISTINCT       | Existing Scala FILTER fallback is retained. The protobuf planner additionally rejects a filter before an aggregate can lose it during native construction. The custom partition constructor rejects DISTINCT state.                                                  |

These fallbacks preserve correct execution and required failures; they are product
eligibility guards, not temporary CI exceptions. They can reduce acceleration for
the listed query shapes. The forward floating fold also trades vector reduction
speed for the original Spark accumulation order. Broadening these paths would
require separate proof, not removal of an inconvenient assertion.

Negative controls temporarily restored only the old floating reduction and old
percent-rank branch. The new tests then failed with the wrong values above. Both
changes were restored and the positive tests rerun. An initial percent-rank test
fixture accidentally fitted one batch; it was corrected to actual 2/2/1 batches
before accepting that negative control. Failed experiments and snapshots remain
available; none of their temporary source mutations is committed.

## Local validation

- **51 native correctness tests passed**, zero failures, using the shared locked
  Comet/common/operators/expression/vendor dependency graph: 8 core, 1 common,
  24 operators and 18 vendor tests. Spill and replay, mixed nodes, exact peer
  values, boundary offsets, required failure, early drop and reservation cleanup
  are covered. The broad layout matrix retains all rows/columns across partitions,
  batch sizes and memory budgets. Its DataFusion comparison is supplementary to,
  and not a replacement for, the independent original-Spark SQL oracle.
- **219 JVM/SQL tests passed in four suites**, zero canceled/ignored/failed:
  the inherited window suite with the partition flag false and true, the new
  focused semantics suite, and Spark error conversion. ANSI=true comparisons
  include full typed rows, duplicate multiplicities, observable order, exact
  floating values and mandatory errors; analysis errors are compared too.
- **88 focused query actions** additionally have exact native executor or explicit
  fallback evidence. The log verifier checks the complete action matrix and
  actual JNI execution plans; the Scala suite asserts real window-owned spills
  for whole aggregates, partition-size functions and suffix values. JVM wrapper
  names alone are not accepted as native executor proof.
- Native build and freshly bundled debug dylib hashes match. Strict workspace
  Clippy passes with only the already-approved statement-local vendor deprecated
  API exception. Formatting, CI configuration/suite registration and source-license
  checks are recorded in the final receipts. Two new window suites are permanently
  registered on Linux and macOS; no trigger is disabled or weakened.
- Both inherited ignored performance tests were explicitly executed separately
  and passed. Their measurements are not counted as correctness tests or as
  justification to ignore a correctness failure.

The complete command/exit/log records, source/library hashes, independently
reviewed Spark source references and intentional failures are in `G06-results.json`
and `${MIGRATION_AUX_ROOT}/20261120_comet12-g06`. That directory's `completion.json`
records actual local commit IDs/tree and protected-ref checks after committing.

## Existing performance measurements

The restored existing paired benchmarks use the same input for the custom and
DataFusion executors. These quick local **dev-profile** measurements show that the
ported paths work as intended, and are not forecasts of real-job speed or a
production performance qualification.

| Existing workload                                             | DataFusion | G06       |
| ------------------------------------------------------------- | ---------- | --------- |
| Sorted ROW_NUMBER, one-row narrow partitions                  | 206.27 ms  | 3.7279 ms |
| Sorted LEAD, about 2.2-row wide partitions                    | 348.13 ms  | 6.1067 ms |
| Whole SUM/COUNT, 193,536 rows, 125 payload columns, 1 row/key | 3.1344 s   | 247.69 ms |
| Same whole aggregate, 10 rows/key                             | 329.61 ms  | 38.00 ms  |
| Same whole aggregate, 1,000 rows/key                          | 21.394 ms  | 16.533 ms |
| Suffix FIRST_VALUE IGNORE NULLS, 4.5M rows / 2M partitions    | 7.9966 s   | 681.92 ms |

The suffix benchmark records 1,347 custom-pool calls for two million partitions;
the separate correctness regression asserts bounded per-batch pool calls and
large-partition spill. The default DataFusion benchmark records zero pool calls,
which reflects its missing accounting, not zero memory consumption. No new
performance framework or external production workload was introduced.

## Limits and final handoff

Local evidence is **macOS ARM64 / Java 17 / debug or dev profile**. Final complete
Linux CI, supported release features and exact final-JAR native AMD64/ARM64 smoke
remain mandatory after all groups. There is no intermediate G06 release gate.
For the final focused window log, run the durable native identity verifier:

```shell
python3 docs/joom-migration/verify-g06-native-windows.py \
  "${G06_WINDOW_LOG:?set the complete merged window-suite log}" \
  --output "${G06_WINDOW_EVIDENCE:?set an explicit evidence JSON path}"
```

Keep JNI native explain enabled, execute the entire semantics suite, and retain
merged stdout/stderr. Missing/truncated actions or native nodes must fail, not skip.
No ignored or canceled applicable case may count as passed. The tests' row/error/
spill assertions and the native identity verifier serve different purposes.

The original accounting model is retained: some expression/coalescing/cursor
allocations and selected suffix scalar values are not fully reserved. Balanced
declared reservations and bounded-pool tests are not an RSS/OOM guarantee. No
unapproved pool/fairness redesign is included. The partition-size error rejects
at the first exceeding row; Spark can report a larger final total if it has already
counted the whole partition. Required failure and the MAX+1 structured condition
are checked; an enormous physical partition was not materialized.

G02's window OffsetBudget consumer is locally verified. Its separate **G07 shuffle
consumer remains pending**. Earlier G04/G05 full fuzz, metric and final-platform
backlogs remain intact. Main, tags, release infrastructure and all previously
accepted source outside the G06 integration points are preserved. G07 requires
separate explicit user authorization; this report grants neither push nor merge.

## Accepted completion and integration (2026-11-20)

The user reports no queries using the three discussed suffix/IGNORE NULLS shapes
and accepted keeping their correctness fallbacks without further native work now.
G06 completion follows the established feature/base publication and fast-forward
integration process. Preserve both substantive commits; fold acceptance documents
into the second commit, with no extra merge or service commit.

Every qualified non-migration byte remains identical to `2aeca4df3`; tests,
assertions, defaults and guards are unchanged. Earlier review holds in this report
describe the historical checkpoint. The actual final local/remote commit and tree,
source/document hashes and preservation checks are recorded after commit in
`${MIGRATION_AUX_ROOT}/20261120_comet12-g06-integration/completion.json`.
Original qualification evidence remains intact. G07 is not authorized; full final
Linux/release/JAR qualification and all earlier backlogs remain mandatory.
