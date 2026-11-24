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

# Runtime contract and correctness limits

This is the maintenance contract for Joom `branch-1.2`. Accepted qualification,
source/JAR identities and remaining coverage limits are in
[QUALIFICATION.md](QUALIFICATION.md). Passing qualification is evidence for the
executed cases, not proof of equivalence for every possible query or permission
to publish, tag or deploy.

## Versions and deployment settings

| Component       | Contract                                                                                                                   |
| --------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Spark           | Official Apache Spark 4.2.1; no Joom Spark binary or replacement Spark classes                                             |
| Scala / Comet   | Scala 2.13.18 (binary 2.13); Comet 1.2.0 with the retained Joom changes                                                    |
| Delta           | `io.delta:delta-spark_4.2_2.13:4.4.1`, with its dependencies, plus the separate Comet Delta adapter                        |
| DataFusion      | 55.2.0, including the local physical-plan vendor; see [DATAFUSION.md](DATAFUSION.md)                                       |
| Java runtime    | Eclipse Temurin 25.0.5.1+2; `java.vendor=Eclipse Adoptium`, `java.version=25.0.5.1`, `java.runtime.version=25.0.5.1+2-LTS` |
| Native platform | Linux AMD64 with x86-64-v3 CPU support, or Linux ARM64; GLIBC imports no newer than 2.31                                   |
| Build target    | Java 17 bytecode and Java 17 release builder; this does not describe the qualified runtime JVM                             |

Set the following before sessions/queries start, in the Spark image defaults or
launcher. Audit application/session overrides:

```properties
spark.sql.ansi.enabled=false
spark.sql.columnVector.offheap.enabled=false
```

Retain existing Comet memory settings. Disabling Spark off-heap column vectors
does not disable native Comet or change Spark's separate execution-memory pool.
The production-default decision of November 24, 2026 keeps ANSI disabled to
preserve the previous Spark 3.5 ANSI setting during migration. This supersedes
the earlier ANSI=true deployment target; enabling ANSI is a separate decision.
Do not change the independent `spark.sql.storeAssignmentPolicy` default `ANSI`.
Keep explicit ANSI=true and off-heap regression variants. The accepted earlier
qualification used ANSI=true, as recorded in [QUALIFICATION.md](QUALIFICATION.md);
this configuration decision does not relabel those runs or requalify the JARs.
Comet supports both modes at runtime; changing this setting does not require
different Comet artifacts. Existing regressions exercise both modes, but no
separate full qualification with ANSI=false as the primary default was performed
for this decision. Known semantic exceptions below remain applicable.

Direct Java launchers must preserve Spark's module exports/opens and include:

```text
--enable-native-access=ALL-UNNAMED
--sun-misc-unsafe-memory-access=allow
```

`dev/ci/java25-runtime.json` and `dev/ci/java-runtime.py` are the executable
vendor/build/flag contract. Use the official Spark launcher or the release
verifier's effective-POM options rather than replacing them with only the two
flags above. Do not require `-XX:-OmitStackTraceInFastThrow` in production: it is
an expression-test diagnostic setting preserving strict exception-text checks;
packaged smoke uses normal fast-throw behavior and still checks mandatory errors.

Java 25.0.3 is Spark's minimum accepted Java 25 update, not a statement that all
later builds have been qualified. A changed JVM must identify its actual build
and repeat the affected qualification.

## Spark Parquet off-heap leak

Official Spark 4.2.1 commit `617618396087429b5627a86ce933bb0f5d6645cb`
lacks the fix for [SPARK-60098](https://issues.apache.org/jira/browse/SPARK-60098): the vectorized
Parquet reader omits closing separately allocated definition/repetition vectors
for affected nested columns. A Spark-only native AMD64 witness retained
4,096,000 bytes after closing 100 nested readers; explicitly closing the omitted
vectors released exactly that amount. The flat-column control did not grow.
This identifies the defect, not every byte responsible for a longer fuzz-run OOM.

`spark.sql.columnVector.offheap.enabled=false` avoids that unmanaged retention by
placing these Spark vectors on the JVM heap. Spark reader/fallback paths may use
more heap and GC; no production slowdown percentage has been established. The
confirmed defect is a resource leak, not a demonstrated successful-result
mismatch. Resource exhaustion can nevertheless fail jobs or interrupt writes.

Keep this persistent requirement until a separately reviewed fixed Spark artifact
passes compatibility/resource and packaged checks. References: upstream fix
[`808d7961`](https://github.com/apache/spark/commit/808d79616144fbf209244e9c7a27927907908768)
and 4.x backport
[`c5808b78`](https://github.com/apache/spark/commit/c5808b781be5cebb93b961aed8fb3e9cc9c26901).

## Deliberately retained safe fallback and dispatch

See [FEATURES.md](FEATURES.md) for the feature/code/test mapping. These guards
are permanent product eligibility decisions, not temporary CI bypasses:

- **Windows:** suffix SUM/AVG (including integral/decimal) use Spark because
  reverse accumulation changes rounding or intermediate ANSI overflow. Suffix
  floating MIN/MAX, including floats nested in arrays/structs, use Spark to keep
  first-equal-value/signed-zero behavior. LEAD/LAG IGNORE NULLS uses Spark because
  the inherited bounded path could choose a default before crossing small batches.
  Whole floating SUM/AVG uses a forward Spark-order fold in the custom partition
  executor; with that executor disabled it falls back. Integral AVG is included
  where its result is Double. Repeated PERCENT_RANK state advancement was fixed.
- **Window selection:** `spark.comet.exec.window.partitionAggregate.enabled=false`
  and `spark.comet.exec.window.sorted.enabled=true` remain the defaults from tag13.
  SortedWindow supports its eligible rank/shift forms; shifts require literal
  offset/default, absolute offset at most 1024 and no IGNORE NULLS. Existing frame,
  decimal, RANGE and child ordering/distribution guards remain. FILTER window
  aggregates use Spark. Struct-valued scalar subquery PR #5889 was not added;
  unsupported projections can also send their dependent UNION/aggregate operators
  to Spark.
- **Expression order:** ambiguous inferred-null-check order, stateful nullable
  filter prefixes or harmful operator-shared CSE use Spark. Unknown UDFs and ANSI
  casts/arithmetic are not declared safe merely to enable reordering. Stateful
  COALESCE and multiple state-bearing IF/CASE occurrences use one Spark JVM kernel
  inside Comet. Filter eligibility follows Spark's actual whole-stage rule,
  including `maxFields` and factory mode. SMJ predicate hoisting additionally
  retains its proven-safe eligibility and outer-join/ANSI guards.
- **Aggregate buffers/order:** mixed SUM/AVG overflow guards and Spark binary state
  compatibility remain required. Wide decimal AVG/TRY_AVG and known non-codegen
  decimal SUM cases retain their existing eligibility guards. SortAggregate may
  remove otherwise-unneeded ordering only for the proven order-insensitive cases;
  SUM/AVG and tie-sensitive aggregates retain the streaming/restore-sort path.
- **Upstream dispatcher:** selected negative-scale decimals, collated string/array
  functions, Reverse(binary), regex/JSON and other incompatible scalar cases use
  Spark-generated code within the Comet pipeline by default. Their Rust-native
  opt-in is not equivalent qualification. A disabled/unavailable dispatcher can
  cause ordinary Spark operator fallback. Do not globally enable
  `allowIncompatible` options to regain native coverage.
- **Cost/format choice:** cost-based engine and wide-row sort/shuffle rules may
  intentionally choose Spark. An AQE wide-row fallback is sticky. Engine-choice
  changes must preserve result/error semantics and approved safety guards.

Spark fallback usually replaces an operator and may include siblings sharing it
(e.g. all expressions of one aggregate/window). Other supported subtrees can
remain native, with format bridges. Dependent operator eligibility, stage
boundaries or cost choices can widen the affected portion; do not promise that
fallback is limited to one scalar expression. JVM dispatch is different: Spark
code computes the expression while the Comet pipeline remains in use.

Tag13 attempted native execution for some paths now guarded above. Retaining
those paths verbatim would restore known correctness risks. The user accepted the
safe fallbacks rather than additional native implementations. Do not infer that
every current fallback is a newly lost tag13 feature: many are inherited upstream.

## Known remaining semantic exceptions

The user explicitly left these known exceptions outside the completed migration
and Java 25 work. They can affect results or required failures; ANSI=true alone
does not eliminate them. The existing, maintained upstream documentation remains
the detailed inventory, including cases beyond the summaries below:

- [Compatibility guide](../source/user-guide/latest/compatibility/index.md),
  including floating-point, regex, expression and Spark-version references.
- [Parquet known issues](../source/user-guide/latest/parquet.md#known-issues) and
  [Delta integration](../source/user-guide/latest/delta.md).

| Area                                 | Concrete risk / boundary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Invalid UTF-8                        | Spark strings can retain arbitrary bytes; conversion to Arrow may replace invalid sequences with U+FFFD. Binary round-trips, hashing, comparison, grouping, joins and string MIN/MAX can then differ. Sources include binary-to-string casts, JVM/text inputs and columnar shuffle; disabling only the final aggregate is insufficient if earlier conversion already changed bytes. Native Parquet UTF8-annotated strings may instead fail the scan. Preserve raw data as Binary or keep the affected query outside Comet when byte identity is required; do not assume rendered equality proves byte equality. |
| Decimal SUM runtime codegen fallback | Static no-codegen guards do not cover Spark abandoning whole-stage codegen after runtime compilation failure or `hugeMethodLimit`. An intermediate overflow that later cancels can recover in Comet where Spark returns NULL or must raise under ANSI.                                                                                                                                                                                                                                                                                                                                                          |
| Window/aggregate boundaries          | The upstream compatibility guide retains explicit RANGE PRECEDING/FOLLOWING boundary-overflow differences for DATE/DECIMAL ordering. Grouped decimal AVG can evaluate an overflowing group that Spark never consumes after LIMIT; this is an error-timing difference. Existing wide-decimal AVG/TRY_AVG fallback guards are not a universal aggregate-equivalence proof.                                                                                                                                                                                                                                        |
| Historical/extreme Parquet values    | Plain native Parquet scans do not apply legacy calendar rebasing as Spark does; affected pre-1582 values/predicates can differ. Delta's scanner has its own per-file metadata/read-mode rebasing and conservative timezone gates, so the plain-scan limitation must not be applied blindly to Delta. Legacy-list INT96 values outside 1677–2262 and overflowing TIMESTAMP_MILLIS conversions retain documented limitations. NANOS/type reinterpretation, field-ID/name collisions and duplicate-field handling can change whether Spark and Comet return rows, NULL or an error.                                |
| Timezone/casts and ANSI diagnostics  | Keep the current timestamp conversion/normalization guards. Historical timezone tables, extreme timestamps and unsupported casts require the documented path-specific review. Correct failure occurrence does not by itself prove identical error class/SQLSTATE/message for every native error. The three selected integral ANSI arithmetic cases passed; that is not proof that every upstream ANSI edge case is resolved.                                                                                                                                                                                    |
| Optional native Parquet writer       | It is disabled by default and documented as testing-only. Enabling it opts into separate limitations around rebasing, timestamp encoding/options, metadata, empty output and write lifecycle; read/Delta qualification does not qualify this writer universally.                                                                                                                                                                                                                                                                                                                                                |

Additional opt-in incompatible expression implementations retain their documented
limitations. No successful CI run makes malformed input, unsupported settings or
an unexecuted case safe. On a production mismatch, preserve input/schema/config,
reproduce with the exact stock Spark/JVM and Comet-off/on, and add a regression
for rows and required errors before fixing or broadening native eligibility.

Memory accounting also retains limits: scratch, FFI, shared/coalescing and some
scalar allocations are not completely reserved. Balanced declared reservations
and bounded-pool tests do not establish an RSS or OOM guarantee; no pool/fairness
redesign was included.

## Exact Docker-image boundary

Qualification used the pinned Temurin **JDK** on Ubuntu 24.04 AMD64/ARM64 runners.
Docker Registry metadata checked on 2026-11-24 confirms that
`eclipse-temurin:25.0.5.1_2-jre` is available for both Linux architectures and
uses the same Java release/build, but its current base is **Ubuntu 26.04** and
its package is **JRE**. The tag metadata is not execution of that image or of a
derived Spark image. The final Spark image still needs its own packaged smoke.
The manifest/config comparison is saved under
`${MIGRATION_AUX_ROOT}/20261124_comet12-java25-image-check/comparison.json`.
