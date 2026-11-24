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

# Qualification and artifact identity

This is the maintenance record for the Spark 4.2.1 / Joom Comet 1.2
qualification. Use [FEATURES.md](FEATURES.md) to follow each improvement to its
implementation and regression tests, [VALIDATION.md](VALIDATION.md) to validate a
future fix, and [RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md) for deployment
requirements and known limits. Finite test coverage does not prove equality for
all inputs or production workloads.

The production default is ANSI=false; see
[RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md). The frozen record below
retains the actual ANSI=true qualification settings. Changing the deployment
default does not change the recorded source/JAR identities or test results.

The expressions cohort executed non-ANSI `abs`, the native cast
suite (ANSI=false by default, with explicit ANSI opt-ins), and timestamp
regressions exercising both modes in the
[Spark 4.2 expressions job](https://github.com/joomcode/datafusion-comet/actions/runs/45292058669/job/138113940824).
This is existing mode-specific coverage, not a new full ANSI=false qualification.

## Frozen source and runtime

| Identity                                  | Accepted value                                                                                                             |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Qualified executable/test/workflow source | `7e3b6bcf87389f9cdcbe27db6f74562b5e792c05`                                                                                 |
| Java 25 integration commit                | `586bbbf614823d70535a9ec1a384ea63aff7f4e0`                                                                                 |
| Feature / integration branch              | `joom/1.2-java25` / `branch-1.2`                                                                                           |
| Java 25 integration base                  | `d2a01d4b6bd5a5e05fcfa712a9c74e6e889fcdbb`                                                                                 |
| Spark / Scala                             | Official Apache Spark 4.2.1 / 2.13.18                                                                                      |
| Comet / Delta / DataFusion                | 1.2.0 / 4.4.1 / 55.2.0                                                                                                     |
| Qualification JVM                         | Eclipse Temurin 25.0.5.1+2; `java.vendor=Eclipse Adoptium`, `java.version=25.0.5.1`, `java.runtime.version=25.0.5.1+2-LTS` |
| Spark defaults                            | `spark.sql.ansi.enabled=true`, `spark.sql.columnVector.offheap.enabled=false`                                              |
| Release build                             | Java 17 compiler/bytecode; Rust 1.100.0; Ubuntu 20.04 / GCC 10; GLIBC imports at most 2.31                                 |
| Native platforms                          | Linux AMD64 (`x86-64-v3` CPU baseline) and ARM64                                                                           |

The Java 25 step changed CI/build tools, validation helpers and one timezone test;
it changed no production Comet algorithm, DataFusion patch or SQL fixture. The
documentation-only integration commit did not rebuild or relabel the JARs. Later
maintenance-documentation-only revisions also retain the qualified source above.
Any future executable/dependency/runtime change needs evidence for its own SHA.

## Actual successful workflows

All four workflows used the same qualified source SHA.

| Workflow               | Run                                                                                  | Actual outcome                                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Comet CI               | [45292058669](https://github.com/joomcode/datafusion-comet/actions/runs/45292058669) | Final attempt 3: 138 successful jobs; original 16 structural skips preserved                                 |
| RC Build & Smoke       | [45292086072](https://github.com/joomcode/datafusion-comet/actions/runs/45292086072) | 7 successful jobs; 27 existing cases in 13 groups on **each** native AMD64/ARM64 runner                      |
| Run Miri Safety Checks | [45292072441](https://github.com/joomcode/datafusion-comet/actions/runs/45292072441) | 107 selected tests passed; recorded integer-to-pointer provenance warnings retained; one structural job skip |
| CodeQL                 | [45292009717](https://github.com/joomcode/datafusion-comet/actions/runs/45292009717) | Actions analysis passed; this is not Rust/JVM whole-program analysis                                         |

Failed-only retries repaired pre-test Maven Central HTTP 429 and Gradle-wrapper
bootstrap failures without changing source or assertions. Real successful
producers are 125 jobs from attempt 1, 11 from attempt 2, and the Delta 4.1 plus
Required Checks jobs from attempt 3. GitHub retry status aliases are neither new
executions nor new artifact producers. The final Delta 4.1 invocation actually
passed 295 ScalaTest cases plus 18 separate encryption JUnit cases; empty module
summaries receive no test credit.

## Required positive coverage

Counts below identify separate, overlapping gates; do not add them together as a
unique-test total. Assertions on actual results, mandatory errors and execution
paths remain authoritative.

| Gate                              | Actual Java 25 evidence                                                                                                                                                         |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Target stock Spark SQL            | All 9 shards: 34,876 passed, 0 failed, **9 canceled and 7,922 ignored**; all nonpasses receive zero positive credit                                                             |
| Migration regression selection    | 103 required selectors; 24 normal JVM cohorts (20 Linux, 4 macOS)                                                                                                               |
| Cost-choice compatibility         | All 6 existing remote profiles, 66 exact CostChoice cases per profile                                                                                                           |
| Runtime and windows               | 98 core cases, 88 actual native window actions, 3 additional prefetch-cleanup cases; actual vendor/build properties in 8 required runtime XML suites                            |
| Aggregate / SMJ full fuzz         | All 31 aggregate registrations (6 default + 25 full) and 33 SMJ registrations (10 default + 23 full), with no nonpasses and actual forked-JVM opt-in flags                      |
| Python prerequisite gap           | All 58 previously canceled pandas registrations passed in 8 concrete suites                                                                                                     |
| Additional corrective regressions | Separate 58 corrective cases passed on Java 25; their full named text report is evidence, not the generic expression XML artifact, which lacks those cases                      |
| Legacy regression                 | 5 original Spark 3.4 Scala-UDF cases passed in its retained Java 17 compatibility profile                                                                                       |
| Native and tooling                | Full native/vendor/JNI, strict Clippy and existing quality gates passed; each invocation retains its own skips/counts                                                           |
| Release artifacts                 | Exact final JARs, bundled native bytes and loaded class origins checked on both native Linux architectures; this bounded smoke does not rerun every CI test inside release JARs |

The target SQL cancellations were:

- `Tags set from session are prefixed with session UUID`: existing Comet timing
  guard in `SparkSessionJobTaggingAndCancellationSuite` (1).
- `SPARK-48792: Fix INSERT with partial column list to a table with char/varchar`:
  existing `supportColDefaultValue` TODO in `CharVarcharTestSuite` (1).
- `inject adaptive query prep rule`, `inject columnar AQE on`,
  `inject columnar AQE off`, `reset column vectors`, and
  `SPARK-38697: Extend SparkSessionExtensions to inject rules into AQE Optimizer`:
  existing `SparkSessionExtensionSuite` guard because Comet replaces the tested
  operators (5).
- Hive V1 `ALTER TABLE .. ADD COLUMNS ... add an ANSI interval columns`, and
  `ALTER TABLE .. ADD PARTITION ... SPARK-37261: Add ANSI intervals as partition values`:
  original Hive interval-type guards (2).

Four anonymous cancellation units in older Java 17 Hive summaries could not be
bound to names; that historical reporting gap was not presented as new passes.
The Java 25 target's nine cancellations above are separately identified. Existing
version guards, ignored tests and early-return bodies remain outside positive
coverage. Four HDFS-cluster tests were not executed because no HDFS cluster was available;
three manual native performance benchmarks remain unrun. Target workloads use
Parquet/Delta, not Iceberg; existing older-profile Iceberg CI was nevertheless
retained. Some Iceberg extension artifacts contain writer JSONL, not JUnit XML:
actual task success and observed event lower bounds were recorded without
inventing an exact XML count.

## Integration validation criteria

All fifteen passed on the qualified runtime in G10 source
`81474a889489da4a4a4aa8e49330e6398c1937e3`; the Java 25 pass above repeats the
required runtime, cohort, selector, fuzz, cost and packaged gates. Preserve these
criteria when changing the relevant feature; a green job alone is insufficient.

| Validation ID                       | Criterion retained for maintenance                                                                                                                                                                                                                           |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| G02-LINUX-AMD64-RELEASE             | Actual Linux FFI/extent/cancellation/explode/JNI/partitioned-batch and JVM TaskContext/ownership/timestamp/Boolean paths; separate release linkage on both architectures                                                                                     |
| G02-CANCEL-SPARK-MEMORY-WAIT        | Six real-JNI wait/race registrations: competing memory remains held, actual blocked Spark acquisition, `interruptOnCancel=false`, bounded TaskKilledException, no successful batch/EOF, zero final reservations/contexts/Arrow allocations                   |
| G02-OFFSET-CONSUMERS                | Real window/coalescer nested and sliced Boolean/List/Map/Struct consumers; full 1,140,850,687-byte allocation and offset-boundary witness, correct rows/schema/order and ownership                                                                           |
| G03-LINUX-RELEASE                   | Sort/wide payload/spill/handoff/early-drop/cancellation/metrics; native spill/grants, no post-cancel success, zero final reservations and removed spill files; ReservationStream owns an error result until drop                                             |
| G03-WORKSPACE-CLIPPY-TOOLCHAIN      | Strict locked whole-workspace/all-targets lint; no vendor exclusion or global deprecated allowance; only the statement-local exemption                                                                                                                       |
| G04-LINUX-RELEASE                   | Aggregate/SortAggregate/specialized/mixed-buffer semantics and emit/offset/memory/drain; native PartialMerge pressure must spill and produce 24,000 exact distinct `(g,k)` outputs/counts/max with zero final grants                                         |
| G04-FULL-FUZZ                       | All 31 exact registrations, actual full-matrix JVM flag, original Spark results/errors and actual native/spill paths                                                                                                                                         |
| G04-INHERITED-UNRUN                 | Two aggregate metric and six mixed/native Bloom fixtures restored; positive owned aggregate peak, four native recorder/replay witnesses and exact original-Spark Bloom buffers in all three Spark/native phase combinations                                  |
| G05-LINUX-RELEASE                   | Ten exact ANSI SMJ cases, FULL unmatched/duplicate rows, lazy branches/mandatory errors, semi/anti and AQE clustering; original Spark oracle plus native or guarded fallback                                                                                 |
| G05-FULL-FUZZ                       | All 33 exact registrations and full flag; 4,881 internal comparisons retain the original 4,869 criterion plus 12 additive route pairs; seeds/multiset/error/spill checks preserved                                                                           |
| G06-LINUX-RELEASE                   | Three window suites plus SparkErrorConverter; 88/88 native actions and three positive window-owned spill-count/bytes actions; suffix and IGNORE NULLS fallbacks retained                                                                                     |
| G07-LINUX-RELEASE                   | C2R/pooling/broadcast/payload/writer/spill/direct-reader/coalescer and default-Kryo selectors; exact final JAR/class/native origins on both architectures                                                                                                    |
| G07-G09-BROADCAST-COST-MATRIX       | Four staged/skewed cases: cost false/true × maxFields 3/100; original Spark rows, actual skewed Spark SMJ and Spark BHJ reading a Comet broadcast stage, expected codegen boundary                                                                           |
| G08-FINAL-LINUX-RELEASE-AND-FULL-CI | Scalar/codegen/HOF/short-circuit/conditional/UDF/serde/QueryContext/injection; rows/schema/mandatory errors, dispatcher/native/fallback and per-task capture ownership; separate 58+5 regression gates and shaded UDF/Delta serialization                    |
| G09-FINAL-LINUX-RELEASE-AND-FULL-CI | Boundary/wide-sort/wide-shuffle/cost selectors, both ordinary TPC plan-stability suites without golden generation, four boundary/cost modes, mixed Spark SUM/AVG/Bloom buffers, all six 66-case profiles and seven packaged engine/wide/hash/range selectors |

The temporary vendor-Clippy exclusion was removed before that complete CI;
normal automatic routing and all standard quality gates were restored.

## Immutable delivered JARs and receipts

Qualified export: `rc-build-smoke-7e3b6bcf87389f9cdcbe27db6f74562b5e792c05`
from [RC run 45292086072](https://github.com/joomcode/datafusion-comet/actions/runs/45292086072).

| JAR                                           | SHA256                                                             |
| --------------------------------------------- | ------------------------------------------------------------------ |
| `comet-spark-spark4.2_2.13-1.2.0.jar`         | `d1dd1b5bcbfcf5b27577ea687ecc5a046b7da73e64d1af81bd5185f4c752e870` |
| `comet-contrib-delta-spark4.2_2.13-1.2.0.jar` | `7f5880dc11533a35c891e15111254e4e2bc979dbdefd81b81f3aa85d0fe0b444` |

All 97 extracted release/provenance/native/smoke files were verified against the
API-digest archive. Preserve `release-qualification.json`, `sha256.txt`, native
build/provenance records, both smoke receipts and the JARs together. Future Maven
publication uses an immutable Joom version; a filename with
`1.2.0` alone does not distinguish this fork or qualify any downloaded artifact.
See [RC-BUILD-SMOKE.md](RC-BUILD-SMOKE.md).

The older G10 Java 17 baseline used CI 45084222684, RC 45084227984, Miri
45084225013 and Actions CodeQL 45084212970 on source `81474a889`.
Its Core JAR SHA256 was
`353974112e4a11d24f1781074385cb913d7f10dc1f2de397e339286f0573815e`;
its Delta JAR matched the current Delta hash above. Those are historical Java 17
proofs, not substitutes for the current runtime or Core JAR.

## Docker image correspondence, not image qualification

Registry metadata verified on 2026-11-24 for
`eclipse-temurin:25.0.5.1_2-jre` identifies the same Java release/build on Linux
AMD64 and ARM64. Its index digest was
`sha256:4ed132f6832e9804b123e89a849a7b4502756ae64a4ee08ebab939d9d5d0a9fa`.
It contains a **JRE on Ubuntu 26.04**. Packaged qualification used the exact
Temurin **JDK on Ubuntu 24.04** native runners. The JRE image and any derived
Spark image were not runtime-qualified by these workflows. Version/build
correspondence is not exact-container qualification.
Preserve that distinction until smoke executes inside the actual target image.
