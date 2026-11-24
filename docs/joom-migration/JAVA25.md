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

# Spark 4.2.1 qualification on Eclipse Temurin 25

## Authorization and boundaries

On 2026-11-23 the user authorized implementation on a separate branch from
`branch-1.2`, full agreed GitHub Actions qualification and final-JAR RC Build &
Smoke on native Linux AMD64 and ARM64, including prerequisites for the 58
previously canceled pandas scenarios. Feature publication and the necessary
GitHub Actions operations are included. **Do not merge, create a stable tag,
publish to JFrog, modify main or datasci, or deploy to production.** The known SQL
semantic exceptions discussed after G10 are explicitly outside this change.

The later2026-11-24 user acceptance explicitly authorizes the documentation
commit, feature push and branch-1.2 integration. It supersedes the original
merge hold above for this operation; stable tag/publication holds remain.

- Base: `d2a01d4b6bd5a5e05fcfa712a9c74e6e889fcdbb`.
- Feature: `joom/1.2-java25`.
- Target: official Spark 4.2.1, Scala 2.13.18, Comet 1.2.0, Delta 4.4.1,
  DataFusion 55.2.0, **Eclipse Temurin 25.0.5.1+2**.
- Runtime properties: `java.vendor=Eclipse Adoptium`,
  `java.version=25.0.5.1`, `java.runtime.version=25.0.5.1+2-LTS`.
- Preserve ANSI=true and `spark.sql.columnVector.offheap.enabled=false` in
  qualification, including the original-Spark differential baseline.

`dev/ci/java25-runtime.json` is the version contract. A major-only declaration
is insufficient evidence: setup preflight and final-JAR receipts must identify
the actual vendor and build. Future JDK updates require new qualification and
honest provenance. Historical G10 Java17 results are not Java25 evidence.

## Implementation and acceptance

1. Make Spark 4.2/Temurin25 the primary Comet and queue SQL/Delta gate. Keep
   older Spark/JDK compatibility rows and the original native/vendor/quality
   checks. Execute the JNI tests and TPC-H/DS against the target profile/JVM.
2. Align direct JVM launch options with official Spark 4.2.1. Keep Java17
   bytecode and the existing Linux release-builder/GLIBC2.31 contract; the
   builder JDK is distinct from the final-JAR qualification JVM.
3. Run stock Spark4.2 SQL tests with a pinned, hash-checked Python environment
   inside each test container. Import PySpark from the exact Spark checkout,
   never a second PyPI pyspark. Fail before discovery if Spark's own pandas and
   PyArrow prerequisites are missing. Do not install Python packages locally.
4. Preserve all result/error/native-execution assertions and required runtime,
   window and full fuzz selectors. Explicitly reconcile the previously canceled
   pandas identities against actual new positive results; a green job alone
   does not close them.
5. Run the full agreed exact-source CI, Miri, Actions CodeQL and RC workflow;
   verify final JAR checksums, loaded classes/native bytes and native AMD64/ARM64
   execution. Preserve failures and skips without awarding positive credit.

## Current qualification status, 2026-11-24

**The agreed Java 25 qualification passed** on source
`7e3b6bcf87389f9cdcbe27db6f74562b5e792c05` in `joom/1.2-java25`.
Full Comet CI succeeded with 138 successful jobs and the same 16 structural skips
as the original complete run. Failed-only retries resolved the pretest Maven
Central 429 and Gradle-wrapper bootstrap failures without changing source,
weakening tests or restarting successful executions. The final Delta 4.1 retry
actually passed 295 ScalaTest cases and 18 separate encryption JUnit cases; an
empty reactor-module summary receives zero test credit. All original failures
remain preserved as historical failures.

| Gate                               | Current actual evidence                                                                                                                                                                                       |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spark 4.2.1 / exact Temurin 25 SQL | All 9 shards succeeded: 34,876 passed, 0 failed, 9 canceled, 7,922 ignored. Nonpasses receive zero positive credit.                                                                                           |
| Original migration requirements    | 103 selectors, 24 normal JVM cohorts, 6 CostChoice profiles with 66 exact cases each.                                                                                                                         |
| Native/ANSI/windows                | 98 core cases, 88 native window actions, 31 aggregate and 33 SMJ full-fuzz registrations; 3 additional prefetch cleanup cases.                                                                                |
| Python prerequisites               | All 58 previously canceled pandas registrations passed in 8 concrete suites; dependencies installed only in CI.                                                                                               |
| Additional regressions             | 58 corrective cases passed on target Java 25; 5 original Spark 3.4 Scala-UDF cases passed in the retained Java 17 compatibility row. These 58 corrective cases are separate from the 58 pandas registrations. |
| Native/vendor/quality              | Full native/vendor/JNI and strict Clippy passed. Miri 107 selected tests and Actions CodeQL passed. Preserve each invocation's separate counts and explicit skips.                                            |
| Final release bytes                | RC 27 existing cases passed on each native Linux AMD64/ARM64, with actual vendor/build, class origins and bundled native-library checks.                                                                      |

Actual Temurin properties were verified in 8 required runtime XML suites, and
separately in both architecture smoke receipts. The complete expressions text
report confirms the 58 corrective registrations; its generic artifact does not
contain those XML cases, so no XML coverage is invented. Existing cancellations,
ignores and version-dependent early-return bodies retain their original limits.
The 9 stock SQL cancellations are named in `spark42-all-nine-actual-SQL-proof.json`.

Current workflow IDs are [Comet CI 45292058669](https://github.com/joomcode/datafusion-comet/actions/runs/45292058669),
[RC 45292086072](https://github.com/joomcode/datafusion-comet/actions/runs/45292086072),
[Miri 45292072441](https://github.com/joomcode/datafusion-comet/actions/runs/45292072441)
and [Actions CodeQL 45292009717](https://github.com/joomcode/datafusion-comet/actions/runs/45292009717).
The immutable actual executions and artifact producers are retained separately
from GitHub's retry status aliases: 125 successful executions originated in
attempt 1 and 11 in attempt 2. The final Delta 4.1 job and Required Checks
executed in attempt 3. No alias is a new execution or a new artifact producer.

Verified JARs and all 97 associated provenance/native/smoke files are in
`target/joom-release/gha-45292086072`:

| File                                          | SHA256                                                             |
| --------------------------------------------- | ------------------------------------------------------------------ |
| `comet-spark-spark4.2_2.13-1.2.0.jar`         | `d1dd1b5bcbfcf5b27577ea687ecc5a046b7da73e64d1af81bd5185f4c752e870` |
| `comet-contrib-delta-spark4.2_2.13-1.2.0.jar` | `7f5880dc11533a35c891e15111254e4e2bc979dbdefd81b81f3aa85d0fe0b444` |

The release builder still uses Java 17 and Java 17 bytecode to preserve the existing
build/GLIBC contract; actual packaged runtime qualification uses the pinned
Temurin 25 build. Do not relabel compiler provenance as Java 25. No production
algorithm, DataFusion vendor patch or SQL fixture changed in this Java 25 step.
The only changed Spark SQL test file follows the independently verified JDK
SHORT_IDS semantics described below. RC verifier unit fixtures were also updated
for the pinned vendor/build; SQL inputs and strict result/error/native assertions
remain unchanged.
The known SQL semantic exceptions excluded by the user are unchanged.

Evidence, source manifests, real execution maps, local preflight checks and the
full failed-attempt history remain under
`${MIGRATION_AUX_ROOT}/20261123_comet12-java25`; start with LIVE.json,
CONTINUE.json and current-evidence-index.json. The current closeout preparation
preserves all 57 prior documents in
`20261124_closeout-preparation/documents-before-finalization.zip`.
The corresponding archive receipt records its SHA256. Nothing was deleted.

All four workflows are complete; periodic polling has stopped. Final evidence
is indexed by `20261124_final-closeout/completion.json` in the auxiliary root.
The user accepted this qualification on2026-11-24 and authorized the final
English documentation commit, feature push and integration into branch-1.2.
The qualified executable source SHA and delivered JAR provenance are unchanged.
Actual integration identities and preservation checks are recorded in
`${MIGRATION_AUX_ROOT}/20261124_comet12-java25-integration/completion.json`.
The documentation-only acceptance commit does not requalify or relabel JARs.
Stable tagging, JFrog publication, main/datasci changes and production operations
remain excluded.

## First remote preflight and correction

Candidate `382812035bad1bb34f9100b1c06d4c4c09e869d7` passed the RC source
preflight on the exact Temurin25 build (RC45274526038). GitHub rejected the
Linux reusable workflow because two TPC jobs had duplicate `env` mappings.
That RC was canceled promptly after the native jobs started; it is not a
qualified candidate. The correction merges the reader setting into the existing
module-access options, adds a duplicate-job-field preflight with a rejecting
negative fixture, and executes CI configuration checks before RC native builds.
The push-triggered Actions CodeQL success on that older SHA is historical only.

The next source `11299ba20cc5aa73e902d19560f7a3e05a6b85be` passed the RC source
checks but full CI45275125268 stopped with startup_failure before creating test
jobs: the older Iceberg reusable workflow requires numeric Java inputs, whereas
the target Spark reusable workflow now accepts a complete version string. Keep
Iceberg's four Java17 arguments numeric. RC45275117723 and Miri45275131628 were
canceled for that source correction. Add pinned actionlint1.7.9 in the RC source
gate to validate workflow syntax, expressions and reusable input types before
native builds. It runs only on GitHub runners; no local tool installation.

Source `006ea1d02ba014c410d6b156a977bc04ba8967de` passed actionlint, exact
Temurin25 verification and RC unit checks. Full CI45276446103 then exposed a
shared tooling incompatibility: setup-java@v4 rejects the four-component
`25.0.5.1+2` version as invalid SemVer. Six target jobs stopped at JDK setup,
before tests. The RC source job already used setup-java@v6 successfully. Update
the common Linux setup to v6 and retain exact vendor/build verification; do not
relax the version pin. RC45276015510 and Miri45276452768 were canceled along
with that obsolete full CI. The next full CI must confirm the shared setup on
the target containers before starting another release build.

## Java25 formatter compatibility

Source `8c387fca0ec1af1510118972078efa6a5fbb05cc` successfully installed and
verified the exact Temurin build in the target CI containers; the target Scala
lint completed. PyArrow job138057269294 then failed before tests because
Spotless2.43.0's google-java-format1.19.2 calls a removed javac API
(`Log.DeferredDiagnosticHandler.getDiagnostics`). CI45278537996,
RC45279620571 and Miri45278543530 were canceled for the source correction;
their partial results do not qualify a replacement source.

Pin Spotless3.0.0, which includes google-java-format1.28.0 and supports Java17
and Java25. Keep the existing Scala formatter version, Java import/license
checks, complete includes and every SQL assertion. Actual local Java25 checks
passed for all106Java and684Scala files without changing source formatting;
the Delta profile also passed. Java17 compatibility checks passed with normal
incremental caching. No formatter check is disabled and no production algorithm
or test body changes. See the [Spotless3.0.0 changelog](https://github.com/diffplug/spotless/blob/main/plugin-maven/CHANGES.md#300---2025-09-24).

## Independent stock-Spark proof for Java25 test oracles

Full CI45281111217 on source955453772 exposed three expression-test failures:
two dictionary variants of trunc_timestamp.sql required the literal error text
`long overflow`, and the timezone adapter test assumed that EST always means
fixed offset-05:00. Independent execution of original Spark4.2.1 JARs on the
installed exact Temurin25 build, with Comet absent from the classpath, confirms:

- The JDK's SHORT_IDS table and Spark both resolve EST to America/Panama,
  MST to America/Phoenix and HST to Pacific/Honolulu. Java17 resolves those
  aliases to fixed offsets. The adapter already follows Spark correctly.
- After JIT warmup, Java25's ordinary fast-throw optimization emits
  ArithmeticException without its message/stack. Every one of350000 tested
  original-Spark truncation calls still threw; none returned a result. With
  `-XX:-OmitStackTraceInFastThrow`, every call retains `long overflow` and a
  stack. The corresponding Java17 control also retained the full diagnostics.

Keep all SQL inputs, native-expression assertions and literal error assertions
unchanged. The target expressions CI cohort disables only fast-throw diagnostic
elision, for Spark and Comet alike. This is a permanent test diagnostic setting,
not an operator fallback or a production requirement. RC packaged smoke keeps
ordinary fast-throw and the original mandatory-error checks. The timezone test
uses the selected JDK's public SHORT_IDS table as its independent oracle and
adds MST/HST coverage. No production timezone/timestamp implementation changes;
the user's excluded known SQL exceptions remain outside scope.

Local Java25 test compilation/formatting and all three formerly failing selected
tests passed (1timezone registration and2dictionary SQL registrations), with
zero cancellations/ignores. Full CI was canceled promptly after diagnosis;
its15pretest MavenCentral429 failures and3dependent missing-shard failures
remain recorded, not passed. The successful RC27/architecture, Miri107 and
Actions CodeQL on955453772 are preserved as historical evidence. A replacement
source still requires full agreed CI and final-JAR qualification.

Proofs are under `${MIGRATION_AUX_ROOT}/20261123_comet12-java25`, especially
`20261123_stock-spark-probe/independent-runtime-proof.json` and
`java25-local-targeted-results.json`. Preserve their exact raw logs and limits.
