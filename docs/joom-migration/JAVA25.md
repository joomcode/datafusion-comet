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

## Current progress and evidence

Implementation is in progress; **Java25 qualification is not complete**.
Local preflight verified the installed Temurin vendor/full build. The current
RC harness has 19 passing tests and the receipt validator has 28, including
rejection of a wrong vendor, old Java and a different build. CI routing checks
pass; Maven's offline effective POM resolves on the selected JDK.

Auxiliary evidence, baseline hashes, logs, metadata and later run identities are
under `${MIGRATION_AUX_ROOT}/20261123_comet12-java25`. The baseline preserves
protected branch/main/tag refs and all tracked file hashes. Do not infer remote
success from this document; record actual run IDs, source SHA and receipts.

Poll active GHA at five-minute intervals. Diagnose genuine failures promptly;
if a source change requires a replacement full run, report before canceling the
old run. A quiet or stale live-log tail alone is not proof of a hang.

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
