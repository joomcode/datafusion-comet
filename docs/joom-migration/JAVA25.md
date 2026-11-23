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
