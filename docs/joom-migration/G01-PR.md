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

> **Final source-freeze note:** G01 implementation and documentation are complete. RC1 acceptance is a post-commit gate recorded by the immutable `joom-1.2-1-rc1` tag and the delivered artifact receipts; see `G01-CLOSEOUT.md`. Earlier source SHAs, runs and pending states below are historical evidence. G02 and Java 21 work remain unauthorized.

## Which issue does this PR close?

Related to apache/datafusion-comet#4967. This is migration G01; it does not claim to close the full upstream ANSI issue.

## Rationale for this change

Prepare the Joom Comet 1.2 baseline for Spark 4.2.1 and Delta 4.4.1, retain the agreed Delta/GCP behavior, and provide a tested vendor and release foundation for the remaining Joom feature ports.

## What changes are included in this PR?

- Pin Spark 4.2.1, Scala 2.13.18 and the Spark-4.2-specific Delta 4.4.1 artifact. Adapt the Spark SQL fixture and CI lanes.
- Preserve GCP ADC admission, safe Delta planning, driver-only payload serialization and DPP/empty scans. Add the conservative Hadoop encryption-context admission guard.
- Import pristine published datafusion-physical-plan 55.2.0 separately, connect it through the root workspace/patch/lockfile, and explicitly test vendor with the Comet feature graph. Vendor production algorithms remain unchanged; four spill tests are adapted to Arrow 59.3.2.
- Match tiny-decimal casts to Spark's patch-level behavior and reject ambiguous projected Parquet field selection with Spark's structured error envelope.
- Preserve Union partition layouts and immutable planning policy through AQE copies, bind layouts to the rewritten output, and fail before an incompatible RDD layout can silently corrupt aggregate results.
- Retain original Spark fixture contracts and add native result/error/layout counterparts. Enable the three existing ANSI arithmetic tests, keep ADD JAR checks on isolated local Ivy fixtures, and correct the synthetic Delta DV offset.
- Provide dualarch release build, integrity and publication scripts. Add a separate manual **RC Build & Smoke** workflow: two native release builds, one final packaging, two exact-JAR smoke jobs and a final receipt check. It exports downloadable artifacts and performs no JFrog/Nexus/production publication.
- Use available Ubuntu runners and retry only transient parent-POM transfers before any compilation/test lifecycle. Migration decisions, evidence and cleanup guidance remain under docs/joom-migration.

## How are these changes tested?

The full native AMD64 Comet CI [44205897075](https://github.com/joomcode/datafusion-comet/actions/runs/44205897075) passed on `26590264b878fb8eae2ca439a8bcc0dfb513cb36`: 127 successful jobs, 17 conditional skips and no failures. It includes native 2649, vendor 1695, statistics 42 and Delta 294 tests, all nine Spark 4.2.1 SQL shards, and the other enabled Spark/Iceberg/macOS matrices. Artifact inspection confirms actual execution of the three formerly canceled ANSI arithmetic cases, both decimal rescaling cases, all ten duplicate-reader regressions and both Union regressions on Spark 4.1/4.2. Existing cancellations and ignored tests are reported separately, never counted as passed.

Subsequent commits change only release/CI tools and migration documentation; native, JVM, Delta runtime sources, Spark fixture and dependency locks remain byte-identical to that CI-qualified commit. Full Comet CI for these commits was intentionally stopped at the owner's request. Exact-source Apache RAT for `b93d553856a0339af7468bcff2070e735a1a3c71` passed separately.

The new manual [RC Build & Smoke 44388252264](https://github.com/joomcode/datafusion-comet/actions/runs/44388252264) passed all seven jobs on `b93d553856a0339af7468bcff2070e735a1a3c71`. Both native architectures built with the release profile. The same final dualarch JARs passed **12/12 existing smoke cases per architecture**: Delta 4, DPP 1, reload 1 and ANSI 6, with no cancellations/ignores. The harness checks actual Spark 4.2.1, Java 17, class origins and the SHA256 of the loaded bundled native library. The final job binds both receipts to the same source, lockfile, package, support and JAR hashes and exports those original bytes without repackaging.

The qualified artifact was downloaded and its GitHub ZIP digest, runtime JAR hashes, embedded Git revision, native graph/ELF provenance and both receipts were verified locally. Local tool validation includes 22 fault tests (9 native-stage, 7 portable-smoke, 6 receipt-binding), actionlint, CI configuration and Bash syntax. The Focal Git ownership issue was reproduced with UID 1001 in the actual builder and fixed with a container-only, checkout-specific system config.

The old source2b8f80b3f JARs and incomplete Rosetta runs are historical. Current release-byte AMD64 qualification is complete on a native runner.

This PR is G01 only. G02-G09, real GCS/GKE reads and production performance are not qualified here. The user has authorized G01 history/documentation finalization and conditional fast-forward integration plus RC1 tagging after exact-SHA RC qualification. The annotated tag and artifact receipts identify final acceptance. No external publication or production operation is included.
