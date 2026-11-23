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

# Spark 4.2.1 deployment requirements

These requirements were explicitly approved on 2026-11-22 for the current
Comet 1.2 migration. They apply to the eventual deployment, not permission to use
an unqualified candidate in production. G10 and final acceptance remain open.

## Required Spark SQL defaults

Configure the driver/session before queries start, for example in the Spark
image defaults or the launcher used by all target jobs:

```properties
spark.sql.ansi.enabled=true
spark.sql.columnVector.offheap.enabled=false
```

Keep both values in production and in Spark-only differential baselines and
Comet qualification. Audit job/session overrides before deployment. Explicit
`columnVector.offheap.enabled=true` is outside the currently qualified production
contract. Setting it false does not disable native Comet or change the separately
configured Spark off-heap execution pool. Keep the existing Comet memory settings.

The current target remains official Apache Spark 4.2.1 / Scala 2.13.18,
Comet 1.2.0, Delta 4.4.1 and DataFusion 55.2.0. The newly selected qualification
runtime is Eclipse Temurin 25.0.5.1+2 (minimum supported Java25 update:25.0.3).
This new qualification is IN PROGRESS; prior G10 receipts qualify Java17 only.
See JAVA25.md and dev/ci/java25-runtime.json. Java17 bytecode/build tools do not
constitute proof of runtime testing on Java25. No custom Spark build, replacement
Spark classes or production configuration changes are performed by this Comet
repository work. Pass this requirement to the owner of the Spark image/launcher;
changes in datasci are outside this task.

## Reason and limits

Stock Spark 4.2.1 has SPARK-60098: the vectorized Parquet reader fails to close
separately allocated auxiliary definition/repetition vectors for affected nested
columns. The upstream fix is absent from target Spark commit
`617618396087429b5627a86ce933bb0f5d6645cb`. An official-Spark-only native AMD64
witness observed 4,096,000 bytes retained after closing 100 nested readers and
exactly that amount released by explicitly closing the omitted auxiliary vectors.
The flat-column control had no growth. This establishes the defect, not its exact
share of every byte in the separate warmed fuzz-suite OOM.

The required false setting makes these Spark vectors use JVM heap and avoids
this unmanaged off-heap retention. It can increase heap occupancy, allocation and
GC costs on Spark reader/fallback paths; no production performance percentage has
been established. Native Comet paths remain enabled. The demonstrated defect is
an ownership leak, not a demonstrated successful SQL result mismatch. Resource
exhaustion can still fail jobs or interrupt writes, so the requirement is part of
resource correctness as well as reproducible qualification.

References: [SPARK-60098](https://issues.apache.org/jira/browse/SPARK-60098),
[upstream fix](https://github.com/apache/spark/commit/808d79616144fbf209244e9c7a27927907908768),
[4.x backport](https://github.com/apache/spark/commit/c5808b781be5cebb93b961aed8fb3e9cc9c26901).
Actual diagnostic receipts are indexed by G10-results.json and STATE.json under
`${MIGRATION_AUX_ROOT}/20261121_comet12-g10`.

## Qualification and future changes

This is a persistent, approved runtime requirement, not a temporary CI bypass.
Target Spark 4.2 test-session defaults and forked Spark SQL test JVMs use the same
setting. Explicit off-heap and ANSI=false regression variants remain in place;
no original input, expected result/error, native-plan assertion or fuzz matrix is
removed. ANSI=true remains the production default.

RC Build & Smoke checks the actual session defaults in every smoke group on
native AMD64 and ARM64, in addition to exact release JAR/native-library bytes and
class origins. The delivered release-qualification.json carries the required SQL
configuration and each group's observed defaults. Those observations describe
session startup; individual tests may deliberately exercise other SQL modes.

Before enabling off-heap Spark column vectors in a future deployment, separately
review the exact fixed Spark artifact, compatibility and resource behavior, and
repeat affected correctness/resource and packaged qualification. This repository
does not silently change the setting when a newer Spark version is installed.
