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

# Joom Comet 1.2 maintenance guide

This directory records the maintained Joom fork for official Apache Spark 4.2.1.
The migration from `joom-1.1-13` and the subsequent Java 25 qualification
are integrated into `branch-1.2`. The integration baseline
is `586bbbf614823d70535a9ec1a384ea63aff7f4e0`; qualification and delivered JARs
refer to executable source `7e3b6bcf87389f9cdcbe27db6f74562b5e792c05`.

## Finding information

| Document                                           | Maintenance purpose                                                                                                                            |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| [FEATURES.md](FEATURES.md)                         | Complete Joom feature inventory, source lineage, implementation and regression entry points, upstream replacements and correctness adaptations |
| [DATAFUSION.md](DATAFUSION.md)                     | Exact vendor origin, retained patches, Cargo dependency graph and upgrade requirements                                                         |
| [RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md) | Version/configuration contract, known limitations and safe fallbacks                                                                           |
| [QUALIFICATION.md](QUALIFICATION.md)               | Qualified source, actual CI coverage and limits, immutable artifact hashes and evidence identities                                             |
| [VALIDATION.md](VALIDATION.md)                     | How to verify future fixes, independent Spark result/error oracles, native execution, fuzz and established performance tools                   |
| [RC-BUILD-SMOKE.md](RC-BUILD-SMOKE.md)             | Reproducible Linux dual-architecture build, packaged smoke and release/publish procedure                                                       |

## Traceability for future fixes

Keep the chain **feature ID / production symptom -> rationale -> implementation
and commit -> regression and independent oracle -> exact qualified source ->
artifact SHA256**. FEATURES.md supplies the implementation map; QUALIFICATION.md
supplies the qualification shared by those implementations.
An upstream analogue replaces a Joom patch only after the retained behavior and
native execution have been checked. Partial analogues do not justify silently
discarding remaining Joom behavior.

For a production finding, retain a deterministic minimal reproduction, schema,
relevant SQL/Comet settings, Java and dependency versions, physical/native plans,
expected Spark-only result or mandatory error, affected artifact hashes and the
fix/regression identity. Distinguish fallback, resource failure and a successful
wrong result. Update the relevant feature and limitation entries after review.
Do not erase an old qualification when a new source or Java build is tested.

## Build paths and release evidence

Project paths in these documents are relative to the Comet repository root.
In Markdown links, `../../` resolves from this directory to that root.

| Notation        | Meaning                                           |
| --------------- | ------------------------------------------------- |
| `${COMET_ROOT}` | Comet checkout used by the build commands         |
| `${HOME}`       | Home directory used by standard build-tool caches |

Set the variables to the appropriate paths before using command examples.

Qualification runs and artifact hashes are indexed by QUALIFICATION.md. Preserve
the complete release bundle, including build manifests and architecture-specific
smoke receipts, independently of GitHub artifact expiry.
