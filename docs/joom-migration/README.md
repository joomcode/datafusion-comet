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
The agreed migration from `joom-1.1-13` and the subsequent Java 25 qualification
were accepted and integrated into `branch-1.2`. The accepted integration baseline
is `586bbbf614823d70535a9ec1a384ea63aff7f4e0`; qualification and delivered JARs
refer to executable source `7e3b6bcf87389f9cdcbe27db6f74562b5e792c05`.
Documentation maintenance does not change that artifact provenance.

Stable tagging, JFrog publication, production deployment and changes in the
separate datasci repository have not been authorized. These are action boundaries,
not claims that production performance or every possible SQL expression was tested.
Use actual Git state when resuming; documentation is not a replacement for it.

## Finding information

| Document                                           | Maintenance purpose                                                                                                                            |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| [FEATURES.md](FEATURES.md)                         | Complete Joom feature inventory, source lineage, implementation and regression entry points, upstream replacements and correctness adaptations |
| [DATAFUSION.md](DATAFUSION.md)                     | Exact vendor origin, retained patches, Cargo dependency graph and upgrade requirements                                                         |
| [RUNTIME-REQUIREMENTS.md](RUNTIME-REQUIREMENTS.md) | Version/configuration contract, known limitations and safe fallbacks                                                                           |
| [QUALIFICATION.md](QUALIFICATION.md)               | Accepted source, actual CI coverage and limits, immutable artifact hashes and evidence identities                                              |
| [VALIDATION.md](VALIDATION.md)                     | How to verify future fixes, independent Spark result/error oracles, native execution, fuzz and established performance tools                   |
| [RC-BUILD-SMOKE.md](RC-BUILD-SMOKE.md)             | Reproducible Linux dual-architecture build, packaged smoke and release/publish procedure                                                       |
| [CLEANUP.md](CLEANUP.md)                           | Outputs, caches, task resources and optional manual cleanup after evidence preservation                                                        |
| [NETWORK.md](NETWORK.md)                           | Previously approved network sources and their scope/conditions                                                                                 |
| [AGENTS.md](AGENTS.md)                             | Current editing and execution rules for this maintenance documentation                                                                         |

## Traceability for future fixes

Keep the chain **feature ID / production symptom -> rationale -> implementation
and commit -> regression and independent oracle -> exact qualified source ->
artifact SHA256**. FEATURES.md supplies the implementation map; QUALIFICATION.md
supplies the final accepted qualification shared by those implementations.
An upstream analogue replaces a Joom patch only after the retained behavior and
native execution have been checked. Partial analogues do not justify silently
discarding remaining Joom behavior.

For a production finding, retain a deterministic minimal reproduction, schema,
relevant SQL/Comet settings, Java and dependency versions, physical/native plans,
expected Spark-only result or mandatory error, affected artifact hashes and the
fix/regression identity. Distinguish fallback, resource failure and a successful
wrong result. Update the relevant feature and limitation entries after review.
Do not erase an old qualification when a new source or Java build is tested.

## Paths and evidence

Project paths in these documents are relative to the Comet repository root.
In Markdown links, `../../` resolves from this directory to that root.
Committed files must not contain a developer's absolute checkout/home/temp paths.

| Notation                                                                                                | Meaning                                                                         |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `${COMET_ROOT}`                                                                                         | Explicitly selected current Comet checkout                                      |
| `${MIGRATION_AUX_ROOT}`                                                                                 | User-selected external auxiliary root containing the dated evidence directories |
| `${UPSTREAM_COMET_ROOT}`, `${UPSTREAM_DATAFUSION_ROOT}`, `${UPSTREAM_SPARK_ROOT}`, `${JOOM_SPARK_ROOT}` | User-provided research checkouts; preserve them                                 |
| `${HOME}`                                                                                               | User home for shared SDK/tool caches; not a prescribed machine path             |

These are literal placeholders, not executable values or new permissions.
Resolve explicit local bindings before use, quote shell variables and never use
`eval` on documentation. New executor-created auxiliary directories, including
nested ones, use `YYYYMMDD_description` under the approved auxiliary root.
Normal package-manager caches and tool-managed internal directories keep their
usual locations. Ask for missing tools/permissions/checkouts instead of inventing
lengthy workarounds. Never infer a datasci checkout as an approved resource.

Raw evidence remains outside Git. Current qualification evidence is indexed by
QUALIFICATION.md. Preserve the complete delivered artifact bundle and important
receipts independently of GitHub artifact expiry or optional local cache cleanup.
The old local path binding receipt is
`${MIGRATION_AUX_ROOT}/20261119_comet12-portable-paths/local-paths.json`.

## Historical migration material

The active directory deliberately contains maintenance facts rather than pause
logs, old permissions, stage reports or duplicate JSON snapshots. Their original
content remains available at the accepted integration baseline, for example:

```bash
git show 586bbbf614823d70535a9ec1a384ea63aff7f4e0:docs/joom-migration/G04-report.md
```

That history is not a current instruction or an unclosed implementation stage.
The maintenance documents preserve the useful decisions and implementation/test
links without requiring a historical file or a laptop-specific receipt merely
to understand the current fork. Historical raw logs still have the retention
requirements described in CLEANUP.md.
