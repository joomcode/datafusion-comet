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

# G08: JVM codegen and task serialization

G08 is implemented and locally qualified on `joom/1.2-jvm-codegen` from accepted
G07 `1a2734992`. Await user review before push/integration. `branch-1.2`, main,
RC1 and the approved scope are unchanged; G09 has not started.

## Retained behavior and adaptations

| Scope | Result                                                                                                                                                                                                                                                                                                           |
| ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| J05   | Scalar subqueries are typed runtime arguments. One cached kernel refreshes length-one/vector masks, values and NULLs per batch, including Boolean/Binary/negative-scale Decimal/Array/Map/Struct getters. Cardinality guards precede unsafe reads.                                                               |
| J06   | Restore deterministic JVM filter masks above a proven safe prefix and keep long AND segments balanced. Mandatory Spark NULL/order guards remain when the optional optimization is disabled.                                                                                                                      |
| J07   | Share live UDF functions and captures once in Spark's task-serialization stream. Fresh actions snapshot current driver state; tasks/retries own independent graphs. Typed references, current digest/occurrence protocol, task/classloader lifecycle and all execution roots are retained.                       |
| J17   | Heavy shuffle plans stay with the map-side RDD/iterator; dependencies retain only small executor-admission configuration. Driver operator caches are transient. Explicit task carriers retain required plans/functions. Common scan/Delta SQL text joins the root pool without changing keys or error locations. |
| C02   | Preserve expression-level lazy AND/OR and three-valued NULL behavior, with row ordering where both operands can fail or advance state. Validate compact output length before scatter. Guard unsafe operator-wide CSE and preserve stateful conditional occurrences.                                              |

The old Joom global cache of serialized function bytes is deliberately replaced
with live task transport. Reusing those bytes can freeze captured values across
queries or repeated DataFrame actions. Shared-object identity and size reduction
remain; only stale snapshots are avoided. Fresh one-stream preflight avoids
walking a shared capture separately for every function in one expression.

Prepared scan predicates can introduce function references after scalar-subquery
resolution. Capture includes finalized commons and opaque Delta envelopes. The
contrib SPI returns checked `AnyRef` objects so protobuf relocation cannot break
its Scala signatures. Missing transport, completed tasks or invalid message
types fail before producing a successful answer.

## Spark evaluation order and safe fallback

Spark projections evaluate RHS on NULL LHS for AND/OR. An eligible whole-stage
Filter rejects a NULL conjunct before later conjuncts. Eligibility is checked
with Spark's actual physical rule, including `maxFields` and factory mode.
Ambiguous inferred-null-check ordering, stateful nullable prefixes and harmful
operator-shared CSE use Spark. Positively pure shared subtrees retain native
eligibility; ANSI casts/arithmetic and unknown UDFs are not declared safe.

Stateful COALESCE and multiple state-bearing IF/CASE occurrences use one Spark
JVM kernel within Comet. Existing single-occurrence/null-guard paths, disabled
expression settings and dispatcher limits remain. G05 SMJ guards are unchanged.
This is not a universal claim about first-error ordering across every vectorized
operator or arbitrary side effects in falsely deterministic user functions.

## Local evidence

- **52 unique native registrations passed**, including 19 logical/masking/type
  cases, existing CASE/IF/null-short-circuit regressions and 3 core logical cases.
- **446 unique JVM/SQL registrations passed**, zero remaining failures,
  cancellations or ignored cases: 415 main, 29 serde/config and 2 additional Delta.
- **11 UDF-pool cases also passed from the shaded developer JAR**, without
  checkout production classes. The loaded classes and bundled native library
  were verified by origin/hash. These are repeated executions, not extra unique
  registrations. Both Delta regressions likewise use the actual shaded Core and
  Delta JARs; the 20-scan task binary was 259141 bytes versus vanilla 275461 bytes.
- Original Apache Spark 4.2.1 without Comet supplies SQL values, duplicates,
  schema and mandatory error/SQLSTATE expectations under ANSI=true. Plan and
  dispatcher assertions distinguish native execution, JVM dispatch and intended
  Spark fallback. The20 evaluation-order registrations include multiple fixtures.
- The same AND/OR SQL fails against the pre-fix native library in four matrix
  cases and passes after the fix. Intermediate compile/style/plan-shape failures
  are retained and superseded, never counted as passes.
- Maven Core/Delta packaging and style, touched-crate strict Clippy including
  tests/benches, Rust formatting and existing CI suite/config inventories passed.
  Five new suites are registered in both existing platform matrices. Full GHA
  was not run and no release infrastructure was changed.

Full origins, commands, source/library/JAR hashes and retained failures are in
[G08-results.json](G08-results.json). Full old-commit path unions and independent
reviews are linked there. [Performance evidence](G08-performance.md) uses only
the project's existing Criterion target.

## Remaining gates

Evidence is local macOS ARM64/Java17/dev evidence. Developer JARs are not a new
Linux RC or production deliverable. Final full agreed CI/fuzz/profile coverage,
cleanup of earlier temporary CI mechanisms and exact final AMD64/ARM64 release
JAR smoke remain required after all groups. Current G08 temporary diagnostics
are removed; no selected applicable local case remains failed or unverified.

The approved plan/register, DataFusion55.2 vendor, dependency locks and PR5889
exclusion are preserved. Java21 work, production and datasci remain excluded.
Post-commit source/document/ref/license verification is recorded in
`${MIGRATION_AUX_ROOT}/20261120_comet12-g08/completion.json`.

## Accepted integration (2026-11-21)

The user approved keeping the safe fallbacks, publishing G08 and integrating it
into `branch-1.2`. Some guarded Filter/Project paths were native-eligible in
`joom-1.1-13`; the narrower coverage is accepted to preserve Spark4.2.1 evaluation
and mandatory-error semantics. Stateful conditional JVM kernels remain inside
Comet. Do not remove the guards to regain coverage without correctness evidence.
See decisions.json#g08_fallback_acceptance.

Acceptance documentation is folded into the one substantive G08 commit. All58
qualified non-migration source/test/CI/reference files remain byte-identical to
`2625e2c8e`; original local evidence and performance limitations are unchanged.
The qualified commit is preserved at `refs/joom-migration/g08-qualified`.
Initial review holds above are historical. Actual final local/remote SHA, tree,
preservation proof and publication results are recorded after commit in
`${MIGRATION_AUX_ROOT}/20261121_comet12-g08-integration/completion.json`.
No runtime rebuild is needed for documentation-only acceptance. Full GHA was not
started; G09 remains unauthorized. Final agreed qualification remains mandatory.
