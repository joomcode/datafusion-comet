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

# G04 independent Spark-semantics review

Status: completed locally on the G04 feature branch, without push or integration.
The reviewed baseline was `5133f0ab7`. This review supersedes the earlier claim
that the initial G04 targeted checks had covered every relevant ordering observer.

## Reference and independence

The reference is Apache Spark 4.2.1, revision
`617618396087429b5627a86ce933bb0f5d6645cb`. The actual Maven test classpath resolves
critical Spark classes only to Central runtime JARs, with matching cached checksums
and embedded upstream revision information. Local Comet outputs contain no shadow
copies. The separate Spark-source test patch has two main integration hooks, but
neither is present in these runtime classes. Provenance evidence is in
`harness-fix-review.json` under the auxiliary directory below.

Every new SQL oracle freshly executes the same query with Comet disabled and
identical input/settings. The reference helper asserts that no native aggregate
is present. The Comet execution asserts the native aggregate path. Known small
fixtures additionally compare both engines to input-derived literal rows. Exact
sequences are compared when ordering is observable; only order destroyed by an
explicit outer shuffle is normalized, preserving all row multiplicities.

FIRST/LAST and tied MIN_BY/MAX_BY are not required to choose the same arbitrary
value across genuinely unordered layouts. Ordered endpoints are asserted only
with one input split, an explicit input sort and one reducer. Tie checks require
one group row and an actual extremal candidate, with disjoint payloads for
nonextremal rows.

## Reproduced correctness defect and repair

The G04 unused-sort proof treated every projection/filter before a shuffle as
order-insensitive and checked aggregate function kinds without their result
expressions. Seeded random filters and generated IDs observe the row sequence
before that shuffle. Spark's own `EliminateSorts.canEliminateSort` crosses only
deterministic projects/filters (`Optimizer.scala` at the pinned Spark revision).

Six differential tests were written before changing the production rule. Four
returned incorrect results without raising errors: filter/project/aggregate-result
cases with AQE disabled and the aggregate-result case with AQE enabled. For
example, the generated ID for key 1 was 45 instead of Spark's 1; the random filter
returned 23 rows in each engine but different keys. Two other AQE cases already
matched. This is a reproduction on the new G04 code, not an execution of the old
Joom release.

The fix allows sort omission only across deterministic wrappers and for
aggregates with deterministic arguments, filters and result expressions.
Otherwise native ordered aggregation remains enabled. The same six comparisons
now pass with AQE off/on; they also require an ordered native Final aggregate.
No Rust production algorithm, dependency, default configuration or CI flag changed.

## Oracle weaknesses repaired

- The tie fixture reused payloads so every value could be accepted as extremal,
  and `toMap` discarded duplicate groups. Payloads are now unique; complete group
  cardinality and uniqueness are checked before conversion to a map.
- Ordered-drain checks previously accepted incorrect per-key counts summing to
  the right total. They now check every count, key, uniqueness and full expected
  key set, retaining original input sizes and memory limits.
- Grouping-set property checks now validate actual NULLs, all physical keys,
  hidden grouping IDs and counts, rather than only row totals.
- The COUNT DISTINCT pressure case now requires the PartialMerge stage itself
  to spill, not just some aggregate in the query.
- Additional SQL fixtures check full GROUPING SETS/NULL/DISTINCT results and
  ordered FIRST/LAST NULL handling against both original Spark and known rows.
- Exact BloomFilter bytes now match original Spark for native/native,
  native/Spark and Spark/native stages on two input partitions. Both formerly
  canceled planning tests now execute with qualified function names required by Spark 4.2.

The existing constant-per-group FIRST/LAST pressure fixture remains a memory and
ownership oracle, not proof of arbitrary unordered first/last selection. Its
unchanged budgets and independent IDs remain meaningful; ordered SQL fixtures
and valid-candidate fuzz checks provide complementary semantic evidence.

## Results and limits

The final targeted selection passed 53 JVM/SQL tests with 0 failures. Its 25 canceled
cases are the explicitly deferred opt-in full fuzz matrix. The native aggregate
selection passed 239 tests; the strengthened NULL-validity fixture was rechecked
on its final source. Strict workspace/all-targets Clippy passed with `-D warnings`
and only the previously approved source-local deprecation allowance.

This is local macOS ARM64/Java17/ANSI=true evidence. It establishes the listed
counterexamples and tested semantics, not universal equivalence or production
acceptance. Two inherited metric ignores, full fuzz, final full agreed Linux CI
and packaged smoke of exact final JARs on native AMD64/ARM64 remain required.
G04 is not merged and G05 is not started.

Raw reviews, source hashes, failed intermediate attempts, exact commands, reports
and preservation receipts are in
`${MIGRATION_AUX_ROOT}/20261119_comet12-g04-independent-semantics`.
See `G04-results.json#independent_semantics_review` for machine-readable results.
