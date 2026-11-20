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

# G08: existing conditional benchmark

Use the existing `native/spark-expr/benches/conditional.rs` Criterion target.
Both implementations process 8192 rows and assert exact values/NULLs before
timing. `--profile dev --quick` is a bounded local diagnostic, not an optimized
release result, statistical no-regression proof or whole-job speed forecast.

There are 56 registrations (28 pairs): 48 logical guard/cast/pure combinations and 8
ordinary string combinations. DataFusion's existing `BinaryExpr` is the baseline;
the candidate is the actual Comet factory. No separate framework was introduced.

An early candidate classified every scalar function as unknown and sent ordinary
string combinations through an unnecessary row loop. The same existing benchmark
exposed roughly20–30x operator cost. The final factory recognizes only concrete
StartsWith/EndsWith/SparkContains implementations, supported physical string pairs
and proven scalar alignment; it does not trust names or arbitrary arguments.
Type/unknown/ANSI canaries ensure the correctness guards remain.

Final string cases use exactly the upstream vector kernel and measured
-0.85…+0.80% against the paired baseline.
Column/literal RHS also retains the same upstream kernel. These quick numbers
are consistent with the structural fastpath proof, without proving job-level
performance.

Valid-but-fallible ANSI casts must mask rows even when DataFusion's 20% heuristic
would evaluate everything. Final quick dev differences span
-50.36…+181.52% across selectivity/NULL/AND/OR cases.
This records the operator-level correctness cost honestly; it is not evidence
that real jobs slow down by that range. Wide unused RHS columns are pruned.

Raw before/after logs, all56 final times and intervals are retained under
`${MIGRATION_AUX_ROOT}/20261120_comet12-g08` in `string-benchmark-before.log`,
`conditional-benchmarks-qualified.log` and `performance-summary.json`.
No new toolchain, Docker image or benchmark dependency was downloaded.
