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

# Performance validation in the established Comet style

User decision, 2026-11-17: follow the project's established practice;
do not create a special testing method/infrastructure solely for the migration.

## Existing tools

- Scala microbenchmarks: `spark/src/test/scala/org/apache/spark/sql/benchmark`,
  `CometBenchmarkBase` on Spark `SqlBasedBenchmark`; Makefile `benchmark-%`.
- Rust microbenchmarks: existing Criterion benches in
  `native/{core,operators,shuffle,spark-expr}/benches`; Makefile `bench` / Cargo bench.
- Macro benchmarks: existing TPC-H/TPC-DS runners in `benchmarks/tpc`.
- General guide: `docs/source/contributor-guide/benchmarking.md`;
  respect the macOS limitations in `benchmarking_macos.md`.

For each change, choose an appropriate existing suite/case, verify that the changed
path actually executes, and record commands, parameters, versions, results and
limitations. If necessary, extend the relevant existing benchmark in the project's
normal style; do not create a separate new framework, unusual generator or special
comparison system.

| Group | Suitable existing suites/benches                                                                                                            |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| G01   | Standard read suites and available baseline; packaging is not a benchmark                                                                   |
| G02   | `CometStringFfiImportBenchmark`, temporal suites and Rust cast/timestamp benches                                                            |
| G03   | `CometExecBenchmark`, `CometTopKBenchmark`, Rust `sort_row_partitions`                                                                      |
| G04   | `CometAggregateExpressionBenchmark`, Rust `aggregate`, `collect`, `aggregate_misc`, `min_max`                                               |
| G05   | `CometSortMergeJoinBenchmark`                                                                                                               |
| G06   | `CometWindowBenchmark`, Rust operators `window`                                                                                             |
| G07   | `CometShuffleBenchmark`, `CometColumnarShuffleWriteBenchmark`, `CometColumnarToRowBenchmark`, `CometC2RIsolatedBench`, Rust shuffle benches |
| G08   | `CometCodegenDispatchBenchmark`, `CometScalaUDFDispatchBenchmark`, `CometOperatorSerdeBenchmark`                                            |
| G09   | Available TPC-H/DS and operator suites; inspect actual plans/engine selection                                                               |

## Validation boundaries

`CometBenchmarkBase` sets ANSI=false because its fixtures include invalid inputs.
Those measurements do not replace mandatory ANSI=true correctness/regression tests.
Previously authorized new correctness tests remain part of implementation;
restrictions on new performance infrastructure do not cancel them.

Prefer a focused suite/bench over an uncontrolled `setup/all/publish`:
`benchmarks/micro/run.py` may install packages, clone/update sources, build a release
and publish changes. Do not run these side effects. Inspect the actual Makefile/runner
before execution: some targets build a release first; old README examples do not
guarantee current flags/versions.

Do not mechanically copy the 100GB generation example from the macOS guide. Select
a bounded representative size and record generated data in the cleanup ledger.
AWS provisioning is outside this GCP-only work. The user will run full measurements
on real GCP jobs later. Do not present unmeasured acceleration as confirmed.
