<!---
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

# Reducing Row/Columnar Conversion Overhead

When a query stage contains many operators that fall back to Spark row-based execution, Comet may insert
repeated columnar-to-row and row-to-columnar conversions that dominate stage runtime. Set
`spark.comet.exec.transitionRevert.enabled=true` to have Comet revert the entire stage to Spark row execution
when the number of columnar-to-row transitions exceeds
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`). This trades native execution of a small
subset of operators for eliminating conversion overhead across the stage. A stage is not reverted when it holds a
native aggregate whose intermediate buffer Spark cannot exchange with Comet across a stage boundary, because
reverting it would split that aggregate between the two engines.

## Wide or Deeply Nested Schemas

The cost of each conversion also grows sharply with schema shape: for wide or deeply nested schemas,
columnar-to-row conversion is especially expensive because the conversion work scales with the number of
columns and nested fields. If profiling shows these conversions dominating a query over such a schema, set
`spark.comet.exec.transitionRevert.enabled=true` and lower
`spark.comet.exec.transitionRevert.maxTransitions` (default `2`) to `1`. Note that this reverts every stage that
exceeds the threshold to Spark row-based execution — Comet removes the stage's native operators rather than running a
mix of native and fallback operators joined by repeated conversions — which can be cheaper than paying the
expensive conversions again and again.

## Choosing Engines and Boundary Formats

The following optional modes reduce conversions between Spark rows and Comet
batches. They preserve operator eligibility and correctness fallbacks.

| Setting                                                | Default | Behavior                                                                                                                                  |
| ------------------------------------------------------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.comet.exec.boundaryFormats.enabled`             | `false` | Chooses shuffle and broadcast formats from both producer and consumer engines. A shuffle between Spark operators remains a Spark shuffle. |
| `spark.comet.exec.costBasedEngines.enabled`            | `false` | Jointly chooses converted operators' engines and boundary formats by estimated cost over the whole plan.                                  |
| `spark.comet.exec.sort.wideRowFallback.enabled`        | `false` | Keeps a wide-row sort on Spark when its consumer runs on Spark.                                                                           |
| `spark.comet.exec.sort.wideRowFallback.minLeafColumns` | `50`    | Sort payload width threshold, excluding columns referenced by the sort key.                                                               |
| `spark.comet.shuffle.wideRowFallback.minLeafColumns`   | `0`     | Shuffle payload width threshold, excluding partition-key columns; zero disables the heuristic.                                            |

A struct contributes the leaves of its fields, an array the leaves of its element,
and a map the leaves of its key and value. Other types contribute one leaf.
Cost-based selection takes precedence over both wide-row heuristics and already
includes boundary-format selection, so its operation does not require enabling
`boundaryFormats` separately. These modes do not make unsupported operators or
incompatible aggregate buffers eligible for mixed execution.

The cost model estimates nanoseconds per row from operator classes, leaf counts,
nested fractions, estimated row bytes and shuffle partition counts. It does not
estimate selectivity or group reduction. The default table retains the Joom
calibration from the previous runtime, including its production-size sort lines;
measure representative workloads before changing the calibration. Whole-plan
choices are repeated when AQE re-optimizes, while isolated stage preparation
retains earlier choices. Already materialized stages cannot change their format.
Inputs that must be co-partitioned retain compatible hash assignments.

Use `spark.comet.exec.costBasedEngines.log.enabled=true` to inspect the chosen
engines and prices. `spark.comet.exec.costBasedEngines.costTable` accepts
semicolon-separated overrides; omitted entries retain their defaults. For example,
`sort.flat.comet=0,0,0;sort.flat.spark=1000,0` changes only the flat sort lines.
A cost-based engine decision must still pay the adjacent conversion costs.
`spark.comet.exec.costBasedEngines.cometOperatorWeight` (default `-1`) and
`spark.comet.exec.costBasedEngines.sparkOperatorWeight` (default `0`) price
operators outside the table. `cometOperatorWeights` under the same prefix accepts
comma-separated per-operator overrides, such as `ShuffledHashJoinExec=-0.5`.
See the generated configuration reference for the complete override syntax.

## Experimental: Direct Columnar-to-Row Conversion

When the JVM columnar-to-row operator is in use (`spark.comet.exec.columnarToRow.native.enabled=false`),
setting `spark.comet.exec.columnarToRow.direct.enabled=true` enables an experimental converter that writes
values straight from Arrow buffers into Spark's row format without allocating an object per value. This is
most beneficial for decimal-heavy schemas, where the default conversion allocates a `Decimal` object per
value (and considerably more for decimals with precision above 18); microbenchmarks show up to 2x faster
conversion and a large reduction in garbage creation for such schemas. Schemas containing data types the
converter does not support fall back to the default conversion automatically.

Batches with fewer rows than `spark.comet.exec.columnarToRow.direct.minBatchSize` (default `128`) also fall
back to the default conversion, since the direct converter's per-batch setup does not pay off on very small
batches. This optimization is experimental: it only affects the operator's non-codegen paths (including
broadcast relation builds), and the default conversion remains enabled unless explicitly opted in.

The direct converter is not faster for every schema: schemas with strings and no expensive decimals can get
slower with it enabled. For them the default conversion is already efficient, because its `UnsafeProjection` is
code generated for the schema, and a string column keeps the direct converter off the fixed-width path described
below. In the microbenchmarks above, a `long, int, double, string` schema at 8,192 rows per batch took 12.1 ns
per row with the direct converter against 9.4 ns per row with the default conversion, about 29% slower, even
though it allocated less. Measure a representative workload before enabling the direct converter, rather than
turning it on for a whole cluster.

The converter takes a fixed-width path when every column has a fixed width in Spark's row format, that is,
when the schema has no strings and no decimals with precision above 18. That path converts the whole batch
as soon as it is set, so a consumer that stops early, such as a `LIMIT`, pays for every row of the batch where
the default conversion is lazy per row. It also keeps one buffer of the batch's rows per task for the life of
the task, on heap and outside Comet's memory accounting: for example about 6.7 MB for 100 fixed-width columns
at 8,192 rows per batch.
