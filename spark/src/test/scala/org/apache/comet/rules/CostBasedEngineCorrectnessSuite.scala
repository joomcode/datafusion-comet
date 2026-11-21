/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.rules

import java.util.IdentityHashMap

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{CometTestBase, QueryTest, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, Final, Partial}
import org.apache.spark.sql.comet.{CometBaseAggregateExec, CometLocalTopKExec, CometNativeScanExec, CometProjectExec, CometSortAggregateExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, FileSourceScanExec, ProjectExec, SparkPlan, SQLExecution, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.functions.{expr, spark_partition_id}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.rules.BoundaryFormats.Engine

/** Independent SQL/partition/error oracles; no expected answer comes from the cost solver. */
class CostBasedEngineCorrectnessSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED.key, "true")
    .set(CometConf.COMET_NATIVE_SCAN_ENABLED.key, "true")
    .set(CometConf.COMET_EXEC_AGGREGATE_ENABLED.key, "true")

  private val costFlag = CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.key
  private val boundaryFlag = CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.key
  private val table = CometConf.COMET_EXEC_COST_BASED_ENGINES_COST_TABLE.key
  private val modes = Seq(
    "neither" -> Seq(costFlag -> "false", boundaryFlag -> "false"),
    "boundary" -> Seq(costFlag -> "false", boundaryFlag -> "true"),
    "cost" -> Seq(costFlag -> "true", boundaryFlag -> "false"),
    "cost+boundary" -> Seq(costFlag -> "true", boundaryFlag -> "true"))

  for (shuffleMode <- Seq("native", "auto", "jvm"); range <- Seq(false, true)) {
    test(
      s"G09 shuffle key CSE context preserves results and mandatory errors: $shuffleMode range=$range") {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "true",
        "spark.sql.subexpressionElimination.skipForShortcutExpr" -> "false",
        costFlag -> "false",
        boundaryFlag -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode,
        CometConf.COMET_SHUFFLE_NATIVE_RANGE_PARTITIONING_ENABLED.key -> "true") {
        withParquetTable(
          Seq((Some(false), "bad"), (Some(true), "2"), (None, "bad")),
          "g09_keys") {
          def partition(keys: String*) = {
            val input = spark.table("g09_keys")
            val columns = keys.map(expr)
            if (range) input.repartitionByRange(3, columns: _*)
            else input.repartition(3, columns: _*)
          }
          // No shared fallible subtree: the false/NULL rows must never cast "bad".
          val (_, safePlan) = checkSparkAnswer(
            partition("IF(_1, CAST(_2 AS INT), 0)").withColumn("p", spark_partition_id()))
          val safeExchanges = collect(safePlan) { case e: CometShuffleExchangeExec => e }
          assert(safeExchanges.size == 1, safePlan)
          assert(
            safeExchanges.head.shuffleType ==
              (if (shuffleMode == "jvm") CometColumnarShuffle else CometNativeShuffle),
            safePlan)

          // Spark's key projection shares this CAST across both keys and must evaluate it
          // even on the false row. A conditional's successful output is not sufficient.
          val failingInput = spark.table("g09_keys").filter("_1 IS NOT NULL")
          val failingKeys =
            Seq(expr("_1 AND CAST(_2 AS INT) > 0"), expr("_1 AND CAST(_2 AS INT) > 1"))
          val failing =
            if (range) failingInput.repartitionByRange(3, failingKeys: _*)
            else failingInput.repartition(3, failingKeys: _*)
          val (expected, actual) = checkSparkAnswerMaybeThrows(failing)
          def structured(error: Option[Throwable]): SparkThrowable =
            error.toSeq
              .flatMap(causeChain)
              .collect { case e: SparkThrowable => e }
              .lastOption
              .getOrElse(fail("Shared CAST must raise a structured Spark error"))
          val reference = structured(expected)
          val observed = structured(actual)
          assert(reference.getErrorClass == "CAST_INVALID_INPUT")
          assert(observed.getErrorClass == reference.getErrorClass)
          assert(observed.getSqlState == reference.getSqlState)
          assert(
            collect(failing.queryExecution.executedPlan) { case _: CometShuffleExchangeExec =>
              1
            }.isEmpty,
            failing.queryExecution.executedPlan)
        }
      }
    }
  }

  private def compare(query: String): SparkPlan = {
    // This helper reparses/replans the query with Comet disabled for the reference.
    val (reference, actual) = checkSparkAnswer(query)
    assert(collect(reference) { case _: CometNativeScanExec => 1 }.isEmpty, reference)
    assert(actual.schema == reference.schema, s"Schema differs from original Spark: $actual")
    actual
  }

  private def withDecimals(precision: Int)(body: => Unit): Unit = {
    withTable("g09_decimal_native", "g09_decimal_spark") {
      // A shared writer bug must not be able to manufacture a matching wrong fixture.
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val maximum = "9" * precision
        Seq("g09_decimal_native" -> "parquet", "g09_decimal_spark" -> "json")
          .foreach { case (name, format) =>
            sql(s"CREATE TABLE $name(id INT, k DECIMAL($precision, 0)) USING $format")
            sql(s"""INSERT INTO $name VALUES
                   |(0, NULL), (1, 0), (2, 1), (3, -1), (4, 127), (5, 128),
                   |(6, -128), (7, -129), (8, $maximum), (9, -$maximum),
                   |(10, 1), (11, NULL)
                   |""".stripMargin)
          }
      }
      body
    }
  }

  for (precision <- Seq(18, 19, 38); adaptive <- Seq(false, true)) {
    test(
      s"G09 mixed decimal boundaries preserve Spark rows: precision=$precision AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "7",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "auto",
        CometConf.COMET_CONVERT_FROM_JSON_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED.key -> "false",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet,json") {
        withDecimals(precision) {
          modes.foreach { case (name, configuration) =>
            withClue(s"$name\n") {
              withSQLConf(configuration: _*) {
                val query = "SELECT n.id AS n, s.id AS s FROM g09_decimal_native n " +
                  "JOIN g09_decimal_spark s ON n.k <=> s.k"
                val plan = compare(query)
                assert(collect(plan) { case _: CometNativeScanExec => 1 }.sum == 1, plan)
                assert(collect(plan) { case _: FileSourceScanExec => 1 }.sum == 1, plan)
                // In the baseline and boundary-only modes the two real writer paths must mix.
                // Cost-enabled shape is checked by its own forced-price case below.
                if (name == "neither" || name == "boundary") {
                  val exchanges = collect(plan) { case e: CometShuffleExchangeExec => e }
                  assert(exchanges.count(_.shuffleType == CometNativeShuffle) == 1, plan)
                  assert(exchanges.count(_.shuffleType == CometColumnarShuffle) == 1, plan)
                }
                compare("SELECT k, sum(id), avg(id), count(*) " +
                  "FROM g09_decimal_native GROUP BY k")
              }
            }
          }
          // A temporary test override selects the mixed native/JVM writer path under the solver;
          // it does not recalibrate or change the library's default table.
          withSQLConf(
            costFlag -> "true",
            boundaryFlag -> "true",
            table -> "shuffleWrite.flat.spark=100000,0") {
            val plan = compare(
              "SELECT n.id AS n, s.id AS s FROM g09_decimal_native n " +
                "JOIN g09_decimal_spark s ON n.k <=> s.k")
            val exchanges = collect(plan) { case e: CometShuffleExchangeExec => e }
            assert(exchanges.exists(_.shuffleType == CometNativeShuffle), plan)
            assert(exchanges.exists(_.shuffleType == CometColumnarShuffle), plan)
          }
        }
      }
    }
  }

  for (precision <- Seq(18, 19, 38)) {
    test(s"G09 root scalar/nested decimal shuffles preserve Spark partition ids: $precision") {
      // Exact partition ids are meaningful before AQE coalesces partitions. Result bags alone
      // would miss a hasher mismatch that only corrupts a later mixed join.
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "auto",
        CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_NESTED_ENABLED.key -> "true") {
        withDecimals(precision) {
          modes.foreach { case (name, configuration) =>
            withClue(s"$name\n") {
              withSQLConf(configuration: _*) {
                Seq("k", "named_struct('d', k, 'a', array(k, k))").foreach { key =>
                  val query = s"SELECT id, k, spark_partition_id() AS p FROM " +
                    s"(SELECT /*+ REPARTITION(7, partition_key) */ id, k, $key AS partition_key " +
                    "FROM g09_decimal_native)"
                  val plan = compare(query)
                  assert(collect(plan) { case _: CometNativeScanExec => 1 }.nonEmpty, plan)
                  assert(collect(plan) { case e: CometShuffleExchangeExec => e }.nonEmpty, plan)
                }
              }
            }
          }
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"G09 cost reversion restores unordered SortAggregate input ordering: AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "3") {
        val rows = (0 until 95).map(i => ((i * 3) % 7, s"v${(i * 31) % 97}"))
        withParquetTable(rows, "g09_string_aggregate") {
          // A root shuffle consumes no grouping order. Both partial and final native
          // aggregates must therefore restore their input sort when returned to Spark.
          val query = "SELECT /*+ REPARTITION(2) */ _1, max(_2) " +
            "FROM g09_string_aggregate GROUP BY _1"
          withSQLConf(costFlag -> "false", boundaryFlag -> "false") {
            val plan = compare(query)
            assert(
              collect(plan) {
                case a: CometSortAggregateExec
                    if a.modes == Seq(Partial) && !a.preservesGroupingOrder =>
                  a
              }.nonEmpty,
              s"The regression must start with a removed grouping sort:\n$plan")
            assert(
              collect(plan) { case a: CometSortAggregateExec => a }
                .exists(a =>
                  a.modes.contains(Final) &&
                    !a.preservesGroupingOrder),
              s"The final aggregate must also omit unused grouping order:\n$plan")
          }
          withSQLConf(
            costFlag -> "true",
            boundaryFlag -> "true",
            table -> ("agg.comet=100000,0,0;agg.spark=0,0;" +
              "sort.comet=100000,0,0;keepPartialAggregatesOverNativeInputs=false")) {
            val plan = compare(query)
            assert(collect(plan) { case _: CometBaseAggregateExec => 1 }.isEmpty, plan)
            val restored = collect(plan) { case a: SortAggregateExec => a }
            assert(restored.nonEmpty, plan)
            restored.foreach { aggregate =>
              assert(
                SortOrder.orderingSatisfies(
                  aggregate.child.outputOrdering,
                  aggregate.requiredChildOrdering.head),
                aggregate)
            }
          }
        }
      }
    }
  }

  test("G09 cost/boundary choices retain G08 shared CAST mandatory ANSI errors") {
    withSQLConf(
      SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "true",
      "spark.sql.subexpressionElimination.skipForShortcutExpr" -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      withParquetTable(Seq((false, "bad"), (true, "2")), "g09_shared_boolean") {
        val query = "SELECT _1 AND CAST(_2 AS INT) > 0 AS a, " +
          "_1 AND CAST(_2 AS INT) > 1 AS b FROM g09_shared_boolean"
        modes.foreach { case (name, configuration) =>
          withSQLConf(configuration: _*) {
            val sparkFailure = withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
              Try(sql(query).collect())
            }
            val df = sql(query)
            val actual = Try(df.collect())
            def error(result: Try[Array[Row]]): SparkThrowable = result match {
              case Failure(e) =>
                causeChain(e)
                  .collect { case s: SparkThrowable => s }
                  .lastOption
                  .getOrElse(fail(s"Missing structured error: $e"))
              case Success(rows) => fail(s"$name silently succeeded: ${rows.toSeq}")
            }
            val expectedError = error(sparkFailure)
            val actualError = error(actual)
            assert(expectedError.getErrorClass == "CAST_INVALID_INPUT")
            assert(actualError.getErrorClass == expectedError.getErrorClass)
            assert(actualError.getSqlState == expectedError.getSqlState)
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) { case _: CometProjectExec => 1 }.isEmpty, plan)
            assert(collect(plan) { case _: ProjectExec => 1 }.nonEmpty, plan)
          }
        }
      }
    }
  }

  test("G09 cost/boundary choices retain G06 suffix window correctness fallback") {
    withParquetTable(Seq((0, 0, 1.0), (0, 1, 1.0e20), (0, 2, -1.0e20)), "g09_suffix") {
      val query = "SELECT _1, _2, sum(_3) OVER (PARTITION BY _1 ORDER BY _2 " +
        "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM g09_suffix"
      modes.foreach { case (_, configuration) =>
        withSQLConf(configuration: _*) {
          val plan = compare(query)
          assert(collect(plan) { case _: WindowExec => 1 }.nonEmpty, plan)
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"G09 fused TopK reversion applies the original offset/projection once: AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true") {
        withParquetTable(
          (0 until 101).map(i => { val key = (i * 37) % 101; (key, key * 10) }),
          "g09_topk") {
          val query = "SELECT _2 FROM g09_topk ORDER BY _1 LIMIT 5 OFFSET 3"
          val expected = withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
            sql(query).collect().toSeq
          }
          assert(expected == Seq(30, 40, 50, 60, 70).map(Row(_)))
          withSQLConf(costFlag -> "false", boundaryFlag -> "false") {
            val df = sql(query)
            assert(df.collect().toSeq == expected)
            assert(
              collect(df.queryExecution.executedPlan) { case _: CometLocalTopKExec =>
                1
              }.nonEmpty,
              s"The baseline must exercise the inserted local candidate selector:\n${df.queryExecution.executedPlan}")
          }
          withSQLConf(
            costFlag -> "true",
            boundaryFlag -> "true",
            CometConf.COMET_EXEC_COST_BASED_ENGINES_OPERATOR_WEIGHTS.key ->
              "TakeOrderedAndProjectExec=100000") {
            val df = sql(query)
            assert(
              df.collect().toSeq == expected,
              "Offset, projection, duplicates and output order must match original Spark")
            val plan = df.queryExecution.executedPlan
            assert(collect(plan) { case _: CometLocalTopKExec => 1 }.isEmpty, plan)
            assert(collect(plan) { case _: TakeOrderedAndProjectExec => 1 }.size == 1, plan)
            assert(collect(plan) { case _: CometNativeScanExec => 1 }.nonEmpty, plan)
          }
        }
      }
    }
  }

  private def withBloomFunction(body: => Unit): Unit = {
    val function = if (isSpark42Plus) {
      new FunctionIdentifier("g09_bloom", Some("builtin"), Some("system"))
    } else {
      new FunctionIdentifier("g09_bloom")
    }
    spark.sessionState.functionRegistry.registerFunction(
      function,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "g09_bloom"),
      (children: Seq[Expression]) =>
        new BloomFilterAggregate(children.head, children(1), children(2)))
    try body
    finally spark.sessionState.functionRegistry.dropFunction(function)
  }

  test("G09 Spark partial buffers feed the retained serialized native final") {
    withBloomFunction {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        costFlag -> "false",
        boundaryFlag -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "auto") {
        // SUM has one state, AVG two, Bloom one: combining them catches state-offset mistakes.
        // Group1 is all NULL; group2 contains valid zero values, which must remain distinct.
        val rows = Seq(
          (0, Some(3L)),
          (0, Some(-1L)),
          (0, Some(8L)),
          (0, None),
          (1, None),
          (1, None),
          (2, Some(0L)),
          (2, Some(0L)))
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          withParquetTable(rows, "g09_retained_buffers") {
            withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
              val query = "SELECT _1, sum(_2), avg(_2), " +
                "g09_bloom(_2, 1000L, 8192L) FROM g09_retained_buffers GROUP BY _1"
              val (expected, expectedSchema) =
                withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
                  val reference = sql(query)
                  (reference.collect().toSeq, reference.schema)
                }
              val df = sql(query)
              // Plan only: neither the original native partial nor its exchange has executed.
              val initial = df.queryExecution.executedPlan
              val partials = collect(initial) {
                case aggregate: CometBaseAggregateExec if aggregate.modes == Seq(Partial) =>
                  aggregate
              }
              val finals = collect(initial) {
                case aggregate: CometBaseAggregateExec if aggregate.modes == Seq(Final) =>
                  aggregate
              }
              assert(partials.size == 1 && finals.size == 1, initial)
              assert(
                EngineSolver.relabelable(partials.head),
                "The regression must use a producer the real solver is allowed to move to Spark")
              val originalFinal = finals.head
              val originalBytes = originalFinal.serializedPlanOpt.plan
                .getOrElse(
                  fail(s"The final must already own a serialized native block:\n$initial"))
                .clone()
              assert(
                collect(initial) {
                  case exchange: CometShuffleExchangeExec
                      if exchange.shuffleType == CometNativeShuffle =>
                    exchange
                }.nonEmpty,
                initial)

              // Force one legal solver label without rerunning aggregate serde. This isolates a state
              // origin change that a cost override may also select, independently of table calibration.
              val labels = new IdentityHashMap[SparkPlan, Engine]()
              labels.put(partials.head, Engine.Spark)
              val relabelled = EngineSolver.relabel(initial, labels)
              val reformatted = BoundaryFormats.applyFormats(relabelled)
              val blocks = CometExecRule.convertBlocks(reformatted)
              val runnable =
                ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false).apply(
                  blocks)
              val retainedFinals = collect(runnable) {
                case aggregate: CometBaseAggregateExec if aggregate.modes == Seq(Final) =>
                  aggregate
              }
              assert(retainedFinals.size == 1, runnable)
              val retainedBytes = retainedFinals.head.serializedPlanOpt.plan.get
              assert(
                java.util.Arrays.equals(originalBytes, retainedBytes),
                "The test must consume Spark buffers with the original native final bytes")
              assert(
                collect(runnable) {
                  case aggregate: CometBaseAggregateExec if aggregate.modes == Seq(Partial) =>
                    aggregate
                }.isEmpty,
                runnable)
              assert(
                collect(runnable) {
                  case aggregate: BaseAggregateExec
                      if aggregate.aggregateExpressions.map(_.mode).distinct == Seq(Partial) =>
                    aggregate
                }.size == 1,
                runnable)
              assert(
                collect(runnable) {
                  case exchange: CometShuffleExchangeExec
                      if exchange.shuffleType == CometColumnarShuffle =>
                    exchange
                }.size == 1,
                runnable)
              assert(runnable.schema == expectedSchema)
              val toRow = CatalystTypeConverters.createToScalaConverter(expectedSchema)
              val actual = SQLExecution.withNewExecutionId(df.queryExecution) {
                runnable.executeCollect().toSeq.map(row => toRow(row).asInstanceOf[Row])
              }
              QueryTest.sameRows(expected, actual).foreach(fail(_))

              // COUNT is deliberately one-way compatible. Its matching buffer alone does not permit
              // Spark Partial -> native Final because AQE's count-specific rewrites also matter.
              val countPlan =
                sql("SELECT _1, count(_2) FROM g09_retained_buffers GROUP BY _1").queryExecution.executedPlan
              val countPartials = collect(countPlan) {
                case aggregate: CometBaseAggregateExec if aggregate.modes == Seq(Partial) =>
                  aggregate
              }
              assert(countPartials.size == 1, countPlan)
              assert(!EngineSolver.relabelable(countPartials.head), countPlan)
            }
          }
        }
      }
    }
  }

}
