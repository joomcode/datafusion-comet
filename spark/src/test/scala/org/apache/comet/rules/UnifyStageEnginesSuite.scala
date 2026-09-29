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

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class UnifyStageEnginesSuite extends CometTestBase {
  import testImplicits._

  private def withTables(f: => Unit): Unit = {
    val data = (0 until 3000).map(i => (i % 11, (i * 7919) % 1000, s"payload_${i % 257}"))
    withParquetTable(data, "t0") {
      spark.table("t0").toDF("k", "v", "s").createOrReplaceTempView("t")
      val small = (0 until 11).map(i => (i, s"name_$i"))
      withParquetTable(small, "d0") {
        spark.table("d0").toDF("k", "name").createOrReplaceTempView("d")
        withTempView("t", "d")(f)
      }
    }
  }

  private def executedPlan(df: DataFrame): SparkPlan = {
    val (_, cometPlan) = checkSparkAnswer(df)
    cometPlan
  }

  private def cometShuffles(plan: SparkPlan): Seq[CometShuffleExchangeExec] =
    collect(plan) { case e: CometShuffleExchangeExec => e }

  private def sparkShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    collect(plan) { case e: ShuffleExchangeExec => e }

  private def cometSorts(plan: SparkPlan): Seq[CometSortExec] =
    collect(plan) { case s: CometSortExec => s }

  private val sortAggregateQuery = "SELECT k, max(s) AS m FROM t GROUP BY k"

  for (aqe <- Seq("false", "true")) {
    test(
      s"Spark aggregates on both sides of a shuffle keep Comet's columnar shuffle when " +
        s"disabled (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "false") {
        withTables {
          val plan = executedPlan(sql(sortAggregateQuery))
          assert(collect(plan) { case a: SortAggregateExec => a }.size == 2, s"plan:\n$plan")
          assert(
            cometShuffles(plan).exists(_.shuffleType == CometColumnarShuffle),
            s"expected Comet's columnar shuffle between the Spark aggregates:\n$plan")
          assert(cometSorts(plan).nonEmpty, s"expected native sorts:\n$plan")
        }
      }
    }

    test(s"Spark stages on both sides of a shuffle get a Spark shuffle (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "true") {
        withTables {
          val plan = executedPlan(sql(sortAggregateQuery))
          assert(collect(plan) { case a: SortAggregateExec => a }.size == 2, s"plan:\n$plan")
          assert(cometShuffles(plan).isEmpty, s"no Comet shuffle between Spark stages:\n$plan")
          assert(sparkShuffles(plan).size == 1, s"expected one Spark shuffle:\n$plan")
          assert(cometSorts(plan).isEmpty, s"sorts of Spark stages should run in Spark:\n$plan")
          assert(collect(plan) { case s: SortExec => s }.size == 2, s"plan:\n$plan")
          assert(
            collect(plan) { case s: CometNativeScanExec => s }.nonEmpty,
            s"the leaf scan should stay native:\n$plan")
        }
      }
    }

    test(s"a fully native query is unchanged (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "true") {
        withTables {
          val plan = executedPlan(sql("SELECT k, sum(v) FROM t GROUP BY k"))
          assert(collect(plan) { case a: HashAggregateExec => a }.isEmpty, s"plan:\n$plan")
          assert(collect(plan) { case a: CometHashAggregateExec => a }.size == 2)
          assert(cometShuffles(plan).map(_.shuffleType) == Seq(CometNativeShuffle))
        }
      }
    }

    test(s"a native producer keeps its native shuffle into a Spark stage (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false",
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "true") {
        withTables {
          val plan = executedPlan(
            sql("SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t"))
          assert(cometShuffles(plan).map(_.shuffleType) == Seq(CometNativeShuffle), s"$plan")
          assert(cometSorts(plan).isEmpty, s"the Spark window stage sorts in Spark:\n$plan")
          val windows = collect(plan) { case w: WindowExec => w }
          assert(windows.size == 1, s"plan:\n$plan")
          assert(windows.head.find(_.isInstanceOf[SortExec]).isDefined, s"plan:\n$plan")
        }
      }
    }
  }

  test("a join in a Spark stage gets a Spark broadcast") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false",
      CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "true") {
      withTables {
        val plan = executedPlan(sql("SELECT t.v + 1 AS x, d.name FROM t JOIN d ON t.k = d.k"))
        assert(
          collect(plan) { case j: CometBroadcastHashJoinExec => j }.isEmpty,
          s"the join of a mixed stage should run in Spark:\n$plan")
        assert(collect(plan) { case j: BroadcastHashJoinExec => j }.size == 1, s"plan:\n$plan")
        assert(
          collect(plan) { case b: CometBroadcastExchangeExec => b }.isEmpty,
          s"a Spark join cannot read Comet's broadcast:\n$plan")
        assert(collect(plan) { case b: BroadcastExchangeExec => b }.size == 1, s"plan:\n$plan")
      }
    }
  }

  for (aqe <- Seq("false", "true")) {
    test(s"a shuffle fed by another native shuffle keeps its format (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "true") {
        withTables {
          val df = spark
            .table("t")
            .repartition(col("k"))
            .withColumn("next", lead(col("v"), 1).over(Window.partitionBy("k", "s").orderBy("v")))
          val plan = executedPlan(df)
          assert(cometShuffles(plan).nonEmpty, s"plan:\n$plan")
          assert(cometShuffles(plan).forall(_.shuffleType == CometNativeShuffle), s"$plan")
        }
      }
    }

    test(s"a local relation feeding a native sort through two shuffles (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> "true") {
        val df =
          (0 until 100).map(i => (i, i * 13 % 17)).toDF("id", "x").repartition(2).sort("x", "id")
        val plan = executedPlan(df)
        assert(cometSorts(plan).size == 1, s"plan:\n$plan")
      }
    }
  }

  test("data changes format only where the engine changes") {
    val queries = Seq(
      sortAggregateQuery,
      "SELECT m, count(*) AS c FROM (SELECT k, max(s) AS m FROM t GROUP BY k) GROUP BY m",
      "SELECT k, max(s) AS m, sum(v) AS total FROM t GROUP BY k",
      "SELECT t.k, max(d.name) AS n FROM t JOIN d ON t.k = d.k GROUP BY t.k",
      "SELECT k, max(s) FROM t GROUP BY k UNION ALL SELECT k, max(name) FROM d GROUP BY k")
    for (enabled <- Seq("false", "true"); query <- queries) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.key -> enabled) {
        withTables {
          val plan = executedPlan(sql(query))
          val columnarBetweenSparkStages = plan.collect {
            case consumer if !consumer.isInstanceOf[CometPlan] =>
              consumer.children.collect {
                case ColumnarToRowExec(e: CometShuffleExchangeExec)
                    if e.shuffleType == CometColumnarShuffle && !e.child
                      .isInstanceOf[CometPlan] =>
                  e
                case e: CometShuffleExchangeExec
                    if e.shuffleType == CometColumnarShuffle && !e.child
                      .isInstanceOf[CometPlan] =>
                  e
              }
          }.flatten
          val mixedSorts = plan.collect {
            case ColumnarToRowExec(s: CometSortExec) => s
            case c: ColumnarToRowTransition if c.child.isInstanceOf[CometSortExec] => c
          }
          if (enabled == "true") {
            assert(
              columnarBetweenSparkStages.isEmpty,
              s"columnar shuffle between Spark stages for $query:\n$plan")
            assert(mixedSorts.isEmpty, s"native sort feeding a Spark stage for $query:\n$plan")
          }
        }
      }
    }
  }
}
