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
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class RevertIsolatedNativeOperatorsSuite extends CometTestBase {

  private val windowQuery =
    "SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t"

  private def withKeyValueTable(f: => Unit): Unit = {
    val data = (0 until 2000).map(i => (i % 7, (i * 7919) % 1000, s"payload_$i"))
    withParquetTable(data, "t0") {
      spark.table("t0").toDF("k", "v", "s").createOrReplaceTempView("t")
      withTempView("t")(f)
    }
  }

  private def executedPlan(df: DataFrame): SparkPlan = {
    val (_, cometPlan) = checkSparkAnswer(df)
    cometPlan
  }

  private def cometSorts(plan: SparkPlan): Seq[CometSortExec] =
    collect(plan) { case s: CometSortExec => s }

  private def sparkSorts(plan: SparkPlan): Seq[SortExec] =
    collect(plan) { case s: SortExec => s }

  private def isC2R(plan: SparkPlan): Boolean = plan.isInstanceOf[ColumnarToRowTransition]

  for (aqe <- Seq("false", "true")) {
    test(s"native sort feeding a Spark operator stays native when disabled (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false",
        CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> "false") {
        withKeyValueTable {
          val plan = executedPlan(sql(windowQuery))
          assert(cometSorts(plan).nonEmpty, s"expected a native sort:\n$plan")
          assert(sparkSorts(plan).isEmpty, s"unexpected Spark sort:\n$plan")
        }
      }
    }

    test(s"native sort feeding a Spark operator reverts to Spark sort over C2R (AQE=$aqe)") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false",
        CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> "true") {
        withKeyValueTable {
          val plan = executedPlan(sql(windowQuery))
          assert(cometSorts(plan).isEmpty, s"native sort should be reverted:\n$plan")
          val sorts = sparkSorts(plan)
          assert(sorts.size == 1, s"expected one Spark sort:\n$plan")
          assert(isC2R(sorts.head.child), s"sort input should be the C2R transition:\n$plan")
          assert(
            sorts.head.child.isInstanceOf[CometPlan],
            s"C2R over Comet batches should be Comet's:\n$plan")
          val windows = collect(plan) { case w: WindowExec => w }
          assert(windows.size == 1, s"expected one Spark window:\n$plan")
          assert(
            windows.head.child.find(_ == sorts.head).isDefined,
            s"Spark sort should feed the Spark window:\n$plan")
          assert(
            collect(plan) { case e: CometShuffleExchangeExec => e }.nonEmpty,
            s"native shuffle producer should stay native:\n$plan")
          assert(
            collect(plan) { case s: CometNativeScanExec => s }.nonEmpty,
            s"native scan should stay native:\n$plan")
        }
      }
    }
  }

  test("native sorts feeding a Spark sort-merge join revert and keep the join ordering") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "false",
      CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> "true") {
      withKeyValueTable {
        val plan =
          executedPlan(sql("SELECT a.k, a.v, b.s FROM t a JOIN t b ON a.v = b.v AND a.k = b.k"))
        val joins = collect(plan) { case j: SortMergeJoinExec => j }
        assert(joins.size == 1, s"expected a Spark sort-merge join:\n$plan")
        assert(cometSorts(plan).isEmpty, s"native sorts should be reverted:\n$plan")
        assert(sparkSorts(plan).size == 2, s"expected two Spark sorts:\n$plan")
        sparkSorts(plan).foreach(sort => assert(isC2R(sort.child), s"plan:\n$plan"))
      }
    }
  }

  test("native sort feeding a native operator is not reverted") {
    withSQLConf(CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> "true") {
      withKeyValueTable {
        val plan = executedPlan(sql(windowQuery))
        assert(cometSorts(plan).nonEmpty, s"expected a native sort under native window:\n$plan")
        assert(sparkSorts(plan).isEmpty, s"unexpected Spark sort:\n$plan")
      }
    }
  }

  test("native operator between Spark operators reverts and removes both transitions") {
    val query = "SELECT id + 1 AS x FROM range(0, 1000, 1, 4) WHERE id % 3 = 1"
    for (enabled <- Seq("false", "true")) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false",
        CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> enabled) {
        val plan = executedPlan(sql(query))
        val filters = collect(plan) { case f: CometFilterExec => f }
        val transitions = collect(plan) {
          case t: ColumnarToRowTransition => t
          case t: RowToColumnarTransition => t
        }
        if (enabled == "true") {
          assert(filters.isEmpty, s"isolated native filter should be reverted:\n$plan")
          assert(transitions.isEmpty, s"no transitions should remain:\n$plan")
          assert(collect(plan) { case f: FilterExec => f }.size == 1, s"plan:\n$plan")
        } else {
          assert(filters.size == 1, s"expected the isolated native filter:\n$plan")
          assert(transitions.nonEmpty, s"expected the transitions around it:\n$plan")
        }
      }
    }
  }

  test("native operator fed by a native producer is not reverted") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false",
      CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> "true") {
      withKeyValueTable {
        val plan = executedPlan(sql("SELECT k + 1 AS x FROM t WHERE v > 500"))
        val filters = collect(plan) { case f: CometFilterExec => f }
        assert(filters.size == 1, s"filter over a native scan should stay native:\n$plan")
      }
    }
  }

  test("aggregates are never reverted") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.key -> "true") {
      withKeyValueTable {
        val plan = executedPlan(sql("SELECT k, sum(v) FROM t GROUP BY k"))
        val aggregates = collect(plan) { case a: CometHashAggregateExec => a }
        assert(aggregates.nonEmpty, s"expected native aggregates:\n$plan")
        val rule = RevertIsolatedNativeOperators(spark)
        aggregates.foreach(a => assert(rule.revert(a).isEmpty))
      }
    }
  }
}
