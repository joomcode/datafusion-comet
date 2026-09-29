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
import org.apache.spark.sql.comet.{CometSortExec, CometSortMergeJoinExec, CometWindowExec}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, RowToColumnarTransition, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class WideRowSortFallbackSuite extends CometTestBase {

  private val flag = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.key
  private val minAvgRowBytes = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_AVG_ROW_BYTES.key
  private val maxKeyFraction = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MAX_KEY_FRACTION.key

  private def withTable(payloadBytes: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(2000)
        .selectExpr(
          "cast(id % 97 AS int) AS k",
          "id AS v",
          s"concat(cast(id AS string), repeat('x', $payloadBytes)) AS p",
          "concat('q', cast(id % 13 AS string)) AS q",
          "concat('r', cast(id % 7 AS string)) AS r")
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("w")
      withTempView("w")(f)
    }
  }

  private def wide(f: => Unit): Unit = withTable(4000)(f)

  private def narrow(f: => Unit): Unit = withTable(10)(f)

  private def run(df: => DataFrame): SparkPlan = checkSparkAnswer(df)._2

  private def nodes(plan: SparkPlan): Seq[SparkPlan] = {
    def visit(node: SparkPlan): Seq[SparkPlan] = node match {
      case a: AdaptiveSparkPlanExec => visit(a.executedPlan)
      case s: QueryStageExec => s +: visit(s.plan)
      case other => other +: other.children.flatMap(visit)
    }
    visit(plan)
  }

  private def sparkSorts(plan: SparkPlan): Seq[SortExec] =
    nodes(plan).collect { case s: SortExec => s }

  private def cometSorts(plan: SparkPlan): Seq[CometSortExec] =
    nodes(plan).collect { case s: CometSortExec => s }

  private def transitions(plan: SparkPlan): Int =
    nodes(plan).count {
      case _: ColumnarToRowTransition | _: RowToColumnarTransition => true
      case _ => false
    }

  private def sparkWindow: DataFrame =
    spark
      .table("w")
      .withColumn("rn", row_number().over(Window.partitionBy("k").orderBy("v")))

  private val sparkWindowConfs = Seq(CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false")

  private def offAndOn(confs: (String, String)*)(f: => SparkPlan): (SparkPlan, SparkPlan) = {
    var off: SparkPlan = null
    var on: SparkPlan = null
    withSQLConf((flag -> "false") +: confs: _*) { off = f }
    withSQLConf((flag -> "true") +: confs: _*) { on = f }
    (off, on)
  }

  test("a sort of wide rows with a narrow key read by a Spark window runs in Spark") {
    wide {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        val (off, on) = offAndOn(sparkWindowConfs: _*)(run(sparkWindow))
        assert(sparkSorts(off).isEmpty && cometSorts(off).size == 1, s"plan:\n$off")
        assert(sparkSorts(on).size == 1 && cometSorts(on).isEmpty, s"plan:\n$on")
        assert(
          sparkSorts(on).forall(_.getTagValue(CometExecRule.KEEP_ON_SPARK_TAG).isDefined),
          s"plan:\n$on")
        assert(nodes(on).exists(_.isInstanceOf[WindowExec]), s"plan:\n$on")
        assert(transitions(on) <= transitions(off), s"transitions added:\n$off\n$on")
      }
    }
  }

  test("a sort of wide rows read by a Spark sort aggregate runs in Spark") {
    wide {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") {
        val plan = run(sql("SELECT k, max(p), count(*) FROM w GROUP BY k"))
        val aggregates = nodes(plan).collect { case a: SortAggregateExec => a }
        assert(aggregates.nonEmpty, s"plan:\n$plan")
        assert(sparkSorts(plan).nonEmpty, s"plan:\n$plan")
      }
    }
  }

  test("a sort of wide rows read by a Spark sort-merge join runs in Spark") {
    wide {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "false") {
        val query = "SELECT a.k, a.p, b.v FROM w a JOIN (SELECT k, v FROM w) b ON a.k = b.k"
        val (off, on) = offAndOn()(run(sql(query)))
        assert(nodes(on).exists(_.isInstanceOf[SortMergeJoinExec]), s"plan:\n$on")
        assert(cometSorts(off).size == 2, s"plan:\n$off")
        assert(sparkSorts(on).size == 1 && cometSorts(on).size == 1, s"plan:\n$on")
        assert(transitions(on) <= transitions(off), s"transitions added:\n$off\n$on")
      }
    }
  }

  test("a sort of narrow rows stays native") {
    narrow {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
        val (off, on) = offAndOn(sparkWindowConfs: _*)(run(sparkWindow))
        assert(cometSorts(on).size == 1 && sparkSorts(on).isEmpty, s"plan:\n$on")
        assert(cometSorts(off).size == cometSorts(on).size, s"plan:\n$off")
      }
    }
  }

  test("a sort keyed by most of the row stays native") {
    wide {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false",
        flag -> "true") {
        val plan = run(
          spark
            .table("w")
            .withColumn("rn", row_number().over(Window.partitionBy("p").orderBy("q"))))
        assert(cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("a sort of wide rows read by a native window stays native") {
    wide {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") {
        val plan = run(sparkWindow)
        assert(nodes(plan).exists(_.isInstanceOf[CometWindowExec]), s"plan:\n$plan")
        assert(cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("sorts of wide rows read by a native sort-merge join stay native") {
    wide {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        flag -> "true") {
        val plan =
          run(sql("SELECT a.k, a.p, b.v FROM w a JOIN (SELECT k, v FROM w) b ON a.k = b.k"))
        assert(nodes(plan).exists(_.isInstanceOf[CometSortMergeJoinExec]), s"plan:\n$plan")
        assert(cometSorts(plan).size == 2 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("without runtime statistics the row width comes from the schema") {
    wide {
      withSQLConf((SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") +: sparkWindowConfs: _*) {
        val (_, byDefault) = offAndOn()(run(sparkWindow))
        assert(cometSorts(byDefault).size == 1, s"plan:\n$byDefault")
        val (off, bySchema) = offAndOn(minAvgRowBytes -> "40")(run(sparkWindow))
        assert(sparkSorts(bySchema).size == 1 && cometSorts(bySchema).isEmpty, s"$bySchema")
        assert(transitions(bySchema) <= transitions(off), s"transitions added:\n$bySchema")
        val (_, keyTooWide) =
          offAndOn(minAvgRowBytes -> "40", maxKeyFraction -> "0.1")(run(sparkWindow))
        assert(cometSorts(keyTooWide).size == 1, s"plan:\n$keyTooWide")
      }
    }
  }

  test("the row width from the schema also applies with shuffle formats from both sides") {
    wide {
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.key -> "true",
          minAvgRowBytes -> "40") ++ sparkWindowConfs): _*) {
        val (off, on) = offAndOn()(run(sparkWindow))
        assert(sparkSorts(on).size == 1 && cometSorts(on).isEmpty, s"plan:\n$on")
        assert(transitions(on) <= transitions(off), s"transitions added:\n$off\n$on")
      }
    }
  }

  test("the rule leaves the plan unchanged when disabled") {
    wide {
      withSQLConf(
        (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") ++
          sparkWindowConfs): _*) {
        val plan = run(sparkWindow)
        assert(WideRowSortFallback(spark).apply(plan) eq plan)
        assert(cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }
}
