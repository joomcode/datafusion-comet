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
import org.apache.spark.sql.comet.{CometPlan, CometSortExec, CometSortMergeJoinExec, CometWindowExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarToRowTransition, RowToColumnarTransition, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
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
  private val variableWidthTypes =
    CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_VARIABLE_WIDTH_TYPES_ENABLED.key

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

  private def withPayload(payloads: String*)(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(2000)
        .selectExpr(Seq("cast(id % 97 AS int) AS k", "id AS v") ++ payloads: _*)
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t")
      withTempView("t")(f)
    }
  }

  private def initialPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case other => other
  }

  private def sparkWindowOver(table: String, partition: String, order: String): DataFrame =
    spark
      .table(table)
      .withColumn("rn", row_number().over(Window.partitionBy(partition).orderBy(order)))

  private val variableWidthPayloads = Seq(
    "binary" -> "cast(concat('b', cast(id AS string)) AS binary) AS x",
    "array" -> "array(id, id + 1) AS x",
    "map" -> "map(cast(id % 5 AS int), id) AS x",
    "struct with binary" ->
      "named_struct('i', cast(id AS int), 'b', cast(cast(id AS string) AS binary)) AS x",
    "struct with array" -> "named_struct('i', cast(id AS int), 'a', array(id)) AS x")

  variableWidthPayloads.foreach { case (name, payload) =>
    test(s"a sort with a $name column outside its key runs in Spark on the initial plan") {
      withPayload(payload) {
        withSQLConf(
          (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") ++
            sparkWindowConfs): _*) {
          val initial = initialPlan(sparkWindowOver("t", "k", "v"))
          assert(sparkSorts(initial).size == 1 && cometSorts(initial).isEmpty, s"$initial")
          assert(
            sparkSorts(initial).forall(_.getTagValue(CometExecRule.KEEP_ON_SPARK_TAG).isDefined),
            s"plan:\n$initial")
          val plan = run(sparkWindowOver("t", "k", "v"))
          assert(sparkSorts(plan).size == 1 && cometSorts(plan).isEmpty, s"plan:\n$plan")
        }
        withSQLConf(
          (Seq(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
            flag -> "true",
            variableWidthTypes -> "false") ++ sparkWindowConfs): _*) {
          val plan = run(sparkWindowOver("t", "k", "v"))
          assert(sparkSorts(plan).isEmpty, s"plan:\n$plan")
        }
      }
    }
  }

  test("variable-width payload types without statistics also move the sort without AQE") {
    withPayload(variableWidthPayloads.head._2) {
      withSQLConf(
        (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "true") ++
          sparkWindowConfs): _*) {
        val plan = run(sparkWindowOver("t", "k", "v"))
        assert(sparkSorts(plan).size == 1 && cometSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("a struct of fixed-width fields does not count as variable width") {
    withPayload("named_struct('i', cast(id AS int), 'l', id) AS x") {
      withSQLConf(
        (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") ++
          sparkWindowConfs): _*) {
        val initial = initialPlan(sparkWindowOver("t", "k", "v"))
        assert(cometSorts(initial).size == 1 && sparkSorts(initial).isEmpty, s"$initial")
        val plan = run(sparkWindowOver("t", "k", "v"))
        assert(cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("a string payload does not count as variable width") {
    narrow {
      withSQLConf(
        (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") ++
          sparkWindowConfs): _*) {
        val initial = initialPlan(sparkWindow)
        assert(cometSorts(initial).size == 1 && sparkSorts(initial).isEmpty, s"$initial")
        val plan = run(sparkWindow)
        assert(cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("variable-width types only in the sort key do not move the sort") {
    withPayload("cast(concat('b', cast(id % 11 AS string)) AS binary) AS x") {
      withSQLConf(
        (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") ++ sparkWindowConfs): _*) {
        val (off, on) = offAndOn()(run(sparkWindowOver("t", "x", "v")))
        assert(cometSorts(off).size == 1, s"plan:\n$off")
        assert(cometSorts(on).size == 1 && sparkSorts(on).isEmpty, s"plan:\n$on")
      }
    }
  }

  test("the row size threshold defaults to 1024 bytes from the stage statistics") {
    assert(
      CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_AVG_ROW_BYTES.defaultValue.get == 1024L)
    Seq(700 -> false, 1400 -> true).foreach { case (payloadBytes, toSpark) =>
      withTable(payloadBytes) {
        withSQLConf(
          (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") ++
            sparkWindowConfs): _*) {
          val initial = initialPlan(sparkWindow)
          assert(cometSorts(initial).size == 1, s"$payloadBytes bytes:\n$initial")
          val plan = run(sparkWindow)
          val sorts = if (toSpark) sparkSorts(plan) else cometSorts(plan)
          assert(sorts.size == 1, s"$payloadBytes bytes:\n$plan")
          val stageRow = nodes(plan)
            .collectFirst { case s: QueryStageExec => s }
            .flatMap(WideRowSortFallback.runtimeAvgRowBytes)
          assert(stageRow.exists(b => (b > 1024) == toSpark), s"row bytes $stageRow:\n$plan")
        }
      }
    }
  }

  test("variable-width payloads read by a native sort-merge join stay native") {
    withPayload("cast(concat('b', cast(id AS string)) AS binary) AS x", "array(id) AS y") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        flag -> "true") {
        val query = "SELECT a.k, a.x, a.y, b.x FROM t a JOIN t b ON a.k = b.k"
        val initial = initialPlan(sql(query))
        assert(cometSorts(initial).size == 2 && sparkSorts(initial).isEmpty, s"$initial")
        val plan = run(sql(query))
        assert(nodes(plan).exists(_.isInstanceOf[CometSortMergeJoinExec]), s"plan:\n$plan")
        assert(cometSorts(plan).size == 2 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("variable-width payloads read by a native window stay native") {
    withPayload("cast(concat('b', cast(id AS string)) AS binary) AS x") {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") {
        val plan = run(sparkWindowOver("t", "k", "v"))
        assert(nodes(plan).exists(_.isInstanceOf[CometWindowExec]), s"plan:\n$plan")
        assert(cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("a sort moved to Spark stays there when AQE re-plans with narrower statistics") {
    val empties = (1 to 10).map(i => s"'' AS e$i")
    withPayload(empties: _*) {
      val threshold = 150
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          flag -> "true",
          minAvgRowBytes -> threshold.toString) ++ sparkWindowConfs): _*) {
        val initial = initialPlan(sparkWindowOver("t", "k", "v"))
        val initialSort = sparkSorts(initial)
        assert(initialSort.size == 1 && cometSorts(initial).isEmpty, s"plan:\n$initial")
        val plan = run(sparkWindowOver("t", "k", "v"))
        val sorts = sparkSorts(plan)
        assert(sorts.size == 1 && cometSorts(plan).isEmpty, s"plan:\n$plan")
        val stageRow = nodes(plan)
          .collectFirst { case s: QueryStageExec => s }
          .flatMap(WideRowSortFallback.runtimeAvgRowBytes)
        assert(stageRow.exists(_ <= threshold), s"row bytes $stageRow:\n$plan")
      }
    }
  }

  private def boundaryChain(plan: SparkPlan): Seq[SparkPlan] = {
    val aggregates = nodes(plan).collect { case a: SortAggregateExec => a }
    val finalAggregate = aggregates.head
    def down(node: SparkPlan): Seq[SparkPlan] = node match {
      case _: SortAggregateExec if node ne finalAggregate => Seq(node)
      case s: QueryStageExec => s +: down(s.plan)
      case other => other +: other.children.flatMap(down)
    }
    down(finalAggregate)
  }

  test("a Spark sort aggregate over a variable-width buffer gets a Spark shuffle on both sides") {
    withPayload("cast(id % 1000 AS double) AS d") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.USE_OBJECT_HASH_AGG.key -> "false",
        CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.key -> "true",
        flag -> "true") {
        val query =
          "SELECT k, percentile_approx(d, 0.5) AS p, count(*) AS c, sum(v) AS s FROM t GROUP BY k"
        val initial = initialPlan(sql(query))
        val initialChain = boundaryChain(initial)
        assert(
          initialChain.exists(_.isInstanceOf[SortExec]) &&
            initialChain.exists(_.isInstanceOf[ShuffleExchangeExec]) &&
            !initialChain.exists(n => n.isInstanceOf[CometPlan]),
          s"plan:\n$initial")
        val plan = run(sql(query))
        val chain = boundaryChain(plan)
        assert(nodes(plan).count(_.isInstanceOf[SortAggregateExec]) == 2, s"plan:\n$plan")
        assert(chain.exists(_.isInstanceOf[SortExec]), s"plan:\n$plan")
        assert(chain.exists(_.isInstanceOf[ShuffleExchangeExec]), s"plan:\n$plan")
        assert(
          !chain.exists {
            case _: CometShuffleExchangeExec | _: ColumnarToRowTransition |
                _: RowToColumnarTransition | _: CometPlan =>
              true
            case _ => false
          },
          s"chain ${chain.map(_.nodeName).mkString(" <- ")}:\n$plan")
      }
    }
  }
}
