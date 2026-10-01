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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.{CometSortExec, CometSortMergeJoinExec, CometWindowExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarToRowTransition, RowToColumnarTransition, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class WideRowSortFallbackSuite extends CometTestBase {

  private val flag = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.key
  private val minLeaves = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.key
  private val threshold = 50
  private val shuffleMinLeaves = CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.key

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(shuffleMinLeaves, "0")

  private def ints(n: Int, prefix: String = "c"): Seq[String] =
    (1 to n).map(i => s"cast(id + $i AS int) AS $prefix$i")

  private def structOf(n: Int): String =
    (1 to n).map(i => s"'f$i', cast(id + $i AS int)").mkString("named_struct(", ", ", ")")

  private val shapes: Seq[(String, Int => Seq[String])] = Seq(
    "flat columns" -> (n => ints(n)),
    "a struct" -> (n => Seq(s"${structOf(n)} AS x")),
    "an array of structs" -> (n => Seq(s"array(${structOf(n)}, ${structOf(n)}) AS x")),
    "a map" -> (n => Seq(s"map(cast(id % 5 AS int), ${structOf(n - 1)}) AS x")),
    "nested and flat columns" -> (n => s"${structOf(n / 2)} AS x" +: ints(n - n / 2)))

  private def withPayload(payloads: Seq[String])(f: => Unit): Unit = {
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

  private def wide(f: => Unit): Unit = withPayload(ints(threshold))(f)

  private def narrow(f: => Unit): Unit = withPayload(ints(threshold - 1))(f)

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

  private def initialPlan(df: DataFrame): SparkPlan = df.queryExecution.executedPlan match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case other => other
  }

  private def sparkWindowOver(partition: String, order: String = "v"): DataFrame =
    spark
      .table("t")
      .withColumn("rn", row_number().over(Window.partitionBy(partition).orderBy(order)))

  private def sparkWindow: DataFrame = sparkWindowOver("k")

  private val sparkWindowConfs = Seq(CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false")

  private def offAndOn(confs: (String, String)*)(f: => SparkPlan): (SparkPlan, SparkPlan) = {
    var off: SparkPlan = null
    var on: SparkPlan = null
    withSQLConf((flag -> "false") +: confs: _*) { off = f }
    withSQLConf((flag -> "true") +: confs: _*) { on = f }
    (off, on)
  }

  private def bothAqeModes(f: => Unit): Unit =
    Seq("false", "true").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe)(f)
    }

  private def inSpark(plan: SparkPlan): Boolean =
    sparkSorts(plan).size == 1 && cometSorts(plan).isEmpty

  private def native(plan: SparkPlan): Boolean =
    cometSorts(plan).size == 1 && sparkSorts(plan).isEmpty

  test("the threshold defaults to 50 leaf columns and the rule to off") {
    assert(CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.defaultValue.get == 50)
    assert(CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.defaultValue.get == false)
  }

  shapes.foreach { case (name, payload) =>
    test(s"a sort over $name moves to Spark at $threshold payload leaves, not at 49") {
      Seq(threshold - 1 -> false, threshold -> true).foreach { case (leaves, toSpark) =>
        val columns = payload(leaves)
        withPayload(columns) {
          assert(
            LeafColumns.count(spark.table("t").schema) == leaves + 2,
            spark.table("t").schema.treeString)
          bothAqeModes {
            withSQLConf((flag -> "true") +: sparkWindowConfs: _*) {
              val initial = initialPlan(sparkWindow)
              val plan = run(sparkWindow)
              if (toSpark) {
                assert(inSpark(initial), s"$leaves leaves:\n$initial")
                assert(inSpark(plan), s"$leaves leaves:\n$plan")
                assert(
                  sparkSorts(plan).forall(
                    _.getTagValue(CometExecRule.KEEP_ON_SPARK_TAG).isDefined),
                  s"plan:\n$plan")
                val reasons = sparkSorts(plan).head
                  .getTagValue(org.apache.comet.CometExplainInfo.FALLBACK_REASONS)
                  .getOrElse(Set.empty)
                assert(reasons.exists(_.contains(s"$leaves leaf columns")), reasons)
              } else {
                assert(native(initial), s"$leaves leaves:\n$initial")
                assert(native(plan), s"$leaves leaves:\n$plan")
              }
            }
          }
        }
      }
    }
  }

  test("a sort of wide rows read by a Spark window runs in Spark without added transitions") {
    wide {
      bothAqeModes {
        val (off, on) = offAndOn(sparkWindowConfs: _*)(run(sparkWindow))
        assert(native(off), s"plan:\n$off")
        assert(inSpark(on), s"plan:\n$on")
        assert(nodes(on).exists(_.isInstanceOf[WindowExec]), s"plan:\n$on")
        assert(transitions(on) <= transitions(off), s"transitions added:\n$off\n$on")
      }
    }
  }

  test("columns of the sort key are not counted") {
    withPayload(Seq(s"${structOf(10)} AS ks") ++ ints(threshold - 5)) {
      bothAqeModes {
        withSQLConf((flag -> "true") +: sparkWindowConfs: _*) {
          val byK = run(sparkWindowOver("k"))
          assert(inSpark(byK), s"plan:\n$byK")
          val byStruct = run(sparkWindowOver("ks"))
          assert(native(byStruct), s"plan:\n$byStruct")
        }
      }
    }
    withPayload(ints(threshold + 2)) {
      bothAqeModes {
        withSQLConf((flag -> "true") +: sparkWindowConfs: _*) {
          assert(inSpark(run(sparkWindowOver("k"))))
          val byColumns = run(
            spark
              .table("t")
              .withColumn(
                "rn",
                row_number().over(Window.partitionBy("k", "c1", "c2").orderBy("v", "c3"))))
          assert(native(byColumns), s"plan:\n$byColumns")
        }
      }
    }
  }

  test("the threshold is configurable") {
    withPayload(ints(10)) {
      withSQLConf((Seq(flag -> "true", minLeaves -> "10") ++ sparkWindowConfs): _*) {
        assert(inSpark(run(sparkWindow)))
      }
      withSQLConf((Seq(flag -> "true", minLeaves -> "11") ++ sparkWindowConfs): _*) {
        assert(native(run(sparkWindow)))
      }
    }
  }

  test("a sort of wide rows read by a Spark sort aggregate runs in Spark") {
    val strings = (1 to threshold).map(i => s"concat('s', cast(id + $i AS string)) AS s$i")
    withPayload(strings) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true", flag -> "true") {
        val maxes = (1 to threshold).map(i => s"max(s$i)").mkString(", ")
        val plan = run(sql(s"SELECT k, $maxes, count(*) FROM t GROUP BY k"))
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
        val query = "SELECT a.*, b.v AS v2 FROM t a JOIN (SELECT k, v FROM t) b ON a.k = b.k"
        val (off, on) = offAndOn()(run(sql(query)))
        assert(nodes(on).exists(_.isInstanceOf[SortMergeJoinExec]), s"plan:\n$on")
        assert(cometSorts(off).size == 2, s"plan:\n$off")
        assert(sparkSorts(on).size == 1 && cometSorts(on).size == 1, s"plan:\n$on")
        assert(transitions(on) <= transitions(off), s"transitions added:\n$off\n$on")
      }
    }
  }

  test("a sort of wide rows read by a native window stays native") {
    withPayload(ints(threshold * 2)) {
      bothAqeModes {
        withSQLConf(flag -> "true") {
          val plan = run(sparkWindow)
          assert(nodes(plan).exists(_.isInstanceOf[CometWindowExec]), s"plan:\n$plan")
          assert(native(plan), s"plan:\n$plan")
        }
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
        val initial =
          initialPlan(sql("SELECT a.*, b.c1 AS b1 FROM t a JOIN t b ON a.k = b.k"))
        assert(cometSorts(initial).size == 2 && sparkSorts(initial).isEmpty, s"$initial")
        val plan = run(sql("SELECT a.*, b.c1 AS b1 FROM t a JOIN t b ON a.k = b.k"))
        assert(nodes(plan).exists(_.isInstanceOf[CometSortMergeJoinExec]), s"plan:\n$plan")
        assert(cometSorts(plan).size == 2 && sparkSorts(plan).isEmpty, s"plan:\n$plan")
      }
    }
  }

  test("the rule leaves the plan unchanged when disabled") {
    withPayload(ints(threshold * 2)) {
      withSQLConf(
        (Seq(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") ++
          sparkWindowConfs): _*) {
        val plan = run(sparkWindow)
        assert(WideRowSortFallback(spark).apply(plan) eq plan)
        assert(native(plan), s"plan:\n$plan")
      }
    }
  }

  test("a sort of narrow rows stays native when the rule is on") {
    narrow {
      bothAqeModes {
        val (off, on) = offAndOn(sparkWindowConfs: _*)(run(sparkWindow))
        assert(native(on) && native(off), s"plan:\n$on")
      }
    }
  }

  test("a sort moved to Spark stays there on repeated runs with boundary formats") {
    wide {
      val confs = Seq(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.key -> "true") ++ sparkWindowConfs
      withSQLConf((flag -> "true") +: confs: _*) {
        assert(inSpark(initialPlan(sparkWindow)))
      }
      Seq(1, 2).foreach { _ =>
        val (off, on) = offAndOn(confs: _*)(run(sparkWindow))
        assert(inSpark(on), s"plan:\n$on")
        assert(transitions(on) <= transitions(off), s"transitions added:\n$off\n$on")
      }
    }
  }

  test("with the shuffle rule at its default a wide sort and its shuffle both run in Spark") {
    wide {
      bothAqeModes {
        withSQLConf(
          (Seq(
            flag -> "true",
            shuffleMinLeaves -> CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.defaultValueString) ++
            sparkWindowConfs): _*) {
          val plan = run(sparkWindow)
          assert(inSpark(plan), s"plan:\n$plan")
          assert(!nodes(plan).exists(_.isInstanceOf[CometShuffleExchangeExec]), s"plan:\n$plan")
        }
      }
    }
  }
}
