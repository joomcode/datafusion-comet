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
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.rules.BoundaryTestHelpers._

class CostBasedEngineChoiceSuite extends CometTestBase {

  private val flag = CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.key

  private def withTables(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(3000)
        .selectExpr(
          "cast(id % 211 AS int) AS k",
          "cast((id * 7919) % 1000 AS int) AS v",
          "concat('payload_', cast(id % 257 AS string)) AS s")
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t")
      withTempView("t")(f)
    }
  }

  private def run(df: => DataFrame): SparkPlan = checkSparkAnswer(df)._2

  private def run(query: String): SparkPlan = run(sql(query))

  private def withAqe(aqe: String, confs: (String, String)*)(f: => Unit): Unit =
    withSQLConf((SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) +: confs: _*)(f)

  private def offAndOn(f: => SparkPlan): (SparkPlan, SparkPlan) = {
    var off: SparkPlan = null
    var on: SparkPlan = null
    withSQLConf(flag -> "false") { off = f }
    withSQLConf(flag -> "true") { on = f }
    (off, on)
  }

  /** Every node of the executed plan, looking into query stages. */
  private def nodes(plan: SparkPlan): Seq[SparkPlan] = {
    def visit(node: SparkPlan): Seq[SparkPlan] = node match {
      case a: AdaptiveSparkPlanExec => visit(a.executedPlan)
      case s: QueryStageExec => s +: visit(s.plan)
      case other => other +: other.children.flatMap(visit)
    }
    visit(plan)
  }

  private def count[T](plan: SparkPlan)(pf: PartialFunction[SparkPlan, T]): Int =
    nodes(plan).collect(pf).size

  private def columnarBetweenSpark(plan: SparkPlan): Seq[Edge] =
    edges(plan).filter(e => e.format == "columnar" && !e.consumerIsComet && !e.producerIsComet)

  for (aqe <- Seq("false", "true")) {
    test(
      s"a native sort between a columnar shuffle and a Spark aggregate runs in Spark (AQE=$aqe)") {
      withTables {
        withAqe(aqe) {
          val (off, on) = offAndOn(run("SELECT k, max(s) FROM t GROUP BY k"))
          assert(count(off) { case s: SortAggregateExec => s } == 2, s"plan:\n$off")
          assert(
            edges(off).exists(e => e.format == "columnar" && e.consumerIsComet),
            s"expected a native sort over a columnar shuffle without the rule:\n$off")
          assert(count(on) { case s: SortAggregateExec => s } == 2, s"plan:\n$on")
          assert(edges(on).map(_.format) == Seq("spark"), s"plan:\n$on")
          // The sort below the partial aggregate reads the native scan and stays native.
          assert(count(on) { case s: CometSortExec => s } == 1, s"plan:\n$on")
          assert(count(on) { case s: SortExec => s } == 1, s"plan:\n$on")
        }
      }
    }

    test(s"native filter and project feeding a Spark aggregate stay native (AQE=$aqe)") {
      withTables {
        withAqe(aqe) {
          var on: SparkPlan = null
          withSQLConf(flag -> "true") {
            on = run(
              "SELECT k2, max(s) FROM (SELECT k + 1 AS k2, s FROM t WHERE v > 10) GROUP BY k2")
          }
          assert(count(on) { case f: CometFilterExec => f } == 1, s"plan:\n$on")
          assert(count(on) { case p: CometProjectExec => p } == 1, s"plan:\n$on")
          assert(edges(on).map(_.format) == Seq("spark"), s"plan:\n$on")
          assert(columnarBetweenSpark(on).isEmpty, s"plan:\n$on")
        }
      }
    }

    test(s"a native filter between Spark operators runs in Spark (AQE=$aqe)") {
      withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
        val (off, on) = offAndOn(
          run(spark.range(0, 1000).filter(col("id") % 3 === 0).select((col("id") + 1).as("x"))))
        assert(count(off) { case f: CometFilterExec => f } == 1, s"plan:\n$off")
        assert(count(off) { case r: CometSparkToColumnarExec => r } == 1, s"plan:\n$off")
        assert(count(on) { case f: CometFilterExec => f } == 0, s"plan:\n$on")
        assert(count(on) { case r: CometSparkToColumnarExec => r } == 0, s"plan:\n$on")
      }
    }

    test(s"a Spark sort-merge join keeps the native sort of its native input (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val query = "SELECT a.k, a.m, b.v FROM (SELECT k, max(s) AS m FROM t GROUP BY k) a " +
            "JOIN t b ON a.k = b.k"
          val (off, on) = offAndOn(run(query))
          for (plan <- Seq(off, on)) {
            assert(count(plan) { case j: SortMergeJoinExec => j } == 1, s"plan:\n$plan")
            val joinSorts = nodes(plan).collect { case j: SortMergeJoinExec => j }.head
            assert(
              nodes(joinSorts).exists(_.isInstanceOf[CometSortExec]),
              s"the native input keeps its native sort:\n$plan")
          }
          val nativeInputs = edges(on).filter(e => e.format == "native")
          assert(nativeInputs.nonEmpty, s"plan:\n$on")
        }
      }
    }

    test(s"a native stage keeps its native shuffle into a Spark stage (AQE=$aqe)") {
      withTables {
        withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false", flag -> "true") {
          val plan = run(
            spark
              .table("t")
              .select("k", "v")
              .repartition(col("k"))
              .select(col("k"), (col("v") + 1).as("w")))
          assert(edges(plan).map(_.format) == Seq("native"), s"plan:\n$plan")
        }
      }
    }

    test(s"a Spark stage keeps its columnar shuffle into a native stage (AQE=$aqe)") {
      withTables {
        withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false", flag -> "true") {
          val plan = run(
            spark
              .table("t")
              .select(col("k"), (col("v") + 1).as("v"))
              .repartition(col("k"))
              .groupBy("k")
              .agg(sum("v")))
          assert(edges(plan).map(_.format) == Seq("columnar"), s"plan:\n$plan")
          assert(edges(plan).head.consumerIsComet, s"plan:\n$plan")
        }
      }
    }

    test(s"a fully native query is unchanged (AQE=$aqe)") {
      withTables {
        withAqe(aqe) {
          val (off, on) = offAndOn(run("SELECT k, sum(v) FROM t WHERE v > 3 GROUP BY k"))
          assert(cometOperatorNames(off) == cometOperatorNames(on), s"$off\n$on")
          assert(edges(on).map(_.format) == Seq("native"), s"plan:\n$on")
        }
      }
    }

    test(s"identical shuffles stay reused (AQE=$aqe)") {
      withTables {
        withAqe(aqe) {
          val query = "WITH x AS (SELECT k, max(s) AS m FROM t GROUP BY k) " +
            "SELECT * FROM x UNION ALL SELECT * FROM x"
          val (off, on) = offAndOn(run(query))
          def reused(plan: SparkPlan): Int = collectWithSubqueries(finalPlan(plan)) {
            case r: ReusedExchangeExec => r
            case s: QueryStageExec if s.plan.isInstanceOf[ReusedExchangeExec] => s
          }.size
          assert(reused(off) > 0, s"expected reuse without the rule:\n$off")
          assert(reused(on) == reused(off), s"reuse lost:\n$on")
        }
      }
    }

    test(s"dynamic partition pruning still works (AQE=$aqe)") {
      withTempDir { dir =>
        val factPath = s"${dir.getCanonicalPath}/fact"
        val dimPath = s"${dir.getCanonicalPath}/dim"
        spark
          .range(2000)
          .selectExpr("id % 20 AS p", "id AS v", "cast(id % 7 AS string) AS s")
          .write
          .partitionBy("p")
          .parquet(factPath)
        spark
          .range(20)
          .selectExpr("id AS k", "concat('n', cast(id AS string)) AS name")
          .write
          .parquet(dimPath)
        withAqe(aqe, flag -> "true", SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
          spark.read.parquet(factPath).createOrReplaceTempView("fact")
          spark.read.parquet(dimPath).createOrReplaceTempView("dim")
          withTempView("fact", "dim") {
            val query = "SELECT f.p, max(f.s), count(*) FROM fact f JOIN dim d ON f.p = d.k " +
              "WHERE d.name IN ('n3', 'n5') GROUP BY f.p"
            run(query)
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
              run(query)
            }
          }
        }
      }
    }
  }

  test("the rule leaves the plan unchanged when disabled") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val plan = finalPlan(run("SELECT k, max(s) FROM t GROUP BY k"))
        assert(CostBasedEngineChoice(spark).apply(plan) eq plan)
        assert(edges(plan).exists(e => e.format == "columnar"), s"plan:\n$plan")
      }
    }
  }

  test("per-operator weights change the choice") {
    withTables {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        flag -> "true",
        CometConf.COMET_EXEC_COST_BASED_ENGINES_OPERATOR_WEIGHTS.key -> "SortExec=-5") {
        val plan = run("SELECT k, max(s) FROM t GROUP BY k")
        assert(count(plan) { case s: CometSortExec => s } == 2, s"plan:\n$plan")
      }
    }
  }

  test("reverted operators are tagged to stay in Spark") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "true") {
        val plan = finalPlan(run("SELECT k, max(s) FROM t GROUP BY k"))
        val sorts = nodes(plan).collect { case s: SortExec => s }
        assert(
          sorts.nonEmpty && sorts.forall(
            _.getTagValue(CometExecRule.KEEP_ON_SPARK_TAG).isDefined))
      }
    }
  }
}
