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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{ExpandExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.rules.BoundaryFormats.Engine
import org.apache.comet.rules.BoundaryTestHelpers._
import org.apache.comet.rules.EngineCostModel.Term
import org.apache.comet.rules.EngineCostTable.{CostClass, Form, Line, Width}
import org.apache.comet.rules.EngineCostTable.CostClass._

class CostBasedEngineChoiceSuite extends CometTestBase {

  private val flag = CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.key
  private val costTable = CometConf.COMET_EXEC_COST_BASED_ENGINES_COST_TABLE.key
  private val expensiveNativeSort = costTable -> "sort.flat.comet=1000,0,0"

  private def same(actual: Double, expected: Double): Unit =
    assert(math.abs(actual - expected) < 1e-9, s"$actual != $expected")

  private def model: EngineCostModel = EngineCostModel(spark.sessionState.conf)

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

  /** Runs `f` with view `name`: an int key `k` and `payload` int columns `c1`, `c2`, ... */
  private def withWide(name: String, payload: Int)(f: => Unit): Unit = {
    withTempPath { dir =>
      val columns = "cast(id % 97 AS int) AS k" +:
        (1 to payload).map(i => s"cast((id * $i) % 1000 AS int) AS c$i")
      spark.range(1000).selectExpr(columns: _*).write.parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView(name)
      withTempView(name)(f)
    }
  }

  /** Runs `f` with view `star`, the schema of star_order_2020's purchases, 4 rows per order. */
  private def withStar(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(2000)
        .selectExpr(
          "cast(id % 500 AS string) AS order_id",
          "id AS event_ts",
          "concat('c', cast(id % 37 AS string)) AS category_id",
          "IF(id % 2 = 0, 'android', 'ios') AS os_type",
          "array(named_struct('amount', cast(id % 13 AS double) / 4, 'type', 'coupon', " +
            "'subType', cast(id % 3 AS string))) AS discounts",
          "named_struct('name', cast(id % 7 AS string), 'activation_ts', id, " +
            "'is_last_context', true, 'is_adtech_promoted', id % 5 = 0) AS last_context",
          "array(named_struct('name', cast(id % 7 AS string), 'activation_ts', id, " +
            "'is_last_context', true, 'is_adtech_promoted', false)) AS normalized_contexts",
          "'joom' AS legal_entity",
          "'joom' AS app_entity",
          "'joom' AS app_entity_group",
          "IF(id % 4 = 0, NULL, 'd') AS custom_domain")
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("star")
      withTempView("star")(f)
    }
  }

  private val starColumns = "order_id, category_id, os_type, discounts, last_context, " +
    "normalized_contexts, legal_entity, app_entity, app_entity_group, custom_domain"

  /** The latest purchase of each order, deduplicated, as star_order_2020 reads them. */
  private def starQuery: DataFrame =
    sql(
      s"SELECT DISTINCT $starColumns FROM (SELECT *, rank() OVER (PARTITION BY order_id " +
        "ORDER BY event_ts DESC) AS r FROM star) WHERE r = 1")

  private def run(df: => DataFrame): SparkPlan = checkSparkAnswer(df)._2

  private def run(query: String): SparkPlan = run(sql(query))

  /** The executed plan of `df`, whose rows are in no order Spark would reproduce. */
  private def runUnordered(df: DataFrame): SparkPlan = {
    df.collect()
    df.queryExecution.executedPlan
  }

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

  for (aqe <- Seq("false", "true")) {
    test(s"native sorts read by Spark sort aggregates move only by their price (AQE=$aqe)") {
      withTables {
        withAqe(aqe) {
          val query = "SELECT k, max(s) FROM t GROUP BY k"
          val (off, on) = offAndOn(run(query))
          assert(count(off) { case s: SortAggregateExec => s } == 2, s"plan:\n$off")
          assert(count(off) { case s: CometSortExec => s } == 2, s"plan:\n$off")
          assert(count(on) { case s: SortAggregateExec => s } == 2, s"plan:\n$on")
          assert(count(on) { case s: CometSortExec => s } == 2, s"plan:\n$on")
          withSQLConf(flag -> "true", expensiveNativeSort) {
            val moved = run(query)
            assert(count(moved) { case s: CometSortExec => s } == 0, s"plan:\n$moved")
            assert(count(moved) { case s: SortExec => s } == 2, s"plan:\n$moved")
          }
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

    test(s"a Spark sort-merge join moves the native sort of its input by price (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val query = "SELECT a.k, a.m, b.v FROM (SELECT k, max(s) AS m FROM t GROUP BY k) a " +
            "JOIN t b ON a.k = b.k"
          def joinHasNativeSort(plan: SparkPlan): Boolean = {
            assert(count(plan) { case j: SortMergeJoinExec => j } == 1, s"plan:\n$plan")
            val join = nodes(plan).collect { case j: SortMergeJoinExec => j }.head
            nodes(join).exists(_.isInstanceOf[CometSortExec])
          }
          val (off, on) = offAndOn(run(query))
          assert(joinHasNativeSort(off), s"plan:\n$off")
          assert(joinHasNativeSort(on), s"the native input keeps its native sort:\n$on")
          withSQLConf(flag -> "true", expensiveNativeSort) {
            val plan = run(query)
            assert(!joinHasNativeSort(plan), s"plan:\n$plan")
            assert(edges(plan).exists(_.format == "native"), s"plan:\n$plan")
          }
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
        withAqe(
          aqe,
          CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false",
          flag -> "true",
          costTable -> "agg.flat.spark=1000,0") {
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

  test("a cube over wide rows reads its Spark sort aggregates through Spark shuffles") {
    withTempPath { dir =>
      val columns = Seq("cast(id % 97 AS int) AS k", "cast(id % 13 AS int) AS a") ++
        (1 to 20).map(i => s"concat('s', cast((id * $i) % 1000 AS string)) AS s$i") ++
        (1 to 90).map(i => s"cast((id * $i) % 1000 AS double) AS d$i")
      spark.range(1000).selectExpr(columns: _*).write.parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("w")
      withTempView("w") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "1600") {
          val aggs = ((1 to 20).map(i => s"max(s$i)") ++ (1 to 90).map(i => s"sum(d$i)"))
            .mkString(", ")
          val (off, on) =
            offAndOn(runUnordered(sql(s"SELECT k, a, $aggs FROM w GROUP BY CUBE(k, a)")))
          assert(edges(off).map(_.format) == Seq("columnar"), s"plan:\n$off")
          assert(count(off) { case c: CometColumnarToRowExec => c } == 2, s"plan:\n$off")
          assert(edges(on).map(_.format) == Seq("spark"), s"plan:\n$on")
          assert(count(on) { case s: SortAggregateExec => s } == 2, s"plan:\n$on")
          assert(count(on) { case c: CometColumnarToRowExec => c } == 1, s"plan:\n$on")
          assert(count(on) { case e: CometExpandExec => e } == 1, s"plan:\n$on")
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

  test("per-operator weights price operators outside the table") {
    withTables {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_EXEC_COST_BASED_ENGINES_OPERATOR_WEIGHTS.key ->
          "ShuffledHashJoinExec=-5,SortExec=-7") {
        val plan =
          run("SELECT /*+ SHUFFLE_HASH(b) */ a.k, a.v, b.s FROM t a JOIN t b ON a.k = b.k")
        val join = nodes(plan).collectFirst { case j: CometHashJoinExec => j }
        assert(join.isDefined, s"plan:\n$plan")
        assert(model.costClasses(join.get).isEmpty)
        assert(model.operatorCost(join.get, Engine.Comet) == -5)
        assert(model.operatorCost(join.get, Engine.Spark) == 0)
        val sort = runUnordered(sql("SELECT * FROM t SORT BY k"))
        val native = nodes(sort).collectFirst { case s: CometSortExec => s }.get
        assert(model.costClasses(native) == Seq(Sort))
        assert(model.operatorCost(native, Engine.Comet) != -7)
      }
    }
  }

  for (aqe <- Seq("false", "true")) {
    test(s"a narrow schema stays native (AQE=$aqe)") {
      withWide("t8", 7) {
        withAqe(aqe) {
          val (off, on) = offAndOn(
            runUnordered(
              spark
                .table("t8")
                .filter(col("c4") > 5)
                .repartition(col("k"))
                .sortWithinPartitions("k")
                .select(col("k"), (col("c1") + col("c2")).as("s"), col("c3"))))
          assert(cometOperatorNames(off) == cometOperatorNames(on), s"$off\n$on")
          assert(count(on) { case s: SortExec => s } == 0, s"plan:\n$on")
          assert(edges(on).map(_.format) == Seq("native"), s"plan:\n$on")
        }
      }
    }

    test(s"a wide sort and shuffle read by a Spark operator run in Spark (AQE=$aqe)") {
      withWide("t300", 299) {
        withAqe(
          aqe,
          CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false",
          flag -> "true",
          SQLConf.SHUFFLE_PARTITIONS.key -> "1000") {
          val plan = runUnordered(
            spark
              .table("t300")
              .repartition(col("k"))
              .sortWithinPartitions("k")
              .select(col("*"), (col("c1") + 1).as("x")))
          assert(edges(plan).map(_.format) == Seq("spark"), s"plan:\n$plan")
          assert(count(plan) { case s: CometSortExec => s } == 0, s"plan:\n$plan")
          assert(count(plan) { case s: SortExec => s } == 1, s"plan:\n$plan")
        }
      }
    }

    test(s"a sort-merge join with one wide input runs in Spark (AQE=$aqe)") {
      withWide("narrow", 7) {
        withWide("wide", 499) {
          withAqe(
            aqe,
            flag -> "true",
            CometConf.COMET_EXEC_COST_BASED_ENGINES_LOG_ENABLED.key -> "true",
            SQLConf.SHUFFLE_PARTITIONS.key -> "1000",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
            val plan = run("SELECT n.c1 AS n1, w.* FROM narrow n JOIN wide w ON n.k = w.k")
            assert(count(plan) { case j: SortMergeJoinExec => j } == 1, s"plan:\n$plan")
            assert(count(plan) { case j: CometSortMergeJoinExec => j } == 0, s"plan:\n$plan")
            val join = nodes(plan).collectFirst { case j: SortMergeJoinExec => j }.get
            val (wideSide, narrowSide) = join.children.partition(_.output.size > 100)
            assert(wideSide.flatMap(nodes).count(_.isInstanceOf[SortExec]) == 1, s"plan:\n$plan")
            assert(
              narrowSide.flatMap(nodes).count(_.isInstanceOf[CometSortExec]) == 1,
              s"plan:\n$plan")
            val (wideInputs, narrowInputs) = edges(plan).partition(_.exchange.output.size > 100)
            assert(wideInputs.map(_.format) == Seq("spark"), s"plan:\n$plan")
            assert(narrowInputs.map(_.format) == Seq("native"), s"plan:\n$plan")
          }
        }
      }
    }

    test(s"a native sort read by a Spark window moves only by its price (AQE=$aqe)") {
      withWide("t8", 7) {
        withAqe(aqe, flag -> "true", CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false") {
          val query = "SELECT k, c1, row_number() OVER (PARTITION BY k ORDER BY c1) AS r FROM t8"
          val mixed = run(query)
          assert(count(mixed) { case s: CometSortExec => s } == 1, s"plan:\n$mixed")
          withSQLConf(expensiveNativeSort) {
            val moved = run(query)
            assert(count(moved) { case s: CometSortExec => s } == 0, s"plan:\n$moved")
            assert(count(moved) { case s: SortExec => s } == 1, s"plan:\n$moved")
          }
        }
      }
    }
  }

  test("a wide sort read by a native operator stays native or moves with its stage by cost") {
    withWide("t300", 299) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "true") {
        def query: DataFrame =
          spark.table("t300").sortWithinPartitions("k").select(col("*"), (col("c1") + 1).as("x"))
        val kept = runUnordered(query)
        assert(count(kept) { case s: CometSortExec => s } == 1, s"plan:\n$kept")
        assert(count(kept) { case p: CometProjectExec => p } == 1, s"plan:\n$kept")
        withSQLConf(costTable -> "sort.flat.comet=0,0,1") {
          val moved = runUnordered(query)
          assert(count(moved) { case s: CometSortExec => s } == 0, s"plan:\n$moved")
          assert(count(moved) { case p: CometProjectExec => p } == 0, s"plan:\n$moved")
          assert(count(moved) { case s: SortExec => s } == 1, s"plan:\n$moved")
        }
      }
    }
  }

  test("the wide-row rules do not run with the cost-based choice") {
    withWide("t60", 59) {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false",
        CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.key -> "50") {
        def query: DataFrame =
          spark
            .table("t60")
            .repartition(col("k"))
            .sortWithinPartitions("k")
            .select(col("*"), (col("c1") + 1).as("x"))
        val (off, on) = offAndOn(runUnordered(query))
        assert(edges(off).map(_.format) == Seq("spark"), s"plan:\n$off")
        assert(count(off) { case s: CometSortExec => s } == 0, s"plan:\n$off")
        assert(edges(on).map(_.format) == Seq("native"), s"plan:\n$on")
        assert(count(on) { case s: CometSortExec => s } == 1, s"plan:\n$on")
      }
    }
  }

  test("the cost table is overridden by its configuration") {
    val table = EngineCostTable.parse(
      " sort.flat.comet=1,2; shuffleRead.nested.spark = 3,4 ;shuffleWrite.flat.comet=5,6,7;" +
        "shuffleWritePartitionSlope=0.5;shuffleWritePartitionSlopePerLeaf=0;" +
        "shuffleWritePartitionBase=100;c2r.nested.comet=7,0;r2c.flat.comet=4,5;" +
        "agg.nested.spark=8,9;aggCollectList.comet=1,2,3;windowAggregate.spark=10,11;" +
        "filterPassThroughPerLeaf.comet=0.25;shuffleReadPerByte.comet=2;" +
        "keepFiltersOverNativeScans=false;keepPartialAggregatesOverNativeInputs=false;" +
        "shuffleReadPerByte.spark=3;cometShuffleBytesRatio=0.75;quadraticLeafCap=10;" +
        "sortSpillFraction=0.5")
    assert(table.line(Sort, Form.Flat) == Line(224, 1, 2, 646, 0))
    assert(table.line(ShuffleRead, Form.Nested) == Line(0, 10.79, 0.019, 3, 4))
    assert(table.line(ShuffleWrite, Form.Flat) == Line(5, 6, 7, 69, 67.21))
    assert(table.line(C2R, Form.Nested).cometK0 == 7)
    assert(table.line(R2C, Form.Flat) == Line(3.4, 4, 5, 0, 0))
    assert(table.line(Agg, Form.Nested) == Line(564, 62.4, 0.242, 8, 9))
    for (form <- Form.all) {
      assert(table.line(AggCollectList, form) == Line(1, 2, 3, 1700, 0))
      assert(table.line(WindowAggregate, form) == Line(380, 0, 0, 10, 11))
    }
    assert(table.shuffleWritePartitionFactor(100, 300) == 2)
    assert(table.filterPassThroughPerLeafComet == 0.25)
    assert(table.filterPassThroughPerLeafSpark == 0)
    same(table.cometShuffleBytes(100), (0.5 + 2) * 0.75 * 100)
    same(table.sparkShuffleBytes(100), (3.6 + 3) * 100)
    same(table.comet(Sort, Width(20, 0)), 224 + 1 * 20 + 2 * 20 * 10)
    assert(table.sortSpillFraction == 0.5)
    assert(!table.keepFiltersOverNativeScans)
    assert(EngineCostTable.default.keepFiltersOverNativeScans)
    assert(!table.keepPartialAggregatesOverNativeInputs)
    assert(EngineCostTable.default.keepPartialAggregatesOverNativeInputs)
    assert(
      table.line(RowLocal, Form.Flat) == EngineCostTable.default
        .line(RowLocal, Form.Flat))
    assert(EngineCostTable.parse("") == EngineCostTable.default)

    for (bad <- Seq(
        "sort.flat.comet=1",
        "sort.flat.comet=1,x",
        "sort.flat.comet=1,2,3,4",
        "sort.flat.spark=1,2,3",
        "sorts.flat.comet=1,2",
        "sort.deep.comet=1,2",
        "sort.flat.velox=1,2",
        "sort.velox=1,2",
        "c2r.flat.spark=1,2",
        "r2c.spark=1,2",
        "expandNoCodegen.comet=1,2",
        "filterPassThroughPerLeaf=0.5",
        "filterPassThroughPerLeaf.comet=NaN",
        "shuffleReadPerByte.comet=1,2",
        "shuffleWritePartitionBase=0",
        "quadraticLeafCap=0",
        "keepFiltersOverNativeScans=1",
        "oomRiskPenalty=1",
        "unknownScalar=1",
        "sort.flat.comet")) {
      val e = intercept[IllegalArgumentException](EngineCostTable.parse(bad))
      assert(e.getMessage.contains(costTable), e.getMessage)
      assert(e.getMessage.contains(bad), e.getMessage)
    }

    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val plan = finalPlan(run("SELECT k, max(s) FROM t GROUP BY k"))
        withSQLConf(flag -> "true", costTable -> "sort.flat.comet=1") {
          val e = intercept[IllegalArgumentException](CostBasedEngineChoice(spark).apply(plan))
          assert(e.getMessage.contains(costTable), e.getMessage)
        }
      }
    }
  }

  test("prices follow the formulas of the table") {
    val t = EngineCostTable.default
    val flat10 = Width(10, 0)
    same(t.comet(ShuffleWrite, flat10), 48.95 * 10 + 0.037 * 100)
    same(t.spark(ShuffleWrite, flat10), 69 + 67.21 * 10)
    same(t.comet(ShuffleRead, Width(4, 4)), 10.79 * 4 + 0.019 * 16)
    same(t.spark(ShuffleRead, Width(4, 4)), 167 + 19.02 * 4)
    same(t.spark(Sort, Width(200, 0)), 646)
    same(t.comet(Sort, Width(200, 0)), 224 + 0.023 * 40000)
    same(t.comet(Sort, Width(200, 200)), 244 + 2.69 * 200 + 0.016 * 40000)
    same(t.comet(Sort, Width(1000, 0)), 224 + 0.023 * 1000 * 600)
    same(t.comet(Smj, flat10), 40)
    same(t.spark(Smj, flat10), 350)
    same(t.comet(Bhj, flat10), 72 + 23 + 0.2)
    same(t.spark(Bhj, Width(10, 10)), 205)
    same(t.comet(Predicate, Width(2, 0)), 11 + 2.14 * 2)
    same(t.spark(Predicate, Width(2, 2)), 8.2 * 2)
    same(t.comet(C2R, flat10), 80 + 0.011 * 100)
    same(t.comet(C2R, Width(10, 10)), 20 + 119 + 0.019 * 100)
    same(t.comet(R2C, flat10), 3.4 + 76.7 + 3.6)
    same(t.spark(AggCollectList, Width(5, 0)), 1700)
    same(t.spark(WindowAggregate, Width(5, 0)), 20 + 0.8 * 5)
    same(t.shuffleWritePartitionFactor(100, 100), 1)
    same(t.shuffleWritePartitionFactor(100, 250), 1)
    same(t.shuffleWritePartitionFactor(100, 2000), 1 + (0.04 + 0.036) * 7)
    same(t.shuffleReadPartitionFactor(100, 2000), 1 + (0.06 + 0.025) * 7)
    same(t.columnarShuffleWritePartitionFactor(100, 2000), 1 + 0.1 * 7)
    same(t.cometShuffleBytes(1000), (0.5 + 0.6) * 1000)
    same(t.sparkShuffleBytes(1000), (3.6 + 0.45) * 1000)
  }

  test("a price blends the flat and nested lines by the nested fraction, with no step") {
    val t = EngineCostTable.default
    for (costClass <- CostClass.all) {
      val flat = t.comet(costClass, Width(100, 0))
      val nested = t.comet(costClass, Width(100, 100))
      for (n <- Seq(0, 25, 49, 50, 51, 75, 100)) {
        same(t.comet(costClass, Width(100, n)), flat + (nested - flat) * n / 100)
      }
      same(
        t.comet(costClass, Width(100, 51)) - t.comet(costClass, Width(100, 49)),
        (nested - flat) * 0.02)
      if (costClass.spark) {
        val sparkFlat = t.spark(costClass, Width(100, 0))
        val sparkNested = t.spark(costClass, Width(100, 100))
        same(
          t.spark(costClass, Width(100, 51)) - t.spark(costClass, Width(100, 49)),
          (sparkNested - sparkFlat) * 0.02)
      }
    }

    def attr(name: String, dataType: DataType): Attribute = AttributeReference(name, dataType)()
    val ints = (1 to 3).map(i => attr(s"i$i", IntegerType))
    val struct3 = attr("s", StructType(Seq("a", "b", "c").map(StructField(_, IntegerType))))
    assert(EngineCostTable.widthOf(ints) == Width(3, 0))
    assert(EngineCostTable.widthOf(ints :+ struct3) == Width(6, 3))
    assert(EngineCostTable.widthOf(ints :+ attr("x", IntegerType) :+ struct3) == Width(7, 3))
    assert(
      EngineCostTable.widthOf(
        Seq(attr("m", MapType(IntegerType, ArrayType(StringType))))) == Width(2, 2))
    assert(EngineCostTable.widthOf(Nil) == Width(0, 0))
    assert(Width(0, 0).nestedFraction == 0)
  }

  test("operators cost their price per row, whatever their rows") {
    def sortCost(rows: Int): Double = {
      var cost = Double.NaN
      withTempPath { dir =>
        spark.range(rows).selectExpr("id AS k", "id + 1 AS v").write.parquet(dir.getCanonicalPath)
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val plan =
            runUnordered(spark.read.parquet(dir.getCanonicalPath).sortWithinPartitions("k"))
          val native = nodes(plan).collectFirst { case s: CometSortExec => s }.get
          assert(model.terms(native, Engine.Comet) == Seq(Term(Sort, Width(2, 0))))
          cost = model.operatorCost(native, Engine.Comet)
        }
      }
      cost
    }
    val small = sortCost(10)
    same(small, EngineCostTable.default.comet(Sort, Width(2, 0)))
    same(sortCost(20000), small)
  }

  test("a sort adds its spill fraction and its bytes beyond the lines") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val plan = runUnordered(spark.table("t").sortWithinPartitions("k"))
        val sort = nodes(plan).collectFirst { case s: CometSortExec => s }.get
        val t = EngineCostTable.default
        val w = Width(3, 0)
        assert(model.excessBytes(sort.output) == 20 - 12)
        same(model.operatorCost(sort, Engine.Comet), t.comet(Sort, w) + 0.15 * 8)
        same(model.operatorCost(sort, Engine.Spark), t.spark(Sort, w))
        val spilling =
          new EngineCostModel(EngineCostTable.parse("sortSpillFraction=0.25"), -1, 0, Map.empty)
        assert(spilling.terms(sort, Engine.Spark) == Seq(Term(Sort, w), Term(SortSpill, w, 0.25)))
        same(
          spilling.operatorCost(sort, Engine.Spark),
          t.spark(Sort, w) + 0.25 * t.spark(SortSpill, w))
      }
    }
  }

  test("a project prices its pass-through by engine and the expressions it computes") {
    withWide("t8", 7) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        def project(df: DataFrame): SparkPlan =
          nodes(runUnordered(df)).collectFirst {
            case p: CometProjectExec => p
            case p: ProjectExec => p
          }.get
        def select(df: DataFrame): DataFrame =
          df.select(col("k"), col("c1").as("a"), (col("c2") + col("c3")).as("s"), col("c4"))
        val overScan = project(select(spark.table("t8")))
        val w = Width(4, 0)
        assert(model.overScan(overScan.children.head))
        assert(
          model.terms(overScan, Engine.Comet) ==
            Seq(Term(ProjectPassThrough, w), Term(Expr, w, 1)))
        assert(model.terms(overScan, Engine.Spark) == Seq(Term(ExprOverScan, w, 1)))
        same(model.operatorPrice(overScan, Engine.Comet), 1.25 + 0.057 * 4 + 1)
        same(model.operatorPrice(overScan, Engine.Spark), 2.5)

        val afterShuffle = project(select(spark.table("t8").repartition(col("k"))))
        assert(!model.overScan(afterShuffle.children.head))
        same(model.operatorPrice(afterShuffle, Engine.Comet), 1.25 + 0.057 * 4 + 1)
        same(model.operatorPrice(afterShuffle, Engine.Spark), 10.5 * 4 + 2.3 + 0.1 * 4)
      }
    }
  }

  test("a filter prices the leaves of its predicate and passes its rows by engine") {
    withWide("t8", 7) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val plan = runUnordered(spark.table("t8").filter(col("c4") > 5 && col("c5") < 900))
        val filter = nodes(plan).collectFirst { case f: CometFilterExec => f }.get
        assert(model.terms(filter, Engine.Comet) == Seq(Term(Predicate, Width(2, 0))))
        same(model.operatorCost(filter, Engine.Comet), 11 + 2.14 * 2 + 1.5 * 8)
        same(model.operatorCost(filter, Engine.Spark), 5.4 * 2)
      }
    }
  }

  test("a selective filter over a wide native scan stays native whatever the prices") {
    withWide("t60", 59) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "true") {
        def query: DataFrame =
          spark
            .table("t60")
            .filter(col("c50") < 5 && col("c51") > 1)
            .select((col("k") +: (1 to 49).map(i => col(s"c$i"))) :+ (col("c2") + 1).as("x"): _*)
        val expensive = "predicate.comet=100000,0,0;filterPassThroughPerLeaf.comet=1000;" +
          "projectPassThrough.comet=100000,0,0"
        for (prices <- Seq("", expensive)) {
          withSQLConf(costTable -> prices) {
            val plan = runUnordered(query)
            assert(count(plan) { case f: CometFilterExec => f } == 1, s"plan:\n$plan")
            assert(count(plan) { case p: CometProjectExec => p } == 1, s"plan:\n$plan")
          }
        }
        withSQLConf(costTable -> s"$expensive;keepFiltersOverNativeScans=false") {
          val moved = runUnordered(query)
          assert(count(moved) { case f: CometFilterExec => f } == 0, s"plan:\n$moved")
          assert(count(moved) { case p: CometProjectExec => p } == 0, s"plan:\n$moved")
        }
      }
    }
  }

  test(
    "a partial aggregate over a wide native scan and filter stays native whatever the prices") {
    withWide("t60", 59) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "true") {
        def query: DataFrame =
          spark
            .table("t60")
            .filter(col("c50") < 500)
            .groupBy("k")
            .agg(sum("c1"), sum("c2"))
        def partials(plan: SparkPlan): Int = count(plan) {
          case a: CometHashAggregateExec if a.aggregateExpressions.forall(_.mode == Partial) => a
        }
        val expensive = "agg.comet=100000,0,0;aggDeclarative.comet=100000,0,0"
        for (prices <- Seq("", expensive)) {
          withSQLConf(costTable -> prices) {
            val plan = runUnordered(query)
            assert(partials(plan) == 1, s"plan:\n$plan")
          }
        }
        withSQLConf(costTable -> s"$expensive;keepPartialAggregatesOverNativeInputs=false") {
          val moved = runUnordered(query)
          assert(partials(moved) == 0, s"plan:\n$moved")
        }
      }
    }
  }

  test("aggregates are priced by their keys and the classes of their functions") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val hash = run("SELECT k, sum(v), count(*) FROM t GROUP BY k")
        val aggs = nodes(hash).collect { case a: CometHashAggregateExec => a }
        assert(aggs.size == 2, s"plan:\n$hash")
        aggs.foreach { agg =>
          assert(model.costClasses(agg) == Seq(Agg))
          assert(
            model.terms(agg, Engine.Comet) ==
              Seq(Term(Agg, Width(1, 0), 0.5), Term(AggDeclarative, Width(2, 0), 1.0)))
          same(model.operatorCost(agg, Engine.Comet), 0.5 * (3.2 + 0.082) + 6)
          same(model.operatorCost(agg, Engine.Spark), 0.5 * 62.1 + 15)
          val narrow = new EngineCostModel(EngineCostTable.default, -1, 0, Map.empty, 1)
          assert(
            narrow.terms(agg, Engine.Spark) ==
              Seq(Term(Agg, Width(1, 0), 0.5), Term(AggDeclarativeNoCodegen, Width(2, 0), 1.0)))
        }

        val objects = runUnordered(
          sql("SELECT k, collect_list(v), collect_list(s), collect_set(v), percentile(v, 0.5), " +
            "percentile_approx(v, 0.5) FROM t GROUP BY k"))
        val objectAggs = nodes(objects).filter(_.nodeName.contains("Aggregate"))
        assert(objectAggs.size == 2, s"plan:\n$objects")
        objectAggs.foreach { agg =>
          val terms = model.terms(agg, Engine.Spark)
          assert(
            terms.map(t => (t.costClass, t.times)) == Seq(
              Agg -> 0.5,
              AggCollectList -> 1.0,
              AggCollectSet -> 0.5,
              AggPercentile -> 0.5,
              AggPercentileApprox -> 0.5,
              AggObjectHash -> 0.5),
            s"plan:\n$objects")
          same(
            model.operatorPrice(agg, Engine.Spark),
            0.5 * (62.1 + 2 * 1700 + 1600 + 2400 + 3900 + 2500))
          same(
            model.operatorPrice(agg, Engine.Comet),
            0.5 * (3.2 + 0.082 + 2 * 28 + 105 + 130 + 270 + 1000))
        }

        val sorted = run("SELECT k, max(s) FROM t GROUP BY k")
        val sortAgg = nodes(sorted).collectFirst { case a: SortAggregateExec => a }.get
        assert(
          model.terms(sortAgg, Engine.Spark) == Seq(
            Term(Agg, Width(1, 0), 0.5),
            Term(AggDeclarative, Width(1, 0), 0.5),
            Term(Sort, Width(2, 0))))
      }
    }
  }

  test("aggregate keys holding arrays and keys computed by the codegen dispatcher add classes") {
    withStar {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val plan = runUnordered(sql(s"SELECT DISTINCT $starColumns FROM star"))
        val aggs = nodes(plan).collect { case a: CometHashAggregateExec => a }
        assert(aggs.size == 2, s"plan:\n$plan")
        val (partial, finals) = aggs.partition(a =>
          model.dispatchedThroughCodegen(
            a.originalPlan
              .asInstanceOf[HashAggregateExec]
              .groupingExpressions
              .find(_.name == "discounts")
              .get))
        assert(partial.size == 1 && finals.size == 1, s"plan:\n$plan")
        val keys = Term(Agg, Width(18, 11), 0.5)
        val arrays = Term(AggArrayKey, Width(7, 7), 0.5)
        assert(
          model.terms(partial.head, Engine.Comet) ==
            Seq(keys, arrays, Term(CodegenDispatch, Width(3, 3))))
        assert(model.terms(finals.head, Engine.Comet) == Seq(keys, arrays))
        val table = EngineCostTable.default
        same(
          model.operatorPrice(partial.head, Engine.Comet),
          0.5 * table.comet(Agg, Width(18, 11)) + 0.5 * 937 * 7 + 217 * 3)
        same(
          model.operatorPrice(partial.head, Engine.Spark),
          0.5 * table.spark(Agg, Width(18, 11)) + 0.5 * 76 * 7 + 133 * 3)
        same(
          model.operatorPrice(finals.head, Engine.Comet),
          0.5 * table.comet(Agg, Width(18, 11)) + 0.5 * 937 * 7)

        val flat = runUnordered(sql("SELECT DISTINCT order_id, category_id FROM star"))
        nodes(flat).collect { case a: CometHashAggregateExec => a }.foreach { agg =>
          assert(model.terms(agg, Engine.Comet) == Seq(Term(Agg, Width(2, 0), 0.5)))
        }
      }
    }
  }

  for (aqe <- Seq("false", "true")) {
    test(
      s"a distinct over array keys after a window runs in Spark, as in star_order_2020 (AQE=$aqe)") {
      withStar {
        withAqe(aqe) {
          def aggregates(plan: SparkPlan): (Int, Int) =
            (
              count(plan) { case a: CometHashAggregateExec => a },
              count(plan) { case a: HashAggregateExec => a })
          val (off, on) = offAndOn(runUnordered(starQuery))
          assert(aggregates(off) == (2, 0), s"plan:\n$off")
          assert(aggregates(on) == (0, 2), s"plan:\n$on")
          withSQLConf(
            flag -> "true",
            costTable -> ("aggArrayKey.comet=0,0,0;aggArrayKey.spark=0,0;" +
              "codegenDispatch.comet=0,0,0;codegenDispatch.spark=0,0")) {
            val before = runUnordered(starQuery)
            assert(aggregates(before) == (2, 0), s"plan:\n$before")
          }
        }
      }
    }
  }

  test("a sort-merge join adds smjCondition only with a join condition") {
    withTables {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        flag -> "false") {
        def join(query: String): CometSortMergeJoinExec = {
          val plan = run(query)
          val joins = nodes(plan).collect { case j: CometSortMergeJoinExec => j }
          assert(joins.size == 1, s"plan:\n$plan")
          joins.head
        }
        val out = Width(4, 0)
        val equi = join("SELECT a.k, a.v, b.v, b.k FROM t a JOIN t b ON a.k = b.k")
        assert(model.terms(equi, Engine.Comet) == Seq(Term(Smj, out)))
        assert(model.terms(equi, Engine.Spark) == Seq(Term(Smj, out)))
        same(model.operatorPrice(equi, Engine.Comet), EngineCostTable.default.comet(Smj, out))

        val band = join(
          "SELECT a.k, a.v, b.v, b.k FROM t a JOIN t b ON a.k = b.k " +
            "AND a.v <= b.v AND b.v <= a.v + 13")
        assert(model.terms(band, Engine.Comet) == Seq(Term(Smj, out), Term(SmjCondition, out)))
        assert(model.terms(band, Engine.Spark) == Seq(Term(Smj, out), Term(SmjCondition, out)))
        val table = EngineCostTable.default
        for (engine <- Engine.all) {
          val price: (CostClass, Width) => Double =
            if (engine == Engine.Comet) table.comet else table.spark
          same(model.operatorPrice(band, engine), price(Smj, out) + price(SmjCondition, out))
        }
      }
    }
  }

  for (aqe <- Seq("false", "true")) {
    test(
      s"a sort-merge join with a join condition runs in Spark, without one natively (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          def joins(plan: SparkPlan): (Int, Int) =
            (
              count(plan) { case j: CometSortMergeJoinExec => j },
              count(plan) { case j: SortMergeJoinExec => j })
          val equi = "SELECT a.k, a.v, b.s FROM t a JOIN t b ON a.k = b.k"
          val band = equi + " AND a.v <= b.v AND b.v <= a.v + 13"
          val (equiOff, equiOn) = offAndOn(run(equi))
          assert(joins(equiOff) == (1, 0), s"plan:\n$equiOff")
          assert(joins(equiOn) == (1, 0), s"plan:\n$equiOn")
          assert(cometOperatorNames(equiOff) == cometOperatorNames(equiOn), s"$equiOff\n$equiOn")
          val (bandOff, bandOn) = offAndOn(run(band))
          assert(joins(bandOff) == (1, 0), s"plan:\n$bandOff")
          assert(joins(bandOn) == (0, 1), s"plan:\n$bandOn")
          withSQLConf(
            flag -> "true",
            costTable -> "smjCondition.comet=0,0,0;smjCondition.spark=0,0") {
            val plan = run(band)
            assert(joins(plan) == (1, 0), s"plan:\n$plan")
          }
        }
      }
    }
  }

  private def withIntervals(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(2000)
        .selectExpr(
          "cast(id % 97 AS int) AS k",
          "timestamp_seconds(id * 3600) AS v",
          "id * 3600 AS vs",
          "concat('s', cast(id % 7 AS string)) AS s")
        .write
        .parquet(s"${dir.getCanonicalPath}/ev")
      spark
        .range(500)
        .selectExpr(
          "cast(id % 97 AS int) AS k",
          "timestamp_seconds(id * 7200) AS l",
          "id * 7200 AS ls",
          "IF(id % 5 = 0, NULL, timestamp_seconds(id * 7200 + 259200)) AS u",
          "timestamp_seconds(id * 7200) AS created_at",
          "IF(id % 5 = 0, NULL, timestamp_seconds(id * 7200 + 259200)) AS completed_dt",
          "concat('s', cast(id % 5 AS string)) AS s")
        .write
        .parquet(s"${dir.getCanonicalPath}/dim")
      spark.read.parquet(s"${dir.getCanonicalPath}/ev").createOrReplaceTempView("ev")
      spark.read.parquet(s"${dir.getCanonicalPath}/dim").createOrReplaceTempView("dim")
      sql(
        "CREATE OR REPLACE TEMP VIEW rates AS SELECT s AS currency, " +
          "CAST(to_date(l) AS timestamp) AS effective_date, " +
          "CAST(to_date(u) AS timestamp) AS next_effective_date FROM dim")
      withTempView("ev", "dim", "rates")(f)
    }
  }

  private val validityIntervals = Seq(
    "e.v >= d.l AND e.v < d.u",
    "e.v BETWEEN d.l AND d.u",
    "e.v > d.l AND e.v <= COALESCE(d.u, timestamp '9999-12-31')",
    "(d.u IS NULL OR e.v < d.u) AND e.v >= d.l",
    "to_date(e.v) >= to_date(d.l) AND CAST(e.v AS date) < CAST(d.u AS date)",
    "date_trunc('day', e.v) >= d.l AND e.v < date_trunc('day', d.u)")

  private val intervalQueries = Seq(
    "SELECT e.k, e.v, r.effective_date FROM ev e LEFT JOIN rates r ON e.s = r.currency " +
      "AND e.v > r.effective_date AND e.v <= r.next_effective_date",
    "SELECT e.k, e.v, d.l FROM ev e LEFT JOIN dim d ON e.k = d.k AND e.v >= d.l " +
      "AND e.v < CAST(COALESCE(CAST(d.u AS string), '9999-12-31') AS timestamp)",
    "WITH p AS (SELECT k, l, LEAD(l) OVER (PARTITION BY k ORDER BY l) AS nl FROM dim) " +
      "SELECT e.k, e.v, p.l FROM ev e LEFT JOIN p ON e.k = p.k AND e.v >= p.l " +
      "AND e.v < COALESCE(p.nl, timestamp '9999-12-31')")

  private val crossInputBounds =
    "SELECT o.k, o.v, g.v AS gv FROM ev o JOIN dim p ON o.k = p.k " +
      "LEFT JOIN ev g ON o.k = g.k AND p.completed_dt < g.v AND o.v > g.v"

  private val chargedConditions = Seq(
    crossInputBounds,
    "SELECT e.k, e.v, d.l FROM ev e JOIN dim d ON e.k = d.k " +
      "AND e.vs BETWEEN d.ls - 2592000 AND d.ls",
    "SELECT e.k, e.v, d.l FROM ev e JOIN dim d ON e.k = d.k " +
      "AND e.v BETWEEN d.l - INTERVAL 30 DAYS AND d.l",
    "WITH p AS (SELECT k, l, l + INTERVAL 30 DAYS AS l_end FROM dim) " +
      "SELECT e.k, e.v, p.l FROM ev e JOIN p ON e.k = p.k AND e.v >= p.l AND e.v < p.l_end",
    "SELECT e.k, e.v, d.l FROM ev e LEFT JOIN dim d ON e.k = d.k AND e.v > d.created_at " +
      "AND COALESCE(d.completed_dt, timestamp '5999-12-31') <= e.v",
    "SELECT e.k, e.v, d.l FROM ev e JOIN dim d ON e.k = d.k " +
      "JOIN dim x ON e.k = x.k AND e.v >= d.l AND e.v < x.u",
    "SELECT e.k, e.v, d.l FROM ev e JOIN dim d ON e.k = d.k AND e.v >= d.l",
    "SELECT e.k, e.v, d.l FROM ev e JOIN dim d ON e.k = d.k AND e.v >= d.l AND e.v < d.u " +
      "AND e.s <> d.s")

  private def conditionTerms(query: String): Seq[Seq[Term]] = {
    val plan = runUnordered(sql(query))
    val joins = nodes(plan).collect {
      case j: CometSortMergeJoinExec
          if j.originalPlan.asInstanceOf[SortMergeJoinExec].condition.isDefined =>
        j
      case j: SortMergeJoinExec if j.condition.isDefined => j
    }
    assert(joins.nonEmpty, s"plan:\n$plan")
    joins.map(j => model.terms(j, Engine.Comet).filter(_.costClass == SmjCondition))
  }

  for (aqe <- Seq("false", "true")) {
    test(
      s"a validity interval adds no smjCondition, a band or another condition does (AQE=$aqe)") {
      withIntervals {
        withAqe(
          aqe,
          flag -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          for (condition <- validityIntervals; joinType <- Seq("JOIN", "LEFT JOIN")) {
            val query =
              s"SELECT e.k, e.v, d.l FROM ev e $joinType dim d ON e.k = d.k AND $condition"
            assert(conditionTerms(query).forall(_.isEmpty), query)
          }
          for (query <- intervalQueries) {
            assert(conditionTerms(query).forall(_.isEmpty), query)
          }
          for (query <- chargedConditions) {
            assert(conditionTerms(query).exists(_.nonEmpty), query)
          }
        }
      }
    }

    test(s"a validity interval join stays native, a band join runs in Spark (AQE=$aqe)") {
      withIntervals {
        withAqe(
          aqe,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          def joins(plan: SparkPlan): (Int, Int) =
            (
              count(plan) { case j: CometSortMergeJoinExec => j },
              count(plan) { case j: SortMergeJoinExec => j })
          val interval = "SELECT e.k, e.v, d.l FROM ev e LEFT JOIN dim d ON e.k = d.k " +
            "AND e.v >= d.l AND e.v < COALESCE(d.u, timestamp '9999-12-31')"
          val (intervalOff, intervalOn) = offAndOn(run(interval))
          assert(joins(intervalOff) == (1, 0), s"plan:\n$intervalOff")
          assert(joins(intervalOn) == (1, 0), s"plan:\n$intervalOn")
          val (bandOff, bandOn) = offAndOn(run(chargedConditions(1)))
          assert(joins(bandOff) == (1, 0), s"plan:\n$bandOff")
          assert(joins(bandOn) == (0, 1), s"plan:\n$bandOn")
          val (crossOff, crossOn) = offAndOn(run(crossInputBounds))
          assert(joins(crossOff) == (2, 0), s"plan:\n$crossOff")
          assert(
            count(crossOn) { case j: SortMergeJoinExec if j.condition.isDefined => j } == 1,
            s"plan:\n$crossOn")
        }
      }
    }
  }

  test("a window costs its line and the classes of its functions") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val plan = run(
          "SELECT k, v, row_number() OVER w AS r, sum(v) OVER w AS s, lag(v) OVER w AS l, " +
            "lead(v) OVER w AS n FROM t WINDOW w AS (PARTITION BY k ORDER BY v)")
        val windows = nodes(plan).filter(_.nodeName.contains("Window"))
        assert(windows.size == 1, s"plan:\n$plan")
        val window = windows.head
        val n = Width(4, 0)
        assert(
          model.terms(window, Engine.Comet) == Seq(
            Term(Window, Width(2, 0)),
            Term(WindowRank, n, 1),
            Term(WindowAggregate, n, 1),
            Term(WindowOffset, n, 2)))
        same(model.operatorPrice(window, Engine.Comet), 53 + 5.08 * 2 + 0.002 * 4 + 380 + 120)
        same(model.operatorPrice(window, Engine.Spark), 15.3 * 2 + 20 + 0.8 * 4 + 2 * 0.25 * 4)
      }
    }
  }

  test("an expand and a generate cost nothing in Spark with codegen, per projection without") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false", flag -> "false") {
        val rollup = run("SELECT k, v, count(*) FROM t GROUP BY ROLLUP(k, v)")
        val expand = nodes(rollup).collectFirst { case e: CometExpandExec => e }.get
        val projections = expand.originalPlan.asInstanceOf[ExpandExec].projections.size
        assert(projections == 3)
        val w = EngineCostTable.widthOf(expand.output)
        assert(model.terms(expand, Engine.Comet) == Seq(Term(Expand, w, 3)))
        same(model.operatorCost(expand, Engine.Comet), 0)
        same(model.operatorCost(expand, Engine.Spark), 0)
        val narrow = new EngineCostModel(EngineCostTable.default, -1, 0, Map.empty, 1)
        assert(narrow.terms(expand, Engine.Spark) == Seq(Term(ExpandNoCodegen, w, 3)))
        same(narrow.operatorCost(expand, Engine.Spark), 3 * 21 * w.leaves)

        val exploded = run("SELECT k, explode(array(v, v + 1)) AS e FROM t")
        val generate = nodes(exploded)
          .find(_.nodeName.contains("Explode"))
          .orElse(nodes(exploded).find(_.nodeName.contains("Generate")))
          .get
        assert(model.terms(generate, Engine.Comet) == Seq(Term(Generate, Width(2, 0))))
        same(model.operatorPrice(generate, Engine.Comet), 2.5 * 2)
        same(model.operatorPrice(generate, Engine.Spark), 0)
        same(narrow.operatorPrice(generate, Engine.Spark), 18 * 2)
      }
    }
  }

  test("a shuffle is priced over every leaf, its partitions and its bytes, by format") {
    withWide("t8", 7) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val plan = runUnordered(spark.table("t8").repartition(col("k")))
        val shuffle = nodes(plan).collectFirst { case s: CometShuffleExchangeExec => s }.get
        val p = shuffle.outputPartitioning.numPartitions
        val t = EngineCostTable.default
        val w = Width(8, 0)
        assert(model.shuffleWidth(shuffle) == w)
        assert(model.excessBytes(shuffle.child.output) == 0)
        val write = 48.95 * 8 + 0.037 * 64
        val read = (14.73 * 8 + 0.032 * 64) * t.shuffleReadPartitionFactor(8, p)
        same(
          model.shuffleCost(shuffle, BoundaryFormats.NativeShuffle),
          write * t.shuffleWritePartitionFactor(8, p) + read)
        same(
          model.shuffleCost(shuffle, BoundaryFormats.ColumnarShuffle),
          write * t.columnarShuffleWritePartitionFactor(8, p) + 400 + read)
        same(
          model.shuffleCost(shuffle, BoundaryFormats.SparkShuffle),
          69 + 67.21 * 8 + 67 + 26.34 * 8)

        val c2r = t.comet(C2R, w)
        val r2c = t.comet(R2C, w)
        val input =
          BoundaryFormats.Input(shuffle, Some(Engine.Spark), Engine.Comet, shuffle.child)
        same(
          model.price(input, BoundaryFormats.ColumnarShuffle, 3),
          r2c + 2 * c2r + model.shuffleCost(shuffle, BoundaryFormats.ColumnarShuffle))
        same(
          model.price(input, BoundaryFormats.NativeShuffle, 1),
          c2r + model.shuffleCost(shuffle, BoundaryFormats.NativeShuffle))
        same(
          model.price(input, BoundaryFormats.SparkShuffle, 1),
          c2r + model.shuffleCost(shuffle, BoundaryFormats.SparkShuffle))
      }
    }
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val plan = runUnordered(spark.table("t").repartition(col("k")))
        val shuffle = nodes(plan).collectFirst { case s: CometShuffleExchangeExec => s }.get
        val bytes = EstimationUtils.getSizePerRow(shuffle.child.output).toDouble
        assert(bytes == 8 + 4 + 4 + 20)
        assert(model.excessBytes(shuffle.child.output) == 8)
        val noBytes = new EngineCostModel(
          EngineCostTable.parse(
            "shuffleReadPerByte.comet=0;shuffleWritePerByte.comet=0;" +
              "shuffleReadPerByte.spark=0;shuffleWritePerByte.spark=0"),
          -1,
          0,
          Map.empty)
        same(
          model.shuffleCost(shuffle, BoundaryFormats.NativeShuffle),
          noBytes.shuffleCost(shuffle, BoundaryFormats.NativeShuffle) + 1.1 * 8)
        same(
          model.shuffleCost(shuffle, BoundaryFormats.SparkShuffle),
          noBytes.shuffleCost(shuffle, BoundaryFormats.SparkShuffle) + 4.05 * 8)
      }
    }
  }

  test("reverted operators are tagged to stay in Spark") {
    withTables {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        flag -> "true",
        expensiveNativeSort) {
        val plan = finalPlan(run("SELECT k, max(s) FROM t GROUP BY k"))
        val sorts = nodes(plan).collect { case s: SortExec => s }
        assert(
          sorts.nonEmpty && sorts.forall(
            _.getTagValue(CometExecRule.ENGINE_CHOICE_SPARK_TAG).isDefined),
          s"plan:\n$plan")
      }
    }
  }

  test("a whole plan converts again the operators an earlier choice reverted") {
    withTables {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        def tagged(tag: org.apache.spark.sql.catalyst.trees.TreeNodeTag[Unit]): SparkPlan = {
          val plan = sql("SELECT * FROM t SORT BY k").queryExecution.sparkPlan
          plan.collect { case s: SortExec => s }.foreach(_.setTagValue(tag, ()))
          CometScanRule(spark).apply(plan)
        }
        def nativeSorts(plan: SparkPlan): Int = plan.collect { case s: CometSortExec => s }.size
        val choice = tagged(CometExecRule.ENGINE_CHOICE_SPARK_TAG)
        assert(nativeSorts(CometExecRule(spark).apply(choice)) == 0)
        assert(nativeSorts(CometExecRule(spark, wholePlan = true).apply(choice)) == 1)
        val kept = tagged(CometExecRule.KEEP_ON_SPARK_TAG)
        assert(nativeSorts(CometExecRule(spark).apply(kept)) == 0)
        assert(nativeSorts(CometExecRule(spark, wholePlan = true).apply(kept)) == 0)
      }
    }
  }

  test("the choice is made again when AQE turns a sort-merge join into a broadcast hash join") {
    withTempPath { dir =>
      spark
        .range(400000)
        .selectExpr("cast(id % 100000 AS int) AS k", "id AS v")
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("big")
      withTempView("big") {
        withWide("w100", 99) {
          val query =
            "SELECT a.k, a.c, w.* FROM (SELECT k, max(v) AS c FROM big GROUP BY k) a " +
              "JOIN (SELECT * FROM w100 WHERE c1 < 5) w ON a.k = w.k"
          def plan(adaptiveBroadcast: String): SparkPlan = {
            var result: SparkPlan = null
            withSQLConf(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
              flag -> "true",
              costTable -> "agg.flat.comet=80,0,0;sort.flat.comet=100000,0,0",
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.NON_EMPTY_PARTITION_RATIO_FOR_BROADCAST_JOIN.key -> "0",
              SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> adaptiveBroadcast) {
              result = runUnordered(sql(query))
            }
            result
          }
          val merged = plan("-1")
          assert(count(merged) { case j: SortMergeJoinExec => j } == 1, s"plan:\n$merged")
          val sparkAggs = nodes(merged).collect { case a: HashAggregateExec => a }
          assert(sparkAggs.size == 1, s"plan:\n$merged")
          assert(
            sparkAggs.head.getTagValue(CometExecRule.ENGINE_CHOICE_SPARK_TAG).isDefined,
            s"plan:\n$merged")

          val broadcast = plan("10MB")
          assert(count(broadcast) { case j: SortMergeJoinExec => j } == 0, s"plan:\n$broadcast")
          assert(
            count(broadcast) {
              case j: BroadcastHashJoinExec => j
              case j: CometBroadcastHashJoinExec => j
            } == 1,
            s"plan:\n$broadcast")
          assert(count(broadcast) { case a: HashAggregateExec => a } == 0, s"plan:\n$broadcast")
          assert(
            count(broadcast) { case a: CometHashAggregateExec => a } == 2,
            s"plan:\n$broadcast")
        }
      }
    }
  }
}
