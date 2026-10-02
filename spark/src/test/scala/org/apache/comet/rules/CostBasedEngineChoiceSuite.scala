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
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.rules.BoundaryTestHelpers._
import org.apache.comet.rules.EngineCostTable.{CostClass, Form, Line, Width}

class CostBasedEngineChoiceSuite extends CometTestBase {

  private val flag = CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.key
  private val costTable = CometConf.COMET_EXEC_COST_BASED_ENGINES_COST_TABLE.key

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
    test(s"native sorts read by Spark sort aggregates run in Spark (AQE=$aqe)") {
      withTables {
        withAqe(aqe) {
          val (off, on) = offAndOn(run("SELECT k, max(s) FROM t GROUP BY k"))
          assert(count(off) { case s: SortAggregateExec => s } == 2, s"plan:\n$off")
          assert(count(off) { case s: CometSortExec => s } == 2, s"plan:\n$off")
          assert(count(on) { case s: SortAggregateExec => s } == 2, s"plan:\n$on")
          assert(count(on) { case s: CometSortExec => s } == 0, s"plan:\n$on")
          assert(count(on) { case s: SortExec => s } == 2, s"plan:\n$on")
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
          assert(count(on) { case s: CometSortExec => s } == 0, s"plan:\n$on")
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

    test(s"a Spark sort-merge join moves the native sort of its input by risk (AQE=$aqe)") {
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
          assert(!joinHasNativeSort(on), s"plan:\n$on")
          assert(edges(on).exists(_.format == "native"), s"plan:\n$on")
          withSQLConf(flag -> "true", costTable -> "oomRiskPenalty=0") {
            val plan = run(query)
            assert(joinHasNativeSort(plan), s"the native input keeps its native sort:\n$plan")
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
        val model = EngineCostModel(spark.sessionState.conf)
        assert(model.costClass(join.get).isEmpty)
        assert(model.operatorCost(join.get, BoundaryFormats.Engine.Comet) == -5)
        assert(model.operatorCost(join.get, BoundaryFormats.Engine.Spark) == 0)
        val sort = runUnordered(sql("SELECT * FROM t SORT BY k"))
        val native = nodes(sort).collectFirst { case s: CometSortExec => s }.get
        assert(model.costClass(native).contains(EngineCostTable.CostClass.Sort))
        assert(model.operatorCost(native, BoundaryFormats.Engine.Comet) != -7)
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
        withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false", flag -> "true") {
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
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
            val plan = run("SELECT n.c1 AS n1, w.* FROM narrow n JOIN wide w ON n.k = w.k")
            assert(count(plan) { case j: SortMergeJoinExec => j } == 1, s"plan:\n$plan")
            assert(count(plan) { case j: CometSortMergeJoinExec => j } == 0, s"plan:\n$plan")
            assert(count(plan) { case s: CometSortExec => s } == 0, s"plan:\n$plan")
            val (wideInputs, narrowInputs) = edges(plan).partition(_.exchange.output.size > 100)
            assert(wideInputs.map(_.format) == Seq("spark"), s"plan:\n$plan")
            assert(narrowInputs.map(_.format) == Seq("native"), s"plan:\n$plan")
          }
        }
      }
    }

    test(s"the memory risk moves a native sort read by a Spark window (AQE=$aqe)") {
      withWide("t8", 7) {
        withAqe(aqe, flag -> "true", CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false") {
          val query = "SELECT k, c1, row_number() OVER (PARTITION BY k ORDER BY c1) AS r FROM t8"
          val moved = run(query)
          assert(count(moved) { case s: CometSortExec => s } == 0, s"plan:\n$moved")
          assert(count(moved) { case s: SortExec => s } == 1, s"plan:\n$moved")
          withSQLConf(costTable -> "oomRiskPenalty=0") {
            val mixed = run(query)
            assert(count(mixed) { case s: CometSortExec => s } == 1, s"plan:\n$mixed")
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
        withSQLConf(costTable -> "sort.flat.comet=0,1") {
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
        CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.key -> "true") {
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
      " sort.flat.comet=1,2; shuffleRead.nested.spark = 3,4 ;oomRiskPenalty=5;" +
        "shuffleWritePartitionSlope=0.5;shuffleWritePartitionBase=100;c2r.nested.comet=7,0")
    assert(table.line(CostClass.Sort, Form.Flat) == Line(1, 2, 34, 28.03))
    assert(table.line(CostClass.ShuffleRead, Form.Nested) == Line(15.8, 0.071, 3, 4))
    assert(table.line(CostClass.C2R, Form.Nested).cometK0 == 7)
    assert(table.oomRiskPenalty == 5)
    assert(table.shuffleWritePartitionFactor(300) == 2)
    assert(
      table.line(CostClass.RowLocal, Form.Flat) == EngineCostTable.default
        .line(CostClass.RowLocal, Form.Flat))
    assert(EngineCostTable.parse("") == EngineCostTable.default)

    for (bad <- Seq(
        "sort.flat.comet=1",
        "sort.flat.comet=1,x",
        "sort.flat.comet=1,2,3",
        "sorts.flat.comet=1,2",
        "sort.deep.comet=1,2",
        "sort.flat.velox=1,2",
        "c2r.flat.spark=1,2",
        "oomRiskPenalty=NaN",
        "shuffleWritePartitionBase=0",
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
    def same(actual: Double, expected: Double): Unit =
      assert(math.abs(actual - expected) < 1e-9, s"$actual != $expected")
    val flat10 = Width(10, Form.Flat)
    same(t.comet(CostClass.ShuffleWrite, flat10), 50.3 * 10 + 0.221 * 100)
    same(t.spark(CostClass.ShuffleWrite, flat10), 303 + 67.07 * 10)
    same(t.comet(CostClass.ShuffleRead, Width(4, Form.Nested)), 15.8 * 4 + 0.071 * 16)
    same(t.spark(CostClass.ShuffleRead, Width(4, Form.Nested)), 170 + 31.87 * 4)
    same(t.spark(CostClass.Sort, Width(200, Form.Nested)), 400)
    same(t.comet(CostClass.Sort, Width(200, Form.Flat)), 0.028 * 40000)
    same(t.comet(CostClass.RowLocal, Width(3, Form.Flat)), 6.9)
    same(t.spark(CostClass.RowLocal, Width(3, Form.Nested)), 4 + 1.66 * 3)
    same(t.comet(CostClass.C2R, flat10), 100)
    same(t.comet(CostClass.C2R, Width(10, Form.Nested)), 200)
    same(t.shuffleWritePartitionFactor(100), 1)
    same(t.shuffleWritePartitionFactor(250), 1)
    same(t.shuffleWritePartitionFactor(1000), 1.24)
  }

  test("the form of a schema is nested when at least half of its leaves are nested") {
    def attr(name: String, dataType: DataType): Attribute = AttributeReference(name, dataType)()
    val ints = (1 to 3).map(i => attr(s"i$i", IntegerType))
    val struct3 = attr("s", StructType(Seq("a", "b", "c").map(StructField(_, IntegerType))))
    assert(EngineCostTable.widthOf(ints) == Width(3, Form.Flat))
    assert(EngineCostTable.widthOf(ints :+ struct3) == Width(6, Form.Nested))
    assert(
      EngineCostTable.widthOf(ints :+ attr("x", IntegerType) :+ struct3) == Width(7, Form.Flat))
    assert(
      EngineCostTable.widthOf(
        Seq(attr("m", MapType(IntegerType, ArrayType(StringType))))) == Width(2, Form.Nested))
    assert(EngineCostTable.widthOf(Nil) == Width(0, Form.Flat))
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
