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
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, IntegerType, LongType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.rules.BoundaryTestHelpers._

class ChooseBoundaryFormatsSuite extends CometTestBase {

  private val flag = CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.key

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.key, "false")

  private def withTables(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(3000)
        .selectExpr(
          "cast(id % 211 AS int) AS k",
          "cast((id * 7919) % 1000 AS int) AS v",
          "concat('payload_', cast(id % 257 AS string)) AS s",
          "id % 211 AS k_long",
          "concat('key_', cast(id % 211 AS string)) AS k_string",
          "date_add(date'2020-01-01', cast(id % 211 AS int)) AS k_date",
          "cast(id % 211 AS decimal(10, 2)) AS k_dec10",
          "cast((id % 211) * 1000003 + 0.5 AS decimal(38, 10)) AS k_dec38")
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t")
      withTempView("t")(f)
    }
  }

  private def run(df: => DataFrame): SparkPlan = checkSparkAnswer(df)._2

  private def run(query: String): SparkPlan = run(sql(query))

  /** Columnar shuffles with Spark operators on both sides. */
  private def columnarBetweenSpark(plan: SparkPlan): Seq[Edge] =
    edges(plan).filter(e => e.format == "columnar" && !e.consumerIsComet && !e.producerIsComet)

  private def withAqe(aqe: String, confs: (String, String)*)(f: => Unit): Unit =
    withSQLConf((SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) +: confs: _*)(f)

  /** Runs `f` with the rule off and on, returning both plans. */
  private def offAndOn(confs: (String, String)*)(f: => SparkPlan): (SparkPlan, SparkPlan) = {
    var off: SparkPlan = null
    var on: SparkPlan = null
    withSQLConf((flag -> "false") +: confs: _*) { off = f }
    withSQLConf((flag -> "true") +: confs: _*) { on = f }
    (off, on)
  }

  private def assertSparkShuffleBetweenSparkOperators(
      off: SparkPlan,
      on: SparkPlan,
      expectedSparkShuffles: Int = 1): Unit = {
    assert(
      columnarBetweenSpark(off).nonEmpty,
      s"expected a columnar shuffle without the rule:\n$off")
    assert(columnarBetweenSpark(on).isEmpty, s"columnar shuffle between Spark operators:\n$on")
    assert(
      edges(on).count(_.format == "spark") >= expectedSparkShuffles,
      s"expected a Spark shuffle:\n$on")
    assert(
      cometOperatorNames(off) == cometOperatorNames(on),
      s"native operators changed:\n$off\n$on")
  }

  for (aqe <- Seq("false", "true")) {
    test(s"Spark hash aggregates on both sides get a Spark shuffle (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false",
          CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false") {
          val (off, on) = offAndOn()(run("SELECT k, sum(v) FROM t GROUP BY k"))
          assertSparkShuffleBetweenSparkOperators(off, on)
        }
      }
    }

    test(s"Spark sort aggregates on both sides get a Spark shuffle (AQE=$aqe)") {
      withTables {
        withAqe(aqe, CometConf.COMET_EXEC_SORT_ENABLED.key -> "false") {
          val (off, on) = offAndOn()(run("SELECT k, max(s) FROM t GROUP BY k"))
          assertSparkShuffleBetweenSparkOperators(off, on)
        }
      }
    }

    test(s"Spark sort-merge join inputs get Spark shuffles (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          CometConf.COMET_EXEC_SORT_ENABLED.key -> "false",
          CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
          val (off, on) = offAndOn()(
            run("SELECT a.k, a.v2, b.w FROM (SELECT k, v + 1 AS v2 FROM t) a " +
              "JOIN (SELECT k, v * 2 AS w FROM t) b ON a.k = b.k"))
          assertSparkShuffleBetweenSparkOperators(off, on, expectedSparkShuffles = 2)
        }
      }
    }

    test(s"Spark window gets a Spark shuffle (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          CometConf.COMET_EXEC_SORT_ENABLED.key -> "false",
          CometConf.COMET_EXEC_WINDOW_ENABLED.key -> "false",
          CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
          val (off, on) = offAndOn()(
            run(
              spark
                .table("t")
                .select(col("k"), (col("v") + 1).as("v"))
                .withColumn("rn", row_number().over(Window.partitionBy("k").orderBy("v")))))
          assertSparkShuffleBetweenSparkOperators(off, on)
        }
      }
    }

    test(s"a Spark write gets a Spark shuffle (AQE=$aqe)") {
      withTables {
        withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") {
          val (off, on) = offAndOn() {
            var plan: SparkPlan = null
            withTable("target") {
              sql("CREATE TABLE target (k INT, v INT) USING parquet")
              val df = sql("INSERT INTO target SELECT /*+ REPARTITION(k) */ k, v + 1 FROM t")
              checkAnswer(spark.table("target"), sql("SELECT k, v + 1 FROM t"))
              plan = df.queryExecution.executedPlan match {
                case c: CommandResultExec => c.commandPhysicalPlan
                case other => other
              }
            }
            plan
          }
          assertSparkShuffleBetweenSparkOperators(off, on)
        }
      }
    }

    test(s"a Spark producer keeps its columnar shuffle into a Comet consumer (AQE=$aqe)") {
      withTables {
        withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false", flag -> "true") {
          val plan = run(
            spark
              .table("t")
              .select(col("k"), (col("v") + 1).as("v"))
              .repartition(col("k"))
              .groupBy("k")
              .agg(sum("v")))
          val shuffles = edges(plan)
          assert(shuffles.map(_.format) == Seq("columnar"), s"plan:\n$plan")
          assert(shuffles.head.consumerIsComet, s"expected a native aggregate reading it:\n$plan")
        }
      }
    }

    test(s"a Comet producer keeps its native shuffle into a Spark consumer (AQE=$aqe)") {
      withTables {
        withAqe(aqe, CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false", flag -> "true") {
          val plan =
            run(
              spark
                .table("t")
                .select("k", "v")
                .repartition(col("k"))
                .select(col("k"), (col("v") + 1).as("w")))
          val shuffles = edges(plan)
          assert(shuffles.map(_.format) == Seq("native"), s"plan:\n$plan")
          assert(!shuffles.head.consumerIsComet, s"expected a Spark project reading it:\n$plan")
        }
      }
    }
  }

  private val keyColumns: Seq[(String, DataType)] = Seq(
    "k" -> IntegerType,
    "k_long" -> LongType,
    "k_string" -> StringType,
    "k_date" -> DateType,
    "k_dec10" -> DecimalType(10, 2),
    "k_dec38" -> DecimalType(38, 10))

  /**
   * One join input written by a native shuffle (scan and filter only) and one by Comet's columnar
   * shuffle (a Spark project in between).
   */
  private def mixedOriginJoin(key: String): DataFrame = {
    val left = spark.table("t").select(col(key), col("v"))
    val right = spark.table("t").select(col(key), (col("v") + 1).as("w"))
    left.join(right, key)
  }

  for (aqe <- Seq("false", "true"); sparkJoin <- Seq(false, true); (key, keyType) <- keyColumns) {
    val consumer = if (sparkJoin) "Spark" else "Comet"
    test(
      s"$consumer join of mixed-origin inputs, key $keyType, never mixes hash functions " +
        s"unless they agree (AQE=$aqe)") {
      withTables {
        val sparkJoinConfs =
          if (sparkJoin) {
            Seq(
              CometConf.COMET_EXEC_SORT_ENABLED.key -> "false",
              CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "false")
          } else {
            Nil
          }
        withAqe(
          aqe,
          Seq(
            flag -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.SHUFFLE_PARTITIONS.key -> "7",
            SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
            CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false") ++ sparkJoinConfs: _*) {
          val plan = run(mixedOriginJoin(key))
          val joins = joinInputs(plan)
          assert(joins.size == 1, s"plan:\n$plan")
          val inputs = joins.head
          assert(inputs.size == 2, s"plan:\n$plan")
          if (!BoundaryFormats.hashesAlike(keyType)) {
            assert(inputs.map(_.hash).distinct.size == 1, s"mixed hash functions:\n$plan")
          }
          if (sparkJoin) {
            assert(inputs.forall(_.format != "columnar"), s"plan:\n$plan")
          } else {
            assert(inputs.forall(_.format != "spark"), s"plan:\n$plan")
          }
        }
      }
    }
  }

  for (aqe <- Seq("false", "true")) {
    test(s"identical shuffles stay reused (AQE=$aqe)") {
      withTables {
        withAqe(
          aqe,
          CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false",
          CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false") {
          val query = "WITH x AS (SELECT k, sum(v) AS total FROM t GROUP BY k) " +
            "SELECT * FROM x UNION ALL SELECT * FROM x"
          val (off, on) = offAndOn()(run(query))
          def reused(plan: SparkPlan): Int = collectWithSubqueries(finalPlan(plan)) {
            case r: ReusedExchangeExec => r
            case s: QueryStageExec if s.plan.isInstanceOf[ReusedExchangeExec] => s
          }.size
          assert(reused(off) > 0, s"expected reuse without the rule:\n$off")
          assert(reused(on) == reused(off), s"reuse lost:\n$on")
          assert(columnarBetweenSpark(on).isEmpty, s"plan:\n$on")
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
        withAqe(
          aqe,
          flag -> "true",
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
          CometConf.COMET_EXEC_SORT_ENABLED.key -> "false") {
          spark.read.parquet(factPath).createOrReplaceTempView("fact")
          spark.read.parquet(dimPath).createOrReplaceTempView("dim")
          withTempView("fact", "dim") {
            run(
              "SELECT f.p, max(f.s), count(*) FROM fact f JOIN dim d ON f.p = d.k " +
                "WHERE d.name IN ('n3', 'n5') GROUP BY f.p")
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
              run(
                "SELECT f.p, max(f.s), count(*) FROM fact f JOIN dim d ON f.p = d.k " +
                  "WHERE d.name IN ('n3', 'n5') GROUP BY f.p")
            }
          }
        }
      }
    }
  }

  test("the rule leaves the plan unchanged when disabled") {
    withTables {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false",
        flag -> "false") {
        val converted = stripAqe(run("SELECT k, sum(v) FROM t GROUP BY k"))
        assert(ChooseBoundaryFormats(spark).apply(converted) eq converted)
        val (off, _) = offAndOn()(run("SELECT k, sum(v) FROM t GROUP BY k"))
        assert(columnarBetweenSpark(off).nonEmpty, s"plan:\n$off")
      }
    }
  }

  test("a Comet operator never reads a Spark shuffle directly") {
    // Comet cannot convert an operator over a Spark shuffle read unless
    // spark.comet.sparkToColumnar.supportedOperatorList names the shuffle stage, so the reverse
    // boundary (Comet producer, Spark shuffle, Comet consumer) does not arise by default.
    withTables {
      for (aqe <- Seq("false", "true")) {
        withAqe(aqe, flag -> "true", CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
          val plan = run("SELECT k, sum(v), max(s) FROM t GROUP BY k")
          val sparkShuffles = edges(plan).filter(_.format == "spark")
          assert(sparkShuffles.nonEmpty, s"plan:\n$plan")
          assert(sparkShuffles.forall(!_.consumerIsComet), s"plan:\n$plan")
        }
      }
    }
  }

  private def stripAqe(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case other => other
  }

  test("reverted shuffles are Spark shuffles tagged to stay Spark") {
    withTables {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false",
        flag -> "true") {
        val plan = stripAqe(run("SELECT k, sum(v) FROM t GROUP BY k"))
        val shuffles = plan.collect { case s: ShuffleExchangeExec => s }
        assert(shuffles.nonEmpty, s"plan:\n$plan")
        assert(shuffles.forall(_.getTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG).isDefined))
        assert(
          plan.collect { case n: CometNativeExec => n }.nonEmpty,
          s"scan stays native:\n$plan")
      }
    }
  }
}
