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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.comet.{CometNativeExec, CometSortExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

class WideRowShuffleFallbackSuite extends CometTestBase {

  private val minLeaves = CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.key

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(minLeaves, "50")
      .set(CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.key, "false")

  test("primitive, string and binary types are one leaf each") {
    Seq(
      BooleanType,
      ByteType,
      IntegerType,
      LongType,
      DoubleType,
      DecimalType(38, 10),
      DateType,
      TimestampType,
      StringType,
      BinaryType).foreach(t => assert(LeafColumns.count(t) == 1, t))
  }

  test("nested types count the leaves of their fields, elements, keys and values") {
    val point = StructType(Seq(StructField("x", DoubleType), StructField("y", DoubleType)))
    val nested = StructType(
      Seq(
        StructField("id", LongType),
        StructField("p", point),
        StructField("tags", ArrayType(StringType))))
    assert(LeafColumns.count(point) == 2)
    assert(LeafColumns.count(nested) == 4)
    assert(LeafColumns.count(ArrayType(IntegerType)) == 1)
    assert(LeafColumns.count(ArrayType(ArrayType(point))) == 2)
    assert(LeafColumns.count(ArrayType(nested)) == 4)
    assert(LeafColumns.count(MapType(StringType, LongType)) == 2)
    assert(LeafColumns.count(MapType(StringType, nested)) == 5)
    assert(LeafColumns.count(MapType(point, ArrayType(point))) == 4)
    assert(LeafColumns.count(StructType(Nil)) == 0)
    assert(
      LeafColumns.count(
        Seq(
          AttributeReference("a", IntegerType)(),
          AttributeReference("b", nested)(),
          AttributeReference("c", MapType(StringType, point))())) == 8)
  }

  test("the rule is disabled by default") {
    assert(CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.defaultValue.contains(0))
  }

  test("at a threshold of 50 a shuffle moves to Spark at 50 payload leaves, not at 49") {
    Seq(49 -> true, 50 -> false).foreach { case (leaves, comet) =>
      withTempPath { dir =>
        spark
          .range(1000)
          .selectExpr("cast(id % 37 AS int) AS k" +: (1 to leaves).map(i =>
            s"cast(id + $i AS int) AS c$i"): _*)
          .write
          .parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("n")
        withTempView("n") {
          bothAqeModes {
            val plan = run(spark.table("n").repartition(7, col("k")))
            assert(cometShuffles(plan).nonEmpty == comet, s"$leaves leaves:\n$plan")
            assert(sparkShuffles(plan).isEmpty == comet, s"$leaves leaves:\n$plan")
          }
        }
      }
    }
  }

  private def withTable(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(3000)
        .selectExpr(
          "cast(id % 37 AS int) AS k",
          "id AS v",
          "cast(id * 7 AS int) AS a",
          "concat('s', cast(id AS string)) AS s",
          "named_struct('x', cast(id AS int), 'y', named_struct('z', cast(id % 11 AS string), " +
            "'w', id / 3.0)) AS st",
          "array(named_struct('p', cast(id AS int), 'q', 'q'), " +
            "named_struct('p', cast(id + 1 AS int), 'q', cast(id AS string))) AS arr",
          "map(cast(id AS string), array(cast(id AS int), 1)) AS m")
        .write
        .parquet(dir.getCanonicalPath)
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("w")
      withTempView("w")(f)
    }
  }

  private val payloadLeaves = 10

  private def nodes(plan: SparkPlan): Seq[SparkPlan] = {
    def visit(node: SparkPlan): Seq[SparkPlan] = node match {
      case a: AdaptiveSparkPlanExec => visit(a.executedPlan)
      case s: QueryStageExec => s +: visit(s.plan)
      case other => other +: other.children.flatMap(visit)
    }
    visit(plan)
  }

  private def sparkShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    nodes(plan).collect { case s: ShuffleExchangeExec => s }

  private def cometShuffles(plan: SparkPlan): Seq[CometShuffleExchangeExec] =
    nodes(plan).collect { case s: CometShuffleExchangeExec => s }

  private def run(df: => DataFrame): SparkPlan = checkSparkAnswer(df)._2

  private def bothAqeModes(f: => Unit): Unit =
    Seq("false", "true").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe)(f)
    }

  test("a shuffle with at least the threshold of payload leaves stays a Spark shuffle") {
    withTable {
      bothAqeModes {
        withSQLConf(minLeaves -> payloadLeaves.toString) {
          val plan = run(spark.table("w").repartition(7, col("k")))
          assert(cometShuffles(plan).isEmpty, s"plan:\n$plan")
          assert(sparkShuffles(plan).size == 1, s"plan:\n$plan")
          val reasons = sparkShuffles(plan).head
            .getTagValue(org.apache.comet.CometExplainInfo.FALLBACK_REASONS)
            .getOrElse(Set.empty)
          assert(reasons.exists(_.contains(s"$payloadLeaves leaf columns")), reasons)
        }
        withSQLConf(minLeaves -> (payloadLeaves + 1).toString) {
          val plan = run(spark.table("w").repartition(7, col("k")))
          assert(
            cometShuffles(plan).map(_.shuffleType) == Seq(CometNativeShuffle),
            s"plan:\n$plan")
        }
        withSQLConf(minLeaves -> "0") {
          val plan = run(spark.table("w").repartition(7, col("k")))
          assert(cometShuffles(plan).size == 1, s"plan:\n$plan")
        }
      }
    }
  }

  test("leaves of the hash partitioning key are not counted") {
    withTable {
      bothAqeModes {
        val keyed = () => spark.table("w").repartition(5, col("k"), col("st"))
        withSQLConf(minLeaves -> (payloadLeaves - 2).toString) {
          assert(cometShuffles(run(keyed())).size == 1)
        }
        withSQLConf(minLeaves -> (payloadLeaves - 3).toString) {
          assert(cometShuffles(run(keyed())).isEmpty)
        }
      }
    }
  }

  test("leaves of the range partitioning key are not counted") {
    withTable {
      withSQLConf(minLeaves -> payloadLeaves.toString) {
        val byK = run(spark.table("w").orderBy(col("k")))
        assert(cometShuffles(byK).isEmpty && sparkShuffles(byK).nonEmpty, s"plan:\n$byK")
        val byKey = run(spark.table("w").orderBy(col("a"), col("s"), col("v"), col("k")))
        assert(cometShuffles(byKey).nonEmpty, s"plan:\n$byKey")
      }
    }
  }

  test("the columnar shuffle stays in Spark too") {
    withTable {
      bothAqeModes {
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
          withSQLConf(minLeaves -> (payloadLeaves + 1).toString) {
            val plan = run(spark.table("w").repartition(7, col("k")))
            assert(
              cometShuffles(plan).map(_.shuffleType) == Seq(CometColumnarShuffle),
              s"plan:\n$plan")
          }
          withSQLConf(minLeaves -> payloadLeaves.toString) {
            val plan = run(spark.table("w").repartition(7, col("k")))
            assert(cometShuffles(plan).isEmpty, s"plan:\n$plan")
          }
        }
      }
    }
  }

  test("the reader of a Spark shuffle runs in Spark and the native producer converts once") {
    withTable {
      bothAqeModes {
        withSQLConf(
          minLeaves -> payloadLeaves.toString,
          CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.key -> "false") {
          val plan = run(
            spark
              .table("w")
              .where(col("a") > 10)
              .repartition(7, col("k"))
              .sortWithinPartitions(col("k"), col("v")))
          assert(cometShuffles(plan).isEmpty, s"plan:\n$plan")
          assert(nodes(plan).exists(_.isInstanceOf[SortExec]), s"plan:\n$plan")
          assert(!nodes(plan).exists(_.isInstanceOf[CometSortExec]), s"plan:\n$plan")
          val shuffle = sparkShuffles(plan).head
          val toRows = shuffle.child.collect { case c: ColumnarToRowTransition => c }
          assert(toRows.size == 1, s"plan:\n$plan")
          assert(toRows.head.exists(_.isInstanceOf[CometNativeExec]), s"plan:\n$plan")
        }
      }
    }
  }

  test("boundary formats keep a wide shuffle in Spark") {
    withTable {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        CometConf.COMET_EXEC_BOUNDARY_FORMATS_ENABLED.key -> "true",
        minLeaves -> payloadLeaves.toString) {
        val plan = run(
          spark
            .table("w")
            .join(spark.table("w").select(col("k"), col("v").as("v2")), "k"))
        val comet = cometShuffles(plan)
        val wide = sparkShuffles(plan).filter(_.child.output.size > 3)
        assert(wide.nonEmpty, s"plan:\n$plan")
        assert(comet.forall(_.child.output.size <= 3), s"plan:\n$plan")
      }
    }
  }
}
