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

package org.apache.comet.exec

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.comet.CometWindowExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.window.{WindowExec => SparkWindowExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus

/**
 * Independent Spark oracles for the custom window executors. The inputs, logical SQL and ANSI
 * setting are identical on both sides. Native planner tests pin the selected executor class; the
 * BEGIN/END records also let a qualification log check the actual JNI native plan for each
 * collected query, rather than infer its implementation from the CometWindowExec wrapper.
 */
class CometWindowSemanticsSuite extends CometTestBase {

  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED.key, "true")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    .set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
    .set(CometConf.COMET_ONHEAP_ENABLED.key, "false")
    .set("spark.memory.offHeap.size", "16m")
    .set(CometConf.COMET_EXEC_WINDOW_ENABLED.key, "true")
    .set(CometConf.COMET_EXEC_WINDOW_SORTED_ENABLED.key, "true")
    .set(CometConf.COMET_EXEC_WINDOW_PARTITION_AGGREGATE_ENABLED.key, "true")
    .set(CometConf.COMET_EXEC_WINDOW_GROUP_LIMIT_ENABLED.key, "false")
    .set("spark.comet.operator.WindowExec.allowIncompatible", "true")
    .set("spark.comet.exec.localTableScan.enabled", "true")
    .set("spark.comet.explain.native.enabled", "true")
    .set(CometConf.COMET_BATCH_SIZE.key, "3")

  // Spark's normal answer helper allows floating tolerance. These fixtures instead compare
  // every bit (including signed zero), nested element, row position and duplicate occurrence.
  // Only NaN payloads are canonicalized, matching Spark's SQL NaN equality.
  private def exact(value: Any): Any = value match {
    case row: Row => row.toSeq.map(exact)
    case values: Seq[_] => values.map(exact)
    case bytes: Array[Byte] => bytes.toSeq
    case value: Double => java.lang.Double.doubleToLongBits(value)
    case value: Float => java.lang.Float.floatToIntBits(value)
    case value: java.math.BigDecimal => (value.unscaledValue(), value.scale())
    case other => other
  }

  private def checkExact(
      name: String,
      query: String,
      requiredNative: String = "",
      forbiddenNative: String = ""): DataFrame = {
    val (expected, expectedSchema) = withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = sql(query)
      (df.collect().toSeq.map(exact), df.schema)
    }
    val actual = sql(query)
    // These markers deliberately bracket this exact action. A collect on a copy would leave
    // the returned plan's metrics unset and would not prove the checked plan actually ran.
    System.err.println(s"G06_WINDOW_BEGIN\t$name\t$requiredNative\t$forbiddenNative")
    val rows =
      try actual.collect().toSeq
      finally System.err.println(s"G06_WINDOW_END\t$name")
    assert(actual.schema == expectedSchema, s"$name schema: ${actual.schema} != $expectedSchema")
    assert(rows.map(exact) == expected, s"$name\n$rows\n!=\n$expected")
    actual
  }

  private def assertNativeWindow(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    val windows = collect(plan) { case w: CometWindowExec => w }
    assert(windows.nonEmpty, plan)
    assert(collect(plan) { case w: SparkWindowExec => w }.isEmpty, plan)
    assert(windows.forall(_.metrics("output_rows").value > 0L), plan)
  }

  private def assertSparkWindow(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(collect(plan) { case w: SparkWindowExec => w }.nonEmpty, plan)
    assert(collect(plan) { case w: CometWindowExec => w }.isEmpty, plan)
  }

  private def checkMandatoryError(query: String, conditionSuffix: String): Unit = {
    def error(enabled: Boolean): SparkThrowable with Throwable = {
      val failure = withSQLConf(CometConf.COMET_ENABLED.key -> enabled.toString) {
        intercept[Exception] { sql(query).collect() }
      }
      causeChain(failure)
        .collect { case e: SparkThrowable with Throwable => e }
        .lastOption
        .getOrElse(fail(s"No Spark error condition: $failure"))
    }
    val expected = error(enabled = false)
    val actual = error(enabled = true)
    assert(expected.getErrorClass.endsWith(conditionSuffix), expected)
    assert(actual.getClass == expected.getClass, s"$actual != $expected")
    assert(actual.getErrorClass == expected.getErrorClass, s"$actual != $expected")
    assert(actual.getSqlState == expected.getSqlState, s"$actual != $expected")
  }

  private def withSmallInput(f: => Unit): Unit = {
    withTempView("g06_small") {
      Seq(
        (1, Option.empty[Int], Option.empty[Int], Option.empty[Int]),
        (2, Option.empty[Int], Some(1), Some(7)),
        (3, Some(1), None, None),
        (4, Some(2), Some(1), None),
        (5, Some(2), Some(1), Some(3)),
        (6, Some(2), Some(2), None),
        (7, Some(2), Some(3), Some(-1)),
        (8, Some(3), None, None),
        (9, Some(3), Some(2), None))
        .toDF("id", "p", "o", "v")
        .createOrReplaceTempView("g06_small")
      f
    }
  }

  test(
    "whole partition aggregates and values match Spark exactly, including all-null partitions") {
    withSmallInput {
      for {
        enabled <- Seq(false, true)
        batch <- Seq("3", "64")
        unit <- Seq("ROWS", "RANGE")
      } {
        withSQLConf(
          CometConf.COMET_EXEC_WINDOW_PARTITION_AGGREGATE_ENABLED.key -> enabled.toString,
          CometConf.COMET_BATCH_SIZE.key -> batch) {
          val native = if (enabled) "PartitionAggregateWindowExec" else ""
          val df = checkExact(
            s"whole-$enabled-$batch-$unit",
            s"""SELECT id, p, o, v, SUM(v) OVER w, AVG(v) OVER w,
               |COUNT(v) OVER w, COUNT(*) OVER w, MIN(v) OVER w, MAX(v) OVER w,
               |FIRST_VALUE(v) OVER w, FIRST_VALUE(v) IGNORE NULLS OVER w,
               |LAST_VALUE(v) OVER w, LAST_VALUE(v) IGNORE NULLS OVER w,
               |NTH_VALUE(v, 2) OVER w, NTH_VALUE(v, 2) IGNORE NULLS OVER w,
               |NTH_VALUE(v, 50) IGNORE NULLS OVER w
               |FROM g06_small WINDOW w AS (PARTITION BY p ORDER BY o, id
               |$unit BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
               |ORDER BY id""".stripMargin,
            requiredNative = native,
            forbiddenNative = if (enabled) "" else "PartitionAggregateWindowExec")
          // AVG returns Double even for integral input, so the disabled custom executor
          // selects Spark's forward fold for this entire mixed window node.
          if (enabled) assertNativeWindow(df) else assertSparkWindow(df)
        }
      }
      val empty = checkExact(
        "whole-empty",
        """SELECT id, SUM(v) OVER (PARTITION BY p),
          |NTH_VALUE(v, 2) IGNORE NULLS OVER (PARTITION BY p ORDER BY id)
          |FROM g06_small WHERE id < 0 ORDER BY id""".stripMargin)
      assert(empty.collect().isEmpty)
    }
  }

  test("whole floating SUM and AVG keep Spark accumulation order across batch boundaries") {
    withTempView("g06_whole_cancel") {
      // Singleton/null partitions misalign the 3-row cancellation partitions with input batches.
      Seq(
        (0, 0, Some(9.0d)),
        (1, 1, Some(1.0e16)),
        (1, 2, Some(-1.0e16)),
        (1, 3, Some(1.0d)),
        (2, 4, None),
        (3, 5, Some(1.0e16)),
        (3, 6, Some(-1.0e16)),
        (3, 7, Some(1.0d)))
        .toDF("p", "id", "v")
        .createOrReplaceTempView("g06_whole_cancel")
      for {
        enabled <- Seq(false, true)
        batch <- Seq("1", "3", "64")
      } {
        withSQLConf(
          CometConf.COMET_EXEC_WINDOW_PARTITION_AGGREGATE_ENABLED.key -> enabled.toString,
          CometConf.COMET_BATCH_SIZE.key -> batch) {
          val df = checkExact(
            s"whole-cancellation-$enabled-$batch",
            """SELECT p, id, SUM(v) OVER w AS s, AVG(v) OVER w AS a
              |FROM g06_whole_cancel WINDOW w AS (PARTITION BY p ORDER BY id
              |ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
              |ORDER BY p, id""".stripMargin,
            requiredNative = if (enabled) "PartitionAggregateWindowExec" else "",
            forbiddenNative = if (enabled) "" else "PartitionAggregateWindowExec")
          if (enabled) assertNativeWindow(df) else assertSparkWindow(df)
          df.collect().filter(row => row.getInt(0) == 1 || row.getInt(0) == 3).foreach { row =>
            assert(row.getDouble(2) == 1.0d)
            assert(java.lang.Double.doubleToLongBits(row.getDouble(3)) ==
              java.lang.Double.doubleToLongBits(1.0d / 3.0d))
          }
        }
      }
    }
  }

  test("partition size functions match Spark exactly for singleton and undersized partitions") {
    withSmallInput {
      for {
        batch <- Seq("3", "64")
        order <- Seq("o ASC NULLS FIRST", "o DESC NULLS LAST")
      } {
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batch) {
          assertNativeWindow(
            checkExact(
              s"distribution-$batch-$order",
              s"""SELECT id, p, o, PERCENT_RANK() OVER w, CUME_DIST() OVER w
               |FROM g06_small WINDOW w AS (PARTITION BY p ORDER BY $order)
               |ORDER BY id""".stripMargin,
              requiredNative = "PartitionAggregateWindowExec"))
          assertNativeWindow(
            checkExact(
              s"ntile-$batch-$order",
              s"""SELECT id, p, NTILE(1) OVER w, NTILE(3) OVER w, NTILE(100) OVER w
               |FROM g06_small WINDOW w AS (PARTITION BY p ORDER BY $order, id)
               |ORDER BY id""".stripMargin,
              requiredNative = "PartitionAggregateWindowExec"))
        }
      }
    }
  }

  test("suffix floating MIN and MAX retain Spark first-equal signed-zero values") {
    withTempView("g06_suffix_zero") {
      Seq((1, -0.0d), (2, 0.0d))
        .toDF("id", "v")
        .createOrReplaceTempView("g06_suffix_zero")
      for {
        dataType <- Seq("FLOAT", "DOUBLE")
        nested <- Seq(false, true)
      } {
        val value = s"CAST(v AS $dataType)"
        val expr = if (nested) s"array($value)" else value
        val df = checkExact(
          s"suffix-zero-$dataType-$nested",
          s"""SELECT id, MIN($expr) OVER w, MAX($expr) OVER w
             |FROM g06_suffix_zero WINDOW w AS (ORDER BY id
             |ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) ORDER BY id""".stripMargin,
          forbiddenNative = "PartitionAggregateWindowExec")
        assertSparkWindow(df)
        if (!nested) {
          val first = df.collect().head
          for (column <- Seq(1, 2)) {
            if (dataType == "DOUBLE") {
              assert(
                java.lang.Double.doubleToRawLongBits(first.getDouble(column)) ==
                  java.lang.Double.doubleToRawLongBits(-0.0d))
            } else {
              assert(
                java.lang.Float.floatToRawIntBits(first.getFloat(column)) ==
                  java.lang.Float.floatToRawIntBits(-0.0f))
            }
          }
        }
      }
    }
  }

  test("suffix ROWS and RANGE preserve peers, null order and descending bounds") {
    withSmallInput {
      for ((frame, order) <- Seq(
          "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING" -> "o, id",
          "ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING" -> "o, id",
          "ROWS BETWEEN 3 FOLLOWING AND UNBOUNDED FOLLOWING" -> "o, id",
          "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING" -> "o ASC NULLS FIRST",
          "RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING" -> "o ASC NULLS LAST",
          "RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING" -> "o DESC NULLS LAST")) {
        val df = checkExact(
          s"suffix-$order-$frame",
          s"""SELECT id, p, o, v, COUNT(v) OVER w, COUNT(*) OVER w,
             |MIN(v) OVER w, MAX(v) OVER w FROM g06_small
             |WINDOW w AS (PARTITION BY p ORDER BY $order $frame) ORDER BY id""".stripMargin,
          requiredNative = "PartitionAggregateWindowExec")
        assertNativeWindow(df)
        if (order.contains("id")) {
          assertNativeWindow(
            checkExact(
              s"suffix-values-$frame",
              s"""SELECT id, FIRST_VALUE(v) OVER w,
               |FIRST_VALUE(v) IGNORE NULLS OVER w, LAST_VALUE(v) OVER w,
               |LAST_VALUE(v) IGNORE NULLS OVER w, NTH_VALUE(v, 2) OVER w,
               |NTH_VALUE(v, 2) IGNORE NULLS OVER w FROM g06_small
               |WINDOW w AS (PARTITION BY p ORDER BY $order $frame) ORDER BY id""".stripMargin,
              requiredNative = "PartitionAggregateWindowExec"))
        }
      }
    }
  }

  test("suffix SUM and AVG retain Spark forward evaluation and mandatory ANSI errors") {
    withTempView("g06_cancel", "g06_long", "g06_decimal") {
      Seq((1, 1.0e16), (2, -1.0e16), (3, 1.0d))
        .toDF("id", "v")
        .createOrReplaceTempView("g06_cancel")
      val df = checkExact(
        "suffix-floating-cancellation",
        """SELECT id, SUM(v) OVER w AS s, AVG(v) OVER w AS a FROM g06_cancel
          |WINDOW w AS (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
          |ORDER BY id""".stripMargin,
        forbiddenNative = "PartitionAggregateWindowExec,CometSortedWindowExec")
      assertSparkWindow(df)
      assert(df.collect().head.getDouble(1) == 1.0d)
      for (values <- Seq(Seq(Long.MaxValue, 1L, -1L), Seq(Long.MinValue, -1L, 1L))) {
        values.zipWithIndex
          .map { case (v, i) => (i, v) }
          .toDF("id", "v")
          .createOrReplaceTempView("g06_long")
        checkMandatoryError(
          """SELECT SUM(v) OVER (ORDER BY id
            |ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM g06_long""".stripMargin,
          "ARITHMETIC_OVERFLOW")
      }
      Seq((1, "0.6"), (2, "0.6"), (3, "-0.6"))
        .toDF("id", "raw")
        .selectExpr("id", "CAST(raw AS DECIMAL(38,38)) AS v")
        .createOrReplaceTempView("g06_decimal")
      for (function <- Seq("SUM", "AVG", "TRY_AVG")) {
        checkExact(
          s"suffix-decimal-$function",
          s"""SELECT id, $function(v) OVER (ORDER BY id
             |ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM g06_decimal
             |ORDER BY id""".stripMargin,
          forbiddenNative = "PartitionAggregateWindowExec")
      }
    }
  }

  test("suffix SUM keeps the original explicit ROWS and RANGE Spark oracle") {
    withTempView("g06_suffix") {
      Seq((1, 1, Option.empty[Int]), (2, 1, Some(1)), (3, 2, None), (4, 3, Some(3)))
        .toDF("id", "o", "v")
        .createOrReplaceTempView("g06_suffix")
      val rows = checkExact(
        "suffix-original-oracle",
        """SELECT id,
          |SUM(v) OVER (ORDER BY o, id ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
          |SUM(v) OVER (ORDER BY o RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
          |FROM g06_suffix ORDER BY id""".stripMargin,
        forbiddenNative = "PartitionAggregateWindowExec")
      assert(
        rows
          .collect()
          .toSeq == Seq(Row(1, 4L, 4L), Row(2, 3L, 4L), Row(3, 3L, 3L), Row(4, null, 3L)))
    }
  }

  test("sorted rank peers span batches with null, NaN and signed-zero keys") {
    withTempView("g06_rank") {
      val keys = Seq(
        None,
        None,
        None,
        None,
        Some(-0.0d),
        Some(0.0d),
        Some(-0.0d),
        Some(0.0d),
        Some(1.0d),
        Some(Double.NaN),
        Some(Double.NaN),
        Some(Double.NaN))
      keys.zipWithIndex
        .map { case (o, i) => (0, i, o) }
        .toDF("p", "id", "o")
        .createOrReplaceTempView("g06_rank")
      for {
        sorted <- Seq(false, true)
        batch <- Seq("3", "64")
        order <- Seq("ASC NULLS FIRST", "DESC NULLS LAST")
      } {
        withSQLConf(
          CometConf.COMET_EXEC_WINDOW_SORTED_ENABLED.key -> sorted.toString,
          CometConf.COMET_BATCH_SIZE.key -> batch) {
          assertNativeWindow(
            checkExact(
              s"rank-$sorted-$batch-$order",
              s"""SELECT id, o, RANK() OVER w, DENSE_RANK() OVER w FROM g06_rank
               |WINDOW w AS (PARTITION BY p ORDER BY o $order) ORDER BY id""".stripMargin,
              requiredNative = if (sorted) "CometSortedWindowExec" else "BoundedWindowAggExec",
              forbiddenNative = if (sorted) "" else "CometSortedWindowExec"))
        }
      }
    }
  }

  test("sorted LEAD and LAG preserve offsets, null values and defaults across batches") {
    withTempView("g06_shift") {
      (0 until 1050)
        .map { id =>
          (if (id < 1048) 0 else 1, id, if (id % 13 == 0) None else Some(id))
        }
        .toDF("p", "id", "v")
        .createOrReplaceTempView("g06_shift")
      for {
        sorted <- Seq(false, true)
        batch <- Seq("3", "128")
        offset <- Seq(-2, 0, 1, 3, 1024, 1025)
      } {
        withSQLConf(
          CometConf.COMET_EXEC_WINDOW_SORTED_ENABLED.key -> sorted.toString,
          CometConf.COMET_BATCH_SIZE.key -> batch) {
          val eligible = sorted && math.abs(offset) <= 1024
          assertNativeWindow(
            checkExact(
              s"shift-$sorted-$batch-$offset",
              s"""SELECT id, v, LEAD(v, $offset, -17) OVER w,
               |LAG(v, $offset, -23) OVER w FROM g06_shift
               |WINDOW w AS (PARTITION BY p ORDER BY id) ORDER BY id""".stripMargin,
              requiredNative = if (eligible) "CometSortedWindowExec" else "",
              forbiddenNative = if (eligible) "" else "CometSortedWindowExec"))
        }
      }
      assertSparkWindow(
        checkExact(
          "shift-ignore-nulls",
          """SELECT id, LEAD(v, 2, -1) IGNORE NULLS OVER w,
          |LAG(v, 2, -2) IGNORE NULLS OVER w FROM g06_shift
          |WINDOW w AS (PARTITION BY p ORDER BY id) ORDER BY id""".stripMargin,
          forbiddenNative = "CometSortedWindowExec,PartitionAggregateWindowExec," +
            "BoundedWindowAggExec,WindowAggExec"))
      assertSparkWindow(
        checkExact(
          "shift-column-default",
          """SELECT id, LEAD(v, 1, id) OVER (PARTITION BY p ORDER BY id)
          |FROM g06_shift ORDER BY id""".stripMargin,
          forbiddenNative = "CometSortedWindowExec"))
    }
  }

  test("window invalid arguments retain Spark mandatory analysis errors") {
    withSmallInput {
      for (query <- Seq(
          "SELECT NTILE(0) OVER (ORDER BY id) FROM g06_small",
          "SELECT NTH_VALUE(v, 0) OVER (ORDER BY id) FROM g06_small")) {
        checkMandatoryError(query, "VALUE_OUT_OF_RANGE")
      }
      checkMandatoryError(
        "SELECT LEAD(v, id) OVER (ORDER BY id) FROM g06_small",
        "NON_FOLDABLE_INPUT")
    }
  }

  test("FILTER window aggregates retain the Spark fallback guard") {
    assume(isSpark42Plus, "Window aggregate FILTER was introduced in Spark 4.2")
    withSmallInput {
      for (frame <- Seq(
          "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
          "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING")) {
        assertSparkWindow(
          checkExact(
            s"filter-$frame",
            s"""SELECT id, SUM(v) FILTER (WHERE v > 0) OVER w,
             |COUNT(*) FILTER (WHERE v IS NULL) OVER w FROM g06_small
             |WINDOW w AS (PARTITION BY p ORDER BY id $frame) ORDER BY id""".stripMargin,
            forbiddenNative =
              "PartitionAggregateWindowExec,CometSortedWindowExec,WindowAggExec,BoundedWindowAggExec"))
      }
    }
  }

  test("AQE keeps the original window child distribution for rebalanced input") {
    withTempView("g06_aqe") {
      (0 until 1500)
        .map(id => (id, if (id % 17 == 0) None else Some(id % 11), id % 19))
        .toDF("id", "p", "v")
        .repartition(13, $"id")
        .createOrReplaceTempView("g06_aqe")
      for {
        adaptive <- Seq(false, true)
        mode <- Seq("native", "jvm")
        function <- Seq("ROW_NUMBER()", "MAX(v)")
      } {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.SHUFFLE_PARTITIONS.key -> "13",
          CometConf.COMET_SHUFFLE_MODE.key -> mode,
          "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
          "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1024",
          "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1") {
          val frame =
            if (function == "ROW_NUMBER()") ""
            else
              "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
          val df = checkExact(
            s"aqe-$adaptive-$mode-$function",
            s"""SELECT id, p, $function OVER (PARTITION BY p ORDER BY id $frame)
               |FROM g06_aqe ORDER BY id""".stripMargin,
            requiredNative =
              if (mode != "native") ""
              else if (function == "ROW_NUMBER()") "CometSortedWindowExec"
              else "PartitionAggregateWindowExec",
            forbiddenNative =
              if (mode == "native") ""
              else
                "CometSortedWindowExec,PartitionAggregateWindowExec," +
                  "BoundedWindowAggExec,WindowAggExec")
          val plan = df.queryExecution.executedPlan
          if (adaptive) {
            assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan, plan)
          }
          if (mode == "native") {
            assertNativeWindow(df)
            if (adaptive) checkCometOperatorsInFinalPlan(plan)
            collect(plan) { case w: CometWindowExec => w }.foreach { window =>
              assert(
                window.requiredChildDistribution ==
                  window.originalPlan.requiredChildDistribution)
              assert(
                window.child.outputPartitioning.satisfies(
                  window.originalPlan.requiredChildDistribution.head))
              assert(
                SortOrder.orderingSatisfies(
                  window.child.outputOrdering,
                  window.originalPlan.requiredChildOrdering.head))
            }
          } else {
            // This explicit JVM shuffle over the repartitioned input leaves a Spark child
            // under the window. The ordinary all-native-children conversion gate preserves
            // Spark execution; this is separate from N01/N03 native-mode coverage above.
            assertSparkWindow(df)
            collect(plan) { case w: SparkWindowExec => w }.foreach { window =>
              assert(
                window.child.outputPartitioning.satisfies(window.requiredChildDistribution.head))
              assert(
                SortOrder.orderingSatisfies(
                  window.child.outputOrdering,
                  window.requiredChildOrdering.head))
            }
          }
        }
      }
    }
  }

  test("partition windows actually spill and replay whole, size-based and suffix frames") {
    withTempPath { dir =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(4096)
          .selectExpr(
            "id",
            "CAST(id % 23 AS INT) AS v",
            "CASE WHEN id % 13 = 0 THEN CAST(NULL AS STRING) " +
              "ELSE concat(CAST(id AS STRING), repeat('x', 8192)) END AS payload")
          .coalesce(1)
          .write
          .parquet(dir.getCanonicalPath)
      }
      withTempView("g06_spill") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("g06_spill")
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "128") {
          val queries = Seq(
            "whole" -> """SELECT id, SUM(v) OVER w, AVG(v) OVER w, COUNT(payload) OVER w,
              |length(FIRST_VALUE(payload) IGNORE NULLS OVER w),
              |length(LAST_VALUE(payload) IGNORE NULLS OVER w),
              |length(NTH_VALUE(payload, 3) IGNORE NULLS OVER w) FROM g06_spill
              |WINDOW w AS (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING
              |AND UNBOUNDED FOLLOWING) ORDER BY id""".stripMargin,
            "size" -> """SELECT id, NTILE(7) OVER w, PERCENT_RANK() OVER w,
              |CUME_DIST() OVER w FROM g06_spill
              |WINDOW w AS (ORDER BY payload, id) ORDER BY id""".stripMargin,
            "suffix" -> """SELECT id, COUNT(payload) OVER w,
              |length(FIRST_VALUE(payload) IGNORE NULLS OVER w),
              |length(NTH_VALUE(payload, 2) IGNORE NULLS OVER w) FROM g06_spill
              |WINDOW w AS (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
              |ORDER BY id""".stripMargin)
          queries.foreach { case (name, query) =>
            val df =
              checkExact(s"spill-$name", query, requiredNative = "PartitionAggregateWindowExec")
            assertNativeWindow(df)
            val windows = collect(df.queryExecution.executedPlan) { case w: CometWindowExec => w }
            assert(windows.map(_.metrics("spill_count").value).sum > 0L, name)
            assert(windows.map(_.metrics("spilled_bytes").value).sum > 0L, name)
            assert(windows.map(_.metrics("output_rows").value).sum == 4096L, name)
          }
        }
      }
    }
  }
}
