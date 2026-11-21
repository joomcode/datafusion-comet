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

import java.util.concurrent.Executors

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{broadcast, col, count, lit}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometOffTaskThreadSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val rows = 12000
  private val days = 5

  private def withTable(f: DataFrame => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(rows.toLong)
        .selectExpr(
          s"date_add(DATE'2026-10-01', cast(id % $days AS int)) AS date",
          "concat('team', cast(id % 7 AS string)) AS team",
          "concat('app', cast(id AS string)) AS app_id",
          "id AS apps",
          "cast(id * 1.5 AS double) AS cost")
        .repartition(12)
        .write
        .parquet(dir.getCanonicalPath)
      f(spark.read.parquet(dir.getCanonicalPath))
    }
  }

  private def splitByDate(df: DataFrame): DataFrame = {
    val counts = df
      .groupBy(col("date"))
      .agg(count(lit(1)) as "cnt")
    df.join(broadcast(counts), Seq("date"), "left_outer")
      .repartitionByRange(col("date"), (col("apps") % 3).as("bucket"))
      .drop("bucket", "cnt")
  }

  private def partitionSizes(rdd: RDD[_]): Seq[(Int, Int)] =
    rdd
      .mapPartitionsWithIndex { case (i, it) => Iterator((i, it.size)) }
      .collect()
      .toSeq

  private def rereadOffTaskThread(rdd: RDD[_], partitions: Seq[Int]): Seq[(Int, Int)] = {
    val pool = Executors.newFixedThreadPool(4)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    try {
      val reads = partitions.map { i =>
        Future(
          (i, CometOffTaskThreadSuite.countOffTaskThread(spark.sparkContext, rdd, Seq(i)).head))
      }
      Await.result(Future.sequence(reads), 5.minutes).sortBy(_._1)
    } finally {
      pool.shutdown()
    }
  }

  private def hasCometExec(df: DataFrame): Boolean =
    find(df.queryExecution.executedPlan)(_.isInstanceOf[CometExec]).isDefined

  Seq("native", "jvm").foreach { mode =>
    Seq("true", "false").foreach { aqe =>
      test(s"a reused $mode shuffle read on a thread other than the task's, aqe=$aqe") {
        withTable { df =>
          withSQLConf(
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            SQLConf.ANSI_ENABLED.key -> "true",
            CometConf.COMET_BATCH_SIZE.key -> "64",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> mode,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
            "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "true",
            SQLConf.SHUFFLE_PARTITIONS.key -> "12",
            SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1k",
            SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1k") {
            val split = splitByDate(df)
            val rdd = split.rdd
            assert(hasCometExec(split))
            val sizes = partitionSizes(rdd).filter(_._2 > 0)
            assert(sizes.size >= days)
            assert(sizes.map(_._2).sum == rows)
            assert(rereadOffTaskThread(rdd, sizes.map(_._1)) == sizes)
          }
        }
      }
    }
  }

  test("a native scan read on a thread other than the task's") {
    withTable { df =>
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true",
        CometConf.COMET_BATCH_SIZE.key -> "64") {
        val filtered = df.filter(col("apps") % 3 === 0).select(col("app_id"), col("cost"))
        assert(hasCometExec(filtered))
        val rdd = filtered.rdd
        val counts = CometOffTaskThreadSuite.countOffTaskThread(
          spark.sparkContext,
          rdd,
          rdd.partitions.indices)
        assert(counts.sum == rows / 3)
      }
    }
  }

  test("off-task-thread output preserves Spark values, native metrics and thread-local context") {
    withTable { df =>
      def query: DataFrame =
        df.filter(col("apps") % 3 === 0).select(col("apps"), col("app_id"), col("cost"))

      val expected =
        withSQLConfResult(
          CometConf.COMET_ENABLED.key -> "false",
          SQLConf.ANSI_ENABLED.key -> "true") {
          query.collect().sortBy(_.getLong(0)).toSeq
        }
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true",
        CometConf.COMET_BATCH_SIZE.key -> "64") {
        val nativeQuery = query
        val execution = nativeQuery.queryExecution
        assert(hasCometExec(nativeQuery))
        // Dataset.rdd creates a sibling QueryExecution for deserializing external Rows. Execute
        // this exact plan instead, so the native metrics inspected below belong to these reads.
        val internalRows = SQLExecution.withNewExecutionId(execution) {
          spark.sparkContext
            .runJob(execution.toRdd, CometOffTaskThreadSuite.internalRowsOnNewThread _)
            .flatten
        }
        val actual = internalRows
          .map(row => Row(row.getLong(0), row.getUTF8String(1).toString, row.getDouble(2)))
          .sortBy(_.getLong(0))
          .toSeq
        assert(actual == expected)
        val outputRows = collect(execution.executedPlan) { case exec: CometExec =>
          exec.metrics
            .get("output_rows")
            .orElse(exec.metrics.get("numOutputRows"))
            .map(_.value)
            .getOrElse(0L)
        }
        assert(outputRows.exists(_ > 0), s"Native output metrics were not updated: $outputRows")
      }
    }
  }
}

object CometOffTaskThreadSuite {

  def countOffTaskThread(sc: SparkContext, rdd: RDD[_], partitions: Seq[Int]): Seq[Int] =
    sc.runJob(rdd, countOnNewThread _, partitions).toSeq

  private def countOnNewThread(it: Iterator[_]): Int = {
    var size = -1
    var error: Throwable = null
    val reader = new Thread(new Runnable {
      override def run(): Unit =
        try {
          assert(TaskContext.get() == null)
          size = it.size
          assert(TaskContext.get() == null)
        } catch {
          case e: Throwable => error = e
        }
    })
    reader.setDaemon(true)
    reader.start()
    reader.join(30000)
    if (reader.isAlive) {
      reader.interrupt()
      throw new AssertionError("Off-task-thread reader did not finish within 30 seconds")
    }
    if (error != null) throw error
    size
  }

  def internalRowsOnNewThread(it: Iterator[InternalRow]): Array[InternalRow] = {
    var rows: Array[InternalRow] = null
    var error: Throwable = null
    val reader = new Thread(() => {
      try {
        assert(TaskContext.get() == null)
        rows = it.map(_.copy()).toArray
        assert(TaskContext.get() == null)
      } catch {
        case failure: Throwable => error = failure
      }
    })
    reader.setDaemon(true)
    reader.start()
    reader.join(30000)
    if (reader.isAlive) {
      reader.interrupt()
      throw new AssertionError("Off-task-thread reader did not finish within 30 seconds")
    }
    if (error != null) throw error
    rows
  }
}
