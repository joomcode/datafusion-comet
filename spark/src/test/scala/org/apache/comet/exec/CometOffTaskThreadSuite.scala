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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{broadcast, col, count, lit, rand, round}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometOffTaskThreadSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val rows = 60000
  private val days = 15

  private def withTable(f: DataFrame => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(rows)
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
      .withColumn("maxRand", round(col("cnt") / 5e6))
    df.join(broadcast(counts), Seq("date"), "left_outer")
      .repartitionByRange(col("date"), round(rand() * col("maxRand")).as("rand"))
      .drop("rand", "maxRand", "cnt")
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
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> mode,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
            "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "true",
            SQLConf.SHUFFLE_PARTITIONS.key -> "50",
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
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "true") {
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
          size = it.size
        } catch {
          case e: Throwable => error = e
        }
    })
    reader.start()
    reader.join()
    if (error != null) throw error
    size
  }
}
