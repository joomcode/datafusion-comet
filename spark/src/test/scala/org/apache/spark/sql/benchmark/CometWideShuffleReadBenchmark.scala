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

package org.apache.spark.sql.benchmark

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

object CometWideShuffleReadBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometWideShuffleReadBenchmark")
      .set("spark.master", "local[4]")
      .setIfMissing("spark.driver.memory", "6g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.comet.exec.onHeap.enabled", "true")
      .set("spark.memory.offHeap.enabled", "false")

    val session = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
    session.conf.set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, "false")
    session.conf.set(SQLConf.FILES_MAX_PARTITION_BYTES.key, "1g")
    session.conf.set(SQLConf.FILES_OPEN_COST_IN_BYTES.key, "1g")
    session.conf.set(CometConf.COMET_ENABLED.key, "false")
    session.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    session
  }

  private def arg(args: Array[String], key: String, default: String): String =
    args
      .collectFirst { case a if a.startsWith(s"$key=") => a.drop(key.length + 1) }
      .getOrElse(default)

  private def payload(n: Int): Seq[Column] =
    (0 until n / 2).flatMap { j =>
      Seq(
        (hash(col("id"), lit(j)).cast("double") / 1000.0).as(f"d$j%03d"),
        lpad(hex(hash(col("id"), lit(j + 100000)).bitwiseAND(lit(0x7fffffff))), 8, "0")
          .as(f"s$j%03d"))
    }

  private def query(t: DataFrame, q: String, parts: Int): DataFrame = q match {
    case "shuffle" => t.repartition(parts, col("k"))
    case "shufproj" => t.repartition(parts, col("k")).withColumn("x", col("id") + 1)
    case "sort" => t.repartition(parts, col("k")).sortWithinPartitions(col("k"), col("id"))
  }

  private val readNanos = new AtomicLong()
  private val writeNanos = new AtomicLong()
  private val readBytes = new AtomicLong()
  private val fetchWaitMs = new AtomicLong()

  private object StageTimes extends SparkListener {
    override def onTaskEnd(end: SparkListenerTaskEnd): Unit = {
      val m = end.taskMetrics
      if (m != null) {
        val run = m.executorRunTime * 1000000L
        if (m.shuffleReadMetrics.totalBlocksFetched > 0) {
          readNanos.addAndGet(run)
          readBytes.addAndGet(m.shuffleReadMetrics.totalBytesRead)
          fetchWaitMs.addAndGet(m.shuffleReadMetrics.fetchWaitTime)
        } else {
          writeNanos.addAndGet(run)
        }
      }
    }
  }

  private def run(df: DataFrame): Unit =
    df.queryExecution.executedPlan.execute().foreach(_ => ())

  private def timed(label: String)(f: => Unit): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty()
    readNanos.set(0)
    writeNanos.set(0)
    readBytes.set(0)
    fetchWaitMs.set(0)
    val start = System.nanoTime()
    f
    val wall = System.nanoTime() - start
    spark.sparkContext.listenerBus.waitUntilEmpty()
    println(
      f"[WSR] $label wall_ms=${wall / 1e6}%.0f map_task_ms=${writeNanos.get / 1e6}%.0f " +
        f"reduce_task_ms=${readNanos.get / 1e6}%.0f read_mb=${readBytes.get / 1e6}%.0f " +
        s"fetch_wait_ms=${fetchWaitMs.get}")
  }

  override def runCometBenchmark(args: Array[String]): Unit = {
    val widths = arg(args, "widths", "8,64,512").split(",").map(_.toInt)
    val leafTotal = arg(args, "leaves", "64000000").toLong
    val maps = arg(args, "maps", "64").toInt
    val parts = arg(args, "parts", "250").toInt
    val queries = arg(args, "queries", "shuffle,shufproj,sort").split(",")
    val modes = arg(args, "modes", "spark,comet,coalesce").split(",")
    spark.sparkContext.addSparkListener(StageTimes)

    widths.foreach { n =>
      val rows = leafTotal / n
      withTempPath { dir =>
        spark
          .range(0, rows, 1, maps)
          .select(Seq(col("id"), pmod(xxhash64(col("id")), lit(rows / 64)).as("k")) ++
            payload(n): _*)
          .write
          .parquet(dir.getCanonicalPath)
        val t = spark.read.parquet(dir.getCanonicalPath)
        queries.foreach { q =>
          val benchmark = new Benchmark(
            s"wide shuffle read: $q, $n leaves, $rows rows, $maps maps, $parts partitions",
            rows,
            minNumIters = 3,
            warmupTime = 1.second,
            minTime = 1.second,
            output = output)
          modes.foreach { mode =>
            val confs = mode match {
              case "spark" => Seq(CometConf.COMET_ENABLED.key -> "false")
              case other =>
                Seq(
                  CometConf.COMET_ENABLED.key -> "true",
                  CometConf.COMET_EXEC_ENABLED.key -> "true",
                  CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
                  CometConf.COMET_SHUFFLE_MODE.key -> "native",
                  CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key ->
                    (!other.startsWith("jvmread")).toString,
                  CometConf.COMET_SHUFFLE_READ_COALESCE_ENABLED.key ->
                    other.endsWith("coalesce").toString)
            }
            benchmark.addCase(mode) { _ =>
              withSQLConf(confs: _*)(timed(s"$q:$n:$mode")(run(query(t, q, parts))))
            }
          }
          benchmark.run()
        }
      }
    }
  }
}
