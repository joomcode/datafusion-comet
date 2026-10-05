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

package org.apache.spark.sql.comet

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, TaskKilled}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{broadcast, col, sum, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import org.apache.comet.CometConf

/**
 * A killed task must stop well within `spark.task.reaper.killTimeout` while Comet native code is
 * running its plan. Spark only marks the task killed, and interrupting the thread (not requested
 * here, as when a failed job cancels its other stages) does not reach native code either.
 */
class CometNativeTaskKillSuite extends CometTestBase {
  import CometNativeTaskKillSuite.busyWaitNanos

  private val killBoundMs = 2000L

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.task.reaper.enabled", "true")
      .set("spark.task.reaper.killTimeout", "10s")

  private def nativeQueryConf(extra: (String, String)*)(f: => Unit): Unit =
    withSQLConf(
      extra ++ Seq(
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "native",
        CometConf.COMET_EXEC_EXPLODE_ENABLED.key -> "true"): _*)(f)

  private def assertRunsNatively(df: DataFrame, operators: Class[_]*): Unit = {
    val plan = df.queryExecution.executedPlan
    operators.foreach { operator =>
      assert(
        plan.collectFirst { case p if operator.isInstance(p) => p }.isDefined ||
          plan.collectWithSubqueries { case p if operator.isInstance(p) => p }.nonEmpty,
        s"expected ${operator.getSimpleName} in\n$plan")
    }
  }

  /**
   * Runs `df` without interrupting on cancel, cancels it once its tasks have run
   * `runBeforeKillMs`, and checks that every task running then ends as killed within
   * `killBoundMs`.
   */
  private def assertKilledPromptly(df: DataFrame, runBeforeKillMs: Long = 1500L): Unit = {
    val sc = spark.sparkContext
    val group = s"comet-kill-${UUID.randomUUID()}"
    val stages = ConcurrentHashMap.newKeySet[Int]()
    val started = ConcurrentHashMap.newKeySet[Long]()
    val ended = new ConcurrentHashMap[Long, (Long, AnyRef)]()
    val listener = new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == group)) {
          event.stageIds.foreach(stages.add)
        }
      }
      override def onTaskStart(event: SparkListenerTaskStart): Unit = {
        if (stages.contains(event.stageId)) started.add(event.taskInfo.taskId)
      }
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
        if (stages.contains(event.stageId)) {
          ended.put(event.taskInfo.taskId, (event.taskInfo.finishTime, event.reason))
        }
      }
    }
    sc.addSparkListener(listener)
    val failure = new AtomicReference[Throwable]()
    val runner = new Thread(() => {
      sc.setJobGroup(group, "kill test", interruptOnCancel = false)
      try {
        df.collect()
      } catch {
        case t: Throwable => failure.set(t)
      } finally {
        sc.clearJobGroup()
      }
    })
    runner.setDaemon(true)
    try {
      runner.start()
      val startDeadline = System.currentTimeMillis() + 60000
      while (started.isEmpty && System.currentTimeMillis() < startDeadline) Thread.sleep(10)
      assert(!started.isEmpty, "no task started")
      Thread.sleep(runBeforeKillMs)
      val running = started.asScala.toSet -- ended.keySet().asScala
      assert(running.nonEmpty, "the query finished before it could be killed")

      val killedAt = System.currentTimeMillis()
      sc.cancelJobGroup(group)
      val endDeadline = killedAt + 30000
      while (running.exists(id => !ended.containsKey(id)) &&
        System.currentTimeMillis() < endDeadline) {
        Thread.sleep(10)
      }
      val stillRunning = running.filterNot(ended.containsKey)
      assert(stillRunning.isEmpty, s"tasks $stillRunning were still running 30 s after the kill")
      val stopMs = running.map(id => ended.get(id)._1 - killedAt).max
      logInfo(s"The killed tasks stopped within $stopMs ms")
      assert(stopMs < killBoundMs, s"the killed tasks took $stopMs ms to stop")
      running.foreach { id =>
        assert(
          ended.get(id)._2.isInstanceOf[TaskKilled],
          s"task $id ended with ${ended.get(id)._2}")
      }
      runner.join(30000)
      assert(failure.get() != null, "the cancelled query did not fail")
    } finally {
      sc.removeSparkListener(listener)
    }
    assert(spark.range(10).count() == 10)
  }

  test("a task killed while a native producer task runs its shuffle write stops promptly") {
    nativeQueryConf(CometConf.COMET_BATCH_SIZE.key -> "16") {
      withTempPath { path =>
        spark.range(0, 4000, 1, 2).write.parquet(path.getAbsolutePath)
        withParquetTable(path.getAbsolutePath, "t") {
          val df = sql(
            "SELECT k % 7 AS g, count(*) FROM " +
              "(SELECT explode(array_repeat(id, 1000000)) AS k FROM t) GROUP BY k % 7")
          assertRunsNatively(df, classOf[CometNativeScanExec], classOf[CometExplodeExec])
          assertKilledPromptly(df)
        }
      }
    }
  }

  test("a task killed inside a native operator that never waits on its input stops promptly") {
    nativeQueryConf() {
      val build = spark.range(0, 400000, 1, 1).selectExpr("id * 0 AS k", "id AS b")
      val df = spark
        .range(0, 16384, 1, 2)
        .selectExpr("id * 0 AS k", "id AS p")
        .join(broadcast(build), "k")
        .agg(sum(col("p") + col("b")))
      assertRunsNatively(
        df,
        classOf[CometSparkToColumnarExec],
        classOf[CometBroadcastHashJoinExec])
      assertKilledPromptly(df)
    }
  }

  test("a task killed while native code waits on a slow JVM input stops promptly") {
    nativeQueryConf() {
      val schema = StructType(Seq(StructField("id", LongType)))
      val rows: RDD[InternalRow] = spark.sparkContext
        .parallelize(0 until 2, 2)
        .mapPartitions { _ =>
          Iterator.range(0, 100000).map { i =>
            busyWaitNanos(500000L)
            InternalRow(i.toLong)
          }
        }
      val input = spark.internalCreateDataFrame(rows, schema)
      val df = input.groupBy(col("id") % 7).count()
      assertRunsNatively(df, classOf[CometSparkToColumnarExec])
      assertKilledPromptly(df)
    }
  }

  test("a task killed while native code evaluates a slow JVM UDF stops promptly") {
    nativeQueryConf() {
      val slow = udf { (id: java.lang.Long) =>
        busyWaitNanos(2000000L)
        id
      }
      withTempPath { path =>
        spark.range(0, 20000, 1, 2).write.parquet(path.getAbsolutePath)
        withParquetTable(path.getAbsolutePath, "t") {
          val df = spark.table("t").select(slow(col("id")).as("v")).agg(sum("v"))
          assertRunsNatively(df, classOf[CometNativeScanExec], classOf[CometProjectExec])
          assertKilledPromptly(df)
        }
      }
    }
  }
}

object CometNativeTaskKillSuite {
  def busyWaitNanos(nanos: Long): Unit = {
    val end = System.nanoTime() + nanos
    while (System.nanoTime() < end) {}
  }
}
