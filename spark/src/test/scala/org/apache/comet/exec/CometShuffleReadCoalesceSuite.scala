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

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometColumnarToRowExec
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometShuffleReadCoalesceSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val maps = 8
  private val partitions = 5
  private val rows = 400

  private def withTable(f: => Unit): Unit = {
    withTempPath { dir =>
      spark
        .range(rows)
        .selectExpr(
          "id",
          "cast(id % 13 AS int) AS k",
          "IF(id % 7 = 0, NULL, concat('s', cast(id AS string))) AS s",
          "cast(id AS decimal(20, 3)) / 7 AS dec",
          "IF(id % 11 = 0, NULL, named_struct('x', IF(id % 6 = 0, NULL, id * 2), 'y', " +
            "named_struct('z', IF(id % 3 = 0, NULL, cast(id AS string)), 'w', id / 3.0))) AS st",
          "IF(id % 5 = 0, NULL, array(named_struct('p', IF(id % 9 = 0, NULL, cast(id AS int)), " +
            "'q', IF(id % 2 = 0, NULL, 'q')), named_struct('p', cast(id + 1 AS int), 'q', " +
            "IF(id % 2 = 1, NULL, 'r')))) AS arr",
          "map(cast(id AS string), array(cast(id AS int), IF(id % 4 = 0, NULL, 1))) AS m",
          "cast(id * 1.5 AS double) AS d")
        .repartition(maps)
        .write
        .parquet(dir.getCanonicalPath)
      withSQLConf(
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1g",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1g") {
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("t")
        withTempView("t")(f)
      }
    }
  }

  private def readModes(f: => Unit): Unit =
    for {
      mode <- Seq("native", "jvm")
      direct <- Seq("true", "false")
      coalesce <- Seq("true", "false")
      batchSize <- Seq("16", "8192")
    } {
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> mode,
        CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> direct,
        CometConf.COMET_SHUFFLE_READ_COALESCE_ENABLED.key -> coalesce,
        CometConf.COMET_BATCH_SIZE.key -> batchSize) {
        withClue(s"mode=$mode direct=$direct coalesce=$coalesce batchSize=$batchSize") {
          f
        }
      }
    }

  private def shuffled: DataFrame = spark.table("t").repartition(partitions, col("k"))

  private def cometExchange(plan: SparkPlan): CometShuffleExchangeExec =
    collect(plan) { case s: CometShuffleExchangeExec => s }.head

  test("rows read with coalescing match Spark for every read path") {
    withTable {
      readModes {
        checkSparkAnswer(shuffled)
        checkSparkAnswer(shuffled.withColumn("x", col("id") + 1).where(col("k") =!= 3))
        checkSparkAnswer(shuffled.sortWithinPartitions(col("k"), col("id")))
        checkSparkAnswer(shuffled.selectExpr("k", "st.y.z", "arr[1].q", "m", "dec"))
      }
    }
  }

  test("a JVM consumer gets batches of the batch size from many small blocks") {
    withTable {
      for (mode <- Seq("native", "jvm"); coalesce <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_MODE.key -> mode,
          CometConf.COMET_SHUFFLE_READ_COALESCE_ENABLED.key -> coalesce.toString,
          CometConf.COMET_BATCH_SIZE.key -> "30") {
          val df = shuffled
          val exchange = cometExchange(df.queryExecution.executedPlan)
          assert(
            exchange.shuffleType == (if (mode == "native") CometNativeShuffle
                                     else CometColumnarShuffle))
          val perPartition = SQLExecution.withNewExecutionId(df.queryExecution) {
            exchange
              .executeColumnar()
              .mapPartitions(batches => Iterator(batches.map(_.numRows()).toList))
              .collect()
              .toSeq
          }
          assert(perPartition.map(_.sum).sum == rows)
          if (coalesce) {
            perPartition.foreach { sizes =>
              assert(sizes.dropRight(1).forall(_ >= 30), sizes)
              assert(sizes.forall(_ > 0), sizes)
            }
          } else {
            assert(perPartition.exists(sizes => sizes.size > (sizes.sum + 29) / 30), perPartition)
          }
        }
      }
    }
  }

  test("a native consumer reading the shuffle directly gets coalesced batches") {
    withTable {
      for (coalesce <- Seq(true, false)) {
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_MODE.key -> "native",
          CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_READ_COALESCE_ENABLED.key -> coalesce.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          val df = shuffled.withColumn("x", col("id") + 1)
          val (_, plan) = checkSparkAnswer(df)
          val c2r = collect(plan) { case c: CometColumnarToRowExec => c }
          assert(c2r.size == 1, plan)
          val batches = c2r.head.metrics("numInputBatches").value
          if (coalesce) assert(batches <= partitions, plan)
          else assert(batches > partitions * 2, plan)
        }
      }
    }
  }

  test("empty partitions and an empty input are read with coalescing") {
    withTable {
      readModes {
        checkSparkAnswer(spark.table("t").where(col("k") === 1).repartition(7, col("k")))
        checkSparkAnswer(spark.table("t").where(col("k") < 0).repartition(3, col("k")))
      }
    }
  }
}
