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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, Final, Partial}
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometHashAggregateExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometNativeException, Native}
import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus
import org.apache.comet.serde.OperatorOuterClass.Operator

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
        SQLConf.ANSI_ENABLED.key -> "true",
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

  test("G07 coalescer rejects invalid direct buffer bounds and IPC field counts") {
    val native = new Native()
    val state = native.createShuffleReadCoalescer(100, 0L)
    try {
      val tiny = java.nio.ByteBuffer.allocateDirect(4)
      val tooLong = intercept[CometNativeException] {
        native.pushShuffleBlock(state, tiny, 5, 1, false)
      }
      assert(tooLong.getMessage.contains("outside direct buffer capacity"))
      val negative = intercept[CometNativeException] {
        native.pushShuffleBlock(state, tiny, -1, 1, false)
      }
      assert(negative.getMessage.contains("Negative shuffle block length"))

      val allocator = new org.apache.arrow.memory.RootAllocator(1024 * 1024)
      val vector = new org.apache.arrow.vector.IntVector("id", allocator)
      val root = org.apache.arrow.vector.VectorSchemaRoot.of(vector)
      val bytes = new java.io.ByteArrayOutputStream()
      bytes.write("NONE".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
      val writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, bytes)
      try {
        vector.allocateNew(2)
        vector.setSafe(0, 11)
        vector.setSafe(1, 22)
        root.setRowCount(2)
        writer.start()
        writer.writeBatch()
        writer.end()
        val payload = bytes.toByteArray
        val buffer = java.nio.ByteBuffer.allocateDirect(payload.length)
        buffer.put(payload)
        val wrongHeader = intercept[CometNativeException] {
          native.pushShuffleBlock(state, buffer, payload.length, 0, false)
        }
        assert(wrongHeader.getMessage.contains("header field count mismatch"))
        assert(!native.pushShuffleBlock(state, buffer, payload.length, 1, false))
        assert(native.finishShuffleRead(state))
        val wrongAddresses = intercept[CometNativeException] {
          native.exportShuffleBatch(state, Array.emptyLongArray, Array.emptyLongArray)
        }
        assert(wrongAddresses.getMessage.contains("address count mismatch"))
        val util = new org.apache.comet.vector.NativeUtil()
        try {
          val batch = util
            .getNextBatch(
              1,
              (arrays, schemas) => native.exportShuffleBatch(state, arrays, schemas))
            .get
          try {
            assert(batch.numRows() == 2)
            assert(batch.column(0).getInt(0) == 11)
            assert(batch.column(0).getInt(1) == 22)
          } finally batch.close()
          assert(!native.finishShuffleRead(state))
        } finally util.close()
      } finally {
        writer.close()
        root.close()
        allocator.close()
      }
    } finally native.releaseShuffleReadCoalescer(state)
  }

  test(
    "G07 coalesced BloomFilter partial batches match original Spark native and mixed buffers") {
    val function = if (isSpark42Plus) {
      new FunctionIdentifier("g07_bloom", Some("builtin"), Some("system"))
    } else {
      new FunctionIdentifier("g07_bloom")
    }
    spark.sessionState.functionRegistry.registerFunction(
      function,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "g07_bloom"),
      (children: Seq[Expression]) =>
        new BloomFilterAggregate(children.head, children(1), children(2)))
    try {
      withTempPath { dir =>
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          // Eight map partitions, including one all-NULL partition. Every map emits one
          // global partial state. The small states all fit one coalesced reducer batch.
          spark
            .range(0, 80, 1, 8)
            .selectExpr(
              "CASE WHEN id < 10 THEN CAST(NULL AS BIGINT) WHEN id % 7 = 0 THEN -id ELSE id END AS v")
            .write
            .parquet(dir.getCanonicalPath)
        }
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1g",
          SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1g",
          CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_BATCH_SIZE.key -> "64") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("g07_bloom_inputs")
          withTempView("g07_bloom_inputs") {
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              assert(spark.table("g07_bloom_inputs").rdd.getNumPartitions == 8)
            }
            val query = "SELECT g07_bloom(v, 100L, 1024L) AS bf FROM g07_bloom_inputs"
            val expected = withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
              sql(query).collect()
            }
            assert(expected.length == 1 && !expected(0).isNullAt(0))
            def leaves(op: Operator): Seq[Operator] =
              if (op.getChildrenCount == 0) Seq(op)
              else op.getChildrenList.asScala.toSeq.flatMap(leaves)
            for {
              mode <- Seq("native", "jvm")
              direct <- Seq(true, false)
              coalesce <- Seq(false, true)
              (partial, last) <- Seq((true, true), (true, false), (false, true))
            } {
              withSQLConf(
                CometConf.COMET_ENABLED.key -> "true",
                // A Spark Partial emits rows. The row-input conversion first requires a
                // permitted JVM shuffle, so auto mode selects that bridge and then upgrades
                // it to a native writer. Verify the resulting writer below rather than
                // assuming the requested mode guarantees it.
                CometConf.COMET_SHUFFLE_MODE.key ->
                  (if (mode == "native" && !partial) "auto" else mode),
                CometConf.COMET_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.key -> "true",
                CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED.key ->
                  (mode == "native" && !partial).toString,
                CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> direct.toString,
                CometConf.COMET_SHUFFLE_READ_COALESCE_ENABLED.key -> coalesce.toString,
                CometConf.COMET_ENABLE_PARTIAL_HASH_AGGREGATE.key -> partial.toString,
                CometConf.COMET_ENABLE_FINAL_HASH_AGGREGATE.key -> last.toString) {
                val clue =
                  s"mode=$mode direct=$direct coalesce=$coalesce partial=$partial final=$last"
                withClue(clue) {
                  val df = sql(query)
                  val actual = df.collect()
                  assert(actual.length == 1 && !actual(0).isNullAt(0))
                  assert(java.util.Arrays
                    .equals(actual(0).getAs[Array[Byte]](0), expected(0).getAs[Array[Byte]](0)))
                  val plan = df.queryExecution.executedPlan
                  val exchange = cometExchange(plan)
                  assert(
                    exchange.shuffleType ==
                      (if (mode == "native") CometNativeShuffle else CometColumnarShuffle),
                    plan)
                  val native = collect(plan) { case a: CometHashAggregateExec => a }
                  val nativeModes = native.flatMap(_.modes).toSet
                  assert(nativeModes.contains(Partial) == partial, plan)
                  assert(nativeModes.contains(Final) == last, plan)
                  assert(
                    collect(plan) { case a: BaseAggregateExec =>
                      a
                    }.nonEmpty == (partial != last),
                    plan)
                  native.filter(_.modes.contains(Final)).foreach { agg =>
                    val inputs = leaves(agg.nativeOp)
                    assert(inputs.exists(_.hasShuffleScan) == direct, agg.nativeOp)
                    inputs.filter(_.hasShuffleScan).foreach { op =>
                      assert(op.getShuffleScan.getCoalesceBatches == coalesce, op)
                    }
                  }
                }
              }
            }
          }
        }
      }
    } finally spark.sessionState.functionRegistry.dropFunction(function)
  }

}
