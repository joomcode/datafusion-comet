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

package org.apache.spark.sql.comet.execution.shuffle

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometSortExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.Operator

/** Exercises the new multipartition JVM encoder through its actual Spark writer and readers. */
class CometShuffleWriterSemanticsSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED.key, "true")
    .set("spark.shuffle.sort.bypassMergeThreshold", "0")
    .set("spark.buffer.pageSize", "1m")
    .set(CometConf.COMET_ONHEAP_ENABLED.key, "false")
    .set("spark.memory.offHeap.size", "64m")

  private def exact(value: Any): Any = value match {
    case row: Row => row.toSeq.map(exact)
    case bytes: Array[Byte] => bytes.toSeq
    case values: Seq[_] => values.map(exact)
    case value: Double => java.lang.Double.doubleToLongBits(value)
    case value: Float => java.lang.Float.floatToIntBits(value)
    case other => other
  }

  test("G07 sorted JVM writer preserves exact rows through both readers, codecs and AQE") {
    val rows = (0 until 32).map { i =>
      val payload = if (i % 7 == 0) null else Array.tabulate[Byte](4097)(j => (i * 31 + j).toByte)
      val key = if (i % 5 == 0) None else Some(i % 4)
      val flags = if (i % 11 == 0) null else Seq(Some(true), None, Some(i % 2 == 0))
      val score = i % 4 match {
        case 0 => -0.0d
        case 1 => 0.0d
        case 2 => Double.NaN
        case _ => i.toDouble
      }
      (i, key, payload, flags, (score, if (i % 3 == 0) None else Some(s"value-$i")))
    }
    // Identical duplicates make multiplicity observable without introducing ambiguous ordering.
    withParquetTable(rows ++ rows, "g07_writer_rows") {
      // A local native sort reads this hash shuffle directly. ORDER BY would add a Spark
      // range exchange in JVM shuffle mode, hiding the direct reader behind a row boundary.
      // Compare complete rows in driver-side id order; each id has two identical copies.
      val query = """SELECT * FROM (
                    |  SELECT /*+ REPARTITION(4, _2) */ * FROM g07_writer_rows
                    |) shuffled SORT BY _1""".stripMargin
      for {
        adaptive <- Seq(false, true)
        direct <- Seq(false, true)
        coalesce <- Seq(false, true)
        codec <- Seq("lz4", "zstd")
        batch <- Seq("1", "17")
      } {
        val label = s"aqe=$adaptive direct=$direct coalesce=$coalesce codec=$codec batch=$batch"
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          // Exercise the reader rather than let the stage-cost heuristic remove its native consumer.
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false",
          CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
          CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> direct.toString,
          CometConf.COMET_SHUFFLE_READ_COALESCE_ENABLED.key -> coalesce.toString,
          CometConf.COMET_SHUFFLE_COMPRESSION_CODEC.key -> codec,
          CometConf.COMET_BATCH_SIZE.key -> batch,
          CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.key -> "11") {
          val (expected, expectedSchema) =
            withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
              val sparkOnly = sql(query)
              (sparkOnly.collect().toSeq.sortBy(_.getInt(0)).map(exact), sparkOnly.schema)
            }
          val actual = sql(query)
          val got = actual.collect().toSeq.sortBy(_.getInt(0)).map(exact)
          assert(got == expected, label)
          assert(actual.schema == expectedSchema, label)
          val plan = actual.queryExecution.executedPlan
          def leaves(op: Operator): Seq[Operator] =
            if (op.getChildrenCount == 0) Seq(op)
            else op.getChildrenList.asScala.toSeq.flatMap(leaves)
          val sorts = collect(plan) { case sort: CometSortExec => sort }
          assert(sorts.nonEmpty, s"The native sort must consume the shuffle: $label\n$plan")
          sorts.foreach { sort =>
            val inputs = leaves(sort.nativeOp)
            assert(inputs.exists(_.hasShuffleScan) == direct, s"$label\n${sort.nativeOp}")
            inputs.filter(_.hasShuffleScan).foreach { input =>
              assert(input.getShuffleScan.getCoalesceBatches == coalesce, s"$label\n$input")
            }
          }
          val exchanges = collect(plan) { case e: CometShuffleExchangeExec => e }
          assert(exchanges.nonEmpty, s"$label\n$plan")
          exchanges.foreach { exchange =>
            assert(exchange.shuffleType == CometColumnarShuffle, s"$label\n$plan")
            assert(
              exchange.shuffleDependency.shuffleHandle
                .isInstanceOf[CometSerializedShuffleHandle[_, _]],
              s"The sorted JVM writer must actually execute: $label\n$plan")
            assert(exchange.metrics("shuffleRecordsWritten").value > 0L, s"$label\n$plan")
          }
          assert(
            collect(plan) { case c: CometColumnarToRowExec => c }.nonEmpty,
            s"The JVM row boundary must execute: $label\n$plan")
          System.err.println(s"G07_SORTED_WRITER_VERIFIED\t$label\trows=${got.size}")
        }
      }
    }
  }
}
