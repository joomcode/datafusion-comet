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

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoSerializer, SerializerHelper}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometConf

/** The executor task-result shape of CometBroadcastExchangeExec, without a registrator. */
class CometBroadcastKryoPayloadSuite extends AnyFunSuite {
  for (unsafe <- Seq(false, true)) {
    test(s"broadcast chunk payload round-trips with default registration: unsafe=$unsafe") {
      val conf = new SparkConf(false).set("spark.kryo.unsafe", unsafe.toString)
      val input = CometBroadcastKryoPayloadProbe.payload()
      val encoded =
        SerializerHelper.serializeToChunkedBuffer(new KryoSerializer(conf).newInstance(), input)
      try {
        val decoded =
          SerializerHelper.deserializeFromChunkedBuffer[Array[(Long, ChunkedByteBuffer)]](
            new KryoSerializer(conf).newInstance(),
            encoded)
        try {
          CometBroadcastKryoPayloadProbe.verify(decoded)
        } finally {
          decoded.foreach(_._2.dispose())
        }
      } finally {
        encoded.dispose()
        input.foreach(_._2.dispose())
      }
    }
  }
}

/** Exercises actual Arrow serialization and driver collection, not only the payload's shape. */
class CometBroadcastDefaultKryoSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "org.apache.spark.CometPlugin")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  for (adaptive <- Seq(false, true)) {
    test(s"string distinct broadcast with default Kryo registration: aqe=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.SHUFFLE_PARTITIONS.key -> "4",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        CometConf.COMET_BATCH_SIZE.key -> "1024",
        CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key -> "true") {
        val right = (0 until 200000).map { i =>
          val key = i % 100000
          (s"variant-$key", s"product-$key", s"merchant-${key % 37}")
        }
        withParquetTable(right, "default_kryo_right") {
          withParquetTable((0 until 100).map(i => (s"variant-$i", i)), "default_kryo_left") {
            val df = spark.sql(
              "SELECT /*+ BROADCAST(b) */ a._2, b._2, b._3 FROM default_kryo_left a " +
                "JOIN (SELECT DISTINCT _1, _2, _3 FROM default_kryo_right) b ON a._1 = b._1")
            assert(df.queryExecution.executedPlan.toString.contains("CometBroadcastExchange"))
            checkSparkAnswer(df)
          }
        }
      }
    }
  }
}

/** Manual two-JVM / cross-architecture probe; no SparkContext or cluster is needed. */
object CometBroadcastKryoPayloadProbe {
  private val sizes = Array(0, 1, 63, 1024, 16384, 1024 * 1024 + 17)

  def payload(): Array[(Long, ChunkedByteBuffer)] = {
    Array.tabulate(48) { i =>
      val bytes = Array.tabulate[Byte](sizes(i % sizes.length))(j => (j * 37 + i).toByte)
      val chunks = bytes.grouped(1024 * 1024).map(ByteBuffer.wrap).toArray
      (i.toLong, new ChunkedByteBuffer(chunks))
    }
  }

  def verify(actual: Array[(Long, ChunkedByteBuffer)]): Unit = {
    val expected = payload()
    try {
      assert(actual.length == expected.length)
      actual.zip(expected).foreach { case ((count, bytes), (expectedCount, expectedBytes)) =>
        assert(count == expectedCount)
        assert(java.util.Arrays.equals(bytes.toArray, expectedBytes.toArray))
      }
    } finally {
      expected.foreach(_._2.dispose())
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 3 || args.length == 4, "write|read file unsafe [serializerClass]")
    val conf = new SparkConf(false).set("spark.kryo.unsafe", args(2))
    val factory = if (args.length == 4) {
      Class
        .forName(args(3))
        .asSubclass(classOf[KryoSerializer])
        .getConstructor(classOf[SparkConf])
        .newInstance(conf)
    } else {
      new KryoSerializer(conf)
    }
    val serializer = factory.newInstance()
    if (args(0) == "write") {
      val stream = serializer.serializeStream(Files.newOutputStream(Paths.get(args(1))))
      val input = payload()
      try stream.writeObject(input)
      finally {
        stream.close()
        input.foreach(_._2.dispose())
      }
    } else {
      require(args(0) == "read")
      val stream = serializer.deserializeStream(Files.newInputStream(Paths.get(args(1))))
      val decoded =
        try stream.readObject[Array[(Long, ChunkedByteBuffer)]]()
        finally stream.close()
      try verify(decoded)
      finally decoded.foreach(_._2.dispose())
    }
    println(s"KRYO_PAYLOAD_OK ${args(0)} arch=${System.getProperty("os.arch")}")
  }
}
