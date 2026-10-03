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

import java.util.concurrent.CountDownLatch

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{FieldVector, FixedSizeBinaryVector, IntVector, LargeVarBinaryVector, VarBinaryVector}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, UnsafeProjection}
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.vector.{CometDictionary, CometDictionaryVector, CometPlainVector}

class CometBatchRowProjectionSuite extends AnyFunSuite {
  private val output = Seq(AttributeReference("payload", BinaryType, nullable = true)())
  private val payloads = Seq(
    Array[Byte](42),
    null,
    Array.emptyByteArray,
    Array[Byte](0, -1, -128, -64, 0, 127),
    Array.tabulate[Byte](192 * 1024)(i => (i * 37).toByte))

  private def binary(allocator: RootAllocator, large: Boolean = false): CometPlainVector = {
    val vector: FieldVector = if (large) {
      new LargeVarBinaryVector("payload", allocator)
    } else {
      new VarBinaryVector("payload", allocator)
    }
    vector.allocateNew()
    payloads.zipWithIndex.foreach { case (bytes, i) =>
      vector match {
        case v: VarBinaryVector => if (bytes == null) v.setNull(i) else v.setSafe(i, bytes)
        case v: LargeVarBinaryVector => if (bytes == null) v.setNull(i) else v.setSafe(i, bytes)
      }
    }
    vector.setValueCount(payloads.size)
    new CometPlainVector(vector)
  }

  for (large <- Seq(false, true); sliced <- Seq(false, true)) {
    test(s"borrowed binary preserves raw bytes and row ownership: large=$large sliced=$sliced") {
      val allocator = new RootAllocator(Long.MaxValue)
      val source = binary(allocator, large)
      val offset = if (sliced) 1 else 0
      val column = if (sliced) source.slice(offset, payloads.size - offset) else source
      val batch = new ColumnarBatch(Array[ColumnVector](column), payloads.size - offset)
      val projections = new CometBatchRowProjection(output)
      val ordinary = UnsafeProjection.create(output, output)
      try {
        val fast = projections.forBatch(batch)
        val rows = batch
          .rowIterator()
          .asScala
          .map { row =>
            val expected = ordinary(row).copy()
            val actual = fast(row).copy()
            assert(actual == expected)
            actual
          }
          .toVector
        batch.close()
        if (sliced) source.close()
        assert(allocator.getAllocatedMemory == 0)
        // No borrowed Arrow address survives in an output row, including invalid UTF-8 and null.
        rows.zip(payloads.drop(offset)).foreach { case (row, bytes) =>
          assert(row.isNullAt(0) == (bytes == null))
          if (bytes != null) assert(row.getBinary(0).sameElements(bytes))
        }
      } finally {
        column.close()
        if (sliced) source.close()
        allocator.close()
      }
    }
  }

  test("eligible binary bypasses getBinary and keeps other fields unchanged") {
    val allocator = new RootAllocator(Long.MaxValue)
    val source = binary(allocator)
    val guarded = new CometPlainVector(source.getValueVector) {
      override def getBinary(rowId: Int): Array[Byte] =
        throw new AssertionError("intermediate byte[] must not be created")
    }
    val integer = new OnHeapColumnVector(payloads.size, IntegerType)
    (0 until payloads.size).foreach(i => integer.putInt(i, i * 11))
    val mixedOutput = output :+ AttributeReference("number", IntegerType, nullable = false)()
    val batch = new ColumnarBatch(Array[ColumnVector](guarded, integer), payloads.size)
    try {
      val projection = new CometBatchRowProjection(mixedOutput).forBatch(batch)
      batch.rowIterator().asScala.zipWithIndex.foreach { case (row, i) =>
        val actual = projection(row)
        assert(actual.getInt(1) == i * 11)
        assert(actual.isNullAt(0) == (payloads(i) == null))
        if (payloads(i) != null) assert(actual.getBinary(0).sameElements(payloads(i)))
      }
    } finally {
      // guarded and source wrap the same owned Arrow vector; close that ownership once.
      batch.close()
      allocator.close()
    }
  }

  test("batch-local fallback handles Spark, dictionary and fixed-size binary vectors") {
    val allocator = new RootAllocator(Long.MaxValue)
    val values = binary(allocator)
    val indices = new IntVector("indices", allocator)
    indices.allocateNew(3)
    indices.set(0, 3)
    indices.setNull(1)
    indices.set(2, 2)
    indices.setValueCount(3)
    val dictionary =
      new CometDictionaryVector(new CometPlainVector(indices), new CometDictionary(values), null)
    val fixed = new FixedSizeBinaryVector("fixed", allocator, 4)
    fixed.allocateNew()
    fixed.setSafe(0, Array[Byte](0, -1, -128, 127))
    fixed.setNull(1)
    fixed.setSafe(2, Array[Byte](1, 2, 3, 4))
    fixed.setValueCount(3)
    val heap = new OnHeapColumnVector(3, BinaryType)
    heap.putByteArray(0, payloads(3))
    heap.putNull(1)
    heap.putByteArray(2, Array.emptyByteArray)
    val constant = new ConstantColumnVector(3, BinaryType)
    constant.setBinary(payloads(3))
    val plain = binary(allocator)
    val batches = Seq(
      new ColumnarBatch(Array[ColumnVector](plain), payloads.size),
      new ColumnarBatch(Array[ColumnVector](heap), 3),
      new ColumnarBatch(Array[ColumnVector](dictionary), 3),
      new ColumnarBatch(Array[ColumnVector](new CometPlainVector(fixed)), 3),
      new ColumnarBatch(Array[ColumnVector](constant), 3))
    try {
      val projections = new CometBatchRowProjection(output)
      val ordinary = UnsafeProjection.create(output, output)
      // Reuse the selector across mixed batches, then return to the fast path.
      (batches :+ batches.head).foreach { batch =>
        val projection = projections.forBatch(batch)
        batch.rowIterator().asScala.foreach { row =>
          val expected = ordinary(row).copy()
          assert(projection(row) == expected)
        }
      }
    } finally {
      batches.foreach(_.close())
      allocator.close()
    }
  }

  private def inTask[T](f: => T): T = {
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try f
    finally {
      context.markTaskCompleted(None)
      TaskContext.unset()
    }
  }

  private def column(dataType: DataType, values: Seq[Any]): ColumnarBatch = {
    val vector = new OnHeapColumnVector(values.size, dataType)
    values.zipWithIndex.foreach {
      case (v: Long, i) => vector.putLong(i, v)
      case (v: Double, i) => vector.putDouble(i, v)
      case (v: String, i) => vector.putByteArray(i, v.getBytes("UTF-8"))
    }
    new ColumnarBatch(Array[ColumnVector](vector), values.size)
  }

  private def project(projection: UnsafeProjection, batch: ColumnarBatch): Unit =
    batch.rowIterator().asScala.foreach(projection(_))

  private def onOtherThread[T](f: => T): T = {
    var result: Option[T] = None
    val thread = new Thread(() => result = Some(f))
    thread.start()
    thread.join()
    result.get
  }

  test("tasks reuse generated projections and never share one within a task") {
    val output = Seq(AttributeReference("n", LongType, nullable = false)())
    val references = Seq(BoundReference(0, LongType, nullable = false))
    val input = column(LongType, Seq(1L, 2L))
    try {
      val (first, second) = inTask {
        val a = new CometBatchRowProjection(output).forBatch(input)
        val b = new CometBatchRowProjection(output).forBatch(input)
        assert(a ne b)
        (a, b)
      }
      assert(CometBatchRowProjection.pooled(references) >= 2)
      inTask {
        val reused = new CometBatchRowProjection(output).forBatch(input)
        assert((reused eq first) || (reused eq second))
        val other = new CometBatchRowProjection(output).forBatch(input)
        assert(other ne reused)
      }
    } finally input.close()
  }

  test("a projection read by another thread is pooled only after that thread finishes") {
    val output = Seq(AttributeReference("d", DoubleType, nullable = false)())
    val input = column(DoubleType, Seq(1.5d, 2.5d, 3.5d))
    val acquired = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try {
      val projections = new CometBatchRowProjection(output)
      @volatile var used: UnsafeProjection = null
      val writer = new Thread(() => {
        TaskContext.setTaskContext(context)
        used = projections.forBatch(input)
        acquired.countDown()
        finish.await()
        project(used, input)
      })
      @volatile var taken: UnsafeProjection = null
      context.addTaskCompletionListener[Unit] { _ =>
        taken = onOtherThread(inTask(new CometBatchRowProjection(output).forBatch(input)))
        finish.countDown()
        writer.join()
      }
      writer.start()
      acquired.await()
      context.markTaskCompleted(None)
      assert(!writer.isAlive)
      assert(taken ne used)
      assert(onOtherThread(inTask(new CometBatchRowProjection(output).forBatch(input))) eq used)
    } finally {
      TaskContext.unset()
      input.close()
    }
  }

  test("a projection whose row buffer grew past the limit is not pooled") {
    val output = Seq(AttributeReference("s", StringType, nullable = true)())
    val small = column(StringType, Seq("a", "bc"))
    val large =
      column(StringType, Seq("x" * (CometBatchRowProjection.MaxPooledBufferBytes + 1)))
    try {
      val first = inTask {
        val projection = new CometBatchRowProjection(output).forBatch(small)
        project(projection, small)
        projection
      }
      val reused = inTask {
        val projection = new CometBatchRowProjection(output).forBatch(small)
        project(projection, large)
        projection
      }
      assert(reused eq first)
      inTask(assert(new CometBatchRowProjection(output).forBatch(small) ne reused))
    } finally {
      small.close()
      large.close()
    }
  }

  test("pooled projections keep rows of different schemas apart") {
    val point = StructType(Seq(StructField("x", IntegerType), StructField("y", StringType)))
    val schemas = Seq(
      Seq(AttributeReference("a", IntegerType)(), AttributeReference("b", StringType)()),
      Seq(AttributeReference("b", StringType)(), AttributeReference("a", IntegerType)()),
      Seq(
        AttributeReference("p", point)(),
        AttributeReference("n", LongType, nullable = false)()))
    def batch(output: Seq[AttributeReference], start: Int): ColumnarBatch = {
      val columns = output.map { a =>
        val v = new OnHeapColumnVector(3, a.dataType)
        (0 until 3).foreach { i =>
          a.dataType match {
            case IntegerType => if (i == 1) v.putNull(i) else v.putInt(i, start + i)
            case LongType => v.putLong(i, (start + i).toLong * 7)
            case StringType => v.putByteArray(i, s"s${start + i}".getBytes("UTF-8"))
            case _: StructType =>
              v.getChild(0).putInt(i, start - i)
              v.getChild(1).putByteArray(i, s"y$i".getBytes("UTF-8"))
          }
        }
        v: ColumnVector
      }
      new ColumnarBatch(columns.toArray, 3)
    }
    for (round <- 0 until 3; output <- schemas) {
      val input = batch(output, round * 10)
      try {
        val expected = UnsafeProjection.create(output, output)
        val rows = inTask {
          val projection = new CometBatchRowProjection(output).forBatch(input)
          input.rowIterator().asScala.map(row => projection(row).copy()).toVector
        }
        assert(rows == input.rowIterator().asScala.map(row => expected(row).copy()).toVector)
      } finally input.close()
    }
  }
}
