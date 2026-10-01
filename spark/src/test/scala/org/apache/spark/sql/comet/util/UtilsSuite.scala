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

package org.apache.spark.sql.comet.util

import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.{BitVectorHelper, IntVector, VarCharVector}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

class UtilsSuite extends CometTestBase {

  test("serializeBatches preserves row count for a zero-column batch") {
    val numRows = 5
    val batch = new ColumnarBatch(Array.empty[ColumnVector], numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val decoded = Utils.decodeBatches(buf, "test").toSeq
    assert(decoded.map(_.numRows()).sum == numRows)
  }

  test("coalesceBroadcastBatches preserves row count across zero-column inputs") {
    val numRows = 5
    val numBatches = 3
    val batches =
      (0 until numBatches).map(_ => new ColumnarBatch(Array.empty[ColumnVector], numRows))

    val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq.iterator
    val (coalesced, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs)

    val expected = numRows.toLong * numBatches
    assert(batchCount == numBatches)
    assert(totalRows == expected)

    val decoded = coalesced.iterator.flatMap(b => Utils.decodeBatches(b, "test")).toSeq
    assert(decoded.map(_.numRows()).sum == expected)
  }

  test("coalesceBroadcastBatches rebases the offsets of batches sliced from one batch") {
    val numRows = 12
    val sliceLength = 4
    val strings = (0 until numRows).map(i => s"value_$i")
    val lists = (0 until numRows).map(i => (0 to i % 3).map(_ + i))

    val parentStrings = new VarCharVector("s", CometArrowAllocator)
    parentStrings.allocateNew()
    strings.zipWithIndex.foreach { case (v, i) => parentStrings.setSafe(i, v.getBytes("UTF-8")) }
    parentStrings.setValueCount(numRows)

    val parentLists = ListVector.empty("l", CometArrowAllocator)
    val writer = parentLists.getWriter
    lists.zipWithIndex.foreach { case (values, i) =>
      writer.setPosition(i)
      writer.startList()
      values.foreach(writer.integer().writeInt(_))
      writer.endList()
    }
    parentLists.setValueCount(numRows)
    val parentElements = parentLists.getDataVector.asInstanceOf[IntVector]

    def allValid(length: Int): ArrowBuf = {
      val validity = CometArrowAllocator.buffer(((length + 7) / 8).toLong)
      (0 until length).foreach(i => BitVectorHelper.setBit(validity, i.toLong))
      validity
    }

    def sliceOffsets(offsets: ArrowBuf, start: Int, length: Int): ArrowBuf =
      offsets.slice(start.toLong * 4, (length + 1).toLong * 4)

    val starts = 0 until numRows by sliceLength
    val sliced = starts.map { start =>
      val validity = allValid(sliceLength)
      val stringSlice = new VarCharVector("s", CometArrowAllocator)
      stringSlice.loadFieldBuffers(
        new ArrowFieldNode(sliceLength, 0),
        java.util.Arrays.asList(
          validity,
          sliceOffsets(parentStrings.getOffsetBuffer, start, sliceLength),
          parentStrings.getDataBuffer))

      val listSlice = ListVector.empty("l", CometArrowAllocator)
      listSlice.addOrGetVector[IntVector](FieldType.nullable(new ArrowType.Int(32, true)))
      listSlice.loadFieldBuffers(
        new ArrowFieldNode(sliceLength, 0),
        java.util.Arrays
          .asList(validity, sliceOffsets(parentLists.getOffsetBuffer, start, sliceLength)))
      val elementCount = parentElements.getValueCount
      val elementValidity = allValid(elementCount)
      listSlice.getDataVector.loadFieldBuffers(
        new ArrowFieldNode(elementCount, 0),
        java.util.Arrays.asList(elementValidity, parentElements.getDataBuffer))
      elementValidity.close()
      validity.close()
      (stringSlice, listSlice)
    }

    try {
      assert(sliced(1)._1.getOffsetBuffer.getInt(0) > 0)
      assert(sliced(1)._2.getOffsetBuffer.getInt(0) > 0)
      val batches = sliced.map { case (stringSlice, listSlice) =>
        val provider = new CDataDictionaryProvider
        new ColumnarBatch(
          Array[ColumnVector](
            CometVector.getVector(stringSlice, provider),
            CometVector.getVector(listSlice, provider)),
          sliceLength)
      }
      val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq.iterator
      val (coalesced, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs)
      assert(batchCount == starts.size)
      assert(totalRows == numRows)

      val got = coalesced.iterator.flatMap { b =>
        Utils.decodeBatches(b, "test").flatMap { out =>
          (0 until out.numRows()).map { i =>
            (out.column(0).getUTF8String(i).toString, out.column(1).getArray(i).toIntArray.toSeq)
          }
        }
      }.toSeq
      assert(got == strings.zip(lists))
    } finally {
      sliced.foreach { case (stringSlice, listSlice) =>
        stringSlice.close()
        listSlice.close()
      }
      parentLists.close()
      parentStrings.close()
    }
  }

  test("serializeBatches materializes ConstantColumnVector columns") {
    // Spark wraps file-source partition columns and other per-batch constants in
    // ConstantColumnVector. When such a batch reaches Comet's serialization/export path
    // (getBatchFieldVectors), it must be materialized to an Arrow vector rather than
    // rejected with "Comet execution only takes Arrow Arrays".
    val numRows = 4

    val valueCol = new ConstantColumnVector(numRows, IntegerType)
    valueCol.setInt(42)
    val nullCol = new ConstantColumnVector(numRows, IntegerType)
    nullCol.setNull()
    val batch = new ColumnarBatch(Array[ColumnVector](valueCol, nullCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    // Read the decoded values eagerly: ArrowReaderIterator releases a batch's buffers once the
    // iterator advances past it (hasNext closes the previous batch), so values must be read from
    // the current batch before calling hasNext/next again.
    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val values = (0 until numRows).map(i => out.column(0).getInt(i))
    val nulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(values.forall(_ == 42), s"expected all 42, got $values")
    assert(nulls.forall(identity), s"expected all null, got $nulls")
  }

  test("serializeBatches materializes a TimestampType ConstantColumnVector") {
    // Covers the TimestampType materialize path (TimestampWriter -> TimeStampMicroTZVector) and
    // pins down the "UTC" timezone choice in materializeConstantColumnVector: Spark stores
    // TimestampType as micros in UTC, and Comet tags its timestamp Arrow vectors "UTC", so the
    // constant micros round-trip unchanged. This guards against anyone later swapping the zone
    // argument, which would make the materialised constant's Arrow field metadata diverge from the
    // sibling non-constant timestamp columns it shares a VectorSchemaRoot with.
    val numRows = 3
    // 2023-11-14T22:13:20Z in micros since epoch.
    val micros = 1700000000000000L

    val tsCol = new ConstantColumnVector(numRows, TimestampType)
    tsCol.setLong(micros)
    val batch = new ColumnarBatch(Array[ColumnVector](tsCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 1)
    assert(out.numRows() == numRows)
    val got = (0 until numRows).map(i => out.column(0).getLong(i))
    assert(!it.hasNext)

    assert(got.forall(_ == micros), s"expected all $micros, got $got")
  }

  test("serializeBatches materializes a nullable StructType ConstantColumnVector") {
    // Exercises a different ArrowFieldWriter path than the scalar cases: a struct constant is
    // written via getStruct(rowId) -> getChild(ordinal). Covers both a non-null struct (with a
    // null nested field) and a wholly-null struct constant.
    val numRows = 3
    val schema = StructType(
      Seq(StructField("id", IntegerType), StructField("name", StringType, nullable = true)))

    // Non-null struct whose `name` field is null, proving nested nullability round-trips.
    val structCol = new ConstantColumnVector(numRows, schema)
    structCol.setNotNull()
    val idChild = new ConstantColumnVector(numRows, IntegerType)
    idChild.setInt(7)
    val nameChild = new ConstantColumnVector(numRows, StringType)
    nameChild.setNull()
    structCol.setChild(0, idChild)
    structCol.setChild(1, nameChild)

    // A wholly-null struct constant.
    val nullStructCol = new ConstantColumnVector(numRows, schema)
    nullStructCol.setNull()
    nullStructCol.setChild(0, new ConstantColumnVector(numRows, IntegerType))
    nullStructCol.setChild(1, new ConstantColumnVector(numRows, StringType))

    val batch =
      new ColumnarBatch(Array[ColumnVector](structCol, nullStructCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val ids = (0 until numRows).map(i => out.column(0).getStruct(i).getInt(0))
    val nameNulls = (0 until numRows).map(i => out.column(0).getStruct(i).isNullAt(1))
    val structNulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(ids.forall(_ == 7), s"expected all id 7, got $ids")
    assert(nameNulls.forall(identity), s"expected all name null, got $nameNulls")
    assert(structNulls.forall(identity), s"expected all struct null, got $structNulls")
  }

  test("isArrowBacked rejects large-offset Arrow vectors") {
    // A CometPlainVector can wrap a LargeVarCharVector or LargeVarBinaryVector -- an accelerated
    // mapInArrow returning pa.large_string() produces one -- but getFieldVector rejects both. If
    // isArrowBacked accepted them, a caller would take the direct write path and then fail, so it
    // must report false and let the caller convert the batch instead.
    val numRows = 2
    Seq[org.apache.arrow.vector.FieldVector](
      {
        val v = new org.apache.arrow.vector.LargeVarCharVector("s", CometArrowAllocator)
        v.allocateNew(numRows)
        v.setSafe(0, "hello".getBytes("UTF-8"))
        v.setSafe(1, "world".getBytes("UTF-8"))
        v.setValueCount(numRows)
        v
      }, {
        val v = new org.apache.arrow.vector.LargeVarBinaryVector("b", CometArrowAllocator)
        v.allocateNew(numRows)
        v.setSafe(0, "hello".getBytes("UTF-8"))
        v.setSafe(1, "world".getBytes("UTF-8"))
        v.setValueCount(numRows)
        v
      }).foreach { vector =>
      try {
        val col = CometVector.getVector(vector, new CDataDictionaryProvider)
        val batch = new ColumnarBatch(Array[ColumnVector](col), numRows)
        assert(
          !Utils.isArrowBacked(batch),
          s"${vector.getClass.getSimpleName} must not be reported as directly writable")
      } finally {
        vector.close()
      }
    }
  }
}
