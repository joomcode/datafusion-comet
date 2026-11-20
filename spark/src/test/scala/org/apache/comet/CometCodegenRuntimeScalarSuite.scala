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

package org.apache.comet

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatchRow
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.codegen.CometBatchKernelCodegen.ArrowColumnSpec
import org.apache.comet.vector.CometVector

/**
 * Direct batch-kernel tests complement scalar-subquery SQL without enabling struct subqueries.
 */
class CometCodegenRuntimeScalarSuite extends CometTestBase {
  private def snapshot(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case st: StructType =>
        val row = value.asInstanceOf[InternalRow]
        st.fields.zipWithIndex.map { case (f, i) =>
          snapshot(if (row.isNullAt(i)) null else row.get(i, f.dataType), f.dataType)
        }.toSeq
      case ArrayType(element, _) =>
        val array = value.asInstanceOf[ArrayData]
        (0 until array.numElements()).map { i =>
          snapshot(if (array.isNullAt(i)) null else array.get(i, element), element)
        }
      case MapType(keyType, valueType, _) =>
        val map = value.asInstanceOf[MapData]
        (0 until map.numElements()).map { i =>
          (
            snapshot(map.keyArray().get(i, keyType), keyType),
            snapshot(
              if (map.valueArray().isNullAt(i)) null else map.valueArray().get(i, valueType),
              valueType))
        }
      case BinaryType => value.asInstanceOf[Array[Byte]].toSeq
      case _: StringType => value.asInstanceOf[UTF8String].getBytes.toSeq
      case _: DecimalType => value.asInstanceOf[Decimal].toJavaBigDecimal
      case DoubleType => java.lang.Double.doubleToLongBits(value.asInstanceOf[Double])
      case FloatType => java.lang.Float.floatToIntBits(value.asInstanceOf[Float])
      case _ => value
    }
  }

  private def input(dataType: DataType, values: Seq[Any]): VectorSchemaRoot = {
    val field = CometBatchKernelCodegen.toFfiArrowField("arg", dataType, nullable = true)
    val root =
      VectorSchemaRoot.create(new Schema(java.util.Arrays.asList(field)), CometArrowAllocator)
    root.allocateNew()
    val writer = ArrowWriter.create(root)
    values.foreach(value => writer.write(new GenericInternalRow(Array(value))))
    writer.finish()
    root
  }

  private def results(output: FieldVector, numRows: Int, dataType: DataType): Seq[Any] = {
    output.setValueCount(numRows)
    val column = CometVector.getVector(output, null)
    val row = new ColumnarBatchRow(Array(column))
    (0 until numRows).map { i =>
      row.rowId = i
      snapshot(if (row.isNullAt(0)) null else row.get(0, dataType), dataType)
    }
  }

  test(
    "one compiled kernel refreshes scalar/vector masks, values, nested NULLs and empty batches") {
    withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
      val decimal = DecimalType(6, -2)
      val array = ArrayType(StringType, containsNull = true)
      val map =
        MapType(StringType, ArrayType(IntegerType, containsNull = true), valueContainsNull = true)
      val struct = StructType(
        Seq(
          StructField("flag", BooleanType),
          StructField("items", array),
          StructField("money", decimal)))
      val a = new GenericArrayData(
        Array[Any](UTF8String.fromString("a"), null, UTF8String.fromString("")))
      val b = new GenericArrayData(Array[Any](UTF8String.fromString("b")))
      val firstDecimal = Decimal(new java.math.BigDecimal("12300").setScale(-2))
      val secondDecimal = Decimal(new java.math.BigDecimal("-4500").setScale(-2))
      val firstMap = new ArrayBasedMapData(
        new GenericArrayData(Array[Any](UTF8String.fromString("x"), UTF8String.fromString("y"))),
        new GenericArrayData(Array[Any](new GenericArrayData(Array[Any](1, null, 3)), null)))
      val secondMap = new ArrayBasedMapData(
        new GenericArrayData(Array[Any](UTF8String.fromString("z"))),
        new GenericArrayData(Array[Any](new GenericArrayData(Array[Any](7)))))
      val cases = Seq[(DataType, Any, Any)](
        (BooleanType, true, false),
        (BinaryType, Array[Byte](0, -1, -128), Array.emptyByteArray),
        (decimal, firstDecimal, secondDecimal),
        (array, a, b),
        (map, firstMap, secondMap),
        (
          struct,
          new GenericInternalRow(Array[Any](true, a, firstDecimal)),
          new GenericInternalRow(Array[Any](false, null, secondDecimal))))
      cases.foreach { case (dataType, first, second) =>
        val expr = BoundReference(0, dataType, nullable = true)
        val initial = input(dataType, Seq(first))
        val spec = CometCodegenRuntimeScalarSuite.spec(initial.getVector(0))
        val kernel = CometBatchKernelCodegen.compile(expr, IndexedSeq(spec)).newInstance()
        kernel.init(0)
        initial.close()
        val steps = Seq(
          (Seq(first), 3),
          (Seq(first, null, second), 3),
          (Seq(null), 4),
          (Seq(second), 5),
          (Seq.empty[Any], 0),
          (Seq(first), 2))
        steps.foreach { case (values, n) =>
          val root = input(dataType, values)
          val out = CometBatchKernelCodegen.allocateOutput(
            CometBatchKernelCodegen.toFfiArrowField("out", dataType, nullable = true),
            n,
            0)
          try {
            kernel.process(Array(root.getVector(0)), out, n)
            val expected = (0 until n).map { i =>
              val source = values(if (values.length == 1) 0 else i)
              snapshot(expr.eval(new GenericInternalRow(Array(source))), dataType)
            }
            assert(
              results(out, n, dataType) == expected,
              s"type=$dataType values=${values.length} rows=$n")
          } finally {
            out.close()
            root.close()
          }
        }
      }
    }
  }

  test("nullable scalar input uses the same mask in the input-null fast path and typed getter") {
    val expr: Expression = Add(
      BoundReference(0, IntegerType, nullable = true),
      BoundReference(1, IntegerType, nullable = true))
    val spec = ArrowColumnSpec(classOf[org.apache.arrow.vector.IntVector], nullable = true)
    val kernel = CometBatchKernelCodegen.compile(expr, IndexedSeq(spec, spec)).newInstance()
    kernel.init(0)
    Seq[Any](10, null, 70).foreach { scalar =>
      val left = input(IntegerType, Seq(scalar))
      val values = Seq[Any](1, null, 3)
      val right = input(IntegerType, values)
      val out = CometBatchKernelCodegen.allocateOutput(
        CometBatchKernelCodegen.toFfiArrowField("out", IntegerType, nullable = true),
        3,
        0)
      try {
        kernel.process(Array(left.getVector(0), right.getVector(0)), out, 3)
        val expected =
          values.map(value => expr.eval(new GenericInternalRow(Array(scalar, value))))
        assert(results(out, 3, IntegerType) == expected)
      } finally {
        out.close()
        right.close()
        left.close()
      }
    }
  }

  test("a malformed input count is rejected before unsafe reads") {
    val expr = BoundReference(0, IntegerType, nullable = true)
    val spec = ArrowColumnSpec(classOf[org.apache.arrow.vector.IntVector], nullable = true)
    val kernel = CometBatchKernelCodegen.compile(expr, IndexedSeq(spec)).newInstance()
    kernel.init(0)
    val root = input(IntegerType, Seq(1, 2))
    val out = CometBatchKernelCodegen.allocateOutput(
      CometBatchKernelCodegen.toFfiArrowField("out", IntegerType, nullable = true),
      3,
      0)
    try {
      val error = intercept[IllegalArgumentException] {
        kernel.process(Array(root.getVector(0)), out, 3)
      }
      assert(error.getMessage.contains("expected a scalar or 3"))
    } finally {
      out.close()
      root.close()
    }
  }
}

object CometCodegenRuntimeScalarSuite {
  // Derive the typed Arrow shape from the concrete vectors, as the runtime dispatcher does.
  def spec(
      vector: org.apache.arrow.vector.ValueVector): CometBatchKernelCodegen.ArrowColumnSpec = {
    import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}
    import org.apache.spark.sql.comet.util.Utils
    vector match {
      case m: org.apache.arrow.vector.complex.MapVector =>
        val entries = m.getDataVector.asInstanceOf[org.apache.arrow.vector.complex.StructVector]
        val k = entries.getChildByOrdinal(0)
        val v = entries.getChildByOrdinal(1)
        MapColumnSpec(
          true,
          Utils.fromArrowField(k.getField),
          Utils.fromArrowField(v.getField),
          spec(k),
          spec(v))
      case l: org.apache.arrow.vector.complex.ListVector =>
        ArrayColumnSpec(
          true,
          Utils.fromArrowField(l.getDataVector.getField),
          spec(l.getDataVector))
      case s: org.apache.arrow.vector.complex.StructVector =>
        StructColumnSpec(
          true,
          (0 until s.size()).map { i =>
            val v = s.getChildByOrdinal(i)
            StructFieldSpec(
              v.getName,
              Utils.fromArrowField(v.getField),
              v.getField.isNullable,
              spec(v))
          })
      case v =>
        ScalarColumnSpec(
          v.getClass.asInstanceOf[Class[_ <: org.apache.arrow.vector.ValueVector]],
          true)
    }
  }
}
