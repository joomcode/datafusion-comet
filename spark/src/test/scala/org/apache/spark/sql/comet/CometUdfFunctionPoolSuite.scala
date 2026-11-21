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

import java.lang.ref.Reference
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, ScalaUDF}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

import org.apache.comet.{CometCodegenAssertions, CometConf}
import org.apache.comet.serde.{CometScalaUDF, ExprOuterClass, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.udf.codegen.CometUdfFunctionPool
import org.apache.comet.udf.codegen.CometUdfFunctionPool.PlanPayload

class CometUdfFunctionPoolSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private def serialize[T: ClassTag](value: T): Array[Byte] = {
    val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(value)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }
  private def deserialize[T: ClassTag](bytes: Array[Byte]): T =
    SparkEnv.get.closureSerializer
      .newInstance()
      .deserialize[T](ByteBuffer.wrap(bytes), getClass.getClassLoader)

  private def expression(function: AnyRef, deterministic: Boolean = true): ScalaUDF =
    ScalaUDF(function, LongType, Seq(Literal(1L)), udfDeterministic = deterministic)

  private def convert(expr: Expression): ExprOuterClass.Expr =
    CometScalaUDF.emitJvmCodegenDispatch(expr, Seq.empty, binding = true).get

  private def root(exprs: ExprOuterClass.Expr*): Operator =
    Operator
      .newBuilder()
      .setProjection(
        OperatorOuterClass.Projection
          .newBuilder()
          .addAllProjectList(java.util.Arrays.asList(exprs: _*)))
      .build()

  private def expressionBytes(expr: ExprOuterClass.Expr): Array[Byte] =
    expr.getJvmScalarUdf.getArgs(1).getLiteral.getBytesVal.toByteArray

  private def function(expr: ExprOuterClass.Expr): G08CapturedFunction =
    CometUdfFunctionPool
      .deserializeExpression(expressionBytes(expr), getClass.getClassLoader)
      .collect { case u: ScalaUDF => u.function.asInstanceOf[G08CapturedFunction] }
      .head

  private def inTask[A](f: TaskContext => A): A = {
    val previous = TaskContext.get()
    val context = TaskContext.empty()
    TaskContext.setTaskContext(context)
    try f(context)
    finally {
      try context.markTaskCompleted(None)
      finally {
        if (previous == null) TaskContext.unset()
        else TaskContext.setTaskContext(previous)
      }
    }
  }

  test("live task payload serializes a shared capture once and retains function identity") {
    val values = Array.tabulate(1 << 19)(_.toLong)
    val a = new G08CapturedFunction(values, 1L)
    val b = new G08CapturedFunction(values, 2L)
    val first = convert(expression(a))
    val second = convert(expression(b))
    val plan = root(first, second, first)
    val payload = CometUdfFunctionPool.capture(plan, plan.toByteArray)
    assert(payload.functionCount == 2)
    val taskBytes = serialize(Array(payload, payload))
    assert(payload.bytes.length < 64 * 1024)
    assert(taskBytes.length > values.length * 8)
    assert(
      taskBytes.length < values.length * 16,
      s"duplicate captured graph: ${taskBytes.length}")
    val restored = deserialize[Array[PlanPayload]](taskBytes)
    assert(
      restored(0) eq restored(1),
      "the outer Spark serialization stream must retain identity")
    inTask { context =>
      restored(0).register(context)
      restored(1).register(context)
      val left = function(first)
      val right = function(second)
      assert(left ne a)
      assert(right ne b)
      assert(left ne right)
      assert(left.values eq right.values)
      assert(function(first) eq left)
      assert(left(1L) == 2L && right(1L) == 3L)
    }
    Reference.reachabilityFence(a)
    Reference.reachabilityFence(b)
  }

  test("capture includes UDF references present only in late scan commons") {
    val expr = expression(new G08CapturedFunction(Array(3L, 4L), 1L))
    val dispatched = convert(expr)
    val common = OperatorOuterClass.NativeScanCommon
      .newBuilder()
      .addDataFilters(dispatched)
      .build()
    val empty = Operator.getDefaultInstance
    assert(CometUdfFunctionPool.capture(empty, empty.toByteArray).functionCount == 0)
    val payload = CometUdfFunctionPool.capture(Seq(empty, common), empty.toByteArray)
    assert(payload.functionCount == 1)
    inTask { context =>
      deserialize[PlanPayload](serialize(payload)).register(context)
      assert(function(dispatched)(1L) == 5L)
    }
    Reference.reachabilityFence(expr)
  }

  test(
    "function references stay available across GC while the original Spark expression is alive") {
    val expr = expression(new G08CapturedFunction(Array(6L, 7L), 2L))
    val dispatched = convert(expr)
    val plan = root(dispatched)
    System.gc()
    System.runFinalization()
    val payload = CometUdfFunctionPool.capture(plan, plan.toByteArray)
    assert(payload.functionCount == 1)
    inTask { context =>
      deserialize[PlanPayload](serialize(payload)).register(context)
      assert(function(dispatched)(1L) == 9L)
    }
    // Comet's originalPlan/projectList retains the ScalaUDF through driver payload capture.
    Reference.reachabilityFence(expr)
  }

  test(
    "missing task transport never resolves from the driver registry and completion clears it") {
    val expr = expression(new G08CapturedFunction(Array(8L, 9L), 1L))
    val dispatched = convert(expr)
    val plan = root(dispatched)
    val payload =
      deserialize[PlanPayload](serialize(CometUdfFunctionPool.capture(plan, plan.toByteArray)))
    inTask { context =>
      intercept[IllegalStateException] { function(dispatched) }
      payload.register(context)
      assert(function(dispatched)(1L) == 10L)
    }
    // The new empty TaskContext intentionally reuses id0: the previous task must be gone.
    inTask { _ => intercept[IllegalStateException] { function(dispatched) } }
    val completed = TaskContext.empty()
    completed.markTaskCompleted(None)
    intercept[IllegalArgumentException] { payload.register(completed) }
    inTask { _ => intercept[IllegalStateException] { function(dispatched) } }
    Reference.reachabilityFence(expr)
  }

  test("nondeterministic ScalaUDF occurrences retain distinct payload digests and wrapper tags") {
    val expr = expression(new G08CapturedFunction(Array(1L, 2L), 0L), deterministic = false)
    val a = convert(expr)
    val b = convert(expr)
    assert(
      !java.util.Arrays.equals(
        a.getJvmScalarUdf.getArgs(0).getLiteral.getBytesVal.toByteArray,
        b.getJvmScalarUdf.getArgs(0).getLiteral.getBytesVal.toByteArray))
    val plan = root(a, b)
    inTask { context =>
      deserialize[PlanPayload](serialize(CometUdfFunctionPool.capture(plan, plan.toByteArray)))
        .register(context)
      val restored =
        CometUdfFunctionPool.deserializeExpression(expressionBytes(a), getClass.getClassLoader)
      assert(restored.getClass.getSimpleName == "DispatchOccurrence")
      assert(function(a) eq function(b), "retain the actual function's shared Java identity")
    }
    Reference.reachabilityFence(expr)
  }

  test("a mutable function graph is revalidated instead of reusing serializability decisions") {
    val fn = new G08MutableFunction(Array(1L))
    val expr = expression(fn)
    assert(CometUdfFunctionPool.isSerializable(fn))
    assert(CometScalaUDF.emitJvmCodegenDispatch(expr, Seq.empty, binding = true).nonEmpty)
    fn.captured = new Object
    assert(!CometUdfFunctionPool.isSerializable(fn))
    assert(CometScalaUDF.emitJvmCodegenDispatch(expr, Seq.empty, binding = true).isEmpty)
    fn.captured = Array(2L)
    assert(CometUdfFunctionPool.isSerializable(fn))
    assert(CometScalaUDF.emitJvmCodegenDispatch(expr, Seq.empty, binding = true).nonEmpty)
    Reference.reachabilityFence(expr)
  }

  test("retry deserializes a fresh function graph instead of retaining a completed attempt") {
    val expr = expression(new G08CapturedFunction(Array(4L, 5L), 1L))
    val dispatched = convert(expr)
    val plan = root(dispatched)
    val taskBytes = serialize(CometUdfFunctionPool.capture(plan, plan.toByteArray))
    var failedAttempt: G08CapturedFunction = null
    inTask { context =>
      deserialize[PlanPayload](taskBytes).register(context)
      failedAttempt = function(dispatched)
      assert(failedAttempt(0L) == 5L)
      failedAttempt.values(0) = 900L
      assert(failedAttempt(0L) == 901L)
    }
    inTask { context =>
      deserialize[PlanPayload](taskBytes).register(context)
      val retry = function(dispatched)
      assert(retry ne failedAttempt)
      assert(retry.values ne failedAttempt.values)
      assert(retry(0L) == 5L)
    }
    Reference.reachabilityFence(expr)
  }

  test("function preflight shares one graph per fresh validation stream") {
    G08SerializationProbe.writes.set(0)
    val shared = new G08SerializationProbe
    val a = new G08ProbeFunction(shared)
    val b = new G08ProbeFunction(shared)
    assert(CometUdfFunctionPool.areSerializable(Seq(a, b, a)))
    assert(G08SerializationProbe.writes.get() == 1)
    assert(CometUdfFunctionPool.areSerializable(Seq(a, b)))
    assert(G08SerializationProbe.writes.get() == 2, "a later attempt must validate afresh")
    assert(CometUdfFunctionPool.isSerializable(a))
    assert(G08SerializationProbe.writes.get() == 3)
  }

  test("a real native project shares large UDF captures and retains Spark results") {
    // Only diagnostic IDs vary between independently planned copies of the same SQL. Strip
    // those numeric fields, leaving every expression byte and other proto field untouched.
    def stableSize(message: com.google.protobuf.Message): Int = {
      def stable(value: com.google.protobuf.Message): com.google.protobuf.Message = {
        val builder = value.toBuilder
        val fields = value.getDescriptorForType.getFields.iterator()
        while (fields.hasNext) {
          val field = fields.next()
          if (Set("plan_id", "expr_id").contains(field.getName)) {
            builder.clearField(field)
          } else if (field.getJavaType == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            if (field.isRepeated) {
              var i = 0
              while (i < value.getRepeatedFieldCount(field)) {
                builder.setRepeatedField(
                  field,
                  i,
                  stable(
                    value.getRepeatedField(field, i).asInstanceOf[com.google.protobuf.Message]))
                i += 1
              }
            } else if (value.hasField(field)) {
              builder.setField(
                field,
                stable(value.getField(field).asInstanceOf[com.google.protobuf.Message]))
            }
          }
        }
        builder.build()
      }
      stable(message).getSerializedSize
    }
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable((0L until 64L).map(Tuple1(_)), "g08_large_capture_input") {
        val query = "SELECT _1, g08_shared_a(_1), g08_shared_b(_1), g08_shared_c(_1), " +
          "g08_shared_a(_1 + 1), g08_shared_b(_1 + 2), g08_shared_c(_1 + 3), " +
          "g08_shared_a(_1 + 4), g08_shared_b(_1 + 5) FROM g08_large_capture_input"
        val sizes = Seq(1, 1 << 19).map { length =>
          val captured = Array.tabulate(length)(_.toLong)
          spark.udf.register(
            "g08_shared_a",
            (id: Long) => captured((id % captured.length).toInt) + 1)
          spark.udf.register(
            "g08_shared_b",
            (id: Long) => captured((id % captured.length).toInt) * 2)
          spark.udf.register("g08_shared_c", (id: Long) => captured.length.toLong - id)
          val df = sql(query)
          assertCodegenRan { checkSparkAnswerAndOperator(df) }
          val plans = collect(df.queryExecution.executedPlan) {
            case op: CometNativeExec if op.serializedPlanOpt.isDefined =>
              (
                op.internedNativeOp,
                CometUdfFunctionPool.capture(op.internedNativeOp, op.serializedPlanOpt.plan.get))
          }.filter(_._2.functionCount != 0)
          assert(plans.nonEmpty)
          if (length > 1) {
            plans.foreach { case (_, payload) =>
              val taskBytes = serialize(payload)
              assert(taskBytes.length > captured.length * 8)
              assert(
                taskBytes.length < captured.length * 16,
                "shared captured map must ship once")
            }
          }
          plans.map { case (op, _) => stableSize(op) }.sorted
        }
        assert(
          sizes.head == sizes.last,
          s"native proto size must be independent of captured graph size: $sizes")
      }
    }
  }

  for (nondeterministic <- Seq(false, true)) {
    test(
      s"fresh and repeated DataFrame actions snapshot mutable UDF captures: nondeterministic=$nondeterministic") {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_BATCH_SIZE.key -> "8",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
        val captured = Array(10L)
        val registered = udf((id: Long) => id + captured(0))
        spark.udf.register(
          "g08_mutable_capture",
          if (nondeterministic) registered.asNondeterministic() else registered)
        withParquetTable((0L until 32L).map(Tuple1(_)), "g08_capture_input") {
          val query = "SELECT _1 AS id, g08_mutable_capture(_1) AS v FROM g08_capture_input"
          val vanilla = withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
            val df = sql(query)
            df.queryExecution.executedPlan
            df
          }
          val accelerated = withSQLConfResult(CometConf.COMET_ENABLED.key -> "true") {
            val df = sql(query)
            df.queryExecution.executedPlan
            df
          }
          for (value <- Seq(10L, 70L, -30L)) {
            captured(0) = value
            val expected = withSQLConfResult(CometConf.COMET_ENABLED.key -> "false") {
              vanilla.collect().toSeq.sortBy(_.getLong(0))
            }
            assert(expected.map(_.getLong(1)) == (0L until 32L).map(_ + value))
            assertCodegenRan {
              assert(accelerated.collect().toSeq.sortBy(_.getLong(0)) == expected)
            }
            assert(collect(accelerated.queryExecution.executedPlan) { case p: CometProjectExec =>
              p
            }.nonEmpty)
            assertCodegenRan { checkSparkAnswerAndOperator(sql(query)) }
          }
        }
      }
    }
  }
}

private object G08SerializationProbe {
  val writes = new java.util.concurrent.atomic.AtomicInteger()
}

private class G08SerializationProbe extends Serializable {
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    G08SerializationProbe.writes.incrementAndGet()
    out.defaultWriteObject()
  }
}

private class G08ProbeFunction(val captured: G08SerializationProbe)
    extends (Long => Long)
    with Serializable {
  override def apply(value: Long): Long = value
}

/**
 * Deliberately value-equal functions require identity-based registration and object-stream
 * sharing.
 */
private class G08CapturedFunction(val values: Array[Long], val delta: Long)
    extends (Long => Long)
    with Serializable {
  override def apply(value: Long): Long = values((value % values.length).toInt) + delta
  override def equals(other: Any): Boolean = other.isInstanceOf[G08CapturedFunction]
  override def hashCode(): Int = 1
}

/** Serializable status can change with the captured object graph between planning attempts. */
private class G08MutableFunction(var captured: AnyRef) extends (Long => Long) with Serializable {
  override def apply(value: Long): Long = value
}
