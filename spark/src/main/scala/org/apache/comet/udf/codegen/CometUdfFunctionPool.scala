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

package org.apache.comet.udf.codegen

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, ObjectOutputStream, OutputStream}
import java.lang.ref.{ReferenceQueue, WeakReference}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.comet.CometTaskContextShim
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}

import com.google.protobuf.{ByteString, Message}
import com.google.protobuf.Descriptors.FieldDescriptor

import org.apache.comet.serde.ExprOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Shares a native plan's UDF functions through Spark's task-serialization stream. Expression
 * bytes contain small stable references, while [[PlanPayload]] carries the live driver function
 * graph. Spark serializes that graph afresh for each job/stage and preserves ordinary Java object
 * identity across functions and payloads. Caching serialized function bytes across executions
 * would retain stale values when the driver updates a captured object between actions.
 */
object CometUdfFunctionPool {
  private val ExprMagic = "CMTUDFX1".getBytes("US-ASCII")
  private val nextId = new AtomicLong()
  private val collected = new ReferenceQueue[AnyRef]()
  private val idsByFunction = new java.util.HashMap[FunctionKey, java.lang.Long]()
  private val functionsById = new java.util.HashMap[java.lang.Long, FunctionKey]()
  private val tasks = new ConcurrentHashMap[java.lang.Long, TaskFunctions]()

  /** Weak identity, including when distinct function instances implement value-based equals. */
  private final class FunctionKey(value: AnyRef, queue: ReferenceQueue[AnyRef])
      extends WeakReference[AnyRef](value, queue) {
    private val hash = System.identityHashCode(value)
    override def hashCode(): Int = hash
    override def equals(other: Any): Boolean = other match {
      case key: FunctionKey =>
        (this eq key) || (get() != null && (get() eq key.get()))
      case _ => false
    }
  }

  private def reapFunctions(): Unit = {
    var key = collected.poll().asInstanceOf[FunctionKey]
    while (key != null) {
      val id = idsByFunction.remove(key)
      if (id != null) functionsById.remove(id)
      key = collected.poll().asInstanceOf[FunctionKey]
    }
  }

  private def idOf(function: AnyRef): Long = idsByFunction.synchronized {
    reapFunctions()
    val lookup = new FunctionKey(function, null)
    val old = idsByFunction.get(lookup)
    if (old != null) old.longValue()
    else {
      val key = new FunctionKey(function, collected)
      val id = java.lang.Long.valueOf(nextId.incrementAndGet())
      idsByFunction.put(key, id)
      functionsById.put(id, key)
      id.longValue()
    }
  }

  private def driverFunction(id: Long): AnyRef = idsByFunction.synchronized {
    reapFunctions()
    Option(functionsById.get(id)).flatMap(key => Option(key.get())).getOrElse {
      throw new IllegalStateException(
        s"Comet UDF function $id is no longer available on the driver")
    }
  }

  def functionsOf(expr: Expression): Seq[AnyRef] = {
    val seen = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]())
    expr.collect { case udf: ScalaUDF => udf.function }.filter(seen.add)
  }

  /**
   * Validate without allocating another serialized copy of a potentially large captured graph.
   */
  def isSerializable(function: AnyRef): Boolean = areSerializable(Seq(function))

  def areSerializable(functions: Seq[AnyRef]): Boolean = {
    if (functions.isEmpty) return true
    // A fresh stream snapshots this attempt. Sharing its handles validates a large captured
    // object once even when several functions refer to it. Never memoize across actions.
    try {
      val out = new ObjectOutputStream(NullOutputStream)
      try out.writeObject(functions.toArray[AnyRef])
      finally out.close()
      true
    } catch {
      case NonFatal(_) => false
    }
  }

  def serializeExpression(expr: Expression, functions: Seq[AnyRef]): Array[Byte] = {
    val ids = new java.util.IdentityHashMap[AnyRef, java.lang.Long]()
    functions.foreach(function => ids.put(function, java.lang.Long.valueOf(idOf(function))))
    // Replacing only ScalaUDF.function retains all encoders, child expressions, and any outer
    // DispatchOccurrence tag. The dispatcher unwraps that tag only after resolving references.
    val withRefs = expr.transform {
      case udf: ScalaUDF if ids.containsKey(udf.function) =>
        udf.copy(function = CometSharedFunctionRef(ids.get(udf.function), udf.children.size))
    }
    val header = new ByteArrayOutputStream()
    val out = new DataOutputStream(header)
    out.write(ExprMagic)
    out.writeInt(functions.length)
    functions.foreach(function => out.writeLong(ids.get(function).longValue()))
    out.flush()
    val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(withRefs)
    val result = new Array[Byte](Math.addExact(header.size(), buffer.remaining()))
    System.arraycopy(header.toByteArray, 0, result, 0, header.size())
    buffer.get(result, header.size(), buffer.remaining())
    result
  }

  def isSharedExpression(bytes: Array[Byte]): Boolean =
    bytes.length >= ExprMagic.length &&
      java.util.Arrays.equals(java.util.Arrays.copyOf(bytes, ExprMagic.length), ExprMagic)

  def deserializeExpression(bytes: Array[Byte], loader: ClassLoader): Expression = {
    val in = new ByteArrayInputStream(bytes)
    val ids = readIds(new DataInputStream(in))
    val functions = ids.map(id => id -> resolve(id)).toMap
    SparkEnv.get.closureSerializer
      .newInstance()
      .deserialize[Expression](
        ByteBuffer.wrap(bytes, bytes.length - in.available(), in.available()),
        loader)
      .transform {
        case udf: ScalaUDF if udf.function.isInstanceOf[CometSharedFunctionRef] =>
          val id = udf.function.asInstanceOf[CometSharedFunctionRef].id
          udf.copy(function = functions.getOrElse(
            id,
            throw new IllegalStateException(
              s"Comet UDF reference $id is not declared by its expression")))
      }
  }

  /**
   * This object must be captured on the DRIVER and carried explicitly in the Spark task closure.
   * Its live functions are intentionally non-transient: the outer Spark serialization stream
   * writes shared captures once and snapshots them again for a later action on the same RDD.
   * Never put this object in an executor-global broadcast or cache shared between Spark tasks.
   */
  final class PlanPayload private[CometUdfFunctionPool] (
      val bytes: Array[Byte],
      private[CometUdfFunctionPool] val ids: Vector[Long],
      private[CometUdfFunctionPool] val functions: Array[AnyRef])
      extends Serializable {
    require(ids.length == functions.length, "UDF function pool cardinality mismatch")
    def functionCount: Int = ids.length
    def register(context: TaskContext): Unit = registerPlan(context, this)
  }

  /** Capture live function identities while constructing a task payload on the driver. */
  def capture(plan: Operator, bytes: Array[Byte]): PlanPayload = capture(Seq(plan), bytes)

  /** Includes late prepared scan commons and decoded contrib payloads supplied by their owner. */
  def capture(messages: Seq[Message], bytes: Array[Byte]): PlanPayload = {
    val ids = mutable.LinkedHashSet.empty[Long]
    messages.foreach(collect(_, ids))
    val ordered = ids.toVector.sorted
    new PlanPayload(bytes, ordered, ordered.map(driverFunction).toArray)
  }

  def capture(bytes: Array[Byte]): PlanPayload = capture(Operator.parseFrom(bytes), bytes)

  private def collect(message: Message, referenced: mutable.LinkedHashSet[Long]): Unit = {
    message match {
      case udf: ExprOuterClass.JvmScalarUdf
          if udf.getClassName == classOf[CometScalaUDFCodegen].getName &&
            udf.getArgsCount > 1 && udf.getArgs(1).hasLiteral =>
        // Current dispatch uses arg0=digest, arg1=expression; old Joom used expression at0.
        val payload = udf.getArgs(1).getLiteral.getBytesVal
        if (payload.startsWith(ByteString.copyFrom(ExprMagic))) {
          referenced ++= readIds(new DataInputStream(payload.newInput()))
        }
      case _ =>
    }
    val fields = message.getDescriptorForType.getFields.iterator()
    while (fields.hasNext) {
      val field = fields.next()
      if (field.getJavaType == FieldDescriptor.JavaType.MESSAGE && !field.isMapField) {
        if (field.isRepeated) {
          var i = 0
          while (i < message.getRepeatedFieldCount(field)) {
            collect(message.getRepeatedField(field, i).asInstanceOf[Message], referenced)
            i += 1
          }
        } else if (message.hasField(field)) {
          collect(message.getField(field).asInstanceOf[Message], referenced)
        }
      }
    }
  }

  private def registerPlan(context: TaskContext, payload: PlanPayload): Unit = {
    if (payload.ids.isEmpty) return
    require(context != null, "A shared UDF task payload requires its Spark TaskContext")
    CometTaskContextShim.killTaskIfInterrupted(context)
    require(!context.isCompleted(), "Cannot register UDF functions after task completion")
    val taskId = java.lang.Long.valueOf(context.taskAttemptId())
    val fresh = new TaskFunctions
    val previous = tasks.putIfAbsent(taskId, fresh)
    val functions = if (previous == null) fresh else previous
    if (previous == null) {
      try {
        // Publish first: Spark invokes a listener immediately when completion already happened.
        // The closed flag also rejects an add/resolve racing that completion.
        context.addTaskCompletionListener[Unit] { _ =>
          fresh.close()
          tasks.remove(taskId, fresh)
        }
      } catch {
        case failure: Throwable =>
          fresh.close()
          tasks.remove(taskId, fresh)
          throw failure
      }
    }
    functions.add(payload.ids, payload.functions)
  }

  private def resolve(id: Long): AnyRef = Option(TaskContext.get()) match {
    case Some(context) =>
      CometTaskContextShim.killTaskIfInterrupted(context)
      require(!context.isCompleted(), "Cannot resolve UDF functions after task completion")
      // A driver-global fallback here hides missing transport in local tests and shares mutable
      // state between tasks. The task's deserialized payload is the only authoritative source.
      Option(tasks.get(context.taskAttemptId())).flatMap(_.resolve(id)).getOrElse {
        throw new IllegalStateException(
          s"Comet UDF function $id is not in this task's function pool")
      }
    case None => driverFunction(id) // Direct driver-only kernel tests, outside Spark execution.
  }

  private final class TaskFunctions {
    private var closed = false
    private val functions = mutable.HashMap.empty[Long, AnyRef]
    def add(ids: Vector[Long], values: Array[AnyRef]): Unit = synchronized {
      require(!closed, "Cannot register UDF functions after task completion")
      ids.zip(values).foreach { case (id, value) =>
        functions.get(id).foreach { existing =>
          require(existing eq value, s"Conflicting UDF function identity $id inside one task")
        }
      }
      functions ++= ids.zip(values)
    }
    def resolve(id: Long): Option[AnyRef] = synchronized {
      require(!closed, "Cannot resolve UDF functions after task completion")
      functions.get(id)
    }
    def close(): Unit = synchronized {
      closed = true
      functions.clear()
    }
  }

  private def readIds(in: DataInputStream): Vector[Long] = {
    val magic = new Array[Byte](ExprMagic.length)
    in.readFully(magic)
    require(java.util.Arrays.equals(magic, ExprMagic), "Not a shared Comet UDF expression")
    val count = in.readInt()
    require(count >= 0 && count <= in.available() / 8, "Invalid UDF reference count")
    val ids = Vector.fill(count)(in.readLong())
    require(ids.distinct.length == ids.length, "Duplicate UDF reference ids")
    ids
  }

  private[comet] def registeredTaskCount: Int = tasks.size()

  private object NullOutputStream extends OutputStream {
    override def write(b: Int): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
  }
}
