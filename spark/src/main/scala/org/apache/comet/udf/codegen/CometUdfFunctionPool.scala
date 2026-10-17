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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.io.{ObjectOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.MapMaker
import com.google.protobuf.{ByteString, CodedInputStream, CodedOutputStream, Message, WireFormat}
import com.google.protobuf.Descriptors.FieldDescriptor

import org.apache.comet.serde.ExprOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometUdfFunctionPool {

  val PoolFieldNumber: Int = 99

  private val ExprMagic: Array[Byte] = "CMTUDFX1".getBytes("US-ASCII")
  private val PoolMagic: Array[Byte] = "CMTUDFP1".getBytes("US-ASCII")

  private val nextId = new AtomicLong(0)
  private val idsByFunction: java.util.concurrent.ConcurrentMap[AnyRef, java.lang.Long] =
    new MapMaker().weakKeys().makeMap()
  private val functionsById: java.util.concurrent.ConcurrentMap[java.lang.Long, AnyRef] =
    new MapMaker().weakValues().makeMap()
  private val serializable: java.util.concurrent.ConcurrentMap[AnyRef, java.lang.Boolean] =
    new MapMaker().weakKeys().makeMap()
  private val pools: Cache[Vector[Long], Array[Byte]] =
    CacheBuilder.newBuilder().softValues().maximumSize(64).build()

  private val poolBuilds = new AtomicLong(0)
  private val poolDeserializations = new AtomicLong(0)

  private val tasks = new ConcurrentHashMap[java.lang.Long, TaskPools]()

  def poolBuildCount: Long = poolBuilds.get()

  def poolDeserializationCount: Long = poolDeserializations.get()

  def functionsOf(expr: Expression): Seq[AnyRef] = {
    val seen = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]())
    expr.collect { case udf: ScalaUDF => udf.function }.filter(seen.add)
  }

  def isSerializable(function: AnyRef): Boolean =
    serializable
      .computeIfAbsent(
        function,
        f =>
          try {
            val out = new ObjectOutputStream(NullOutputStream)
            out.writeObject(f)
            out.close()
            java.lang.Boolean.TRUE
          } catch {
            case scala.util.control.NonFatal(_) => java.lang.Boolean.FALSE
          })
      .booleanValue()

  def serializeExpression(expr: Expression, functions: Seq[AnyRef]): Array[Byte] = {
    val ids = new java.util.IdentityHashMap[AnyRef, java.lang.Long]()
    functions.foreach(f => ids.put(f, idOf(f)))
    val withRefs = expr.transform {
      case udf: ScalaUDF if ids.containsKey(udf.function) =>
        udf.copy(function = CometSharedFunctionRef(ids.get(udf.function), udf.children.size))
    }
    withHeader(ExprMagic, functions.map(f => ids.get(f).longValue()), withRefs)
  }

  def isSharedExpression(bytes: Array[Byte]): Boolean = startsWith(bytes, ExprMagic)

  def deserializeExpression(bytes: Array[Byte], loader: ClassLoader): Expression = {
    val in = new ByteArrayInputStream(bytes)
    val ids = readIds(new DataInputStream(in), ExprMagic)
    val functions = ids.map(id => id -> resolve(id, loader)).toMap
    readObject[Expression](bytes, bytes.length - in.available(), loader).transform {
      case udf: ScalaUDF if udf.function.isInstanceOf[CometSharedFunctionRef] =>
        udf.copy(function = functions(udf.function.asInstanceOf[CometSharedFunctionRef].id))
    }
  }

  def attachPools(plan: Operator, planBytes: Array[Byte]): Array[Byte] = {
    if (functionsById.isEmpty) return planBytes
    val referenced = mutable.LinkedHashSet.empty[Long]
    val pooled = mutable.HashSet.empty[Long]
    collect(plan, referenced, pooled)
    val missing = referenced.filterNot(pooled.contains).toVector.sorted
    if (missing.isEmpty) return planBytes
    val functions = missing.map(id => Option(functionsById.get(id)))
    if (functions.exists(_.isEmpty)) return planBytes
    val pool = pools.get(
      missing,
      () => {
        poolBuilds.incrementAndGet()
        withHeader(PoolMagic, missing, functions.map(_.get).toArray[AnyRef])
      })
    val size = planBytes.length + CodedOutputStream.computeByteArraySize(PoolFieldNumber, pool)
    val result = java.util.Arrays.copyOf(planBytes, size)
    val coded = CodedOutputStream.newInstance(result, planBytes.length, size - planBytes.length)
    coded.writeByteArray(PoolFieldNumber, pool)
    coded.checkNoSpaceLeft()
    result
  }

  def registerPlan(context: TaskContext, planBytes: Array[Byte]): Unit = {
    if (context == null) return
    val found = mutable.ArrayBuffer.empty[Array[Byte]]
    scanPools(CodedInputStream.newInstance(planBytes), found)
    if (found.isEmpty) return
    val taskId = java.lang.Long.valueOf(context.taskAttemptId())
    val taskPools = tasks.computeIfAbsent(
      taskId,
      _ => {
        val created = new TaskPools
        context.addTaskCompletionListener[Unit](_ => tasks.remove(taskId))
        created
      })
    found.foreach(taskPools.add)
  }

  private def idOf(function: AnyRef): Long = {
    val id = idsByFunction.computeIfAbsent(function, _ => nextId.incrementAndGet())
    functionsById.putIfAbsent(id, function)
    id
  }

  private def resolve(id: Long, loader: ClassLoader): AnyRef = {
    val fromTask = Option(TaskContext.get())
      .flatMap(context => Option(tasks.get(context.taskAttemptId())))
      .flatMap(_.resolve(id, loader))
    fromTask
      .orElse(Option(functionsById.get(id)))
      .getOrElse(throw new IllegalStateException(
        s"CometScalaUDFCodegen: UDF function $id is not in any function pool of this task"))
  }

  private final class TaskPools {
    private val slots = mutable.ArrayBuffer.empty[PoolSlot]

    def add(bytes: Array[Byte]): Unit = synchronized {
      val ids = readIds(new DataInputStream(new ByteArrayInputStream(bytes)), PoolMagic)
      if (!slots.exists(_.ids == ids)) slots += new PoolSlot(ids, bytes)
    }

    def resolve(id: Long, loader: ClassLoader): Option[AnyRef] = {
      val slot = synchronized(slots.find(_.ids.contains(id)))
      slot.map(_.function(id, loader))
    }
  }

  private final class PoolSlot(val ids: Vector[Long], bytes: Array[Byte]) {
    private val functions = new AtomicReference[Map[Long, AnyRef]]()

    def function(id: Long, loader: ClassLoader): AnyRef = synchronized {
      if (functions.get() == null) {
        poolDeserializations.incrementAndGet()
        val in = new ByteArrayInputStream(bytes)
        readIds(new DataInputStream(in), PoolMagic)
        val values = readObject[Array[AnyRef]](bytes, bytes.length - in.available(), loader)
        functions.set(ids.zip(values).toMap)
      }
      functions.get()(id)
    }
  }

  private def readIds(in: DataInputStream, magic: Array[Byte]): Vector[Long] = {
    val head = new Array[Byte](magic.length)
    in.readFully(head)
    require(java.util.Arrays.equals(head, magic), "not a Comet UDF function pool payload")
    val count = in.readInt()
    Vector.fill(count)(in.readLong())
  }

  private def startsWith(bytes: Array[Byte], magic: Array[Byte]): Boolean =
    bytes.length >= magic.length &&
      java.util.Arrays.equals(java.util.Arrays.copyOf(bytes, magic.length), magic)

  private def collect(
      message: Message,
      referenced: mutable.LinkedHashSet[Long],
      pooled: mutable.HashSet[Long]): Unit = {
    message match {
      case op: Operator =>
        Option(op.getUnknownFields.getField(PoolFieldNumber)).foreach { field =>
          field.getLengthDelimitedList.forEach { pool =>
            pooled ++= readIds(new DataInputStream(pool.newInput()), PoolMagic)
          }
        }
      case udf: ExprOuterClass.JvmScalarUdf
          if udf.getClassName == classOf[CometScalaUDFCodegen].getName &&
            udf.getArgsCount > 0 && udf.getArgs(0).hasLiteral =>
        val payload = udf.getArgs(0).getLiteral.getBytesVal
        if (payload.startsWith(ByteString.copyFrom(ExprMagic))) {
          referenced ++= readIds(new DataInputStream(payload.newInput()), ExprMagic)
        }
      case _ =>
    }
    val fields = message.getDescriptorForType.getFields.iterator()
    while (fields.hasNext) {
      val field = fields.next()
      if (field.getJavaType == FieldDescriptor.JavaType.MESSAGE && !field.isMapField) {
        if (field.isRepeated) {
          val count = message.getRepeatedFieldCount(field)
          var i = 0
          while (i < count) {
            collect(message.getRepeatedField(field, i).asInstanceOf[Message], referenced, pooled)
            i += 1
          }
        } else if (message.hasField(field)) {
          collect(message.getField(field).asInstanceOf[Message], referenced, pooled)
        }
      }
    }
  }

  private def scanPools(in: CodedInputStream, found: mutable.ArrayBuffer[Array[Byte]]): Unit = {
    in.setSizeLimit(Int.MaxValue)
    var tag = in.readTag()
    while (tag != 0) {
      WireFormat.getTagFieldNumber(tag) match {
        case PoolFieldNumber
            if WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED =>
          found += in.readByteArray()
        case Operator.CHILDREN_FIELD_NUMBER
            if WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED =>
          val limit = in.pushLimit(in.readRawVarint32())
          scanPools(in, found)
          in.popLimit(limit)
        case _ =>
          in.skipField(tag)
      }
      tag = in.readTag()
    }
  }

  private object NullOutputStream extends OutputStream {
    override def write(b: Int): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
  }

  private def withHeader(magic: Array[Byte], ids: Seq[Long], obj: AnyRef): Array[Byte] = {
    val header = new ByteArrayOutputStream()
    val out = new DataOutputStream(header)
    out.write(magic)
    out.writeInt(ids.size)
    ids.foreach(id => out.writeLong(id))
    out.flush()
    val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(obj)
    val result = new Array[Byte](header.size() + buffer.remaining())
    System.arraycopy(header.toByteArray, 0, result, 0, header.size())
    buffer.get(result, header.size(), buffer.remaining())
    result
  }

  private def readObject[T: ClassTag](bytes: Array[Byte], offset: Int, loader: ClassLoader): T =
    SparkEnv.get.closureSerializer
      .newInstance()
      .deserialize[T](ByteBuffer.wrap(bytes, offset, bytes.length - offset), loader)
}
