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

package org.apache.comet.serde

import scala.collection.mutable

import com.google.protobuf.{CodedInputStream, CodedOutputStream, Message, WireFormat}
import com.google.protobuf.Descriptors.FieldDescriptor

import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Deduplicates `QueryContext.sql_text` across a serialized native plan.
 *
 * Every expression that Comet converts carries a `QueryContext` so that native ANSI errors can
 * render Spark's `== SQL (line N, position M) ==` block, and that block prints the *whole* query
 * text -- so the text cannot be trimmed to a fragment. But in a typical plan every expression
 * carries the same text, and embedding it per expression made the query text the overwhelming
 * majority of the serialized plan: on a 778-character query with 90 expressions, 56 KB of a 58 KB
 * plan (96%) was duplicated SQL text.
 *
 * This rewrites the tree so each distinct text appears once, in `Operator.sql_text_pool` on the
 * root operator, with each `QueryContext` referring to it by `sql_text_idx`. The plan bytes are
 * shipped in the stage's task binary, re-serialized per task by `CometExecRDD.compute` whenever
 * the plan contains a native scan, and decoded per task by the native planner, so the saving is
 * paid back on every task.
 *
 * The walk is descriptor-driven rather than a hand-written recursion over `Expr`'s ~75-way
 * `oneof`: a `QueryContext` can sit on any `Expr` or `AggExpr` at any nesting depth, and a
 * hand-written walk would silently miss newly added expression variants.
 * `QueryContextInternerSuite` asserts that no un-pooled context survives in a real plan.
 */
object QueryContextInterner {

  /**
   * Returns `op` with all `QueryContext.sql_text` values hoisted into the root's `sql_text_pool`,
   * or `op` itself when there is nothing to intern.
   */
  def intern(op: Operator): Operator = {
    // Defensive no-op so interning is idempotent. Not reachable from the single call site in
    // `CometNativeExec.convertBlock`, which always starts from the un-interned `nativeOp`.
    if (op.getSqlTextPoolCount > 0) return op

    val builder = op.toBuilder
    // Each text is appended to the pool the first time it is seen and its position becomes the
    // index every context referring to it carries, so the pool needs no separate second pass.
    val indexOf = mutable.Map.empty[String, Int]
    walk(
      builder,
      ctx => {
        val idx = indexOf.getOrElseUpdate(
          ctx.getSqlText, {
            builder.addSqlTextPool(ctx.getSqlText)
            builder.getSqlTextPoolCount - 1
          })
        Some(ctx.toBuilder.clearSqlText().setSqlTextIdx(idx).build())
      })
    if (indexOf.isEmpty) {
      // No expression carried a context (e.g. a DataFrame-API query, where Spark records no SQL
      // text). Return the original rather than rebuilding an identical message.
      op
    } else {
      builder.build()
    }
  }

  def sqlTextPool(planBytes: Array[Byte]): IndexedSeq[String] = {
    val in = CodedInputStream.newInstance(planBytes)
    val pool = IndexedSeq.newBuilder[String]
    var tag = in.readTag()
    while (tag != 0) {
      if (WireFormat.getTagFieldNumber(tag) == Operator.SQL_TEXT_POOL_FIELD_NUMBER) {
        pool += in.readString()
      } else {
        in.skipField(tag)
      }
      tag = in.readTag()
    }
    pool.result()
  }

  final class Pool(initial: Seq[String]) {
    private val indexOf = mutable.HashMap(initial.zipWithIndex: _*)
    private val extra = mutable.ArrayBuffer.empty[String]

    def index(text: String): Int =
      indexOf.getOrElseUpdate(
        text, {
          extra += text
          initial.size + extra.size - 1
        })

    def added: Seq[String] = extra.toSeq
  }

  def internAgainst[M <: Message](message: M, pool: Pool): M = {
    var changed = false
    val builder = message.toBuilder
    walk(
      builder,
      ctx =>
        if (ctx.hasSqlTextIdx) {
          Some(ctx)
        } else {
          changed = true
          Some(ctx.toBuilder.clearSqlText().setSqlTextIdx(pool.index(ctx.getSqlText)).build())
        })
    if (changed) builder.build().asInstanceOf[M] else message
  }

  def internScanCommon(
      common: OperatorOuterClass.NativeScanCommon,
      pool: Pool): OperatorOuterClass.NativeScanCommon =
    internAgainst(common, pool)

  def appendToPool(planBytes: Array[Byte], texts: Seq[String]): Array[Byte] = {
    if (texts.isEmpty) return planBytes
    val out = new java.io.ByteArrayOutputStream(planBytes.length + texts.map(_.length + 8).sum)
    out.write(planBytes)
    val coded = CodedOutputStream.newInstance(out)
    texts.foreach(coded.writeString(Operator.SQL_TEXT_POOL_FIELD_NUMBER, _))
    coded.flush()
    out.toByteArray
  }

  /**
   * Returns `expr` with every `QueryContext` in it removed.
   *
   * Used to derive content hashes that must agree between the driver and the executor even though
   * only one side sees the interned form - see `NativeScanPlanDataInjector.sourceKey`. Dropping
   * contexts makes such a hash independent of how they happen to be encoded.
   */
  def stripQueryContexts(expr: ExprOuterClass.Expr): ExprOuterClass.Expr = {
    val builder = expr.toBuilder
    walk(builder, _ => None)
    builder.build()
  }

  /**
   * Depth-first walk over every nested message, applying `f` to each `QueryContext` found on an
   * `Expr` or `AggExpr`. `f` returns the replacement context, or `None` to clear the field.
   */
  private def walk(
      builder: Message.Builder,
      f: ExprOuterClass.QueryContext => Option[ExprOuterClass.QueryContext]): Unit = {
    builder match {
      case e: ExprOuterClass.Expr.Builder if e.hasQueryContext =>
        f(e.getQueryContext) match {
          case Some(ctx) => e.setQueryContext(ctx)
          case None => e.clearQueryContext()
        }
      case a: ExprOuterClass.AggExpr.Builder if a.hasQueryContext =>
        f(a.getQueryContext) match {
          case Some(ctx) => a.setQueryContext(ctx)
          case None => a.clearQueryContext()
        }
      case _ =>
    }

    val fields = builder.getDescriptorForType.getFields.iterator()
    while (fields.hasNext) {
      val field = fields.next()
      // Map fields are repeated messages on the wire but reject `getRepeatedFieldBuilder`, and no
      // Comet map field has a message value type, so there is nothing to visit inside them.
      if (field.getJavaType == FieldDescriptor.JavaType.MESSAGE && !field.isMapField) {
        if (field.isRepeated) {
          var i = 0
          val count = builder.getRepeatedFieldCount(field)
          while (i < count) {
            walk(builder.getRepeatedFieldBuilder(field, i), f)
            i += 1
          }
        } else if (builder.hasField(field)) {
          walk(builder.getFieldBuilder(field), f)
        }
      }
    }
  }
}
