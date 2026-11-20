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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, CaseWhen, Coalesce, Expression, If, IsNotNull, Nondeterministic, ScalaUDF}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{canSkipEvaluation, exprToProtoInternal}

object CometIf extends CometExpressionSerde[If] {
  override def convert(
      expr: If,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!ConditionalEvaluation.maySkip(expr, Seq(expr.trueValue, expr.falseValue))) return None
    if (ConditionalEvaluation.requiresSingleJvmOccurrence(expr)) {
      // Keep the entire conditional, including Spark's primitive-UDF null guard, in one kernel.
      // This is conditional-local dispatch, not a substitute for operator-wide Spark CSE.
      if (!ConditionalEvaluation.mayDispatch(expr)) return None
      return CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
    CometScalaUDF
      .emitNullGuardDispatch(expr, inputs, binding)
      .orElse(convertNative(expr, inputs, binding))
  }

  private def convertNative(
      expr: If,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val predicateExpr = exprToProtoInternal(expr.predicate, inputs, binding)
    val trueExpr = exprToProtoInternal(expr.trueValue, inputs, binding)
    val falseExpr = exprToProtoInternal(expr.falseValue, inputs, binding)
    if (predicateExpr.isDefined && trueExpr.isDefined && falseExpr.isDefined) {
      val builder = ExprOuterClass.IfExpr.newBuilder()
      builder.setIfExpr(predicateExpr.get)
      builder.setTrueExpr(trueExpr.get)
      builder.setFalseExpr(falseExpr.get)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setIf(builder)
          .build())
    } else {
      None
    }
  }
}

object CometCaseWhen extends CometExpressionSerde[CaseWhen] {
  override def convert(
      expr: CaseWhen,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!ConditionalEvaluation.maySkip(
        expr,
        expr.branches.drop(1).map(_._1) ++ expr.branches.map(_._2) ++ expr.elseValue.toSeq)) {
      return None
    }
    if (ConditionalEvaluation.requiresSingleJvmOccurrence(expr)) {
      if (!ConditionalEvaluation.mayDispatch(expr)) return None
      return CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
    var allBranches: Seq[Expression] = Seq()
    val whenSeq = expr.branches.map(elements => {
      allBranches = allBranches :+ elements._1
      exprToProtoInternal(elements._1, inputs, binding)
    })
    val thenSeq = expr.branches.map(elements => {
      allBranches = allBranches :+ elements._2
      exprToProtoInternal(elements._2, inputs, binding)
    })
    assert(whenSeq.length == thenSeq.length)
    if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
      val builder = ExprOuterClass.CaseWhen.newBuilder()
      builder.addAllWhen(whenSeq.map(_.get).asJava)
      builder.addAllThen(thenSeq.map(_.get).asJava)
      if (expr.elseValue.isDefined) {
        val elseValueExpr =
          exprToProtoInternal(expr.elseValue.get, inputs, binding)
        if (elseValueExpr.isDefined) {
          builder.setElseExpr(elseValueExpr.get)
        } else {
          return None
        }
      }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(builder)
          .build())
    } else {
      None
    }
  }
}

object CometCoalesce extends CometExpressionSerde[Coalesce] {
  override def convert(
      expr: Coalesce,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!ConditionalEvaluation.maySkip(expr, expr.children.tail)) return None
    if (ConditionalEvaluation.requiresSingleJvmOccurrence(expr)) {
      if (!ConditionalEvaluation.mayDispatch(expr)) return None
      return CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
    val branches = expr.children.dropRight(1).map { child =>
      (IsNotNull(child), child)
    }
    val elseValue = expr.children.last
    val whenSeq = branches.map(elements => {
      exprToProtoInternal(elements._1, inputs, binding)
    })
    val thenSeq = branches.map(elements => {
      exprToProtoInternal(elements._2, inputs, binding)
    })
    assert(whenSeq.length == thenSeq.length)
    if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
      val builder = ExprOuterClass.CaseWhen.newBuilder()
      builder.addAllWhen(whenSeq.map(_.get).asJava)
      builder.addAllThen(thenSeq.map(_.get).asJava)
      val elseValueExpr = exprToProtoInternal(elseValue, inputs, binding)
      if (elseValueExpr.isDefined) {
        builder.setElseExpr(elseValueExpr.get)
      } else {
        return None
      }
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setCaseWhen(builder)
          .build())
    } else {
      None
    }
  }
}

private object ConditionalEvaluation {
  def maySkip(expr: Expression, skippable: Seq[Expression]): Boolean = {
    val allowed = canSkipEvaluation(skippable, QueryPlanSerde.mayEvaluateEagerly)
    if (!allowed) {
      withFallbackReason(expr, "Conditional shared-expression evaluation order requires Spark")
    }
    allowed
  }

  // A kernel owns this conditional's generated code. It cannot hoist or reuse a subexpression
  // with another project column the way Spark's whole-operator codegen does.
  def mayDispatch(expr: Expression): Boolean = {
    val disabled = expr.collectFirst {
      case e if QueryPlanSerde.exprSerdeMap.get(e.getClass).exists { serde =>
            !CometConf.isExprEnabled(
              serde.asInstanceOf[CometExpressionSerde[Expression]].getExprConfigName(e))
          } =>
        e
    }
    disabled.foreach { e =>
      val serde = QueryPlanSerde
        .exprSerdeMap(e.getClass)
        .asInstanceOf[CometExpressionSerde[Expression]]
      withFallbackReason(
        expr,
        "Expression support is disabled. Set " +
          s"${CometConf.getExprEnabledConfigKey(serde.getExprConfigName(e))}=true to enable it.")
    }
    disabled.isEmpty && maySkip(expr, expr.children)
  }

  // COALESCE's CASE lowering evaluates nonfinal children twice. Native CASE/IF evaluate
  // whole branch masks, which can interleave shared user/nondeterministic state differently
  // from Spark's one-row expression loop. Dispatch the entire conditional in these cases.
  def requiresSingleJvmOccurrence(expr: Expression): Boolean = {
    val occurrences = expr.collect {
      case e: ScalaUDF => e
      case e: Nondeterministic => e
    }
    expr match {
      // Coalesce's CASE lowering repeats the first successful child, even for one UDF.
      case _: Coalesce => occurrences.nonEmpty
      // One state-bearing subtree in one branch retains its row order under native masking.
      // Multiple occurrences can interleave across branches, so put them in one row kernel.
      // In particular preserve the existing optimized Spark primitive-UDF null-guard path.
      case _ => occurrences.size > 1
    }
  }
}
