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

package org.apache.comet.rules

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, Alias, And, Attribute, AttributeSet, Cast, Coalesce, DateAdd, DateAddInterval, DateAddYMInterval, DateSub, Expression, ExprId, GreaterThan, GreaterThanOrEqual, IsNull, LessThan, LessThanOrEqual, Or, Subtract, TimestampAddYMInterval, TruncDate, TruncTimestamp, WindowExpression}
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/**
 * Recognizes a join condition that is a single validity interval: `L <= V < U` with `V` from one
 * side, `L` and `U` from the other, `U` not derived from `L` by a constant offset, and `L` and
 * `U` read from one row of one input of the other side, with no join or union between them.
 */
object JoinConditionShape {

  private case class Bound(small: Expression, big: Expression, nullCheck: Option[Expression])

  private def unwrap(e: Expression): Expression = e match {
    case c: Cast => unwrap(c.child)
    case t: TruncTimestamp => unwrap(t.timestamp)
    case t: TruncDate => unwrap(t.date)
    case c: Coalesce if c.children.size > 1 && c.children.tail.forall(_.foldable) =>
      unwrap(c.children.head)
    case other => other
  }

  private val timeAddClasses = Set("TimeAdd", "TimestampAddInterval")

  private def offsetBase(e: Expression): Option[Expression] = e match {
    case a: Add if a.right.foldable => Some(a.left)
    case a: Add if a.left.foldable => Some(a.right)
    case s: Subtract if s.right.foldable => Some(s.left)
    case d: DateAdd if d.days.foldable => Some(d.startDate)
    case d: DateSub if d.days.foldable => Some(d.startDate)
    case t
        if timeAddClasses(t.getClass.getSimpleName) && t.children.size >= 2 &&
          t.children(1).foldable =>
      Some(t.children.head)
    case d: DateAddInterval if d.interval.foldable => Some(d.start)
    case d: DateAddYMInterval if d.interval.foldable => Some(d.date)
    case t: TimestampAddYMInterval if t.interval.foldable => Some(t.timestamp)
    case m: AddMonths if m.numMonths.foldable => Some(m.startDate)
    case _ => None
  }

  private def strip(e: Expression): Expression = {
    val u = unwrap(e)
    offsetBase(u).map(strip).getOrElse(u)
  }

  private def comparison(e: Expression): Option[(Expression, Expression)] = e match {
    case LessThan(a, b) => Some((a, b))
    case LessThanOrEqual(a, b) => Some((a, b))
    case GreaterThan(a, b) => Some((b, a))
    case GreaterThanOrEqual(a, b) => Some((b, a))
    case _ => None
  }

  private def bound(e: Expression): Option[Bound] = e match {
    case Or(IsNull(x), c) => comparison(c).map { case (s, b) => Bound(s, b, Some(x)) }
    case Or(c, IsNull(x)) => comparison(c).map { case (s, b) => Bound(s, b, Some(x)) }
    case c => comparison(c).map { case (s, b) => Bound(s, b, None) }
  }

  private def conjuncts(e: Expression): Seq[Expression] = e match {
    case And(a, b) => conjuncts(a) ++ conjuncts(b)
    case other => Seq(other)
  }

  private def same(a: Expression, b: Expression): Boolean = unwrap(a).semanticEquals(unwrap(b))

  /** The aliases defined in `plans` and below, by the id of the attribute each defines. */
  def aliases(plans: Seq[SparkPlan]): Map[ExprId, Expression] = {
    val result = mutable.Map[ExprId, Expression]()
    def visit(node: SparkPlan): Unit = {
      val operators = node match {
        case c: CometExec => Seq(c, c.originalPlan)
        case other => Seq(other)
      }
      operators
        .flatMap(_.expressions)
        .foreach(_.foreach {
          case a: Alias => result.getOrElseUpdate(a.exprId, a.child)
          case _ =>
        })
      node match {
        case s: QueryStageExec => visit(s.plan)
        case _ => node.children.foreach(visit)
      }
    }
    plans.foreach(visit)
    result.toMap
  }

  private def sources(e: Expression, aliases: Map[ExprId, Expression], depth: Int): Set[ExprId] =
    strip(e) match {
      case a: Attribute =>
        aliases.get(a.exprId) match {
          case Some(_: WindowExpression) => Set(a.exprId)
          case Some(child) if depth < 64 => sources(child, aliases, depth + 1)
          case _ => Set(a.exprId)
        }
      case other => other.references.map(_.exprId).toSet
    }

  private def inputs(node: SparkPlan): Seq[SparkPlan] = node match {
    case s: QueryStageExec => Seq(s.plan)
    case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
    case other => other.children
  }

  private def original(node: SparkPlan): SparkPlan = node match {
    case c: CometExec => c.originalPlan
    case other => other
  }

  private def outputs(node: SparkPlan, id: ExprId): Boolean = node.output.exists(_.exprId == id)

  private def producers(node: SparkPlan, id: ExprId, depth: Int): Option[Seq[List[SparkPlan]]] =
    if (depth > 256 || !outputs(node, id)) {
      None
    } else {
      node match {
        case r: ReusedExchangeExec =>
          val i = r.output.indexWhere(_.exprId == id)
          producers(r.child, r.child.output(i).exprId, depth + 1).map(_.map(r :: _))
        case _ =>
          inputs(node).find(outputs(_, id)) match {
            case Some(child) => producers(child, id, depth + 1).map(_.map(node :: _))
            case None =>
              val projected = original(node) match {
                case p: ProjectExec =>
                  p.projectList.collectFirst { case a: Alias if a.exprId == id => a.child }
                case _ => None
              }
              (projected, inputs(node)) match {
                case (Some(e), Seq(child)) if e.references.nonEmpty =>
                  val traced = e.references.toSeq.map(a => producers(child, a.exprId, depth + 1))
                  if (traced.forall(_.isDefined)) {
                    Some(traced.flatMap(_.get).map(node :: _))
                  } else {
                    None
                  }
                case _ => Some(Seq(List(node)))
              }
          }
      }
    }

  private def oneRow(side: SparkPlan, refs: AttributeSet): Boolean = {
    val traced = refs.toSeq.map(a => producers(side, a.exprId, 0))
    traced.nonEmpty && traced.forall(_.isDefined) && {
      val paths = traced.flatMap(_.get)
      val common = (0 until paths.map(_.size).min)
        .takeWhile(i => paths.forall(_(i) eq paths.head(i)))
        .size
      val below = paths.map(_.drop(common))
      val heads = below.flatMap(_.headOption)
      heads.forall(_ eq heads.head) &&
      paths.forall(_.forall(n => !original(n).isInstanceOf[UnionExec])) &&
      below.forall(_.forall(n => inputs(n).size <= 1))
    }
  }

  /**
   * Whether `condition` of a join between `left` and `right` is one validity interval whose
   * bounds `aliases` does not derive from one another and that read one row of one input.
   */
  def isValidityInterval(
      condition: Expression,
      left: SparkPlan,
      right: SparkPlan,
      aliases: Map[ExprId, Expression]): Boolean = {
    conjuncts(condition).map(bound) match {
      case Seq(Some(first), Some(second)) =>
        val shapes = Seq((first, second), (second, first)).collect {
          case (lower, upper) if same(lower.big, upper.small) && lower.nullCheck.isEmpty =>
            (upper.small, lower.small, upper.big, upper.nullCheck)
        }
        shapes.exists { case (v, l, u, nullCheck) =>
          val (vRefs, lRefs, uRefs) = (v.references, l.references, u.references)
          def onOtherSides(side: SparkPlan, other: SparkPlan): Boolean =
            vRefs.subsetOf(side.outputSet) && lRefs.subsetOf(other.outputSet) &&
              uRefs.subsetOf(other.outputSet)
          val boundsSide =
            if (onOtherSides(left, right)) Some(right)
            else if (onOtherSides(right, left)) Some(left)
            else None
          vRefs.nonEmpty && lRefs.nonEmpty && uRefs.nonEmpty &&
          nullCheck.forall(same(_, u)) &&
          sources(l, aliases, 0).intersect(sources(u, aliases, 0)).isEmpty &&
          boundsSide.exists(oneRow(_, lRefs ++ uRefs))
        }
      case _ => false
    }
  }
}
