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

import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.comet.CometPlan
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, InputAdapter, RowToColumnarTransition, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}

/** Reads shuffle formats and the engines around them off an executed plan. */
object BoundaryTestHelpers {

  /** A shuffle with the operator reading it and the operator writing it. */
  case class Edge(
      consumer: Option[SparkPlan],
      exchange: ShuffleExchangeLike,
      producer: SparkPlan) {
    def format: String = exchange match {
      case e: CometShuffleExchangeExec if e.shuffleType == CometNativeShuffle => "native"
      case e: CometShuffleExchangeExec if e.shuffleType == CometColumnarShuffle => "columnar"
      case _ => "spark"
    }
    def hash: String = if (format == "native") "comet" else "spark"
    def consumerIsComet: Boolean = consumer.exists(isCometOperator)
    def producerIsComet: Boolean = isCometOperator(producer)
  }

  def finalPlan(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => finalPlan(a.executedPlan)
    case other => other
  }

  private def isWrapper(plan: SparkPlan): Boolean = plan match {
    case _: ColumnarToRowTransition | _: RowToColumnarTransition | _: InputAdapter |
        _: WholeStageCodegenExec | _: AQEShuffleReadExec | _: QueryStageExec |
        _: ReusedExchangeExec =>
      true
    case _ => false
  }

  def isCometOperator(plan: SparkPlan): Boolean =
    plan.isInstanceOf[CometPlan] && !isWrapper(plan) && !plan.isInstanceOf[ShuffleExchangeLike]

  private def unwrap(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => unwrap(a.executedPlan)
    case stage: QueryStageExec => unwrap(stage.plan)
    case reused: ReusedExchangeExec => unwrap(reused.child)
    case w if isWrapper(w) && w.children.size == 1 => unwrap(w.children.head)
    case other => other
  }

  def edges(plan: SparkPlan): Seq[Edge] = {
    val seen = new java.util.IdentityHashMap[SparkPlan, Unit]()
    def visit(node: SparkPlan, consumer: Option[SparkPlan]): Seq[Edge] = {
      val real = unwrap(node)
      real match {
        case e: ShuffleExchangeLike =>
          if (seen.containsKey(e)) {
            Seq(Edge(consumer, e, unwrap(e.child)))
          } else {
            seen.put(e, ())
            Edge(consumer, e, unwrap(e.child)) +: visit(e.child, None)
          }
        case other =>
          other.children.flatMap(visit(_, Some(other)))
      }
    }
    visit(finalPlan(plan), None)
  }

  /** The shuffles read by each co-partitioned join, found within the join's stage. */
  def joinInputs(plan: SparkPlan): Seq[Seq[Edge]] = {
    val all = edges(plan)
    def stageShuffles(node: SparkPlan): Seq[ShuffleExchangeLike] = unwrap(node) match {
      case e: ShuffleExchangeLike => Seq(e)
      case _: BroadcastExchangeLike => Nil
      case other => other.children.flatMap(stageShuffles)
    }
    def joins(node: SparkPlan): Seq[SparkPlan] = {
      val real = unwrap(node)
      val here = real match {
        case j: SortMergeJoinExec => Seq(j)
        case j: ShuffledHashJoinExec => Seq(j)
        case j if j.getClass.getSimpleName.matches("Comet(SortMergeJoin|HashJoin)Exec") => Seq(j)
        case _ => Nil
      }
      here ++ real.children.flatMap(joins)
    }
    joins(finalPlan(plan)).map { j =>
      val shuffles = j.children.flatMap(stageShuffles)
      shuffles
        .flatMap(s => all.find(_.exchange eq s))
        .filter(_.exchange.outputPartitioning match {
          case _: HashPartitioning => true
          case _ => false
        })
    }
  }

  /** Comet operators other than shuffles and transitions, by name. */
  def cometOperatorNames(plan: SparkPlan): Seq[String] = {
    def visit(node: SparkPlan): Seq[String] = {
      val here = if (isCometOperator(node)) Seq(node.nodeName) else Nil
      val children = node match {
        case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
        case stage: QueryStageExec => Seq(stage.plan)
        case other => other.children
      }
      here ++ children.flatMap(visit)
    }
    visit(finalPlan(plan)).sorted
  }
}
