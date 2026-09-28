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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometBaseAggregate, CometNativeExec, CometPlan, CometScanWrapper, CometSinkPlaceHolder, CometSortExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason

/**
 * Reverts a single native operator to Spark when its output goes straight to a Spark operator
 * through a columnar-to-row (C2R) transition, and running it natively gains nothing:
 *
 *   - every input comes from Spark rows through a row-to-columnar (R2C) transition. The operator
 *     is an island between Spark operators, and reverting it removes both transitions.
 *   - the operator is a sort. Spark sorts row pointers and writes each spilled row once, while
 *     the native sort moves the rows through every sort, spill and merge step, and its sorted
 *     batches are then converted to rows for the Spark consumer anyway. Reverting moves the C2R
 *     below the sort and leaves its native producer untouched.
 *
 * This is [[RevertNativeForTransitionHeavyStages]] at the granularity of one operator: the rest
 * of the stage stays native. Aggregates are never reverted, since a native partial feeding a
 * Spark final, or the reverse, is not always safe.
 */
case class RevertIsolatedNativeOperators(session: SparkSession)
    extends Rule[SparkPlan]
    with Logging {

  private def enabled = CometConf.COMET_EXEC_REVERT_ISOLATED_OPERATORS_ENABLED.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!enabled) return plan
    plan.transformUp { case c2r: ColumnarToRowTransition =>
      c2r.children match {
        case Seq(op: CometNativeExec) => revert(op).getOrElse(c2r)
        case _ => c2r
      }
    }
  }

  private sealed trait Input
  private case class FromRows(rows: SparkPlan) extends Input
  private case class FromColumnar(columnar: SparkPlan) extends Input

  private def input(child: SparkPlan): Option[Input] = child match {
    case r2c: RowToColumnarTransition if !r2c.children.head.supportsColumnar =>
      Some(FromRows(r2c.children.head))
    case CometSinkPlaceHolder(_, _, source) if source.supportsColumnar =>
      Some(FromColumnar(source))
    case wrapper: CometScanWrapper if wrapper.originalPlan.supportsColumnar =>
      Some(FromColumnar(wrapper.originalPlan))
    case _: CometNativeExec => None
    case columnar if columnar.supportsColumnar => Some(FromColumnar(columnar))
    case _ => None
  }

  private lazy val transitions = EliminateRedundantTransitions(session)

  private def producesCometBatches(plan: SparkPlan): Boolean = plan match {
    case stage: QueryStageExec => producesCometBatches(stage.plan)
    case read: AQEShuffleReadExec => producesCometBatches(read.child)
    case reused: ReusedExchangeExec => producesCometBatches(reused.child)
    case _: CometPlan => true
    case _ => false
  }

  private def columnarToRow(columnar: SparkPlan): SparkPlan =
    if (producesCometBatches(columnar)) transitions.createColumnarToRowExec(columnar)
    else ColumnarToRowExec(columnar)

  private[rules] def revert(op: CometNativeExec): Option[SparkPlan] = {
    if (op.children.isEmpty || op.isInstanceOf[CometBaseAggregate]) return None
    if (op.originalPlan.children.size != op.children.size) return None
    val inputs = op.children.map(input)
    if (inputs.exists(_.isEmpty)) return None
    val resolved = inputs.flatten
    val islandOfRows = resolved.forall(_.isInstanceOf[FromRows])
    if (!islandOfRows && !op.isInstanceOf[CometSortExec]) return None

    val newChildren = resolved.map {
      case FromRows(rows) => rows
      case FromColumnar(columnar) => columnarToRow(columnar)
    }
    val reverted = op.originalPlan.withNewChildren(newChildren)
    if (reverted.supportsColumnar) return None
    val reason = if (islandOfRows) {
      "Reverted: native operator between Spark operators only adds transitions"
    } else {
      "Reverted: native sort feeding a Spark operator; Spark sorts row pointers"
    }
    logDebug(s"$reason: ${op.nodeName}")
    Some(withFallbackReason(reverted, reason))
  }
}
