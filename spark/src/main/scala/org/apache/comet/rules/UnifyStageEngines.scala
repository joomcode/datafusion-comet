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
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometExec, CometIcebergWriteExec, CometNativeWriteExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason

/**
 * Runs each query stage wholly in Comet or wholly in Spark, then picks the format of each stage
 * boundary from the engines on its two sides, so that data changes format only where the engine
 * changes.
 *
 * Phase 1 classifies the stages. A stage whose operators all run in Comet stays native. A stage
 * that mixes Comet and Spark operators would convert between columns and rows inside the stage,
 * so all of it runs in Spark. Leaf scans do not count: a native scan read through one
 * columnar-to-row transition costs what Spark's vectorized reader does. Neither do writes, which
 * consume the stage's output whichever engine produced it.
 *
 * Phase 2 picks each boundary:
 *   - Comet producer: native shuffle. A Spark consumer converts once, when it reads.
 *   - Spark producer, Comet consumer: columnar shuffle, which converts once, when it writes.
 *   - Spark producer, Spark consumer: Spark shuffle, with no conversion. Comet's columnar shuffle
 *     there would convert rows to Arrow when writing and back when reading.
 *   - A broadcast feeding a Spark join is a Spark broadcast, since Spark cannot read Comet's.
 *
 * A stage stays mixed when reverting it is unsafe or has no Spark equivalent: a native aggregate
 * whose buffer Spark cannot exchange across the stage boundary, a native write, a build side
 * feeding a Comet broadcast, or a native shuffle into a Comet consumer that a columnar shuffle
 * cannot replace.
 *
 * The rule needs the consumer of each boundary, so it runs on whole plans only: the initial plan
 * and each re-optimization under AQE, and the plan without AQE. Its decisions are tagged so that
 * the per-stage conversion under AQE leaves them in place.
 */
case class UnifyStageEngines(session: SparkSession) extends Rule[SparkPlan] with Logging {

  private def enabled = CometConf.COMET_EXEC_UNIFY_STAGE_ENGINES_ENABLED.get()

  private lazy val stageRevert = RevertNativeForTransitionHeavyStages(session)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!enabled) return plan
    // A subquery or a query stage can be rooted at a boundary whose consumer is outside this
    // plan, so its format is kept; only the stages below it are unified.
    if (isBoundary(plan)) root(plan) else stage(plan, output = RowOutput)
  }

  private def root(boundary: SparkPlan): SparkPlan = boundary match {
    case exchange: CometShuffleExchangeExec =>
      exchange.withNewChildren(Seq(stage(exchange.child, KeptOutput)))
    case broadcast: CometBroadcastExchangeExec =>
      broadcast.withNewChildren(Seq(stage(broadcast.child, CometBroadcastOutput)))
    case exchange @ (_: ShuffleExchangeExec | _: BroadcastExchangeExec) =>
      exchange.withNewChildren(Seq(stage(exchange.children.head, RowOutput)))
    case other => other
  }

  /** What consumes a stage's output. */
  private sealed trait Output
  private case object RowOutput extends Output
  private case class ShuffleOutput(exchange: CometShuffleExchangeExec, consumerIsComet: Boolean)
      extends Output
  private case object CometBroadcastOutput extends Output

  /** A boundary whose format is kept, since its consumer is not in the plan. */
  private case object KeptOutput extends Output

  private def isBoundary(plan: SparkPlan): Boolean = plan match {
    case _: ShuffleExchangeLike | _: BroadcastExchangeLike | _: QueryStageExec |
        _: ReusedExchangeExec | _: AQEShuffleReadExec =>
      true
    case _ => false
  }

  private def isWrite(plan: SparkPlan): Boolean = plan match {
    case _: DataWritingCommandExec | _: WriteFilesExec | _: V2TableWriteExec |
        _: CometNativeWriteExec | _: CometIcebergWriteExec =>
      true
    case _ => false
  }

  private def isCometOperator(plan: SparkPlan): Boolean =
    plan.isInstanceOf[CometExec] && plan.children.nonEmpty && !isWrite(plan)

  private def isSparkOperator(plan: SparkPlan): Boolean =
    !plan.isInstanceOf[CometPlan] && plan.children.nonEmpty && !isWrite(plan)

  /** The operators of the stage rooted at `plan`, not descending into its boundaries. */
  private def stageNodes(plan: SparkPlan): Seq[SparkPlan] =
    plan +: plan.children.filterNot(isBoundary).flatMap(stageNodes)

  /** Whether `plan` produces Comet batches, looking through materialized and reused stages. */
  private def isComet(plan: SparkPlan): Boolean = plan match {
    case stage: QueryStageExec => isComet(stage.plan)
    case reused: ReusedExchangeExec => isComet(reused.child)
    case read: AQEShuffleReadExec => isComet(read.child)
    case _ => plan.isInstanceOf[CometPlan]
  }

  private def stage(root: SparkPlan, output: Output): SparkPlan = {
    if (isBoundary(root)) {
      val consumerIsComet = output match {
        case ShuffleOutput(_, _) | CometBroadcastOutput | KeptOutput => true
        case RowOutput => false
      }
      return boundary(root, consumerIsComet)
    }
    val unified = revertIfMixed(root, output).getOrElse(root)
    withBoundaries(unified)
  }

  /** Processes the stages below the boundaries of this stage, the stage being their consumer. */
  private def withBoundaries(plan: SparkPlan): SparkPlan = {
    val newChildren = plan.children.map { child =>
      if (isBoundary(child)) {
        boundary(child, consumerIsComet = isComet(plan))
      } else {
        withBoundaries(child)
      }
    }
    if (newChildren == plan.children) plan else plan.withNewChildren(newChildren)
  }

  private def boundary(plan: SparkPlan, consumerIsComet: Boolean): SparkPlan = plan match {
    case exchange: CometShuffleExchangeExec =>
      val producer = stage(exchange.child, ShuffleOutput(exchange, consumerIsComet))
      shuffle(exchange, producer, consumerIsComet)
    case exchange: ShuffleExchangeExec =>
      exchange.withNewChildren(Seq(stage(exchange.child, RowOutput)))
    case broadcast: CometBroadcastExchangeExec =>
      val producer = stage(broadcast.child, CometBroadcastOutput)
      if (consumerIsComet) {
        broadcast.withNewChildren(Seq(producer))
      } else {
        val reverted = broadcast.originalPlan.withNewChildren(Seq(producer))
        reverted.setTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG, ())
        withFallbackReason(reverted, "Spark stage consumes the broadcast")
      }
    case broadcast: BroadcastExchangeExec =>
      broadcast.withNewChildren(Seq(stage(broadcast.child, RowOutput)))
    case other =>
      other
  }

  private def shuffle(
      exchange: CometShuffleExchangeExec,
      producer: SparkPlan,
      consumerIsComet: Boolean): SparkPlan = {
    val producerIsComet = isComet(producer)
    (exchange.shuffleType, producerIsComet, consumerIsComet) match {
      case (CometNativeShuffle, true, _) | (CometColumnarShuffle, false, true) =>
        exchange.withNewChildren(Seq(producer))
      case (_, false, true) =>
        columnarShuffle(sparkShuffle(exchange, producer)).getOrElse(
          throw new IllegalStateException(
            s"Stage feeding a Comet consumer was reverted without a columnar shuffle:\n$exchange"))
      case (_, false, false) =>
        val reverted = sparkShuffle(exchange, producer)
        reverted.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
        withFallbackReason(reverted, "Spark stages on both sides of the shuffle")
      case _ =>
        exchange.withNewChildren(Seq(producer))
    }
  }

  private def sparkShuffle(exchange: CometShuffleExchangeExec, producer: SparkPlan) =
    exchange.originalPlan.withNewChildren(Seq(producer)).asInstanceOf[ShuffleExchangeExec]

  private def columnarShuffle(sparkExchange: ShuffleExchangeExec): Option[SparkPlan] =
    CometShuffleExchangeExec.shuffleSupported(sparkExchange) match {
      case Some(CometColumnarShuffle) =>
        Some(CometShuffleExchangeExec(sparkExchange, shuffleType = CometColumnarShuffle))
      case _ => None
    }

  private def revertIfMixed(root: SparkPlan, output: Output): Option[SparkPlan] = {
    val nodes = stageNodes(root)
    val mixed = nodes.exists(isCometOperator) && nodes.exists(isSparkOperator)
    if (!mixed) return None
    if (nodes.exists(n =>
        n.isInstanceOf[CometNativeWriteExec] ||
          n.isInstanceOf[CometIcebergWriteExec])) {
      return None
    }
    if (output == CometBroadcastOutput || output == KeptOutput) return None
    if (stageRevert.hasUnsafeMixedAggregateAtStageBoundary(root)) return None
    val revertible = nodes.forall {
      case op: CometExec if isCometOperator(op) =>
        op.originalPlan.children.size == op.children.size
      case _ => true
    }
    if (!revertible) return None

    val reverted = revert(root)
    output match {
      case ShuffleOutput(exchange, true)
          if exchange.shuffleType == CometNativeShuffle &&
            columnarShuffle(sparkShuffle(exchange, reverted)).isEmpty =>
        None
      case _ =>
        logDebug(s"Stage runs in Spark: ${root.nodeName}")
        Some(reverted)
    }
  }

  private def revert(plan: SparkPlan): SparkPlan = {
    val newChildren = plan.children.map { child =>
      if (isBoundary(child)) child else revert(child)
    }
    val withChildren =
      if (newChildren == plan.children) plan else plan.withNewChildren(newChildren)
    withChildren match {
      case r2c: CometSparkToColumnarExec => r2c.child
      case op: CometExec if isCometOperator(op) =>
        val reverted = op.originalPlan.withNewChildren(op.children)
        reverted.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
        withFallbackReason(reverted, "Stage mixes Comet and Spark operators; it runs in Spark")
      case other => other
    }
  }
}
