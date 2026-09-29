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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometSortExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.rules.BoundaryFormats.{consumerEngineOf, isBoundary, Engine}

case class WideRowSortFallback(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.get(conf) ||
      !CometConf.COMET_EXEC_ENABLED.get(conf)) {
      return plan
    }
    val minAvgRowBytes = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_AVG_ROW_BYTES.get(conf)
    val maxKeyFraction = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MAX_KEY_FRACTION.get(conf)
    var changed = false

    def visit(node: SparkPlan): SparkPlan = {
      val children = node.children.map(visit).map {
        case sort: CometSortExec
            if readsRows(node) && WideRowSortFallback.revertible(sort) &&
              WideRowSortFallback.wideRowNarrowKey(sort, minAvgRowBytes, maxKeyFraction) =>
          changed = true
          WideRowSortFallback.revert(sort)
        case other => other
      }
      if (children.zip(node.children).forall { case (a, b) => a eq b }) node
      else node.withNewChildren(children)
    }

    val result = visit(plan)
    if (changed) CometExecRule.convertBlocks(result) else plan
  }

  private def readsRows(consumer: SparkPlan): Boolean =
    !isBoundary(consumer) && !consumer.isInstanceOf[ColumnarToRowTransition] &&
      consumerEngineOf(consumer) == Engine.Spark
}

object WideRowSortFallback extends Logging {

  val reason = "Wide rows with a narrow sort key: Spark sorts row pointers"

  private[rules] def revertible(sort: CometSortExec): Boolean =
    sort.originalPlan.isInstanceOf[SortExec]

  def runtimeAvgRowBytes(input: SparkPlan): Option[Double] = input match {
    case stage: QueryStageExec =>
      stage.computeStats().flatMap { stats =>
        stats.rowCount.filter(_ > 0).map(rows => stats.sizeInBytes.toDouble / rows.toDouble)
      }
    case read: AQEShuffleReadExec => runtimeAvgRowBytes(read.child)
    case _ => None
  }

  def schemaBytes(attributes: Seq[Attribute]): Long =
    attributes.map(_.dataType.defaultSize.toLong).sum

  def avgRowBytes(sort: CometSortExec): Double =
    runtimeAvgRowBytes(sort.child).getOrElse(schemaBytes(sort.child.output).toDouble)

  def keyFraction(sort: CometSortExec): Double = {
    val keyBytes = sort.sortOrder.map(_.child.dataType.defaultSize.toLong).sum
    keyBytes.toDouble / math.max(schemaBytes(sort.child.output), 1L).toDouble
  }

  def wideRowNarrowKey(
      sort: CometSortExec,
      minAvgRowBytes: Long,
      maxKeyFraction: Double): Boolean = {
    val rowBytes = avgRowBytes(sort)
    val fraction = keyFraction(sort)
    val decided = rowBytes > minAvgRowBytes && fraction < maxKeyFraction
    if (decided) {
      logInfo(
        f"$reason: average row $rowBytes%.0f bytes, key $fraction%.3f of the row, " +
          s"sort ${sort.sortOrder.mkString(", ")}")
    }
    decided
  }

  def revert(sort: CometSortExec): SparkPlan = {
    val input = sort.child match {
      case r2c: CometSparkToColumnarExec =>
        r2c.child.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
        r2c.child
      case other => other
    }
    val reverted = sort.originalPlan.withNewChildren(Seq(input))
    reverted.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
    withFallbackReason(reverted, reason)
  }
}
