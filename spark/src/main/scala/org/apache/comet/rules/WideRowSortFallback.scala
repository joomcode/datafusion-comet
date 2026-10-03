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
import org.apache.spark.sql.comet.{CometSortExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SortExec, SparkPlan}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.rules.BoundaryFormats.{consumerEngineOf, isBoundary, Engine}

case class WideRowSortFallback(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_ENABLED.get(conf) ||
      CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.get(conf) ||
      !CometConf.COMET_EXEC_ENABLED.get(conf)) {
      return plan
    }
    val minLeaves = CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.get(conf)
    var changed = false

    def visit(node: SparkPlan): SparkPlan = {
      val children = node.children.map(visit).map {
        case sort: CometSortExec if readsRows(node) && WideRowSortFallback.revertible(sort) =>
          WideRowSortFallback.fallbackReason(sort, minLeaves) match {
            case Some(why) =>
              changed = true
              WideRowSortFallback.revert(sort, why)
            case None => sort
          }
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

  private[rules] def revertible(sort: CometSortExec): Boolean =
    sort.originalPlan.isInstanceOf[SortExec]

  def payloadLeaves(sort: CometSortExec): Int =
    LeafColumns.count(LeafColumns.outside(sort.child.output, sort.sortOrder))

  def fallbackReason(sort: CometSortExec, minLeaves: Int): Option[String] = {
    val leaves = payloadLeaves(sort)
    if (leaves >= minLeaves) {
      val why =
        s"Wide rows: $leaves leaf columns outside the sort key, at least " +
          s"${CometConf.COMET_EXEC_SORT_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.key}=$minLeaves"
      logInfo(s"$why: sort ${sort.sortOrder.mkString(", ")}")
      Some(why)
    } else {
      None
    }
  }

  def revert(sort: CometSortExec, why: String): SparkPlan = {
    val input = sort.child match {
      case r2c: CometSparkToColumnarExec =>
        r2c.child.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
        r2c.child
      case other => other
    }
    val result = sort.originalPlan.withNewChildren(Seq(input))
    result.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
    withFallbackReason(result, why)
  }
}
