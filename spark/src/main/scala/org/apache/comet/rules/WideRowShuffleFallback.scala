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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

import org.apache.comet.CometConf

object WideRowShuffleFallback {

  def keyExpressions(partitioning: Partitioning): Seq[Expression] = partitioning match {
    case h: HashPartitioning => h.expressions
    case r: RangePartitioning => r.ordering
    case _ => Nil
  }

  def payloadLeaves(shuffle: ShuffleExchangeExec): Int =
    LeafColumns.count(
      LeafColumns.outside(shuffle.child.output, keyExpressions(shuffle.outputPartitioning)))

  def fallbackReason(shuffle: ShuffleExchangeExec): Option[String] = {
    val minLeaves = CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.get(shuffle.conf)
    if (minLeaves <= 0 || CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.get(shuffle.conf)) {
      None
    } else {
      val leaves = payloadLeaves(shuffle)
      if (leaves >= minLeaves) {
        Some(
          s"Wide rows: $leaves leaf columns outside the partitioning key, at least " +
            s"${CometConf.COMET_SHUFFLE_WIDE_ROW_FALLBACK_MIN_LEAF_COLUMNS.key}=$minLeaves")
      } else {
        None
      }
    }
  }
}
