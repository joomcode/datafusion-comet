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

package org.apache.spark.sql.comet.shims

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, SQLPartitioningAwareUnionRDD}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioningLike, Partitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

object ShimCometUnionExec extends Logging {

  // Spark 4.2.1 added UnionConfSnapshot and records that immutable preparation policy on
  // UnionExec. Earlier 4.1/4.2 releases read the mutable session conf instead. Detect the public
  // snapshot type rather than a version string so a backport also retains Spark's own policy.
  private val preservesUnionPreparationPolicy: Boolean =
    try {
      val _ = Class.forName(
        "org.apache.spark.sql.execution.UnionConfSnapshot",
        false,
        classOf[SparkPlan].getClassLoader)
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  def captureLegacyPartitioningPolicy(originalPlan: SparkPlan): Option[Boolean] =
    if (preservesUnionPreparationPolicy) None
    else Some(originalPlan.conf.getConf(SQLConf.UNION_OUTPUT_PARTITIONING))

  def outputPartitioning(
      originalPlan: SparkPlan,
      output: Seq[Attribute],
      children: Seq[SparkPlan],
      legacyPartitioningEnabled: Option[Boolean]): Partitioning = {
    legacyPartitioningEnabled match {
      case Some(true) if children.nonEmpty => inferLegacyPartitioning(output, children)
      case Some(_) => UnknownPartitioning(0)
      // Spark 4.2.1's immutable preparation policy and stamped plain-union decision remain
      // authoritative. Deriving a new flag from the live conf here would regress late AQE
      // conversion, including a union prepared disabled and converted after it was enabled.
      case None =>
        val rebuilt = originalPlan.withNewChildren(children)
        // Spark Union derives its output from its current first child. A Comet copy retains
        // its own output attributes even when a rewrite changes/reorders the children, so bind
        // Spark's inferred layout to that declared output without changing its policy decision.
        val attributes = AttributeMap(rebuilt.output.zip(output))
        rebuilt.outputPartitioning match {
          case e: Expression =>
            e.transform {
              case a: Attribute if attributes.contains(a) => attributes(a)
            }.asInstanceOf[Partitioning]
          case p => p
        }
    }
  }

  // The Spark 4.1 SPARK-52921 inference, with its mutable conf read replaced by the policy
  // captured at conversion. Keep attribute remapping in both directions: different branches
  // have different exprIds, and the union output can also differ in nullability/metadata.
  private def inferLegacyPartitioning(
      output: Seq[Attribute],
      children: Seq[SparkPlan]): Partitioning = {
    def remap(partitioning: Partitioning, attributes: AttributeMap[Attribute]): Partitioning =
      partitioning match {
        case e: Expression =>
          e.transform {
            case a: Attribute if attributes.contains(a) => attributes(a)
          }.asInstanceOf[Partitioning]
        case p => p
      }

    val first = children.head
    val headPartitioning = first.outputPartitioning
    val agree = children.tail.forall { child =>
      val childPartitioning =
        remap(child.outputPartitioning, AttributeMap(child.output.zip(first.output)))
      (headPartitioning, childPartitioning) match {
        case (SinglePartition, SinglePartition) => true
        case (left: HashPartitioningLike, right: HashPartitioningLike) => left == right
        // Equal range expressions do not imply equal partition bounds.
        case _ => false
      }
    }
    if (agree) remap(headPartitioning, AttributeMap(first.output.zip(output)))
    else UnknownPartitioning(0)
  }

  /**
   * Unions a sequence of RDDs while preserving the declared output partitioning. Spark 4.1
   * introduced [[org.apache.spark.sql.internal.SQLConf.UNION_OUTPUT_PARTITIONING]] (SPARK-52921),
   * which lets [[org.apache.spark.sql.execution.UnionExec]] report a non-trivial output
   * partitioning when all children share the same partitioning. Downstream operators may then
   * skip an otherwise-required shuffle, so the columnar Union path must honor that contract by
   * routing through [[SQLPartitioningAwareUnionRDD]] rather than plain `SparkContext.union`,
   * which concatenates partitions and breaks the partitioning invariant.
   */
  def unionRDDs[T: ClassTag](
      sc: SparkContext,
      rdds: Seq[RDD[T]],
      outputPartitioning: Partitioning): RDD[T] = {
    outputPartitioning match {
      case SinglePartition | _: HashPartitioningLike =>
        val numPartitions = outputPartitioning.numPartitions
        val nonEmpty = rdds.filter(_.partitions.nonEmpty)
        // The parent may already have elided its exchange based on this known partitioning.
        // Concatenating on a stale child count would split a key between output partitions while
        // still advertising the old layout, and can silently produce duplicate aggregate groups.
        // Fail before executing that layout rather than change the RDD contract behind the parent.
        if (numPartitions <= 0 || nonEmpty.exists(_.partitions.length != numPartitions)) {
          val childCounts = rdds.map(_.partitions.length).mkString(", ")
          throw new IllegalStateException(
            s"CometUnionExec: child partition counts ($childCounts) do not match " +
              s"declared output partitioning numPartitions=$numPartitions")
        }
        if (nonEmpty.isEmpty) {
          // SQLPartitioningAwareUnionRDD requires a parent. Preserve the declared number of empty
          // partitions so a parent global aggregate still receives a task on empty input.
          sc.parallelize(Seq.empty[T], numPartitions)
        } else {
          new SQLPartitioningAwareUnionRDD(sc, nonEmpty, numPartitions)
        }
      case _ => sc.union(rdds)
    }
  }
}
