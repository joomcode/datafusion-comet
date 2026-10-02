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

import java.util.IdentityHashMap

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometExec, CometHashAggregateExec, CometIcebergWriteExec, CometNativeWriteExec, CometPlan, CometSparkToColumnarExec, CometWriteFilesExec}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.rules.BoundaryFormats._
import org.apache.comet.serde.QueryPlanSerde

/**
 * The cost of [[CostBasedEngineChoice]], in ns: rows times a price per row from
 * [[EngineCostTable]], for the operators it prices, the shuffles and the columnar-to-row
 * conversions. An operator of a class outside the table costs `cometOperatorWeight` when native
 * (or its per-operator override) and `sparkOperatorWeight` in Spark, not scaled by rows.
 *
 * Widths: an operator's leaf columns are those of its output outside the columns its key
 * references (the sort order of a sort, the ordering a buffering operator requires of its input,
 * the partitioning of a shuffle), counted by [[LeafColumns]]; a conversion converts every leaf of
 * its rows.
 *
 * Rows ([[rows]]): the runtime statistics of a materialized query stage, its row count or else
 * its size over the estimated size of a row; else the row count of the operator's logical plan;
 * else the largest estimate among its children, so that operators inside a stage take the rows of
 * the stage's input; else 1, which compares the engines per row.
 */
class EngineCostModel(
    val table: EngineCostTable,
    cometOperatorWeight: Double,
    sparkOperatorWeight: Double,
    cometOperatorWeights: Map[String, Double])
    extends BoundaryFormats.Pricing {

  import EngineCostTable._

  private val rowEstimates = new IdentityHashMap[SparkPlan, java.lang.Double]()

  def rows(plan: SparkPlan): Double = {
    val known = rowEstimates.get(plan)
    if (known != null) {
      known
    } else {
      val estimate = runtimeRows(plan)
        .orElse(logicalRows(plan))
        .getOrElse(if (plan.children.isEmpty) 1.0 else plan.children.map(rows).max)
      rowEstimates.put(plan, estimate)
      estimate
    }
  }

  private def runtimeRows(plan: SparkPlan): Option[Double] = plan match {
    case stage: QueryStageExec =>
      stage.computeStats().map { stats =>
        stats.rowCount.map(_.toDouble).getOrElse {
          val rowSize = EstimationUtils.getSizePerRow(stage.output)
          math.max(1.0, (stats.sizeInBytes / rowSize.max(1)).toDouble)
        }
      }
    case read: AQEShuffleReadExec => runtimeRows(read.child)
    case _ => None
  }

  private def logicalRows(plan: SparkPlan): Option[Double] =
    plan.logicalLink.flatMap { logical =>
      try {
        logical.stats.rowCount.map(_.toDouble)
      } catch {
        case NonFatal(_) => None
      }
    }

  private def nameOf(plan: SparkPlan): String = plan match {
    case op: CometExec => op.originalPlan.getClass.getSimpleName
    case other => other.getClass.getSimpleName
  }

  /** The class the table prices `op` as, a native operator Comet converted. */
  def costClass(op: CometExec): Option[CostClass] = operatorClasses.get(nameOf(op))

  /** The leaf columns `op` processes per row, outside its key. */
  def width(op: CometExec): Width = {
    val keys = op.originalPlan match {
      case sort: SortExec => sort.sortOrder
      case other if costClass(op).contains(CostClass.Sort) => other.requiredChildOrdering.flatten
      case _ => Nil
    }
    widthOf(LeafColumns.outside(op.output, keys))
  }

  /** Cost of running `op`, a native operator Comet converted, in `engine`. */
  def operatorCost(op: CometExec, engine: Engine): Double = costClass(op) match {
    case Some(c) =>
      val perRow = engine match {
        case Engine.Comet => table.comet(c, width(op))
        case Engine.Spark => table.spark(c, width(op))
      }
      rows(op) * perRow
    case None =>
      engine match {
        case Engine.Comet => cometOperatorWeights.getOrElse(nameOf(op), cometOperatorWeight)
        case Engine.Spark => sparkOperatorWeight
      }
  }

  /** Cost of converting the output of `plan` between rows and Arrow once. */
  def conversion(plan: SparkPlan): Double =
    rows(plan) * table.comet(CostClass.C2R, widthOf(plan.output))

  /** A native operator holding memory, such as a sort or a hash aggregate. */
  def holdsNativeMemory(plan: SparkPlan): Boolean =
    plan.isInstanceOf[CometPlan] && nativeMemoryHolders.contains(nameOf(plan))

  /** An operator that holds memory when it runs in Spark, such as a sort or a window. */
  def holdsSparkMemory(plan: SparkPlan): Boolean = sparkMemoryOperators.contains(nameOf(plan))

  /** Cost of the risk of running out of memory with the rows of native `plan` in a stage. */
  def oomRisk(plan: SparkPlan): Double = rows(plan) * table.oomRiskPenalty

  /** The leaf columns a shuffle moves per row, outside its partitioning key. */
  def shuffleWidth(boundary: SparkPlan): Width =
    widthOf(
      LeafColumns.outside(
        boundary.children.head.output,
        WideRowShuffleFallback.keyExpressions(boundary.outputPartitioning)))

  /** Cost of writing and reading the shuffle `boundary` in `engine`. */
  def shuffleCost(boundary: SparkPlan, engine: Engine): Double = {
    val w = shuffleWidth(boundary)
    val perRow = engine match {
      case Engine.Comet =>
        val partitions = boundary.outputPartitioning.numPartitions
        table.comet(CostClass.ShuffleWrite, w) * table.shuffleWritePartitionFactor(partitions) +
          table.comet(CostClass.ShuffleRead, w)
      case Engine.Spark =>
        table.spark(CostClass.ShuffleWrite, w) + table.spark(CostClass.ShuffleRead, w)
    }
    rows(boundary) * perRow
  }

  override def price(input: Input, format: Format, conversions: Int): Double = {
    val converting = conversions * conversion(input.boundary)
    format match {
      case NativeShuffle | ColumnarShuffle =>
        converting + shuffleCost(input.boundary, Engine.Comet)
      case SparkShuffle => converting + shuffleCost(input.boundary, Engine.Spark)
      case _ => converting
    }
  }
}

object EngineCostModel {
  def apply(conf: SQLConf): EngineCostModel = {
    val overrides = CometConf.COMET_EXEC_COST_BASED_ENGINES_OPERATOR_WEIGHTS
      .get(conf)
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { entry =>
        entry.split("=") match {
          case Array(name, weight) => name.trim -> weight.trim.toDouble
          case _ =>
            throw new IllegalArgumentException(
              s"${CometConf.COMET_EXEC_COST_BASED_ENGINES_OPERATOR_WEIGHTS.key}: expected " +
                s"<operator>=<weight>, got '$entry'")
        }
      }
      .toMap
    new EngineCostModel(
      EngineCostTable(conf),
      CometConf.COMET_EXEC_COST_BASED_ENGINES_COMET_WEIGHT.get(conf),
      CometConf.COMET_EXEC_COST_BASED_ENGINES_SPARK_WEIGHT.get(conf),
      overrides)
  }
}

/**
 * Chooses, for each operator [[CometExecRule]] converted, whether it runs natively or in Spark,
 * minimizing the cost of [[EngineCostModel]] over the whole plan. It only ever moves operators
 * from Comet to Spark: an operator can be native only where Comet converted it. It labels
 * operators only; the formats of shuffles and broadcasts, and the conversions each implies, come
 * from [[BoundaryFormats]], which it asks for the cost of every labelling it considers and then
 * applies to the labelling it picks.
 *
 * Constraints, beyond those of [[BoundaryFormats]]:
 *   - A native operator reads Arrow: its inputs inside the stage are native, or a row-to-columnar
 *     transition over a leaf, which is kept (costing one conversion) or removed.
 *   - Leaf scans, writes, and native aggregates whose buffers Spark and Comet cannot exchange
 *     keep the engine they were converted to (the aggregate test is the one of
 *     `COMET_UNSAFE_PARTIAL` and [[RevertNativeForTransitionHeavyStages]]).
 *   - Materialized and reused stages are leaves of fixed format, and a boundary with no consumer
 *     in the plan (a subquery or stage root) keeps its format.
 *   - The plan's own output is rows, so a native root pays one conversion. The root of a subquery
 *     keeps its engine: an operator outside the plan, such as the broadcast that dynamic
 *     partition pruning builds around it, may rely on it.
 *
 * A boundary costs the conversions of its format and, for a shuffle, writing and reading it in
 * the engine of its format, all priced by [[EngineCostModel]], which [[BoundaryFormats]] then
 * also uses to apply formats. A conversion from a native operator to a Spark consumer inside a
 * stage also costs [[EngineCostModel.oomRisk]] when a native operator holding memory is at or
 * below the native side and a Spark operator holding memory is at or above the Spark side, up to
 * the stage's boundaries or a row-to-columnar transition: the two engines then hold memory in one
 * task. Every operator below a native one in its stage is native, and every operator above a
 * Spark one is Spark, so the conversion is the one place the solver can see both sides and avoid
 * it by moving the whole stage to one engine.
 *
 * With `spark.comet.exec.costBasedEngines.log.enabled` or `spark.comet.explain.fallback.enabled`,
 * every decided operator, shuffle and conversion is logged with its class, form, leaf columns,
 * rows and costs.
 *
 * Algorithm: an exact dynamic program over the plan tree. Each operator gets two costs, the best
 * cost of everything feeding it given that it is native or not. A boundary contributes, for each
 * label of its producer, the producer's best cost plus the conversions the shared function
 * reports for that pair of labels. The inputs of one stage that must share a hash function are
 * handled by solving the stage once per hash mode the shared function allows (at most two), so no
 * search is exponential. Labels are then read back top-down, preferring the current engine on
 * ties.
 *
 * Sharing: a physical plan is a tree, and materialized or reused stages are leaves, so every
 * operator is decided once. Identical subtrees that Spark would reuse later are decided
 * independently; when their consumers differ they can get different engines, and are then no
 * longer reused. Formats of identical exchanges are unified by [[BoundaryFormats.applyFormats]]
 * where one format suits every copy.
 *
 * Reverted operators are tagged [[CometExecRule.KEEP_ON_SPARK_TAG]] so AQE's per-stage conversion
 * leaves them in Spark. Runs on whole plans only, like [[ChooseBoundaryFormats]].
 */
case class CostBasedEngineChoice(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = apply(plan, keepRoot = false)

  /**
   * @param keepRoot
   *   keep the engine of the plan's root operator, whose consumer is outside the plan
   */
  def apply(plan: SparkPlan, keepRoot: Boolean): SparkPlan = {
    if (!CometConf.COMET_EXEC_COST_BASED_ENGINES_ENABLED.get(conf) ||
      !CometConf.COMET_EXEC_ENABLED.get(conf)) {
      return plan
    }
    val model = EngineCostModel(conf)
    val solver = new EngineSolver(model, if (keepRoot) Some(plan) else None)
    solver.solve(plan) match {
      case Some(labels) =>
        if (CometConf.COMET_EXEC_COST_BASED_ENGINES_LOG_ENABLED.get(conf) ||
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get(conf)) {
          logWarning(s"Cost-based engine choice:\n${solver.explain(plan, labels)}")
        }
        val relabelled = EngineSolver.relabel(plan, labels)
        CometExecRule.convertBlocks(BoundaryFormats.applyFormats(relabelled, model))
      case None =>
        logWarning("Cost-based engine choice found no feasible plan; keeping Comet's choice")
        plan
    }
  }
}

private[rules] object EngineSolver {

  val reason = "Cost-based engine choice: cheaper in Spark than with the conversions around it"

  private def isWrite(plan: SparkPlan): Boolean = plan match {
    case _: CometNativeWriteExec | _: CometIcebergWriteExec | _: CometWriteFilesExec => true
    case _ => false
  }

  private def unsafeAggregate(agg: CometHashAggregateExec): Boolean =
    !QueryPlanSerde.allAggsSupportNativePartialToSparkFinal(agg.aggregateExpressions) ||
      QueryPlanSerde.aggsNotSupportingSparkPartialToNativeFinal(agg.aggregateExpressions).nonEmpty

  /** A native operator that may run in Spark instead. */
  def relabelable(plan: SparkPlan): Boolean = plan match {
    case _: CometSparkToColumnarExec => false
    case op: CometExec =>
      op.children.nonEmpty && !isWrite(op) &&
      !op.originalPlan.isInstanceOf[CometPlan] &&
      op.originalPlan.children.size == op.children.size &&
      !op.originalPlan.supportsColumnar &&
      (op match {
        case agg: CometHashAggregateExec => !unsafeAggregate(agg)
        case _ => true
      })
    case _ => false
  }

  /** A row-to-columnar transition that can be removed, leaving its input to a Spark consumer. */
  def removableTransition(plan: SparkPlan): Boolean = plan match {
    case r2c: CometSparkToColumnarExec =>
      r2c.child.children.isEmpty || isBoundary(r2c.child)
    case _ => false
  }

  def relabel(plan: SparkPlan, labels: IdentityHashMap[SparkPlan, Engine]): SparkPlan = {
    def visit(node: SparkPlan): SparkPlan = {
      val children = node.children.map(visit)
      val unchanged = children.zip(node.children).forall { case (a, b) => a eq b }
      val label = Option(labels.get(node))
      node match {
        case r2c: CometSparkToColumnarExec if label.contains(Engine.Spark) =>
          val input = children.head
          input.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
          input
        case op: CometExec if label.contains(Engine.Spark) =>
          val reverted = op.originalPlan.withNewChildren(children)
          reverted.setTagValue(CometExecRule.KEEP_ON_SPARK_TAG, ())
          withFallbackReason(reverted, reason)
        case _ =>
          if (unchanged) node else node.withNewChildren(children)
      }
    }
    visit(plan)
  }
}

private[rules] class EngineSolver(model: EngineCostModel, fixedRoot: Option[SparkPlan] = None) {
  import EngineSolver._

  private val Inf = Double.PositiveInfinity
  private val Epsilon = 1e-9
  private val engines = Engine.all
  private def index(e: Engine): Int = if (e == Engine.Comet) 0 else 1

  private class Stage(val root: SparkPlan) {
    val modes: Seq[Mode] =
      BoundaryFormats.modes(stageInputs(root).map(_._2), stageLeaves(root))
    val costs = new IdentityHashMap[SparkPlan, Array[Array[Double]]]()

    /** The best cost of the stage and its mode, given the engine of its root. */
    def best(engine: Engine): (Double, Int) =
      modes.indices
        .map(m => (nodeCosts(root, m, this)(index(engine)), m))
        .minBy(_._1)
  }

  private val stages = new IdentityHashMap[SparkPlan, Stage]()
  private val hypotheticalProducers = new IdentityHashMap[SparkPlan, SparkPlan]()

  private def stage(root: SparkPlan): Stage = {
    var s = stages.get(root)
    if (s == null) {
      s = new Stage(root)
      stages.put(root, s)
    }
    s
  }

  private def allowed(node: SparkPlan): Seq[Engine] = node match {
    case root if fixedRoot.exists(_ eq root) => Seq(engineOf(root))
    case r2c: CometSparkToColumnarExec =>
      if (removableTransition(r2c)) engines else Seq(Engine.Comet)
    case op if relabelable(op) => engines
    case _: CometPlan => Seq(Engine.Comet)
    case _ => Seq(Engine.Spark)
  }

  private def current(node: SparkPlan): Engine = engineOf(node)

  private def operatorCost(node: SparkPlan, engine: Engine): Double = node match {
    case r2c: CometSparkToColumnarExec =>
      if (engine == Engine.Comet) model.conversion(r2c) else 0.0
    case op: CometExec if relabelable(op) => model.operatorCost(op, engine)
    case _ => 0.0
  }

  private val sparkMemoryAbove = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
  private val nativeMemoryBelow = new IdentityHashMap[SparkPlan, java.lang.Boolean]()

  /**
   * Whether `node` or an operator above it in its stage, up to a row-to-columnar transition,
   * holds memory when it runs in Spark.
   */
  private def markSparkMemory(node: SparkPlan, above: Boolean): Unit = {
    val here = above || model.holdsSparkMemory(node)
    sparkMemoryAbove.put(node, here)
    node.children.foreach { child =>
      val reset = isBoundary(child) || node.isInstanceOf[CometSparkToColumnarExec]
      markSparkMemory(child, !reset && here)
    }
  }

  /** Whether `node` or an operator below it in its stage holds memory when native. */
  private def holdsNativeMemoryBelow(node: SparkPlan): Boolean = {
    val known = nativeMemoryBelow.get(node)
    if (known != null) {
      known
    } else {
      val result = model.holdsNativeMemory(node) || (!node
        .isInstanceOf[CometSparkToColumnarExec] && node.children.exists(c =>
        !isBoundary(c) && holdsNativeMemoryBelow(c)))
      nativeMemoryBelow.put(node, result)
      result
    }
  }

  /** Cost of converting the output of native `child` for its Spark `parent` in one stage. */
  private def conversionInStage(parent: SparkPlan, child: SparkPlan): Double = {
    val risky = Option(sparkMemoryAbove.get(parent)).exists(_.booleanValue) &&
      holdsNativeMemoryBelow(child)
    model.conversion(child) + (if (risky) model.oomRisk(child) else 0.0)
  }

  /** The engine `node` reads its inputs in, given its own engine. */
  private def consumerEngine(node: SparkPlan, engine: Engine): Engine = node match {
    case _: CometSparkToColumnarExec => Engine.Spark
    case _ => engine
  }

  /** Cost of the conversion between a parent and its child inside one stage. */
  private def edge(
      parent: SparkPlan,
      engine: Engine,
      child: SparkPlan,
      childEngine: Engine): Double =
    (consumerEngine(parent, engine), childEngine) match {
      case (a, b) if a == b => 0.0
      case (Engine.Spark, Engine.Comet) => conversionInStage(parent, child)
      case _ => Inf
    }

  /** The producer of a boundary as it would be with `engine`. */
  private def producerPlan(producer: SparkPlan, engine: Engine): SparkPlan =
    if (engine == current(producer)) {
      producer
    } else {
      var p = hypotheticalProducers.get(producer)
      if (p == null) {
        p = producer match {
          case r2c: CometSparkToColumnarExec => r2c.child
          case op: CometExec => op.originalPlan.withNewChildren(op.children)
          case other => other
        }
        hypotheticalProducers.put(producer, p)
      }
      p
    }

  private def conversionCost(input: Input, mode: Mode): Double =
    choose(input, mode, model).map(_.cost).getOrElse(Inf)

  private def nodeCosts(node: SparkPlan, mode: Int, stage: Stage): Array[Double] = {
    var perMode = stage.costs.get(node)
    if (perMode == null) {
      perMode = Array.fill(stage.modes.size)(null: Array[Double])
      stage.costs.put(node, perMode)
    }
    if (perMode(mode) == null) {
      val result = Array(Inf, Inf)
      allowed(node).foreach { engine =>
        var cost = operatorCost(node, engine)
        node.children.foreach { child =>
          if (!cost.isInfinite) cost += childCost(node, engine, child, mode, stage)
        }
        result(index(engine)) = cost
      }
      perMode(mode) = result
    }
    perMode(mode)
  }

  private def childCost(
      node: SparkPlan,
      engine: Engine,
      child: SparkPlan,
      mode: Int,
      stage: Stage): Double = {
    if (isBoundary(child)) {
      boundaryCost(child, consumerEngine(node, engine), stage.modes(mode))
    } else {
      val costs = nodeCosts(child, mode, stage)
      allowed(child).map(c => costs(index(c)) + edge(node, engine, child, c)).min
    }
  }

  private def producerOptions(
      boundary: SparkPlan,
      consumer: Option[Engine],
      mode: Mode): Seq[(Engine, Double, Int)] = {
    val producer = boundary.children.head
    if (isBoundary(producer)) {
      val input = Input(boundary, consumer, engineOf(producer), producer)
      Seq((engineOf(producer), keptCost(producer) + conversionCost(input, mode), -1))
    } else {
      val s = stage(producer)
      allowed(producer).map { engine =>
        val (cost, bestMode) = s.best(engine)
        val input = Input(boundary, consumer, engine, producerPlan(producer, engine))
        (engine, cost + conversionCost(input, mode), bestMode)
      }
    }
  }

  private def boundaryCost(boundary: SparkPlan, consumer: Engine, mode: Mode): Double = {
    if (!isDecidable(boundary)) {
      conversionCost(Input(boundary, Some(consumer), engineOf(boundary), boundary), mode)
    } else {
      producerOptions(boundary, Some(consumer), mode).map(_._2).min
    }
  }

  /** Cost below a boundary whose format is kept because its consumer is not in the plan. */
  private def keptCost(boundary: SparkPlan): Double =
    if (!isDecidable(boundary)) 0.0
    else producerOptions(boundary, None, Unconstrained).map(_._2).min

  /** Labels for the whole plan, or `None` if no labelling is feasible. */
  def solve(plan: SparkPlan): Option[IdentityHashMap[SparkPlan, Engine]] = {
    val labels = new IdentityHashMap[SparkPlan, Engine]()
    markSparkMemory(plan, above = false)

    def pick[T](options: Seq[(Engine, Double, T)], preferred: Engine): (Engine, Double, T) = {
      val min = options.map(_._2).min
      options
        .filter(_._2 <= min + Epsilon)
        .sortBy(o => if (o._1 == preferred) 0 else 1)
        .head
    }

    def assignNode(node: SparkPlan, engine: Engine, mode: Int, s: Stage): Unit = {
      labels.put(node, engine)
      node.children.foreach { child =>
        if (isBoundary(child)) {
          assignBoundary(child, Some(consumerEngine(node, engine)), s.modes(mode))
        } else {
          val costs = nodeCosts(child, mode, s)
          val options =
            allowed(child).map(c => (c, costs(index(c)) + edge(node, engine, child, c), ()))
          assignNode(child, pick(options, current(child))._1, mode, s)
        }
      }
    }

    def assignBoundary(boundary: SparkPlan, consumer: Option[Engine], mode: Mode): Unit = {
      if (isDecidable(boundary)) {
        val producer = boundary.children.head
        if (isBoundary(producer)) {
          assignBoundary(producer, None, Unconstrained)
        } else {
          val (engine, _, bestMode) =
            pick(producerOptions(boundary, consumer, mode), current(producer))
          assignNode(producer, engine, bestMode, stage(producer))
        }
      }
    }

    val total = if (isBoundary(plan)) {
      val cost = keptCost(plan)
      if (!cost.isInfinite) assignBoundary(plan, None, Unconstrained)
      cost
    } else {
      val s = stage(plan)
      val options = allowed(plan).map { engine =>
        val (cost, mode) = s.best(engine)
        val output = if (engine == Engine.Comet) model.conversion(plan) else 0.0
        (engine, cost + output, mode)
      }
      val (engine, cost, mode) = pick(options, current(plan))
      if (!cost.isInfinite) assignNode(plan, engine, mode, s)
      cost
    }
    planCost = total
    if (total.isInfinite) None else Some(labels)
  }

  private var planCost = Inf

  /** One line per decided operator, shuffle and conversion of `plan`, for debugging. */
  def explain(plan: SparkPlan, labels: IdentityHashMap[SparkPlan, Engine]): String = {
    val lines = mutable.ArrayBuffer(f"total=$planCost%.1f")
    def label(node: SparkPlan): Engine = Option(labels.get(node)).getOrElse(current(node))
    def describe(node: SparkPlan, w: EngineCostTable.Width, rows: Double): String =
      f"${node.nodeName}#${node.id} form=${w.form} L=${w.leaves} rows=$rows%.0f"

    def visit(node: SparkPlan, consumer: Option[Engine]): Unit = {
      val engine = label(node)
      node match {
        case op: CometExec if relabelable(op) =>
          val costClass = model.costClass(op).map(_.name).getOrElse("unpriced")
          lines += f"${describe(op, model.width(op), model.rows(op))} class=$costClass " +
            f"comet=${model.operatorCost(op, Engine.Comet)}%.1f " +
            f"spark=${model.operatorCost(op, Engine.Spark)}%.1f -> $engine"
        case r2c: CometSparkToColumnarExec if removableTransition(r2c) =>
          val kept = if (engine == Engine.Comet) "kept" else "removed"
          lines += f"${describe(r2c, EngineCostTable.widthOf(r2c.output), model.rows(r2c))} " +
            f"class=c2r cost=${model.conversion(r2c)}%.1f -> $kept"
        case shuffle: ShuffleExchangeLike if isDecidable(shuffle) =>
          lines += f"${describe(shuffle, model.shuffleWidth(shuffle), model.rows(shuffle))} " +
            f"class=shuffle partitions=${shuffle.outputPartitioning.numPartitions} " +
            f"comet=${model.shuffleCost(shuffle, Engine.Comet)}%.1f " +
            f"spark=${model.shuffleCost(shuffle, Engine.Spark)}%.1f " +
            f"conversion=${model.conversion(shuffle)}%.1f " +
            f"producer=${label(shuffle.child)} consumer=${consumer.getOrElse("none")}"
        case _ =>
      }
      node.children.foreach { child =>
        if (!isBoundary(child) && consumerEngine(node, engine) == Engine.Spark &&
          label(child) == Engine.Comet) {
          val w = EngineCostTable.widthOf(child.output)
          lines += f"conversion above ${describe(child, w, model.rows(child))} class=c2r " +
            f"cost=${conversionInStage(node, child)}%.1f"
        }
        visit(child, Some(consumerEngine(node, engine)))
      }
    }

    visit(plan, None)
    if (!isBoundary(plan) && label(plan) == Engine.Comet) {
      val w = EngineCostTable.widthOf(plan.output)
      lines += f"conversion of the output of ${describe(plan, w, model.rows(plan))} " +
        f"class=c2r cost=${model.conversion(plan)}%.1f"
    }
    lines.mkString("\n")
  }
}
