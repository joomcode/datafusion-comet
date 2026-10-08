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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Partial}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometExec, CometFilterExec, CometHashAggregateExec, CometIcebergWriteExec, CometNativeWriteExec, CometPlan, CometProjectExec, CometSparkToColumnarExec, CometWriteFilesExec}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, ExpandExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.rules.BoundaryFormats._
import org.apache.comet.serde.{CometCodegenDispatch, QueryPlanSerde}
import org.apache.comet.shims.ShimCometWindowGroupLimit

/**
 * The cost of [[CostBasedEngineChoice]], in ns per row: a price per row from [[EngineCostTable]]
 * for the operators it prices, the shuffles and the conversions between rows and Arrow. Every
 * operator counts one row, so the engines are compared per row and the choice depends only on the
 * schema and the shape of the plan. An operator of a class outside the table costs
 * `cometOperatorWeight` when native (or its per-operator override) and `sparkOperatorWeight` in
 * Spark.
 *
 * An operator costs the sum of its [[EngineCostModel.Term]]s, each the price of a class over a
 * width times a count, plus the filter's pass-through and the sort's per-byte price. Widths,
 * counted by [[LeafColumns]], are every leaf of the operator's output, except: a window's are
 * those of its input; a filter's predicate those it references; an aggregate's those of its
 * grouping keys, at half the price for each phase of a two-phase aggregate. A project computes
 * once per leaf of the expressions it does not pass through; an expand copies once per
 * projection. The functions of an aggregate cost the price of their class each, the leaves of its
 * grouping keys that hold an array `aggArrayKey` on top of `agg`, and the leaves of a grouping
 * key computed through the JVM codegen dispatcher `codegenDispatch` once; those of a window the
 * price of their class at the number of its window functions. In Spark, an operator whose rows,
 * or its inputs', have more leaves than `codegenMaxFields` runs without whole-stage codegen and
 * takes the classes of [[EngineCostTable.CostClass.withoutCodegen]], and a project whose input
 * comes from a scan through filters, projects and conversions only passes its columns for free
 * and computes at `expressionOverScan`. Shuffles cost their write and read over every leaf of the
 * shuffled rows, scaled by their partitions, and their bytes beyond `perByteLeafAllowance` per
 * leaf. A conversion to rows costs `c2r` over every leaf, and Comet's columnar shuffle also pays
 * one `r2c` when it writes.
 */
class EngineCostModel(
    val table: EngineCostTable,
    cometOperatorWeight: Double,
    sparkOperatorWeight: Double,
    cometOperatorWeights: Map[String, Double],
    codegenMaxFields: Int = 100)
    extends BoundaryFormats.Pricing {

  import EngineCostModel.Term
  import EngineCostTable._
  import EngineCostTable.CostClass._

  private def sparkOperator(plan: SparkPlan): SparkPlan = plan match {
    case op: CometExec => op.originalPlan
    case other => other
  }

  private def nameOf(plan: SparkPlan): String = sparkOperator(plan).getClass.getSimpleName

  /** The classes the table prices `plan` as, a Spark operator or a native one Comet converted. */
  def costClasses(plan: SparkPlan): Seq[CostClass] = operatorClasses.getOrElse(nameOf(plan), Nil)

  private def passesThrough(expression: NamedExpression): Boolean = expression match {
    case _: Attribute => true
    case Alias(_: Attribute, _) => true
    case _ => false
  }

  /** Whether Spark runs `plan` with whole-stage codegen. */
  def sparkCodegen(plan: SparkPlan): Boolean =
    (plan +: plan.children).forall(p => LeafColumns.count(p.output) <= codegenMaxFields)

  /** Whether `input` comes from a scan through filters, projects and conversions only. */
  def overScan(input: SparkPlan): Boolean = input match {
    case b if isBoundary(b) => false
    case leaf if leaf.children.isEmpty => true
    case t: ColumnarToRowTransition => overScan(t.child)
    case r2c: CometSparkToColumnarExec => overScan(r2c.child)
    case other =>
      sparkOperator(other) match {
        case _: FilterExec | _: ProjectExec => overScan(other.children.head)
        case _ => false
      }
  }

  /** Whether Comet evaluates `expression` through the JVM codegen dispatcher. */
  def dispatchedThroughCodegen(expression: Expression): Boolean = expression.exists {
    case _: ScalaUDF => true
    case e =>
      QueryPlanSerde.exprSerdeMap.get(e.getClass).exists(_.isInstanceOf[CometCodegenDispatch[_]])
  }

  private def aggregateShare(agg: BaseAggregateExec): Double =
    if (agg.aggregateExpressions.exists(_.mode == Complete)) 1.0 else 0.5

  private def classTerms(costClass: CostClass, plan: SparkPlan, engine: Engine): Seq[Term] = {
    val op = sparkOperator(plan)
    lazy val out = widthOf(op.output)
    lazy val sparkOverScan = engine == Engine.Spark && overScan(plan.children.head)
    (costClass, op) match {
      case (Sort, _) =>
        val w = op match {
          case _: BaseAggregateExec => widthOf(op.children.head.output)
          case _ => out
        }
        val spill = table.sortSpillFraction
        if (spill > 0) Seq(Term(Sort, w), Term(SortSpill, w, spill)) else Seq(Term(Sort, w))
      case (Window, window: WindowExec) =>
        val functions = window.windowExpression.map(windowFunctionClass)
        Term(Window, widthOf(window.child.output)) +: functions.distinct.map { c =>
          Term(c, Width(functions.size, 0), functions.count(_ == c))
        }
      case (WglPartial | WglFinal, _) =>
        val mode = ShimCometWindowGroupLimit.extract(op).map(_.mode)
        val phase = if (mode.contains("Partial")) WglPartial else WglFinal
        if (phase == costClass) Seq(Term(phase, out)) else Nil
      case (Expand, expand: ExpandExec) => Seq(Term(Expand, out, expand.projections.size))
      case (Predicate, filter: FilterExec) =>
        Seq(Term(Predicate, widthOf(filter.condition.references.toSeq)))
      case (ProjectPassThrough, _) => if (sparkOverScan) Nil else Seq(Term(costClass, out))
      case (Expr, project: ProjectExec) =>
        val computed =
          project.projectList.filterNot(passesThrough).map(e => LeafColumns.count(e.dataType)).sum
        if (computed == 0) {
          Nil
        } else {
          Seq(Term(if (sparkOverScan) ExprOverScan else Expr, out, computed))
        }
      case (Agg, agg: BaseAggregateExec) =>
        val share = aggregateShare(agg)
        val keys = agg.groupingExpressions
        val functions =
          agg.aggregateExpressions.map(e => aggregateFunctionClass(e.aggregateFunction))
        val arrayKeys = widthOfTypes(keys.map(_.dataType).filter(LeafColumns.containsArray))
        val dispatched = widthOfTypes(keys.filter(dispatchedThroughCodegen).map(_.dataType))
        Seq(
          Some(Term(Agg, widthOfTypes(keys.map(_.dataType)), share)),
          Some(Term(AggArrayKey, arrayKeys, share)).filter(_.width.leaves > 0),
          Some(Term(CodegenDispatch, dispatched)).filter(_.width.leaves > 0)).flatten ++
          functions.distinct.map { c =>
            Term(c, Width(functions.size, 0), share * functions.count(_ == c))
          }
      case (AggObjectHash, agg: BaseAggregateExec) =>
        Seq(Term(AggObjectHash, Width(0, 0), aggregateShare(agg)))
      case _ => Seq(Term(costClass, out))
    }
  }

  /** The terms `plan`, an operator the table prices, costs in `engine`. */
  def terms(plan: SparkPlan, engine: Engine): Seq[Term] = {
    val withoutCodegen = engine == Engine.Spark && !sparkCodegen(plan)
    costClasses(plan).flatMap(classTerms(_, plan, engine)).map { t =>
      if (withoutCodegen) {
        t.copy(costClass = CostClass.withoutCodegen.getOrElse(t.costClass, t.costClass))
      } else {
        t
      }
    }
  }

  private def price(term: Term, engine: Engine): Double =
    term.times * (engine match {
      case Engine.Comet => table.comet(term.costClass, term.width)
      case Engine.Spark => table.spark(term.costClass, term.width)
    })

  /** Bytes of a Spark row of `attributes` beyond `perByteLeafAllowance` per leaf of a column. */
  def excessBytes(attributes: Seq[Attribute]): Double =
    attributes.map { a =>
      val bytes = (EstimationUtils.getSizePerRow(Seq(a)) - 8).toDouble
      math.max(0.0, bytes - table.perByteLeafAllowance * LeafColumns.count(a.dataType))
    }.sum

  /** ns per row of running `plan`, an operator the table prices, in `engine`. */
  def operatorPrice(plan: SparkPlan, engine: Engine): Double = {
    val extra = sparkOperator(plan) match {
      case filter: FilterExec =>
        val perLeaf = engine match {
          case Engine.Comet => table.filterPassThroughPerLeafComet
          case Engine.Spark => table.filterPassThroughPerLeafSpark
        }
        perLeaf * LeafColumns.count(filter.output)
      case sort: SortExec =>
        val perByte = engine match {
          case Engine.Comet => table.sortPerByteComet
          case Engine.Spark => table.sortPerByteSpark
        }
        perByte * excessBytes(sort.output)
      case _ => 0.0
    }
    terms(plan, engine).map(price(_, engine)).sum + extra
  }

  /** Cost of running `op`, a native operator Comet converted, in `engine`. */
  def operatorCost(op: CometExec, engine: Engine): Double =
    if (costClasses(op).nonEmpty) {
      operatorPrice(op, engine)
    } else {
      engine match {
        case Engine.Comet => cometOperatorWeights.getOrElse(nameOf(op), cometOperatorWeight)
        case Engine.Spark => sparkOperatorWeight
      }
    }

  /** Cost of converting the output of `plan` from Arrow to rows once. */
  def conversion(plan: SparkPlan): Double = table.comet(C2R, widthOf(plan.output))

  /** Cost of converting the output of `plan` from rows to Arrow once. */
  def rowToColumnar(plan: SparkPlan): Double = table.comet(R2C, widthOf(plan.output))

  /** The leaf columns a shuffle moves per row, its partitioning key included. */
  def shuffleWidth(boundary: SparkPlan): Width = widthOf(boundary.children.head.output)

  /** Cost of writing and reading the shuffle `boundary` in `format`, conversions excluded. */
  def shuffleCost(boundary: SparkPlan, format: Format): Double = {
    val w = shuffleWidth(boundary)
    val partitions = boundary.outputPartitioning.numPartitions
    val bytes = excessBytes(boundary.children.head.output)
    def read: Double =
      table.comet(ShuffleRead, w) * table.shuffleReadPartitionFactor(w.leaves, partitions)
    format match {
      case NativeShuffle =>
        table.comet(ShuffleWrite, w) * table.shuffleWritePartitionFactor(w.leaves, partitions) +
          read + table.cometShuffleBytes(bytes)
      case ColumnarShuffle =>
        table.comet(ShuffleWrite, w) *
          table.columnarShuffleWritePartitionFactor(w.leaves, partitions) +
          table.columnarShuffleConstant + read + table.cometShuffleBytes(bytes)
      case _ =>
        table.spark(ShuffleWrite, w) + table.spark(ShuffleRead, w) + table.sparkShuffleBytes(
          bytes)
    }
  }

  override def price(input: Input, format: Format, conversions: Int): Double = {
    val c2r = conversion(input.boundary)
    format match {
      case NativeShuffle | SparkShuffle =>
        conversions * c2r + shuffleCost(input.boundary, format)
      case ColumnarShuffle =>
        rowToColumnar(input.boundary) + (conversions - 1) * c2r +
          shuffleCost(input.boundary, format)
      case _ => conversions * c2r
    }
  }
}

object EngineCostModel {

  /** `times` the price of `costClass` over `width`. */
  case class Term(
      costClass: EngineCostTable.CostClass,
      width: EngineCostTable.Width,
      times: Double = 1)

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
      overrides,
      conf.wholeStageMaxNumFields)
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
 *   - With `keepFiltersOverNativeScans`, a native filter over a native scan and the native
 *     projects over it stay native: the rows a filter drops are not estimated, so a Spark filter
 *     reading every row of the scan through a conversion would look cheaper than it is.
 *   - With `keepPartialAggregatesOverNativeInputs`, a native partial aggregate directly over a
 *     native scan, filter or project stays native, so the conversion is over its few output rows
 *     rather than over every input row.
 *   - Leaf scans, writes, and native aggregates whose buffers Spark and Comet cannot exchange
 *     keep the engine they were converted to (the aggregate test is the one of
 *     `COMET_UNSAFE_PARTIAL` and [[RevertNativeForTransitionHeavyStages]]).
 *   - Materialized and reused stages are leaves of fixed format, and a boundary with no consumer
 *     in the plan (a subquery or stage root) keeps its output, Arrow or rows: a Comet shuffle
 *     there may switch between native and columnar, so its producer may run in Spark.
 *   - The plan's own output is rows, so a native root pays one conversion. The root of a subquery
 *     keeps its engine: an operator outside the plan, such as the broadcast that dynamic
 *     partition pruning builds around it, may rely on it.
 *
 * A boundary costs the conversions of its format and, for a shuffle, writing and reading it in
 * the engine of its format, all priced by [[EngineCostModel]], which [[BoundaryFormats]] then
 * also uses to apply formats.
 *
 * With `spark.comet.exec.costBasedEngines.log.enabled` or `spark.comet.explain.fallback.enabled`,
 * every decided operator, shuffle and conversion is logged with its classes, leaf columns and
 * costs.
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
 * Reverted operators are tagged [[CometExecRule.ENGINE_CHOICE_SPARK_TAG]] so AQE's per-stage
 * conversion leaves them in Spark. Runs on whole plans only, like [[ChooseBoundaryFormats]]: the
 * plan without AQE, and the initial plan and every re-optimization under AQE, where [[CometRule]]
 * converts again the operators this rule reverted on an earlier plan, so that each plan is
 * decided from its own shape.
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
        case _: CometSparkToColumnarExec if label.contains(Engine.Spark) =>
          val input = children.head
          input.setTagValue(CometExecRule.ENGINE_CHOICE_SPARK_TAG, ())
          input
        case op: CometExec if label.contains(Engine.Spark) =>
          val reverted = op.originalPlan.withNewChildren(children)
          reverted.setTagValue(CometExecRule.ENGINE_CHOICE_SPARK_TAG, ())
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

  private def nativeScan(plan: SparkPlan): Boolean =
    plan.children.isEmpty && plan.isInstanceOf[CometPlan]

  /** A native filter over a native scan, or a native project over one, kept native. */
  private def keptOverScan(plan: SparkPlan): Boolean = plan match {
    case filter: CometFilterExec => nativeScan(filter.child)
    case project: CometProjectExec => keptOverScan(project.child)
    case _ => false
  }

  /** A native scan, or native filters and projects over one. */
  private def nativeInput(plan: SparkPlan): Boolean = plan match {
    case filter: CometFilterExec => nativeInput(filter.child)
    case project: CometProjectExec => nativeInput(project.child)
    case other => nativeScan(other)
  }

  /** A native partial aggregate directly over a native input, kept native. */
  private def keptPartialAggregate(plan: SparkPlan): Boolean = plan match {
    case agg: CometHashAggregateExec =>
      agg.aggregateExpressions.nonEmpty && agg.aggregateExpressions.forall(_.mode == Partial) &&
      nativeInput(agg.child)
    case _ => false
  }

  private def allowed(node: SparkPlan): Seq[Engine] = node match {
    case root if fixedRoot.exists(_ eq root) => Seq(engineOf(root))
    case op if model.table.keepFiltersOverNativeScans && keptOverScan(op) => Seq(Engine.Comet)
    case op if model.table.keepPartialAggregatesOverNativeInputs && keptPartialAggregate(op) =>
      Seq(Engine.Comet)
    case r2c: CometSparkToColumnarExec =>
      if (removableTransition(r2c)) engines else Seq(Engine.Comet)
    case op if relabelable(op) => engines
    case _: CometPlan => Seq(Engine.Comet)
    case _ => Seq(Engine.Spark)
  }

  private def current(node: SparkPlan): Engine = engineOf(node)

  private def operatorCost(node: SparkPlan, engine: Engine): Double = node match {
    case r2c: CometSparkToColumnarExec =>
      if (engine == Engine.Comet) model.rowToColumnar(r2c) else 0.0
    case op: CometExec if relabelable(op) => model.operatorCost(op, engine)
    case _ => 0.0
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
      case (Engine.Spark, Engine.Comet) => model.conversion(child)
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
    def describe(node: SparkPlan, w: EngineCostTable.Width): String =
      f"${node.nodeName}#${node.id} L=${w.leaves} nested=${w.nestedFraction}%.2f"

    def visit(node: SparkPlan, consumer: Option[Engine]): Unit = {
      val engine = label(node)
      node match {
        case op: CometExec if relabelable(op) =>
          def describeTerms(e: Engine): String = {
            val terms = model.terms(op, e).map { t =>
              f"${t.costClass}(L=${t.width.leaves} " +
                f"nested=${t.width.nestedFraction}%.2f x${t.times}%.2f)"
            }
            if (model.costClasses(op).isEmpty) "unpriced" else terms.mkString("+")
          }
          lines += f"${op.nodeName}#${op.id} " +
            f"comet=${model.operatorCost(op, Engine.Comet)}%.1f [${describeTerms(Engine.Comet)}] " +
            f"spark=${model.operatorCost(op, Engine.Spark)}%.1f [${describeTerms(Engine.Spark)}] " +
            f"-> $engine"
        case r2c: CometSparkToColumnarExec if removableTransition(r2c) =>
          val kept = if (engine == Engine.Comet) "kept" else "removed"
          lines += f"${describe(r2c, EngineCostTable.widthOf(r2c.output))} " +
            f"class=r2c cost=${model.rowToColumnar(r2c)}%.1f -> $kept"
        case shuffle: ShuffleExchangeLike if isDecidable(shuffle) =>
          lines += f"${describe(shuffle, model.shuffleWidth(shuffle))} " +
            f"class=shuffle partitions=${shuffle.outputPartitioning.numPartitions} " +
            f"native=${model.shuffleCost(shuffle, NativeShuffle)}%.1f " +
            f"columnar=${model.shuffleCost(shuffle, ColumnarShuffle)}%.1f " +
            f"spark=${model.shuffleCost(shuffle, SparkShuffle)}%.1f " +
            f"c2r=${model.conversion(shuffle)}%.1f r2c=${model.rowToColumnar(shuffle)}%.1f " +
            f"producer=${label(shuffle.child)} consumer=${consumer.getOrElse("none")}"
        case _ =>
      }
      node.children.foreach { child =>
        if (!isBoundary(child) && consumerEngine(node, engine) == Engine.Spark &&
          label(child) == Engine.Comet) {
          val w = EngineCostTable.widthOf(child.output)
          lines += f"conversion above ${describe(child, w)} class=c2r " +
            f"cost=${model.conversion(child)}%.1f"
        }
        visit(child, Some(consumerEngine(node, engine)))
      }
    }

    visit(plan, None)
    if (!isBoundary(plan) && label(plan) == Engine.Comet) {
      val w = EngineCostTable.widthOf(plan.output)
      lines += f"conversion of the output of ${describe(plan, w)} " +
        f"class=c2r cost=${model.conversion(plan)}%.1f"
    }
    lines.mkString("\n")
  }
}
