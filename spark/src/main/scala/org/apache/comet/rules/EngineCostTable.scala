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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, Attribute, Expression, FrameLessOffsetWindowFunction, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ApproximatePercentile, CollectList, CollectSet, DeclarativeAggregate, Percentile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

import org.apache.comet.CometConf
import org.apache.comet.rules.EngineCostTable._

/**
 * The prices of [[EngineCostModel]], in ns per row: one [[EngineCostTable.Line]] per class and
 * schema form, the scalars of shuffles, filters, sorts and per-byte terms, and the classes of the
 * operators and functions it prices ([[EngineCostTable.operatorClasses]],
 * [[EngineCostTable.aggregateFunctionClass]], [[EngineCostTable.windowFunctionClass]]). The
 * defaults are [[EngineCostTable.default]]; `spark.comet.exec.costBasedEngines.costTable`
 * overrides any line or scalar through [[EngineCostTable.parse]].
 *
 * A price over rows of `L` leaf columns, a fraction `f` of them inside structs, arrays or maps,
 * is `(1 - f)` times the price of the flat line plus `f` times the price of the nested one. The
 * quadratic term of a Comet line grows as `k1 * L * min(L, quadraticLeafCap)`.
 *
 * @param shuffleWritePartitionSlope
 *   with `shuffleWritePartitionSlopePerLeaf`, a native shuffle write over `L` leaves with P
 *   output partitions costs its line times `1 + (slope + slopePerLeaf * L) * max(0, P /
 *   shuffleWritePartitionBase - 1)`
 * @param shuffleReadPartitionSlope
 *   with `shuffleReadPartitionSlopePerLeaf`, the same factor for a Comet shuffle read
 * @param columnarShuffleWritePartitionSlope
 *   with `columnarShuffleWritePartitionSlopePerLeaf`, the same factor for the write of Comet's
 *   columnar shuffle, which also costs `columnarShuffleConstant` and one `r2c`
 * @param filterPassThroughPerLeafComet
 *   ns per row and output leaf a native filter adds to the price of its predicate, for copying
 *   the rows that pass; `filterPassThroughPerLeafSpark` the same in Spark
 * @param perByteLeafAllowance
 *   bytes per leaf the lines already price: the per-byte terms apply to the estimated bytes of a
 *   column beyond this many per leaf
 * @param cometShuffleBytesRatio
 *   bytes a Comet shuffle moves per byte of a Spark `UnsafeRow`
 * @param sortSpillFraction
 *   fraction of the rows of every sort priced as spilled, by the `sortSpill` line on top of the
 *   `sort` one
 * @param quadraticLeafCap
 *   leaves beyond which the quadratic term of a Comet line grows linearly
 * @param keepFiltersOverNativeScans
 *   keep a native filter over a native scan, and the native projects over it, native whatever
 *   their prices: the rows a filter drops are not estimated, so the model cannot see that a Spark
 *   filter reads every row of the scan through a conversion
 * @param keepPartialAggregatesOverNativeInputs
 *   keep a native partial aggregate directly over a native scan, filter or project native
 *   whatever its price: it reduces its rows many times, which the model does not see, and a
 *   conversion below it would convert every row
 */
case class EngineCostTable(
    lines: Map[(CostClass, Form), Line],
    shuffleWritePartitionBase: Double,
    shuffleWritePartitionSlope: Double,
    shuffleWritePartitionSlopePerLeaf: Double,
    shuffleReadPartitionSlope: Double,
    shuffleReadPartitionSlopePerLeaf: Double,
    columnarShuffleWritePartitionSlope: Double,
    columnarShuffleWritePartitionSlopePerLeaf: Double,
    columnarShuffleConstant: Double,
    filterPassThroughPerLeafComet: Double,
    filterPassThroughPerLeafSpark: Double,
    shuffleWritePerByteComet: Double,
    shuffleWritePerByteSpark: Double,
    shuffleReadPerByteComet: Double,
    shuffleReadPerByteSpark: Double,
    sortPerByteComet: Double,
    sortPerByteSpark: Double,
    perByteLeafAllowance: Double,
    cometShuffleBytesRatio: Double,
    sortSpillFraction: Double,
    quadraticLeafCap: Double,
    keepFiltersOverNativeScans: Boolean,
    keepPartialAggregatesOverNativeInputs: Boolean) {

  def line(costClass: CostClass, form: Form): Line = lines((costClass, form))

  private def blend(costClass: CostClass, width: Width)(price: Line => Double): Double = {
    val f = width.nestedFraction
    (1 - f) * price(line(costClass, Form.Flat)) + f * price(line(costClass, Form.Nested))
  }

  /** ns per row of `costClass` run natively over rows of `width`. */
  def comet(costClass: CostClass, width: Width): Double =
    blend(costClass, width)(_.comet(width.leaves, quadraticLeafCap))

  /** ns per row of `costClass` run in Spark over rows of `width`. */
  def spark(costClass: CostClass, width: Width): Double =
    blend(costClass, width)(_.spark(width.leaves))

  private def partitionFactor(slope: Double, perLeaf: Double, leaves: Int, partitions: Int) =
    1 + (slope + perLeaf * leaves) * math.max(0.0, partitions / shuffleWritePartitionBase - 1)

  def shuffleWritePartitionFactor(leaves: Int, partitions: Int): Double =
    partitionFactor(
      shuffleWritePartitionSlope,
      shuffleWritePartitionSlopePerLeaf,
      leaves,
      partitions)

  def shuffleReadPartitionFactor(leaves: Int, partitions: Int): Double =
    partitionFactor(
      shuffleReadPartitionSlope,
      shuffleReadPartitionSlopePerLeaf,
      leaves,
      partitions)

  def columnarShuffleWritePartitionFactor(leaves: Int, partitions: Int): Double =
    partitionFactor(
      columnarShuffleWritePartitionSlope,
      columnarShuffleWritePartitionSlopePerLeaf,
      leaves,
      partitions)

  /** ns per row a Comet shuffle adds for `excessBytes` bytes of a Spark row beyond the lines. */
  def cometShuffleBytes(excessBytes: Double): Double =
    (shuffleWritePerByteComet + shuffleReadPerByteComet) * cometShuffleBytesRatio * excessBytes

  /** ns per row a Spark shuffle adds for `excessBytes` bytes of a Spark row beyond the lines. */
  def sparkShuffleBytes(excessBytes: Double): Double =
    (shuffleWritePerByteSpark + shuffleReadPerByteSpark) * excessBytes
}

object EngineCostTable {

  /**
   * A class of prices. `comet` and `spark` tell whether it has a price in that engine: `c2r` and
   * `r2c` are conversions only Comet pays, and the classes ending in `NoCodegen` or `OverScan`
   * are the Spark prices of another class in another situation.
   */
  sealed abstract class CostClass(val name: String) {
    def comet: Boolean = true
    def spark: Boolean = true
    override def toString: String = name
  }

  abstract class SparkOnly(name: String) extends CostClass(name) {
    override def comet: Boolean = false
  }

  abstract class CometOnly(name: String) extends CostClass(name) {
    override def spark: Boolean = false
  }

  object CostClass {
    case object ShuffleWrite extends CostClass("shuffleWrite")
    case object ShuffleRead extends CostClass("shuffleRead")
    case object Sort extends CostClass("sort")
    case object SortSpill extends CostClass("sortSpill")
    case object Smj extends CostClass("smj")
    case object Bhj extends CostClass("bhj")
    case object Predicate extends CostClass("predicate")
    case object ProjectPassThrough extends CostClass("projectPassThrough")
    case object Expr extends CostClass("expression")
    case object ExprOverScan extends SparkOnly("expressionOverScan")
    case object Agg extends CostClass("agg")
    case object AggObjectHash extends CostClass("aggObjectHash")
    case object AggDeclarative extends CostClass("aggDeclarative")
    case object AggDeclarativeNoCodegen extends SparkOnly("aggDeclarativeNoCodegen")
    case object AggCollectList extends CostClass("aggCollectList")
    case object AggCollectSet extends CostClass("aggCollectSet")
    case object AggPercentile extends CostClass("aggPercentile")
    case object AggPercentileApprox extends CostClass("aggPercentileApprox")
    case object AggOther extends CostClass("aggOther")
    case object AggArrayKey extends CostClass("aggArrayKey")
    case object CodegenDispatch extends CostClass("codegenDispatch")
    case object Window extends CostClass("window")
    case object WindowAggregate extends CostClass("windowAggregate")
    case object WindowOffset extends CostClass("windowOffset")
    case object WindowRank extends CostClass("windowRank")
    case object WglPartial extends CostClass("wglPartial")
    case object WglFinal extends CostClass("wglFinal")
    case object Expand extends CostClass("expand")
    case object ExpandNoCodegen extends SparkOnly("expandNoCodegen")
    case object Generate extends CostClass("generate")
    case object GenerateNoCodegen extends SparkOnly("generateNoCodegen")
    case object RowLocal extends CostClass("rowLocal")
    case object C2R extends CometOnly("c2r")
    case object R2C extends CometOnly("r2c")
    val all: Seq[CostClass] = Seq(
      ShuffleWrite,
      ShuffleRead,
      Sort,
      SortSpill,
      Smj,
      Bhj,
      Predicate,
      ProjectPassThrough,
      Expr,
      ExprOverScan,
      Agg,
      AggObjectHash,
      AggDeclarative,
      AggDeclarativeNoCodegen,
      AggCollectList,
      AggCollectSet,
      AggPercentile,
      AggPercentileApprox,
      AggOther,
      AggArrayKey,
      CodegenDispatch,
      Window,
      WindowAggregate,
      WindowOffset,
      WindowRank,
      WglPartial,
      WglFinal,
      Expand,
      ExpandNoCodegen,
      Generate,
      GenerateNoCodegen,
      RowLocal,
      C2R,
      R2C)

    /** The Spark class of `costClass` for an operator Spark runs without whole-stage codegen. */
    val withoutCodegen: Map[CostClass, CostClass] = Map(
      AggDeclarative -> AggDeclarativeNoCodegen,
      Expand -> ExpandNoCodegen,
      Generate -> GenerateNoCodegen)
  }

  sealed abstract class Form(val name: String) {
    override def toString: String = name
  }

  object Form {
    case object Flat extends Form("flat")
    case object Nested extends Form("nested")
    val all: Seq[Form] = Seq(Flat, Nested)
  }

  /** The leaf columns an operator processes per row, `nested` of them inside a nested type. */
  case class Width(leaves: Int, nested: Int) {
    def nestedFraction: Double = if (leaves > 0) nested.toDouble / leaves else 0.0
  }

  def widthOfTypes(dataTypes: Seq[DataType]): Width =
    Width(
      dataTypes.map(LeafColumns.count).sum,
      dataTypes.filter(LeafColumns.isNested).map(LeafColumns.count).sum)

  def widthOf(attributes: Seq[Attribute]): Width = widthOfTypes(attributes.map(_.dataType))

  /**
   * Prices per row for L leaf columns: `cometC0 + cometK0 * L + cometK1 * L * min(L, cap)`
   * natively, `sparkC0 + sparkK * L` in Spark.
   */
  case class Line(
      cometC0: Double,
      cometK0: Double,
      cometK1: Double,
      sparkC0: Double,
      sparkK: Double) {
    def comet(leaves: Int, cap: Double): Double =
      cometC0 + leaves * (cometK0 + cometK1 * math.min(leaves.toDouble, cap))
    def spark(leaves: Int): Double = sparkC0 + sparkK * leaves
  }

  import CostClass._
  import Form._

  private def both(costClass: CostClass, line: Line): Seq[((CostClass, Form), Line)] =
    Seq((costClass, Flat) -> line, (costClass, Nested) -> line)

  /**
   * The default prices, measured on pr29 (calib29, calib29c and calib29d) with L counting every
   * leaf of the row. Lines are `(class, form) -> Line(Comet c0, Comet k0, Comet k1, Spark c0,
   * Spark k)`; a class priced alike in both forms has one line for both.
   *
   *   - `shuffleWrite` (at `shuffleWritePartitionBase` partitions) and `shuffleRead`: every leaf
   *     of the shuffled rows, Spark's fetch wait and the read conversion excluded. Spark is
   *     averaged over 250 to 4800 partitions, on which it does not depend.
   *   - `sort`: every leaf of the sorted rows. Spark sorts pointers, so its price barely depends
   *     on the width; Comet flat is noisy (maxrel 0.6). `sortSpill` is what spilling adds, on a
   *     fraction `sortSpillFraction` of rows, none by default: rows are not estimated, so a spill
   *     cannot be predicted.
   *   - `smj`: the join over its sorted inputs, every output leaf, noisy. `bhj`: the probe side,
   *     every output leaf; the nested `bhj` is noisy (Comet 0 to 350, Spark 50 to 4200 ns) and
   *     takes the flat line.
   *   - `predicate`: a filter over the leaves its predicate reads, Spark noisy (maxrel 0.5 to
   *     0.8); passing rows costs the filter scalars.
   *   - `projectPassThrough`: a project over every output leaf. Spark copies the row only when
   *     its input is already a Spark row: over a scan, possibly through filters and projects, the
   *     copy is fused into the scan and costs nothing. `expression`: once per leaf a project
   *     computes, Spark growing with the output leaves (2 to 51 ns); `expressionOverScan` is
   *     Spark's price over a scan (1 to 4 ns).
   *   - `agg`: the grouping keys of an aggregate, partial and final together, so each phase of a
   *     two-phase aggregate costs half. Comet flat is noisy (maxrel 1.2) and Spark's nested point
   *     at 65 leaves an outlier. Each aggregate function adds the price of its class:
   *     `aggDeclarative` (sum, count, min, max, avg, first, ...; Spark `aggDeclarativeNoCodegen`
   *     beyond `spark.sql.codegen.maxFields`), `aggCollectList`, `aggCollectSet`,
   *     `aggPercentile`, `aggPercentileApprox` or `aggOther` (other imperative aggregates, by
   *     analogy, unmeasured); an object hash aggregate adds `aggObjectHash`.
   *   - `aggArrayKey`: what each leaf of a grouping key holding an array adds to `agg`, partial
   *     and final together, fitted by least squares on two distincts without reduction:
   *     star_order_2020 (18 key leaves, 7 in arrays of 2.7 and 1.1 elements on average; Comet 4.0
   *     us per row in each phase, Spark 0.71 to 0.98) and calib29's `aggkeys` over an array of
   *     3-leaf structs of 2.7 elements (5 leaves, 3 in the array; Comet 2.4 us, Spark 1.7).
   *     `codegenDispatch`: once per leaf of a grouping key computed through the JVM codegen
   *     dispatcher, such as the `transform` normalizing the floating-point fields of an array, in
   *     the phase that computes it: Comet 0.65 us for 3 leaves on star_order_2020 (partial minus
   *     final), Spark 0.4 us in a local micro-benchmark (Spark's codegen of the same transform).
   *   - `window`: the operator with one `row_number`, over the leaves of its input (Spark noisy,
   *     maxrel 0.46). Each window function adds the price of its class at L = the number of
   *     window functions of the operator: `windowAggregate` (measured on a running sum, other
   *     aggregates by analogy), `windowOffset` (measured on lag, lead by symmetry) or
   *     `windowRank` (in the line already).
   *   - `wglPartial` and `wglFinal`: the two phases of a window group limit, every output leaf.
   *     Spark's prices are noisy; its final phase at 514 leaves (6 us) is unexplained and not
   *     taken.
   *   - `expand`: Comet passes the arrays of its projections without copying, and Spark's codegen
   *     leaves the copy to its consumer; without codegen Spark copies every output leaf of every
   *     projection (`expandNoCodegen`, nested fitted on one point).
   *   - `generate`: every output leaf, once per input row. Spark costs nothing with codegen and
   *     `generateNoCodegen` without; the nested lines are fitted on struct explodes.
   *   - `rowLocal`: unions, coalesces and limits, which cost nothing measurable in either engine.
   *   - `c2r` and `r2c`: one conversion of a row between Spark and Arrow, `r2c` from a
   *     micro-benchmark, since on a cluster it is not measurable.
   *
   * An array counts the leaves of its element once, whatever its length: the plan has no average
   * length, so an array of structs is priced as one struct, underestimating long arrays (by up to
   * 37 times at 171 elements in a Comet shuffle write, partly cancelled in the ratio of the
   * engines).
   */
  val defaultLines: Map[(CostClass, Form), Line] = Map[(CostClass, Form), Line](
    (ShuffleWrite, Flat) -> Line(0, 48.95, 0.037, 69, 67.21),
    (ShuffleWrite, Nested) -> Line(46, 39.59, 0.031, 686, 47.77),
    (ShuffleRead, Flat) -> Line(0, 14.73, 0.032, 67, 26.34),
    (ShuffleRead, Nested) -> Line(0, 10.79, 0.019, 167, 19.02),
    (Sort, Flat) -> Line(224, 0, 0.023, 646, 0),
    (Sort, Nested) -> Line(244, 2.69, 0.016, 770, 2.34),
    (SortSpill, Flat) -> Line(265, 49.0, 0, 495, 38.5),
    (SortSpill, Nested) -> Line(324, 45.3, 0, 502, 33.5),
    (Smj, Flat) -> Line(0, 4, 0, 0, 35),
    (Smj, Nested) -> Line(0, 0.45, 0.046, 0, 32),
    (Bhj, Flat) -> Line(72, 2.3, 0.002, 0, 20.5),
    (Bhj, Nested) -> Line(72, 2.3, 0.002, 0, 20.5),
    (Predicate, Flat) -> Line(11, 2.14, 0, 0, 5.4),
    (Predicate, Nested) -> Line(17, 2.19, 0, 0, 8.2),
    (ProjectPassThrough, Flat) -> Line(1.25, 0.057, 0, 0, 10.5),
    (ProjectPassThrough, Nested) -> Line(1.15, 0.015, 0, 15, 0.5),
    (Expr, Flat) -> Line(1, 0, 0, 2.3, 0.1),
    (Expr, Nested) -> Line(1, 0, 0, 9.8, 0.15),
    (Agg, Flat) -> Line(0, 3.2, 0.082, 0, 62.1),
    (Agg, Nested) -> Line(564, 62.4, 0.242, 0, 107.5),
    (Window, Flat) -> Line(53, 5.08, 0.002, 0, 15.3),
    (Window, Nested) -> Line(48, 3.87, 0.012, 35, 7.61),
    (WglPartial, Flat) -> Line(41, 3.82, 0, 250, 0),
    (WglPartial, Nested) -> Line(38, 2.80, 0, 300, 0),
    (WglFinal, Flat) -> Line(5.1, 0.26, 0.0002, 0, 0),
    (WglFinal, Nested) -> Line(5.4, 0.19, 0.0004, 0, 0),
    (ExpandNoCodegen, Flat) -> Line(0, 0, 0, 0, 21),
    (ExpandNoCodegen, Nested) -> Line(0, 0, 0, 0, 1.7),
    (Generate, Flat) -> Line(0, 2.5, 0, 0, 0),
    (Generate, Nested) -> Line(0, 1.9, 0, 0, 0),
    (GenerateNoCodegen, Flat) -> Line(0, 0, 0, 0, 18),
    (GenerateNoCodegen, Nested) -> Line(0, 0, 0, 0, 2.9),
    (C2R, Flat) -> Line(0, 8.0, 0.011, 0, 0),
    (C2R, Nested) -> Line(20, 11.9, 0.019, 0, 0),
    (R2C, Flat) -> Line(3.4, 7.67, 0.036, 0, 0),
    (R2C, Nested) -> Line(0, 9.74, 0.040, 0, 0)) ++
    both(ExprOverScan, Line(0, 0, 0, 2.5, 0)) ++
    both(AggObjectHash, Line(1000, 0, 0, 2500, 0)) ++
    both(AggDeclarative, Line(6, 0, 0, 15, 0)) ++
    both(AggDeclarativeNoCodegen, Line(0, 0, 0, 170, 0)) ++
    both(AggCollectList, Line(28, 0, 0, 1700, 0)) ++
    both(AggCollectSet, Line(105, 0, 0, 1600, 0)) ++
    both(AggPercentile, Line(130, 0, 0, 2400, 0)) ++
    both(AggPercentileApprox, Line(270, 0, 0, 3900, 0)) ++
    both(AggOther, Line(100, 0, 0, 1700, 0)) ++
    both(AggArrayKey, Line(0, 937, 0, 0, 76)) ++
    both(CodegenDispatch, Line(0, 217, 0, 0, 133)) ++
    both(WindowAggregate, Line(380, 0, 0, 20, 0.8)) ++
    both(WindowOffset, Line(60, 0, 0, 0, 0.25)) ++
    both(WindowRank, Line(0, 0, 0, 0, 0)) ++
    both(Expand, Line(0, 0, 0, 0, 0)) ++
    both(RowLocal, Line(0, 0, 0, 0, 0))

  /**
   * The default scalars. The partition slopes grow with the leaves because the overhead of a
   * partition in a map task scales with the Arrow columns; Spark's shuffle does not depend on the
   * partitions. The columnar write slope (0.0008 to 0.0013 per leaf) and constant are fitted on
   * five widths. Spark's filter passes its rows lazily; Comet copies those that pass (1.5 ns per
   * leaf with half the rows passing). The per-byte terms are fitted on binary and array columns
   * of 856 to 20520 bytes per row and apply beyond the 12 bytes per leaf the lines were fitted
   * on: Comet's write 0.5, read 0.6 and sort 0.15 ns per byte, Spark's write 3.6 (CPU and disk)
   * and read 0.45 (CPU). Arrays cost about twice as much per byte and take the same prices, and a
   * Comet shuffle moves about as many bytes as Spark's on such columns.
   */
  val default: EngineCostTable = EngineCostTable(
    defaultLines,
    shuffleWritePartitionBase = 250,
    shuffleWritePartitionSlope = 0.04,
    shuffleWritePartitionSlopePerLeaf = 0.00036,
    shuffleReadPartitionSlope = 0.06,
    shuffleReadPartitionSlopePerLeaf = 0.00025,
    columnarShuffleWritePartitionSlope = 0,
    columnarShuffleWritePartitionSlopePerLeaf = 0.001,
    columnarShuffleConstant = 400,
    filterPassThroughPerLeafComet = 1.5,
    filterPassThroughPerLeafSpark = 0,
    shuffleWritePerByteComet = 0.5,
    shuffleWritePerByteSpark = 3.6,
    shuffleReadPerByteComet = 0.6,
    shuffleReadPerByteSpark = 0.45,
    sortPerByteComet = 0.15,
    sortPerByteSpark = 0,
    perByteLeafAllowance = 12,
    cometShuffleBytesRatio = 1.0,
    sortSpillFraction = 0,
    quadraticLeafCap = 600,
    keepFiltersOverNativeScans = true,
    keepPartialAggregatesOverNativeInputs = true)

  /**
   * The classes of each converted Spark operator the table prices, by the simple name of its
   * class; the operator costs the sum of their prices. A window group limit costs `wglPartial` or
   * `wglFinal` by its mode, aggregates and windows also the classes of their functions, and Spark
   * without whole-stage codegen the classes of [[CostClass.withoutCodegen]]. Shuffles are priced
   * as `shuffleWrite` and `shuffleRead` by their format, and conversions as `c2r` and `r2c`. Any
   * other operator keeps the constant weights of [[EngineCostModel]].
   */
  val operatorClasses: Map[String, Seq[CostClass]] = Map(
    "SortExec" -> Seq(Sort),
    "SortMergeJoinExec" -> Seq(Smj),
    "BroadcastHashJoinExec" -> Seq(Bhj),
    "WindowExec" -> Seq(Window),
    "WindowGroupLimitExec" -> Seq(WglPartial, WglFinal),
    "ExpandExec" -> Seq(Expand),
    "GenerateExec" -> Seq(Generate),
    "FilterExec" -> Seq(Predicate),
    "ProjectExec" -> Seq(ProjectPassThrough, Expr),
    "UnionExec" -> Seq(RowLocal),
    "CoalesceExec" -> Seq(RowLocal),
    "LocalLimitExec" -> Seq(RowLocal),
    "GlobalLimitExec" -> Seq(RowLocal),
    "HashAggregateExec" -> Seq(Agg),
    "ObjectHashAggregateExec" -> Seq(Agg, AggObjectHash),
    "SortAggregateExec" -> Seq(Agg, Sort))

  /** The class of an aggregate function. */
  def aggregateFunctionClass(function: AggregateFunction): CostClass = function match {
    case _: CollectList => AggCollectList
    case _: CollectSet => AggCollectSet
    case _: Percentile => AggPercentile
    case _: ApproximatePercentile => AggPercentileApprox
    case _: DeclarativeAggregate => AggDeclarative
    case _ => AggOther
  }

  /** The class of a window function, given the expression of a window that computes it. */
  def windowFunctionClass(expression: Expression): CostClass =
    expression.collectFirst { case w: WindowExpression => w.windowFunction } match {
      case Some(_: FrameLessOffsetWindowFunction) => WindowOffset
      case Some(_: AggregateWindowFunction) => WindowRank
      case Some(_: AggregateExpression) => WindowAggregate
      case _ => WindowAggregate
    }

  private val scalars: Map[String, (EngineCostTable, Double) => EngineCostTable] = Map(
    "shuffleWritePartitionBase" -> ((t, v) => t.copy(shuffleWritePartitionBase = v)),
    "shuffleWritePartitionSlope" -> ((t, v) => t.copy(shuffleWritePartitionSlope = v)),
    "shuffleWritePartitionSlopePerLeaf" ->
      ((t, v) => t.copy(shuffleWritePartitionSlopePerLeaf = v)),
    "shuffleReadPartitionSlope" -> ((t, v) => t.copy(shuffleReadPartitionSlope = v)),
    "shuffleReadPartitionSlopePerLeaf" ->
      ((t, v) => t.copy(shuffleReadPartitionSlopePerLeaf = v)),
    "columnarShuffleWritePartitionSlope" ->
      ((t, v) => t.copy(columnarShuffleWritePartitionSlope = v)),
    "columnarShuffleWritePartitionSlopePerLeaf" ->
      ((t, v) => t.copy(columnarShuffleWritePartitionSlopePerLeaf = v)),
    "columnarShuffleConstant" -> ((t, v) => t.copy(columnarShuffleConstant = v)),
    "filterPassThroughPerLeaf.comet" -> ((t, v) => t.copy(filterPassThroughPerLeafComet = v)),
    "filterPassThroughPerLeaf.spark" -> ((t, v) => t.copy(filterPassThroughPerLeafSpark = v)),
    "shuffleWritePerByte.comet" -> ((t, v) => t.copy(shuffleWritePerByteComet = v)),
    "shuffleWritePerByte.spark" -> ((t, v) => t.copy(shuffleWritePerByteSpark = v)),
    "shuffleReadPerByte.comet" -> ((t, v) => t.copy(shuffleReadPerByteComet = v)),
    "shuffleReadPerByte.spark" -> ((t, v) => t.copy(shuffleReadPerByteSpark = v)),
    "sortPerByte.comet" -> ((t, v) => t.copy(sortPerByteComet = v)),
    "sortPerByte.spark" -> ((t, v) => t.copy(sortPerByteSpark = v)),
    "perByteLeafAllowance" -> ((t, v) => t.copy(perByteLeafAllowance = v)),
    "cometShuffleBytesRatio" -> ((t, v) => t.copy(cometShuffleBytesRatio = v)),
    "sortSpillFraction" -> ((t, v) => t.copy(sortSpillFraction = v)),
    "quadraticLeafCap" -> ((t, v) => t.copy(quadraticLeafCap = v)))

  private val positiveScalars = Set("shuffleWritePartitionBase", "quadraticLeafCap")

  private val flags: Map[String, (EngineCostTable, Boolean) => EngineCostTable] = Map(
    "keepFiltersOverNativeScans" -> ((t, v) => t.copy(keepFiltersOverNativeScans = v)),
    "keepPartialAggregatesOverNativeInputs" ->
      ((t, v) => t.copy(keepPartialAggregatesOverNativeInputs = v)))

  def apply(conf: SQLConf): EngineCostTable =
    parse(CometConf.COMET_EXEC_COST_BASED_ENGINES_COST_TABLE.get(conf))

  /**
   * `base` with the entries of `spec` applied, in the format of
   * `spark.comet.exec.costBasedEngines.costTable`.
   */
  def parse(spec: String, base: EngineCostTable = default): EngineCostTable = {
    val key = CometConf.COMET_EXEC_COST_BASED_ENGINES_COST_TABLE.key
    def fail(entry: String, expected: String): Nothing =
      throw new IllegalArgumentException(s"$key: expected $expected, got '$entry'")

    def numbers(entry: String, value: String, counts: Set[Int], expected: String): Seq[Double] = {
      val parsed = value.split(",", -1).map(v => Try(v.trim.toDouble).toOption)
      if (!counts.contains(parsed.length) || parsed.exists(v =>
          v.isEmpty || v.get.isNaN ||
            v.get.isInfinite)) {
        fail(entry, expected)
      }
      parsed.map(_.get).toSeq
    }

    def costClass(entry: String, name: String): CostClass =
      CostClass.all
        .find(_.name == name)
        .getOrElse(fail(entry, s"a class among ${CostClass.all.mkString(", ")}"))

    def setLine(
        table: EngineCostTable,
        entry: String,
        name: String,
        value: String,
        costClass: CostClass,
        forms: Seq[Form],
        engine: String): EngineCostTable = {
      def update(change: Line => Line): EngineCostTable =
        table.copy(lines = forms.foldLeft(table.lines) { (lines, form) =>
          lines.updated((costClass, form), change(lines((costClass, form))))
        })
      engine match {
        case "comet" if costClass.comet =>
          numbers(entry, value, Set(2, 3), s"$name=<k0>,<k1> or <c0>,<k0>,<k1>") match {
            case Seq(k0, k1) => update(_.copy(cometK0 = k0, cometK1 = k1))
            case Seq(c0, k0, k1) => update(_.copy(cometC0 = c0, cometK0 = k0, cometK1 = k1))
          }
        case "spark" if costClass.spark =>
          val k = numbers(entry, value, Set(2), s"$name=<c0>,<k>")
          update(_.copy(sparkC0 = k(0), sparkK = k(1)))
        case "comet" | "spark" => fail(entry, s"no $engine line for $costClass")
        case _ => fail(entry, "an engine among comet, spark")
      }
    }

    spec.split(";").map(_.trim).filter(_.nonEmpty).foldLeft(base) { (table, entry) =>
      entry.split("=", -1) match {
        case Array(rawName, value) =>
          val name = rawName.trim
          (scalars.get(name), flags.get(name)) match {
            case (_, Some(set)) =>
              value.trim match {
                case "true" => set(table, true)
                case "false" => set(table, false)
                case _ => fail(entry, s"$name=<true or false>")
              }
            case (Some(set), _) =>
              val v = numbers(entry, value, Set(1), s"$name=<number>").head
              if (positiveScalars.contains(name) && v <= 0) {
                fail(entry, s"$name=<number above 0>")
              }
              set(table, v)
            case _ =>
              name.split("\\.") match {
                case Array(c, f, engine) =>
                  val form = Form.all
                    .find(_.name == f)
                    .getOrElse(fail(entry, s"a form among ${Form.all.mkString(", ")}"))
                  setLine(table, entry, name, value, costClass(entry, c), Seq(form), engine)
                case Array(c, engine) =>
                  setLine(table, entry, name, value, costClass(entry, c), Form.all, engine)
                case _ =>
                  fail(
                    entry,
                    "<class>[.<form>].<engine>=<numbers> or one of " +
                      s"${scalars.keys.toSeq.sorted.mkString(", ")}=<number>, or " +
                      s"${flags.keys.toSeq.sorted.mkString(", ")}=<true or false>")
              }
          }
        case _ => fail(entry, "<key>=<value>")
      }
    }
  }
}
