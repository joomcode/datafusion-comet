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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

import org.apache.comet.CometConf
import org.apache.comet.rules.EngineCostTable._

/**
 * The prices of [[EngineCostModel]], in ns per row: one [[EngineCostTable.Line]] per operator
 * class and schema form, the scalars of the shuffles and of the filter, and the classes of the
 * operators it prices ([[EngineCostTable.operatorClasses]]). The defaults are
 * [[EngineCostTable.default]]; `spark.comet.exec.costBasedEngines.costTable` overrides any line
 * or scalar through [[EngineCostTable.parse]].
 *
 * A price over rows of `L` leaf columns, a fraction `f` of them inside structs, arrays or maps,
 * is `(1 - f)` times the price of the flat line plus `f` times the price of the nested one.
 *
 * @param shuffleWritePartitionSlope
 *   a Comet shuffle write with P output partitions costs its line times `1 + slope * max(0, P /
 *   shuffleWritePartitionBase - 1)`
 * @param filterPassThroughPerLeaf
 *   ns per row and output leaf a filter adds in both engines to the price of its predicate
 * @param shuffleReadPerByteComet
 *   ns per byte a Comet shuffle read adds, over `cometShuffleBytesRatio` times the bytes of a
 *   Spark row
 * @param shuffleReadPerByteSpark
 *   ns per byte a Spark shuffle read adds, over the estimated size of an `UnsafeRow`
 */
case class EngineCostTable(
    lines: Map[(CostClass, Form), Line],
    shuffleWritePartitionSlope: Double,
    shuffleWritePartitionBase: Double,
    filterPassThroughPerLeaf: Double,
    shuffleReadPerByteComet: Double,
    shuffleReadPerByteSpark: Double,
    cometShuffleBytesRatio: Double) {

  def line(costClass: CostClass, form: Form): Line = lines((costClass, form))

  private def blend(costClass: CostClass, width: Width)(price: Line => Double): Double = {
    val f = width.nestedFraction
    (1 - f) * price(line(costClass, Form.Flat)) + f * price(line(costClass, Form.Nested))
  }

  /** ns per row of `costClass` run natively over rows of `width`. */
  def comet(costClass: CostClass, width: Width): Double =
    blend(costClass, width)(_.comet(width.leaves))

  /** ns per row of `costClass` run in Spark over rows of `width`. */
  def spark(costClass: CostClass, width: Width): Double =
    blend(costClass, width)(_.spark(width.leaves))

  def shuffleWritePartitionFactor(partitions: Int): Double =
    1 + shuffleWritePartitionSlope * math.max(0.0, partitions / shuffleWritePartitionBase - 1)

  /** ns per row a Comet shuffle read adds for rows of `sparkRowBytes` bytes in Spark. */
  def cometShuffleReadBytes(sparkRowBytes: Double): Double =
    shuffleReadPerByteComet * cometShuffleBytesRatio * sparkRowBytes

  /** ns per row a Spark shuffle read adds for rows of `sparkRowBytes` bytes. */
  def sparkShuffleReadBytes(sparkRowBytes: Double): Double =
    shuffleReadPerByteSpark * sparkRowBytes
}

object EngineCostTable {

  sealed abstract class CostClass(val name: String) {
    override def toString: String = name
  }

  object CostClass {
    case object ShuffleWrite extends CostClass("shuffleWrite")
    case object ShuffleRead extends CostClass("shuffleRead")
    case object Sort extends CostClass("sort")
    case object RowLocal extends CostClass("rowLocal")
    case object Agg extends CostClass("agg")
    case object C2R extends CostClass("c2r")
    val all: Seq[CostClass] = Seq(ShuffleWrite, ShuffleRead, Sort, RowLocal, Agg, C2R)
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
   * Prices per row for L leaf columns: `cometC0 + cometK0 * L + cometK1 * L * L` natively,
   * `sparkC0 + sparkK * L` in Spark. `c2r` has no Spark price.
   */
  case class Line(
      cometC0: Double,
      cometK0: Double,
      cometK1: Double,
      sparkC0: Double,
      sparkK: Double) {
    def comet(leaves: Int): Double = cometC0 + leaves * (cometK0 + cometK1 * leaves)
    def spark(leaves: Int): Double = sparkC0 + sparkK * leaves
  }

  import CostClass._
  import Form._

  /**
   * The default prices, measured per operator class and form. `c2r` prices one columnar-to-row
   * conversion, and also a row-to-columnar transition over a leaf; a Comet columnar shuffle over
   * Spark rows is priced as a Comet `shuffleWrite` plus one `c2r` of the same width, an
   * extrapolation that was not measured. The `agg` lines are provisional, set before any
   * measurement: Spark at 400 + 60 * L and Comet at 0.6 times that. Lines are `(class, form) ->
   * Line(Comet c0, Comet k0, Comet k1, Spark c0, Spark k)`.
   */
  val defaultLines: Map[(CostClass, Form), Line] = Map(
    (ShuffleWrite, Flat) -> Line(250, 50.3, 0.221, 303, 67.07),
    (ShuffleWrite, Nested) -> Line(250, 44.8, 0.209, 366, 68.96),
    (ShuffleRead, Flat) -> Line(0, 35.9, 0.043, 360, 59.38),
    (ShuffleRead, Nested) -> Line(0, 15.8, 0.071, 170, 31.87),
    (Sort, Flat) -> Line(0, 15, 0.028, 34, 28.03),
    (Sort, Nested) -> Line(0, 15, 0.009, 400, 0),
    (RowLocal, Flat) -> Line(0, 2.3, 0, 17, 2.98),
    (RowLocal, Nested) -> Line(0, 1.2, 0, 4, 1.66),
    (Agg, Flat) -> Line(240, 36, 0, 400, 60),
    (Agg, Nested) -> Line(240, 36, 0, 400, 60),
    (C2R, Flat) -> Line(0, 10.0, 0, 0, 0),
    (C2R, Nested) -> Line(0, 20.0, 0, 0, 0))

  val default: EngineCostTable = EngineCostTable(
    defaultLines,
    shuffleWritePartitionSlope = 0.08,
    shuffleWritePartitionBase = 250,
    filterPassThroughPerLeaf = 0.5,
    shuffleReadPerByteComet = 0,
    shuffleReadPerByteSpark = 0,
    cometShuffleBytesRatio = 0.5)

  /**
   * The classes of each converted Spark operator the table prices, by the simple name of its
   * class; an operator with several classes costs the sum of their prices over the same leaf
   * columns. Shuffles are priced as `shuffleWrite` and `shuffleRead` by their format, and
   * conversions as `c2r`. Any other operator keeps the constant weights of [[EngineCostModel]].
   */
  val operatorClasses: Map[String, Seq[CostClass]] = Map(
    "SortExec" -> Seq(Sort),
    "SortMergeJoinExec" -> Seq(Sort),
    "WindowExec" -> Seq(Sort),
    "WindowGroupLimitExec" -> Seq(RowLocal),
    "ExpandExec" -> Seq(RowLocal),
    "FilterExec" -> Seq(RowLocal),
    "ProjectExec" -> Seq(RowLocal),
    "BroadcastHashJoinExec" -> Seq(RowLocal),
    "UnionExec" -> Seq(RowLocal),
    "CoalesceExec" -> Seq(RowLocal),
    "LocalLimitExec" -> Seq(RowLocal),
    "GlobalLimitExec" -> Seq(RowLocal),
    "HashAggregateExec" -> Seq(Agg),
    "ObjectHashAggregateExec" -> Seq(Agg),
    "SortAggregateExec" -> Seq(Agg, Sort))

  private val scalars: Map[String, (EngineCostTable, Double) => EngineCostTable] = Map(
    "shuffleWritePartitionSlope" -> ((t, v) => t.copy(shuffleWritePartitionSlope = v)),
    "shuffleWritePartitionBase" -> ((t, v) => t.copy(shuffleWritePartitionBase = v)),
    "filterPassThroughPerLeaf" -> ((t, v) => t.copy(filterPassThroughPerLeaf = v)),
    "shuffleReadPerByte.comet" -> ((t, v) => t.copy(shuffleReadPerByteComet = v)),
    "shuffleReadPerByte.spark" -> ((t, v) => t.copy(shuffleReadPerByteSpark = v)),
    "cometShuffleBytesRatio" -> ((t, v) => t.copy(cometShuffleBytesRatio = v)))

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

    spec.split(";").map(_.trim).filter(_.nonEmpty).foldLeft(base) { (table, entry) =>
      entry.split("=", -1) match {
        case Array(rawName, value) =>
          val name = rawName.trim
          scalars.get(name) match {
            case Some(set) =>
              val v = numbers(entry, value, Set(1), s"$name=<number>").head
              if (name == "shuffleWritePartitionBase" && v <= 0) {
                fail(entry, s"$name=<number above 0>")
              }
              set(table, v)
            case None =>
              name.split("\\.") match {
                case Array(c, f, engine) =>
                  val costClass = CostClass.all
                    .find(_.name == c)
                    .getOrElse(fail(entry, s"a class among ${CostClass.all.mkString(", ")}"))
                  val form = Form.all
                    .find(_.name == f)
                    .getOrElse(fail(entry, s"a form among ${Form.all.mkString(", ")}"))
                  val line = table.line(costClass, form)
                  val updated = engine match {
                    case "comet" =>
                      numbers(
                        entry,
                        value,
                        Set(2, 3),
                        s"$name=<k0>,<k1> or <c0>,<k0>,<k1>") match {
                        case Seq(k0, k1) => line.copy(cometK0 = k0, cometK1 = k1)
                        case Seq(c0, k0, k1) =>
                          line.copy(cometC0 = c0, cometK0 = k0, cometK1 = k1)
                      }
                    case "spark" if costClass != C2R =>
                      val k = numbers(entry, value, Set(2), s"$name=<c0>,<k>")
                      line.copy(sparkC0 = k(0), sparkK = k(1))
                    case "spark" => fail(entry, s"no spark line for $C2R")
                    case _ => fail(entry, "an engine among comet, spark")
                  }
                  table.copy(lines = table.lines.updated((costClass, form), updated))
                case _ =>
                  fail(
                    entry,
                    s"<class>.<form>.<engine>=<numbers> or one of " +
                      s"${scalars.keys.toSeq.sorted.mkString(", ")}=<number>")
              }
          }
        case _ => fail(entry, "<key>=<value>")
      }
    }
  }
}
