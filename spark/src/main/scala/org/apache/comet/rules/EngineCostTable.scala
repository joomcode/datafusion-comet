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

import org.apache.comet.CometConf
import org.apache.comet.rules.EngineCostTable._

/**
 * The prices of [[EngineCostModel]], in ns: one [[EngineCostTable.Line]] per operator class and
 * schema form, and the scalars of the shuffle write and of the memory risk. The defaults are
 * [[EngineCostTable.default]]; `spark.comet.exec.costBasedEngines.costTable` overrides any of
 * them through [[EngineCostTable.parse]].
 *
 * @param shuffleWritePartitionSlope
 *   a Comet shuffle write with P output partitions costs its line times `1 + slope * max(0, P /
 *   shuffleWritePartitionBase - 1)`
 * @param oomRiskPenalty
 *   ns per row added where a native operator holding memory runs below a Spark operator holding
 *   memory in the same stage
 */
case class EngineCostTable(
    lines: Map[(CostClass, Form), Line],
    shuffleWritePartitionSlope: Double,
    shuffleWritePartitionBase: Double,
    oomRiskPenalty: Double) {

  def line(costClass: CostClass, form: Form): Line = lines((costClass, form))

  /** ns per row of `costClass` run natively over rows of `width`. */
  def comet(costClass: CostClass, width: Width): Double =
    line(costClass, width.form).comet(width.leaves)

  /** ns per row of `costClass` run in Spark over rows of `width`. */
  def spark(costClass: CostClass, width: Width): Double =
    line(costClass, width.form).spark(width.leaves)

  def shuffleWritePartitionFactor(partitions: Int): Double =
    1 + shuffleWritePartitionSlope * math.max(0.0, partitions / shuffleWritePartitionBase - 1)
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
    case object C2R extends CostClass("c2r")
    val all: Seq[CostClass] = Seq(ShuffleWrite, ShuffleRead, Sort, RowLocal, C2R)
  }

  /** `Nested` when at least half of the leaf columns are inside a struct, array or map. */
  sealed abstract class Form(val name: String) {
    override def toString: String = name
  }

  object Form {
    case object Flat extends Form("flat")
    case object Nested extends Form("nested")
    val all: Seq[Form] = Seq(Flat, Nested)
  }

  /** The leaf columns an operator processes per row, and their form. */
  case class Width(leaves: Int, form: Form)

  def widthOf(attributes: Seq[Attribute]): Width = {
    val leaves = LeafColumns.count(attributes)
    val nested = LeafColumns.nestedCount(attributes)
    Width(leaves, if (leaves > 0 && 2 * nested >= leaves) Form.Nested else Form.Flat)
  }

  /**
   * Prices per row for L leaf columns: `cometK0 * L + cometK1 * L * L` natively, `sparkC0 +
   * sparkK * L` in Spark. `c2r` has no Spark price.
   */
  case class Line(cometK0: Double, cometK1: Double, sparkC0: Double, sparkK: Double) {
    def comet(leaves: Int): Double = leaves * (cometK0 + cometK1 * leaves)
    def spark(leaves: Int): Double = sparkC0 + sparkK * leaves
  }

  import CostClass._
  import Form._

  /**
   * The default prices, measured per operator class and form. `sort` also prices the operators
   * that buffer rows the same way ([[operatorClasses]]). `c2r` prices one columnar-to-row
   * conversion, and also a row-to-columnar transition over a leaf; a Comet columnar shuffle over
   * Spark rows is priced as a Comet `shuffleWrite` plus one `c2r` of the same width, an
   * extrapolation that was not measured. Lines are `(class, form) -> Line(Comet k0, Comet k1,
   * Spark c0, Spark k)`.
   */
  val defaultLines: Map[(CostClass, Form), Line] = Map(
    (ShuffleWrite, Flat) -> Line(50.3, 0.221, 303, 67.07),
    (ShuffleWrite, Nested) -> Line(44.8, 0.209, 366, 68.96),
    (ShuffleRead, Flat) -> Line(35.9, 0.043, 360, 59.38),
    (ShuffleRead, Nested) -> Line(15.8, 0.071, 170, 31.87),
    (Sort, Flat) -> Line(0, 0.028, 34, 28.03),
    (Sort, Nested) -> Line(6.4, 0.009, 400, 0),
    (RowLocal, Flat) -> Line(2.3, 0, 17, 2.98),
    (RowLocal, Nested) -> Line(1.2, 0, 4, 1.66),
    (C2R, Flat) -> Line(10.0, 0, 0, 0),
    (C2R, Nested) -> Line(20.0, 0, 0, 0))

  val default: EngineCostTable = EngineCostTable(
    defaultLines,
    shuffleWritePartitionSlope = 0.08,
    shuffleWritePartitionBase = 250,
    oomRiskPenalty = 2000)

  /**
   * The class of each converted Spark operator the table prices, by the simple name of its class.
   * Shuffles are priced as `shuffleWrite` and `shuffleRead` by their format, and conversions as
   * `c2r`. Any other operator keeps the constant weights of [[EngineCostModel]].
   */
  val operatorClasses: Map[String, CostClass] = Map(
    "SortExec" -> Sort,
    "SortMergeJoinExec" -> Sort,
    "WindowExec" -> Sort,
    "WindowGroupLimitExec" -> Sort,
    "ExpandExec" -> Sort,
    "FilterExec" -> RowLocal,
    "ProjectExec" -> RowLocal,
    "BroadcastHashJoinExec" -> RowLocal,
    "UnionExec" -> RowLocal,
    "HashAggregateExec" -> RowLocal,
    "CoalesceExec" -> RowLocal,
    "LocalLimitExec" -> RowLocal,
    "GlobalLimitExec" -> RowLocal)

  /** Spark operators whose native version holds memory, by the simple name of their class. */
  val nativeMemoryHolders: Set[String] = Set(
    "SortExec",
    "SortMergeJoinExec",
    "ShuffledHashJoinExec",
    "BroadcastHashJoinExec",
    "HashAggregateExec")

  /** Spark operators that hold memory in Spark, by the simple name of their class. */
  val sparkMemoryOperators: Set[String] = Set(
    "SortExec",
    "WindowExec",
    "SortMergeJoinExec",
    "SortAggregateExec",
    "ObjectHashAggregateExec")

  private val scalars: Map[String, (EngineCostTable, Double) => EngineCostTable] = Map(
    "shuffleWritePartitionSlope" -> ((t, v) => t.copy(shuffleWritePartitionSlope = v)),
    "shuffleWritePartitionBase" -> ((t, v) => t.copy(shuffleWritePartitionBase = v)),
    "oomRiskPenalty" -> ((t, v) => t.copy(oomRiskPenalty = v)))

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

    def numbers(entry: String, value: String, count: Int, expected: String): Seq[Double] = {
      val parsed = value.split(",", -1).map(v => Try(v.trim.toDouble).toOption)
      if (parsed.length != count || parsed.exists(v =>
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
              val v = numbers(entry, value, 1, s"$name=<number>").head
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
                      val k = numbers(entry, value, 2, s"$name=<k0>,<k1>")
                      line.copy(cometK0 = k(0), cometK1 = k(1))
                    case "spark" if costClass != C2R =>
                      val k = numbers(entry, value, 2, s"$name=<c0>,<k>")
                      line.copy(sparkC0 = k(0), sparkK = k(1))
                    case "spark" => fail(entry, s"no spark line for $C2R")
                    case _ => fail(entry, "an engine among comet, spark")
                  }
                  table.copy(lines = table.lines.updated((costClass, form), updated))
                case _ =>
                  fail(
                    entry,
                    s"<class>.<form>.<engine>=<a>,<b> or one of " +
                      s"${scalars.keys.toSeq.sorted.mkString(", ")}=<number>")
              }
          }
        case _ => fail(entry, "<key>=<value>")
      }
    }
  }
}
