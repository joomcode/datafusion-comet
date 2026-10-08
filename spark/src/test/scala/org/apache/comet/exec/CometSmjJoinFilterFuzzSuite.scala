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

package org.apache.comet.exec

import java.io.File
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.collection.mutable
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometSortMergeJoinExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * Differential tests of Comet's sort-merge join with a join filter against Spark, over generated
 * data with key groups of one row, about a batch and several batches on either side, NULL keys
 * and NULL filter columns, and a wide streamed side with strings, decimals and nested columns.
 *
 * Every join type runs with filters of different shapes and selectivities (a validity interval
 * bounded by one side, a band bounded by the other, one-sided conditions, always true and always
 * false, almost nothing or almost everything passing, casts of one or both sides) at several
 * batch sizes and under a memory pool small enough to make the join spill. Results are compared
 * with Spark as multisets.
 *
 * The default run covers each join type and filter at one batch size and a subset at the others.
 * The full matrix runs only with `-Dcomet.test.smjFuzz.full=true`;
 * `-Dcomet.test.smjFuzz.seed=<n>` changes the data seed.
 */
class CometSmjJoinFilterFuzzSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val seed: Long =
    sys.props.get("comet.test.smjFuzz.seed").map(_.toLong).getOrElse(20261007L)
  private val fullMatrix: Boolean = sys.props.get("comet.test.smjFuzz.full").contains("true")

  // ---------------------------------------------------------------- data generation

  private val leftSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("k", IntegerType),
      StructField("kc", StringType),
      StructField("t", LongType),
      StructField("w", LongType),
      StructField("lv", IntegerType),
      StructField("s", StringType),
      StructField("d38", DecimalType(38, 10)),
      StructField("d10", DecimalType(10, 2)),
      StructField("dbl", DoubleType),
      StructField("ts", TimestampType),
      StructField("dt", DateType),
      StructField("b", BooleanType),
      StructField(
        "st",
        StructType(
          Seq(
            StructField("a", IntegerType),
            StructField("b", StringType),
            StructField("c", ArrayType(IntegerType))))),
      StructField("arr", ArrayType(StringType)),
      StructField("m", MapType(StringType, IntegerType)),
      StructField("ss", StringType),
      StructField("li", StringType),
      StructField("k2", IntegerType),
      StructField("flag", BooleanType),
      StructField("ldt", DateType)))

  private val rightSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("k", IntegerType),
      StructField("kc", StringType),
      StructField("eff", LongType),
      StructField("next_eff", LongType),
      StructField("rv", IntegerType),
      StructField("rs", StringType),
      StructField("rdec", DecimalType(18, 4)),
      StructField("rarr", ArrayType(IntegerType)),
      StructField(
        "rst",
        StructType(Seq(StructField("x", DoubleType), StructField("y", StringType)))),
      StructField("rt", TimestampType),
      StructField("rss", StringType),
      StructField("rk2", IntegerType),
      StructField("rdt", DateType)))

  private case class DataSet(name: String, left: String, right: String, stats: String)

  private var tempRoot: File = _
  private val dataSets = mutable.Map[String, DataSet]()

  override protected def afterAll(): Unit = {
    try {
      if (tempRoot != null) FileUtils.deleteQuietly(tempRoot)
    } finally {
      super.afterAll()
    }
  }

  private def withConfs[T](pairs: (String, String)*)(f: => T): T = {
    var result: Option[T] = None
    withSQLConf(pairs: _*) { result = Some(f) }
    result.get
  }

  private def pick[T](r: Random, xs: Seq[T]): T = xs(r.nextInt(xs.length))

  private def orNull(r: Random, pct: Int)(v: => Any): Any = if (r.nextInt(100) < pct) null else v

  private def randomDecimal(r: Random, precision: Int, scale: Int): java.math.BigDecimal = {
    val digits = (0 until 1 + r.nextInt(precision)).map(_ => ('0' + r.nextInt(10)).toChar)
    val unscaled = new java.math.BigInteger(digits.mkString)
    new java.math.BigDecimal(if (r.nextBoolean()) unscaled.negate() else unscaled, scale)
  }

  private def randomString(r: Random): String = r.nextInt(30) match {
    case 0 => ""
    case 1 => "ж€😀 ünï"
    case 2 => r.alphanumeric.take(1500 + r.nextInt(1000)).mkString
    case _ => r.alphanumeric.take(1 + r.nextInt(12)).mkString
  }

  private def randomDouble(r: Random): Double =
    if (r.nextInt(4) == 0) {
      pick(r, Seq(Double.NaN, -0.0d, 0.0d, Double.PositiveInfinity, Double.MinPositiveValue))
    } else r.nextDouble() * 2e6 - 1e6

  private case class KeyGroup(k: Int, nl: Int, nr: Int, base: Long, mode: Int)

  private def leftRow(r: Random, g: KeyGroup): Seq[Any] = {
    val k: Any = if (g.k < 0) null else g.k
    val kc =
      if (g.k < 0) "c0" else if (g.k % 23 == 0 && r.nextInt(3) == 0) null else s"c${g.k % 5}"
    val span = math.max(g.nr, 1) * 10L + 40
    Seq(
      k,
      kc,
      orNull(r, 8)(g.base - 20 + (r.nextLong() & Long.MaxValue) % span),
      orNull(r, 5)(pick(r, Seq(0L, 5L, 15L, 30L))),
      orNull(r, 10)(r.nextInt(100)),
      orNull(r, 10)(randomString(r)),
      orNull(r, 10)(randomDecimal(r, 38, 10)),
      orNull(r, 10)(randomDecimal(r, 10, 2)),
      orNull(r, 10)(randomDouble(r)),
      orNull(r, 10)(
        Timestamp.from(Instant.ofEpochSecond(r.nextInt(2000000000) * 2L - 1500000000L, 1000L))),
      orNull(r, 10)(Date.valueOf(LocalDate.ofEpochDay(r.nextInt(60000) - 20000L))),
      orNull(r, 10)(r.nextBoolean()),
      orNull(r, 10)(
        Row(
          orNull(r, 20)(r.nextInt(1000)),
          orNull(r, 20)(randomString(r)),
          orNull(r, 20)(Seq.fill(r.nextInt(4))(orNull(r, 20)(r.nextInt(50)))))),
      orNull(r, 10)(
        Seq.fill(r.nextInt(5))(orNull(r, 20)(r.alphanumeric.take(r.nextInt(8)).mkString))),
      orNull(r, 10)((0 until r.nextInt(3)).map(i => s"k$i" -> orNull(r, 20)(r.nextInt(9))).toMap))
  }

  private def rightRow(r: Random, g: KeyGroup, j: Int): Seq[Any] = {
    val k: Any = if (g.k < 0) null else g.k
    val kc = if (g.k < 0) "c0" else s"c${g.k % 5}"
    val eff = g.base + j * 10L
    val width = g.mode match {
      case 0 => 10L
      case 1 => 25L
      case _ => 5L
    }
    Seq(
      k,
      kc,
      orNull(r, 4)(eff),
      orNull(r, 4)(eff + width),
      orNull(r, 10)(r.nextInt(100)),
      orNull(r, 10)(randomString(r)),
      orNull(r, 10)(randomDecimal(r, 18, 4)),
      orNull(r, 10)(Seq.fill(r.nextInt(4))(orNull(r, 20)(r.nextInt(50)))),
      orNull(r, 10)(Row(orNull(r, 20)(randomDouble(r)), orNull(r, 20)(randomString(r)))))
  }

  private val tsFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private def formatSeconds(sec: Long): String =
    LocalDateTime.ofEpochSecond(sec, 0, ZoneOffset.UTC).format(tsFormat)

  // A string column cast to TIMESTAMP in the filters: mostly valid timestamps near the key
  // group's values, but also NULL, empty, malformed, out of range, date-only, padded and
  // year-only strings, which Legacy casts turn into NULL or partial values.
  private def timestampString(r: Random, sec: Long): Any = r.nextInt(24) match {
    case 0 | 1 | 2 => null
    case 3 => ""
    case 4 => "not a timestamp"
    case 5 => "2026-13-45 99:00:00"
    case 6 => formatSeconds(sec).replace(' ', 'T').dropRight(3) + ":75"
    case 7 => "  " + formatSeconds(sec) + " "
    case 8 => formatSeconds(sec).take(10)
    case 9 => formatSeconds(sec).take(4)
    case 10 => String.valueOf(sec)
    case 11 => "ж€😀"
    case _ => formatSeconds(sec)
  }

  // A string column cast to INT in the filters: numbers, padded numbers, fractions, NULL and
  // strings that Legacy casts turn into NULL.
  private def intString(r: Random): Any = r.nextInt(16) match {
    case 0 | 1 => null
    case 2 => ""
    case 3 => "x1"
    case 4 => " " + r.nextInt(100) + " "
    case 5 => s"${r.nextInt(100)}.${r.nextInt(10)}"
    case 6 => "99999999999"
    case _ => String.valueOf(r.nextInt(100))
  }

  private def leftExtra(r: Random, g: KeyGroup): Seq[Any] = {
    val span = math.max(g.nr, 1) * 10L + 40
    val sec = g.base - 20 + r.nextInt(span.toInt)
    Seq(
      timestampString(r, sec),
      intString(r),
      orNull(r, 10)(r.nextInt(6)),
      orNull(r, 15)(r.nextBoolean()),
      orNull(r, 30)(Date.valueOf(LocalDate.ofEpochDay(sec / 86400 + r.nextInt(3) - 1))))
  }

  private def rightExtra(r: Random, g: KeyGroup, j: Int): Seq[Any] = {
    val sec = g.base + j * 10L + r.nextInt(31) - 15
    Seq(
      orNull(r, 10)(Timestamp.from(Instant.ofEpochSecond(sec))),
      timestampString(r, sec + r.nextInt(21) - 10),
      orNull(r, 10)(r.nextInt(6)),
      orNull(r, 30)(Date.valueOf(LocalDate.ofEpochDay(sec / 86400 + r.nextInt(3) - 1))))
  }

  private val mainCombos: Seq[(Int, Int)] = Seq(
    1 -> 1,
    1 -> 7,
    7 -> 1,
    7 -> 7,
    8 -> 6,
    6 -> 8,
    63 -> 64,
    64 -> 64,
    65 -> 1,
    1 -> 65,
    130 -> 2,
    2 -> 130,
    140 -> 140,
    200 -> 70,
    1023 -> 1,
    1 -> 1024,
    1025 -> 3,
    3 -> 1025,
    2100 -> 1,
    1 -> 2100,
    0 -> 10,
    10 -> 0,
    0 -> 1100,
    1100 -> 0)

  private val bigCombos: Seq[(Int, Int)] =
    Seq(5000 -> 2, 2 -> 5000, 3000 -> 40, 40 -> 3000, 600 -> 600, 9000 -> 1, 1 -> 9000)

  private def dataSet(name: String): DataSet = synchronized {
    dataSets.getOrElseUpdate(
      name,
      name match {
        case "main" => createDataSet(name, seed, mainCombos, 300)
        case "alt" => createDataSet(name, seed + 1, mainCombos, 300)
        case "big" => createDataSet(name, seed, bigCombos, 200)
      })
  }

  private def createDataSet(
      name: String,
      dataSeed: Long,
      combos: Seq[(Int, Int)],
      numRandom: Int): DataSet = {
    val r = new Random(dataSeed ^ name.hashCode)
    def small(): Int = r.nextInt(10) match {
      case 0 => 0
      case 1 | 2 | 3 => 1
      case 9 => 6 + r.nextInt(15)
      case _ => 2 + r.nextInt(4)
    }
    val sized = r.shuffle(combos ++ Seq.fill(numRandom)((small(), small())))
    val groups = sized.zipWithIndex.map { case ((nl, nr), i) =>
      KeyGroup(i * 3 + r.nextInt(3), nl, nr, r.nextInt(100000) * 10L, r.nextInt(3))
    } ++ Seq(KeyGroup(-1, 40 + r.nextInt(40), 40 + r.nextInt(40), 5000L, 0))
    val lx = new Random(dataSeed ^ name.hashCode ^ 0x5eed1L)
    val rx = new Random(dataSeed ^ name.hashCode ^ 0x5eed2L)
    val leftRows =
      r.shuffle(groups.flatMap(g => Seq.fill(g.nl)(leftRow(r, g) ++ leftExtra(lx, g))))
    val rightRows = r.shuffle(
      groups.flatMap(g => (0 until g.nr).map(j => rightRow(r, g, j) ++ rightExtra(rx, g, j))))

    if (tempRoot == null) tempRoot = Files.createTempDirectory("comet-smj-fuzz").toFile
    val leftPath = new File(tempRoot, s"$name-left").getCanonicalPath
    val rightPath = new File(tempRoot, s"$name-right").getCanonicalPath
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val lr = leftRows.zipWithIndex.map { case (v, i) => Row.fromSeq(i.toLong +: v) }
      val rr = rightRows.zipWithIndex.map { case (v, i) => Row.fromSeq(i.toLong +: v) }
      spark
        .createDataFrame(spark.sparkContext.parallelize(lr, 4), leftSchema)
        .write
        .option("parquet.block.size", 65536)
        .parquet(leftPath)
      spark
        .createDataFrame(spark.sparkContext.parallelize(rr, 4), rightSchema)
        .write
        .option("parquet.block.size", 65536)
        .parquet(rightPath)
    }
    val ds = DataSet(
      name,
      s"smj_fuzz_${name}_l",
      s"smj_fuzz_${name}_r",
      s"left rows ${leftRows.size}, right rows ${rightRows.size}, keys ${groups.size}, " +
        s"max group ${groups.map(_.nl).max}/${groups.map(_.nr).max}, " +
        s"key pairs ${groups.filter(_.k >= 0).map(g => g.nl.toLong * g.nr).sum}")
    spark.read.parquet(leftPath).createOrReplaceTempView(ds.left)
    spark.read.parquet(rightPath).createOrReplaceTempView(ds.right)
    ds
  }

  // ---------------------------------------------------------------- query shapes

  private case class JoinKind(name: String, sql: String, existence: Boolean = false)

  private val inner = JoinKind("inner", "INNER JOIN")
  private val leftOuter = JoinKind("left", "LEFT JOIN")
  private val rightOuter = JoinKind("right", "RIGHT JOIN")
  private val fullOuter = JoinKind("full", "FULL JOIN")
  private val leftSemi = JoinKind("semi", "LEFT SEMI JOIN", existence = true)
  private val leftAnti = JoinKind("anti", "LEFT ANTI JOIN", existence = true)
  private val joinKinds = Seq(inner, leftOuter, rightOuter, fullOuter, leftSemi, leftAnti)

  private case class Filter(name: String, sql: String)

  private val interval = Filter("interval", "l.t > r.eff AND l.t <= r.next_eff")
  private val band = Filter("band", "r.eff >= l.t - l.w AND r.eff < l.t + l.w")
  private val leftOnly = Filter("left_only", "l.lv % 3 = 0")
  private val rightOnly = Filter("right_only", "r.rv % 3 = 0")
  private val alwaysFalse = Filter("false", "l.id + r.id < 0")
  private val alwaysTrue = Filter("true", "l.id + r.id >= 0")
  private val rare = Filter("rare", "(l.id * 31 + r.id * 17) % 997 = 0")
  private val most = Filter("most", "(l.id + r.id) % 10 <> 0")
  private val nullable = Filter("nullable_cmp", "l.lv < r.rv")
  private val typed = Filter("typed", "l.d10 * 2 >= r.rdec OR l.s < r.rs")
  private val leftCast =
    Filter("left_cast", "CAST(CAST(l.t AS STRING) AS BIGINT) > r.eff AND l.t <= r.next_eff")
  private val leftTimestampCast = Filter(
    "left_ts_cast",
    "CAST(CAST(l.ts AS STRING) AS TIMESTAMP) < CAST(r.eff * 2000 AS TIMESTAMP) AND l.t > r.eff")
  private val bothCast = Filter(
    "both_cast",
    "CAST(CAST(l.t AS STRING) AS BIGINT) > CAST(CAST(r.eff AS STRING) AS BIGINT) AND " +
      "CAST(l.t AS DECIMAL(20, 0)) <= CAST(r.next_eff AS DECIMAL(20, 0))")
  private val filters =
    Seq(
      interval,
      band,
      leftOnly,
      rightOnly,
      alwaysFalse,
      alwaysTrue,
      rare,
      most,
      nullable,
      typed,
      leftCast,
      leftTimestampCast,
      bothCast)

  // Boolean and conditional operators around subexpressions that read one side only, which
  // the native join lifts out of the filter and evaluates once per row: OR, CASE, IF, NOT,
  // IS [NOT] NULL, IN and NOT IN, COALESCE and null-safe equality, with casts of strings that
  // are often invalid, so the lifted parts are often NULL and three-valued logic matters.
  private val lts = "CAST(l.ss AS TIMESTAMP)"
  private val rts = "CAST(r.rss AS TIMESTAMP)"
  private val lint = "CAST(l.li AS INT)"
  private val orTs = Filter("or_ts", s"l.t > r.eff OR $lts < r.rt")
  private val orTsNull = Filter("or_ts_null", s"$lts < r.rt OR l.lv IS NULL")
  private val orMostlyTrue =
    Filter("or_mostly_true", s"(l.id + r.id) % 10 <> 0 OR $lts >= r.rt")
  private val orBufferedLifted =
    Filter("or_buffered_lifted", s"l.lv < r.rv OR $rts > CAST(l.t AS TIMESTAMP)")
  private val orSingleSides =
    Filter("or_single_sides", s"(l.flag AND $lts IS NOT NULL) OR r.rv % 3 = 0")
  private val orBothLifted =
    Filter("or_both_lifted", s"$lts < $rts OR ($lts IS NULL AND $rts IS NULL)")
  private val caseFlag =
    Filter("case_flag", s"CASE WHEN l.flag THEN $lts < r.rt ELSE r.eff <= l.t END")
  private val caseRightWhen = Filter(
    "case_right_when",
    s"CASE WHEN r.rv % 2 = 0 THEN $lts < TIMESTAMP '1970-01-06 00:00:00' " +
      s"WHEN r.rv IS NULL THEN l.flag ELSE $lint > 50 END")
  private val caseLeftWhen = Filter(
    "case_left_when",
    s"CASE WHEN l.flag THEN $rts > TIMESTAMP '1970-01-06 00:00:00' ELSE r.rk2 > 2 END")
  private val caseNested = Filter(
    "case_nested",
    "CASE WHEN l.k2 IS NULL THEN r.rv IS NULL WHEN l.k2 > 2 THEN " +
      s"CASE WHEN $lts IS NULL THEN r.rk2 = l.k2 ELSE $lts < r.rt END " +
      s"ELSE CASE WHEN r.rk2 IS NULL THEN l.flag ELSE $lint < r.rv END END")
  private val caseValue =
    Filter("case_value", s"CASE WHEN l.flag THEN $lts ELSE CAST(l.t AS TIMESTAMP) END < r.rt")
  private val ifNull =
    Filter("if_null", s"IF($lint IS NULL, r.rk2 > 2, $lint <= r.rv)")
  private val notAndUpper = Filter("not_and_upper", "NOT (l.lv > r.rv AND upper(l.s) = r.rs)")
  private val notOr = Filter("not_or", s"NOT ($lts >= r.rt OR l.flag)")
  private val isNullMix =
    Filter("is_null_mix", s"($lts IS NULL) = (r.rt IS NULL) OR ($lint + r.rv) IS NULL")
  private val isNotNullCmp =
    Filter("is_not_null_cmp", s"($lts < r.rt) IS NOT NULL AND NOT ($lts < r.rt)")
  private val inList =
    Filter("in_list", "l.k2 IN (1, 2, 3) AND l.t > r.eff OR r.rk2 IN (0, 4) AND l.lv < r.rv")
  private val notInList = Filter(
    "not_in_list",
    s"l.k2 NOT IN (1, 3) AND r.rk2 NOT IN (2) OR $lint NOT IN (1, 2, 50) AND l.t <= r.next_eff")
  private val inCross =
    Filter("in_cross", s"r.rk2 IN (l.k2, l.k2 + 1, $lint) OR l.k2 IN (r.rk2, 3)")
  private val coalesceTs =
    Filter("coalesce_ts", s"coalesce($lts, r.rt) > CAST(r.eff AS TIMESTAMP)")
  private val coalesceDate =
    Filter("coalesce_date", "coalesce(l.ldt, r.rdt) >= CAST(r.rt AS DATE)")
  private val threeValued =
    Filter("three_valued", s"($lint > r.rv OR $lts < r.rt) AND NOT ($rts > $lts AND l.flag)")
  private val notNullPart =
    Filter("not_null_part", "NOT (CAST(l.ss AS INT) > 0) OR r.rv > 50")
  private val nullSafeEq =
    Filter("null_safe_eq", s"$lts <=> r.rt OR $lint <=> r.rk2")
  private val liftedTwice =
    Filter("lifted_twice", s"$lts >= r.rt AND $lts <= CAST(r.eff + 20 AS TIMESTAMP)")
  private val upperInvalid =
    Filter("upper_invalid", "upper(l.ss) = upper(r.rss) OR upper(l.s) < r.rs")
  private val boolFilters = Seq(
    orTs,
    orTsNull,
    orMostlyTrue,
    orBufferedLifted,
    orSingleSides,
    orBothLifted,
    caseFlag,
    caseRightWhen,
    caseLeftWhen,
    caseNested,
    caseValue,
    ifNull,
    notAndUpper,
    notOr,
    isNullMix,
    isNotNullCmp,
    inList,
    notInList,
    inCross,
    coalesceTs,
    coalesceDate,
    threeValued,
    notNullPart,
    nullSafeEq,
    liftedTwice,
    upperInvalid)
  private val defaultBoolFilters =
    Seq(orTs, caseFlag, notAndUpper, inList, coalesceTs, threeValued)

  private case class Shape(name: String, confs: Seq[(String, String)], spill: Boolean = false)

  private def batch(n: Int) = CometConf.COMET_BATCH_SIZE.key -> n.toString
  private def aqe(on: Boolean) = SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> on.toString
  private def partitions(n: Int) = SQLConf.SHUFFLE_PARTITIONS.key -> n.toString
  private val tinyPool = CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key -> "0.0005"
  private val manySplits = Seq(
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> "65536",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1")

  private val b7 = Shape("batch7", Seq(batch(7), aqe(false), partitions(1)))
  private val b64 = Shape("batch64", Seq(batch(64), aqe(true), partitions(2)))
  private val b1024 = Shape("batch1024", Seq(batch(1024), aqe(true), partitions(2)))
  private val b8192 = Shape("batch8192", Seq(batch(8192), aqe(false), partitions(3)))
  private val spill64 =
    Shape(
      "spill64",
      Seq(batch(64), aqe(true), partitions(1), tinyPool) ++ manySplits,
      spill = true)
  private val spill7 =
    Shape(
      "spill7",
      Seq(batch(7), aqe(false), partitions(2), tinyPool) ++ manySplits,
      spill = true)
  private val allShapes = Seq(b7, b64, b1024, b8192, spill7, spill64)

  private val baseConfs: Seq[(String, String)] = Seq(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.ANSI_ENABLED.key -> "false",
    CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true",
    CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key -> "true",
    CometConf.COMET_FORCE_SHJ.key -> "false")

  private case class Q(
      label: String,
      sql: String,
      expectNative: Boolean = true,
      expectCondition: Boolean = true)

  private def joinQuery(
      ds: DataSet,
      kind: JoinKind,
      filter: Filter,
      flipped: Boolean,
      twoKeys: Boolean): Q = {
    val on = (if (twoKeys) "l.k = r.k AND l.kc = r.kc" else "l.k = r.k") + s" AND (${filter.sql})"
    val (from, select) =
      if (flipped) (s"${ds.right} r ${kind.sql} ${ds.left} l", if (kind.existence) "r.*" else "*")
      else (s"${ds.left} l ${kind.sql} ${ds.right} r", if (kind.existence) "l.*" else "*")
    val pushable = (filter, kind.name, flipped) match {
      case (`leftOnly`, "inner" | "right", false) => true
      case (`leftOnly`, "inner" | "left", true) => true
      case (`leftOnly`, "semi" | "anti", true) => true
      case (`leftOnly`, "semi", false) => true
      case (`rightOnly`, "inner" | "left", false) => true
      case (`rightOnly`, "inner" | "right", true) => true
      case (`rightOnly`, "semi" | "anti", false) => true
      case (`rightOnly`, "semi", true) => true
      case _ => false
    }
    val label = s"${kind.name}${if (flipped) "_rl" else ""}${if (twoKeys) "_2keys" else ""}" +
      s"/${filter.name}"
    Q(label, s"SELECT $select FROM $from ON $on", expectCondition = !pushable)
  }

  private def existenceQueries(ds: DataSet): Seq[Q] = Seq(
    Q(
      "exists_or",
      s"SELECT l.* FROM ${ds.left} l WHERE l.lv > 90 OR EXISTS (SELECT 1 FROM ${ds.right} r " +
        "WHERE r.k = l.k AND l.t > r.eff AND l.t <= r.next_eff)",
      expectNative = false),
    Q(
      "in_or",
      s"SELECT l.* FROM ${ds.left} l WHERE l.lv > 90 OR l.k IN (SELECT r.k FROM ${ds.right} r " +
        "WHERE l.t > r.eff AND l.t <= r.next_eff)",
      expectNative = false),
    Q(
      "not_exists",
      s"SELECT l.* FROM ${ds.left} l WHERE NOT EXISTS (SELECT 1 FROM ${ds.right} r " +
        "WHERE r.k = l.k AND r.eff >= l.t - l.w AND r.eff < l.t + l.w)"))

  // ---------------------------------------------------------------- comparison

  private def norm(v: Any): Any = v match {
    case null => null
    case b: Array[Byte] => ("bin", b.toList)
    case d: Double => ("d", java.lang.Double.doubleToLongBits(d))
    case f: Float => ("f", java.lang.Float.floatToIntBits(f))
    case r: Row => r.toSeq.map(norm).toList
    case m: scala.collection.Map[_, _] => m.map { case (k, x) => (norm(k), norm(x)) }.toMap
    case s: scala.collection.Seq[_] => s.map(norm).toList
    case other => other
  }

  private def counts(rows: Seq[Row]): Map[Any, Int] =
    rows.groupBy(r => norm(r)).map { case (k, v) => k -> v.size }

  private def showRow(v: Any): String = {
    val s = String.valueOf(v)
    if (s.length > 300) s.take(300) + "..." else s
  }

  private case class Outcome(
      label: String,
      shape: String,
      data: String,
      sparkRows: Int,
      cometRows: Int,
      native: Boolean,
      condition: Boolean,
      spills: Long,
      error: Option[String]) {
    def line: String =
      f"SMJFUZZ ${if (error.isEmpty) "PASS" else "FAIL"} seed=$seed data=$data%-4s " +
        f"shape=$shape%-9s case=$label%-28s spark=$sparkRows%7d comet=$cometRows%7d " +
        s"native=$native cond=$condition spills=$spills"
  }

  private def cometJoins(plan: SparkPlan): Seq[CometSortMergeJoinExec] =
    collect(plan) { case j: CometSortMergeJoinExec => j }

  private def runCase(ds: DataSet, shape: Shape, q: Q): Outcome = {
    val ctx = s"[seed=$seed data=${ds.name} shape=${shape.name} case=${q.label}]"
    withConfs(baseConfs ++ shape.confs: _*) {
      val expected = withConfs(CometConf.COMET_ENABLED.key -> "false") {
        sql(q.sql).collect().toSeq
      }
      val df = sql(q.sql)
      val actual =
        try Right(df.collect().toSeq)
        catch { case e: Throwable => Left(e) }
      val plan = df.queryExecution.executedPlan
      val joins = cometJoins(plan)
      val sparkJoins = collect(plan) { case j: SortMergeJoinExec => j }
      val native = joins.nonEmpty && sparkJoins.isEmpty
      val condition = joins.exists(_.condition.isDefined)
      val spills = joins.flatMap(_.metrics.get("spill_count")).map(_.value).sum
      val errors = mutable.ArrayBuffer[String]()
      actual match {
        case Left(e) =>
          errors += s"Comet failed: $e\n${e.getStackTrace.take(15).mkString("\n")}"
        case Right(rows) =>
          val exp = counts(expected)
          val act = counts(rows)
          if (exp != act) {
            val missing = exp.toSeq.flatMap { case (k, n) =>
              val d = n - act.getOrElse(k, 0)
              if (d > 0) Some(k -> d) else None
            }
            val extra = act.toSeq.flatMap { case (k, n) =>
              val d = n - exp.getOrElse(k, 0)
              if (d > 0) Some(k -> d) else None
            }
            errors += s"results differ: Spark ${expected.size} rows, Comet ${rows.size} rows, " +
              s"${missing.map(_._2).sum} missing in Comet, ${extra.map(_._2).sum} extra in " +
              "Comet\nmissing (row x count):\n" +
              missing.take(5).map { case (k, d) => s"  ${showRow(k)} x$d" }.mkString("\n") +
              "\nextra (row x count):\n" +
              extra.take(5).map { case (k, d) => s"  ${showRow(k)} x$d" }.mkString("\n")
          }
      }
      if (native != q.expectNative) {
        errors += s"expected native=${q.expectNative}, got $native"
      }
      if (q.expectNative && condition != q.expectCondition) {
        errors += s"expected a join filter in CometSortMergeJoin=${q.expectCondition}"
      }
      val error =
        if (errors.isEmpty) None
        else Some(s"$ctx ${errors.mkString("\n")}\nquery: ${q.sql}\nplan:\n$plan")
      val o = Outcome(
        q.label,
        shape.name,
        ds.name,
        expected.size,
        actual.fold(_ => -1, _.size),
        native,
        condition,
        spills,
        error)
      // scalastyle:off println
      println(o.line)
      // scalastyle:on println
      o
    }
  }

  private def runAll(ds: DataSet, shape: Shape, qs: Seq[Q]): Seq[Outcome] = {
    val outcomes = qs.map(q => runCase(ds, shape, q))
    val failed = outcomes.filter(_.error.nonEmpty)
    if (failed.nonEmpty) {
      fail(
        s"[seed=$seed data=${ds.name} (${ds.stats}) shape=${shape.name}] " +
          s"${failed.size} of ${outcomes.size} cases failed: " +
          failed.map(_.label).mkString(", ") + "\n\n" +
          failed.take(3).flatMap(_.error).mkString("\n\n"))
    }
    outcomes
  }

  private def matrix(
      ds: DataSet,
      kinds: Seq[JoinKind],
      fs: Seq[Filter],
      flipped: Boolean = false,
      twoKeys: Boolean = false): Seq[Q] =
    for (k <- kinds; f <- fs) yield joinQuery(ds, k, f, flipped, twoKeys)

  private def assumeFull(): Unit =
    assume(fullMatrix, "the full matrix runs only with -Dcomet.test.smjFuzz.full=true")

  // ---------------------------------------------------------------- default tests

  test("every join type and filter shape, batch 64") {
    val ds = dataSet("main")
    runAll(ds, b64, matrix(ds, joinKinds, filters))
  }

  test("flipped sides and two join keys, batch 64") {
    val ds = dataSet("main")
    runAll(
      ds,
      b64,
      matrix(ds, Seq(rightOuter, leftSemi, leftAnti), Seq(interval, band), flipped = true) ++
        matrix(ds, joinKinds, Seq(interval), twoKeys = true))
  }

  test("batch 7: key groups spanning many batches") {
    val ds = dataSet("main")
    runAll(ds, b7, matrix(ds, joinKinds, Seq(interval, band, most, leftCast, bothCast)))
  }

  test("batch 1024: key groups around and over one batch") {
    val ds = dataSet("main")
    runAll(ds, b1024, matrix(ds, joinKinds, Seq(interval, rare, alwaysTrue)))
  }

  test("boolean and conditional operators around lifted subexpressions, batch 64") {
    val ds = dataSet("main")
    runAll(ds, b64, matrix(ds, joinKinds, defaultBoolFilters))
  }

  test("boolean and conditional operators, batch 7 and spilling") {
    val ds = dataSet("main")
    runAll(ds, b7, matrix(ds, joinKinds, Seq(caseFlag, threeValued)))
    runAll(ds, spill64, matrix(ds, Seq(inner, fullOuter, leftAnti), Seq(orTs, coalesceTs)))
  }

  test("spilling join under a tiny memory pool") {
    val ds = dataSet("main")
    val outcomes = runAll(ds, spill64, matrix(ds, joinKinds, Seq(interval, most)))
    assert(outcomes.exists(_.spills > 0), s"[seed=$seed] the join did not spill")
  }

  test("existence joins from EXISTS and IN in a disjunction") {
    val ds = dataSet("main")
    runAll(ds, b64, existenceQueries(ds))
  }

  // ---------------------------------------------------------------- full matrix

  for (shape <- allShapes) {
    test(s"full: ${shape.name}, every join type, filter, side order and key count") {
      assumeFull()
      val ds = dataSet("main")
      val outcomes = runAll(
        ds,
        shape,
        matrix(ds, joinKinds, filters) ++
          matrix(ds, joinKinds, filters, flipped = true) ++
          matrix(ds, joinKinds, filters, twoKeys = true) ++
          existenceQueries(ds))
      if (shape.spill) {
        assert(outcomes.exists(_.spills > 0), s"[seed=$seed] the join did not spill")
      }
    }
  }

  for (shape <- allShapes) {
    test(s"full: ${shape.name}, boolean and conditional operators, both side orders") {
      assumeFull()
      val ds = dataSet("main")
      val outcomes = runAll(
        ds,
        shape,
        matrix(ds, joinKinds, boolFilters) ++ matrix(ds, joinKinds, boolFilters, flipped = true))
      if (shape.spill) {
        assert(outcomes.exists(_.spills > 0), s"[seed=$seed] the join did not spill")
      }
    }
  }

  for (shape <- Seq(b7, spill64)) {
    test(s"full: second seed, boolean and conditional operators, ${shape.name}") {
      assumeFull()
      val ds = dataSet("alt")
      runAll(ds, shape, matrix(ds, joinKinds, boolFilters))
    }
  }

  for (shape <- Seq(b1024, spill64)) {
    test(s"full: groups of thousands of rows, boolean and conditional operators, ${shape.name}") {
      assumeFull()
      val ds = dataSet("big")
      val fs = Seq(orTs, caseFlag, caseNested, notOr, inCross, coalesceTs, threeValued)
      runAll(ds, shape, matrix(ds, joinKinds, fs) ++ matrix(ds, joinKinds, fs, flipped = true))
    }
  }

  for (shape <- Seq(b7, b64, spill64)) {
    test(s"full: second seed, ${shape.name}") {
      assumeFull()
      val ds = dataSet("alt")
      runAll(
        ds,
        shape,
        matrix(ds, joinKinds, filters) ++ matrix(ds, joinKinds, filters, flipped = true))
    }
  }

  for (shape <- Seq(b64, b1024, b8192, spill64)) {
    test(s"full: groups of thousands of rows, ${shape.name}") {
      assumeFull()
      val ds = dataSet("big")
      val fs = Seq(interval, band, rare, alwaysFalse, nullable, leftCast, bothCast)
      runAll(ds, shape, matrix(ds, joinKinds, fs) ++ matrix(ds, joinKinds, fs, flipped = true))
    }
  }
}
