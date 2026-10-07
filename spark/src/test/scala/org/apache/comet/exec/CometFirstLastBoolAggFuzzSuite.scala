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

import scala.collection.mutable
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometHashAggregateExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * Differential tests of first/last (with and without IGNORE NULLS, FILTER, and the FILTER shape
 * produced by Spark's rewrite of several COUNT(DISTINCT ...)), any_value, min/max over booleans
 * and bool_and/bool_or/every/some/any, comparing Comet against Spark on generated data.
 *
 * first/last without ordering depend on the input order. On a single input split read in file
 * order the answer is deterministic and compared exactly. Over many partitions the Comet (and
 * Spark) value must be one of the values of its group that pass the aggregate's FILTER, or NULL
 * when the group has a NULL such value (or, with IGNORE NULLS, only when it has no other).
 *
 * The default run covers every function on the main types and one case of each execution shape.
 * The full matrix (all types, shapes, groupings and a 200k-group dataset) runs only with
 * `-Dcomet.test.aggFuzz.full=true`; `-Dcomet.test.aggFuzz.seed=<n>` changes the data seed.
 */
class CometFirstLastBoolAggFuzzSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val seed: Long =
    sys.props.get("comet.test.aggFuzz.seed").map(_.toLong).getOrElse(20261006L)
  private val fullMatrix: Boolean = sys.props.get("comet.test.aggFuzz.full").contains("true")

  private case class Col(name: String, dt: DataType, main: Boolean)

  private val structType =
    StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))

  private val allCols: Seq[Col] = Seq(
    Col("c_bool", BooleanType, main = true),
    Col("c_byte", ByteType, main = false),
    Col("c_short", ShortType, main = false),
    Col("c_int", IntegerType, main = true),
    Col("c_long", LongType, main = true),
    Col("c_float", FloatType, main = false),
    Col("c_double", DoubleType, main = true),
    Col("c_dec10", DecimalType(10, 2), main = false),
    Col("c_dec38", DecimalType(38, 10), main = true),
    Col("c_str", StringType, main = true),
    Col("c_bin", BinaryType, main = true),
    Col("c_date", DateType, main = true),
    Col("c_ts", TimestampType, main = true),
    Col("c_ntz", TimestampNTZType, main = false),
    Col("c_arr", ArrayType(IntegerType), main = true),
    Col("c_struct", structType, main = false),
    Col("c_map", MapType(StringType, IntegerType), main = false))

  private val largeColNames =
    Seq("c_bool", "c_int", "c_long", "c_double", "c_dec38", "c_date", "c_ts")

  private val F1 = "id % 3 <> 1"
  private val F2 = "id % 4 = 0"

  // ---------------------------------------------------------------- data generation

  private case class DataSet(name: String, cols: Seq[Col], singleView: String, multiView: String)

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

  private def randomDigits(r: Random, maxDigits: Int): String = {
    val n = 1 + r.nextInt(maxDigits)
    (0 until n).map(_ => ('0' + r.nextInt(10)).toChar).mkString
  }

  private def genValue(dt: DataType, r: Random): Any = dt match {
    case BooleanType => r.nextBoolean()
    case ByteType =>
      if (r.nextInt(5) == 0) pick(r, Seq(Byte.MinValue, Byte.MaxValue, 0.toByte))
      else (r.nextInt(256) - 128).toByte
    case ShortType =>
      if (r.nextInt(5) == 0) pick(r, Seq(Short.MinValue, Short.MaxValue, 0.toShort))
      else (r.nextInt(65536) - 32768).toShort
    case IntegerType =>
      if (r.nextInt(5) == 0) pick(r, Seq(Int.MinValue, Int.MaxValue, 0, -1)) else r.nextInt()
    case LongType =>
      if (r.nextInt(5) == 0) pick(r, Seq(Long.MinValue, Long.MaxValue, 0L, -1L)) else r.nextLong()
    case FloatType =>
      if (r.nextInt(3) == 0) {
        pick(
          r,
          Seq(
            Float.NaN,
            -0.0f,
            0.0f,
            Float.PositiveInfinity,
            Float.NegativeInfinity,
            Float.MinPositiveValue,
            Float.MaxValue))
      } else r.nextFloat() * 2000f - 1000f
    case DoubleType =>
      if (r.nextInt(3) == 0) {
        pick(
          r,
          Seq(
            Double.NaN,
            -0.0d,
            0.0d,
            Double.PositiveInfinity,
            Double.NegativeInfinity,
            Double.MinPositiveValue,
            Double.MaxValue))
      } else r.nextDouble() * 2e6 - 1e6
    case d: DecimalType =>
      val unscaled = new java.math.BigInteger(randomDigits(r, d.precision))
      val signed = if (r.nextBoolean()) unscaled.negate() else unscaled
      new java.math.BigDecimal(signed, d.scale)
    case StringType =>
      r.nextInt(20) match {
        case 0 => ""
        case 1 => "ж€😀 ünï"
        case 2 => r.alphanumeric.take(1500 + r.nextInt(1000)).mkString
        case _ => r.alphanumeric.take(1 + r.nextInt(16)).mkString
      }
    case BinaryType =>
      if (r.nextInt(10) == 0) Array.emptyByteArray
      else Array.fill(1 + r.nextInt(24))(r.nextInt(256).toByte)
    case DateType =>
      Date.valueOf(LocalDate.ofEpochDay(r.nextInt(70000) - 20000L))
    case TimestampType =>
      Timestamp.from(
        Instant
          .ofEpochSecond(r.nextInt(2000000000) * 3L - 1800000000L, r.nextInt(1000000) * 1000L))
    case TimestampNTZType =>
      LocalDateTime.ofEpochSecond(
        r.nextInt(2000000000) * 3L - 1800000000L,
        r.nextInt(1000000) * 1000,
        ZoneOffset.UTC)
    case ArrayType(IntegerType, _) =>
      Seq.fill(r.nextInt(5))(if (r.nextInt(4) == 0) null else r.nextInt(100))
    case s: StructType if s == structType =>
      Row(
        if (r.nextInt(4) == 0) null else r.nextInt(100),
        if (r.nextInt(4) == 0) null else r.alphanumeric.take(r.nextInt(6)).mkString)
    case MapType(StringType, IntegerType, _) =>
      (0 until r.nextInt(4))
        .map(i => s"k$i" -> (if (r.nextInt(4) == 0) null else r.nextInt(100)))
        .toMap
    case other => throw new IllegalArgumentException(s"unsupported type $other")
  }

  private def genGroupColumn(c: Col, size: Int, r: Random, pool: IndexedSeq[Any]): Seq[Any] = {
    def value(): Any = if (r.nextInt(10) < 7) pool(r.nextInt(pool.length)) else genValue(c.dt, r)
    val boolMode = if (c.dt == BooleanType) r.nextInt(3) else -1
    def nonNull(): Any = boolMode match {
      case 0 => true
      case 1 => false
      case _ => value()
    }
    val single = r.nextInt(size)
    r.nextInt(7) match {
      case 0 => Seq.fill(size)(nonNull())
      case 1 => Seq.fill(size)(null)
      case 2 => (0 until size).map(i => if (i == 0) null else nonNull())
      case 3 => (0 until size).map(i => if (i == size - 1) null else nonNull())
      case 4 => (0 until size).map(i => if (i == single) nonNull() else null)
      case 5 => (0 until size).map(i => if (i % 2 == 0) null else nonNull())
      case _ => (0 until size).map(_ => if (r.nextBoolean()) null else nonNull())
    }
  }

  private def dataSet(name: String): DataSet = synchronized {
    dataSets.getOrElseUpdate(
      name, {
        val (numGroups, sizeOf, cols) = name match {
          case "mixed" =>
            val sizes: Random => Int = r =>
              r.nextInt(4) match {
                case 0 => 1
                case 1 => 2 + r.nextInt(4)
                case _ => 6 + r.nextInt(75)
              }
            (600, sizes, allCols)
          case "large" =>
            (
              200000,
              (r: Random) => 1 + r.nextInt(4),
              allCols.filter(c => largeColNames.contains(c.name)))
        }
        createDataSet(name, numGroups, sizeOf, cols)
      })
  }

  private def createDataSet(
      name: String,
      numGroups: Int,
      sizeOf: Random => Int,
      cols: Seq[Col]): DataSet = {
    val r = new Random(seed ^ name.hashCode)
    val pools = cols.map(c => IndexedSeq.fill(30)(genValue(c.dt, r)))
    val sizes = Array.fill(numGroups)(sizeOf(r))
    val groups: Array[Array[Array[Any]]] = Array.tabulate(numGroups) { g =>
      val perCol = cols.zip(pools).map { case (c, p) => genGroupColumn(c, sizes(g), r, p) }
      Array.tabulate(sizes(g))(i => perCol.map(_(i)).toArray)
    }
    val order = r.shuffle(sizes.indices.flatMap(g => Seq.fill(sizes(g))(g)))
    val next = new Array[Int](numGroups)
    val rows = order.zipWithIndex.map { case (g, id) =>
      val values = groups(g)(next(g))
      next(g) += 1
      val k1 = if (g % 41 == 7) null else g / 2
      val k2 = if (g % 3 == 0) null else if (g % 2 == 0) "a" else "b"
      Row.fromSeq(Seq[Any](id.toLong, g, k1, k2) ++ values)
    }
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("g", IntegerType, nullable = false),
        StructField("k1", IntegerType),
        StructField("k2", StringType)) ++ cols.map(c => StructField(c.name, c.dt)))

    if (tempRoot == null) tempRoot = Files.createTempDirectory("comet-agg-fuzz").toFile
    val singlePath = new File(tempRoot, s"$name-single").getCanonicalPath
    val multiPath = new File(tempRoot, s"$name-multi").getCanonicalPath
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      spark
        .createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
        .write
        .option("parquet.block.size", 256 * 1024)
        .parquet(singlePath)
      spark
        .createDataFrame(spark.sparkContext.parallelize(rows, 8), schema)
        .write
        .parquet(multiPath)
    }
    val ds = DataSet(name, cols, s"fuzz_${name}_single", s"fuzz_${name}_multi")
    spark.read.parquet(singlePath).createOrReplaceTempView(ds.singleView)
    spark.read.parquet(multiPath).createOrReplaceTempView(ds.multiView)
    ds
  }

  // ---------------------------------------------------------------- aggregates

  private sealed trait Check
  private case object Exact extends Check
  private case class Member(value: String, filter: Option[String], ignoreNulls: Boolean)
      extends Check

  private case class Agg(sql: String, check: Check)

  private def firstLastAggs(c: String, all: Boolean): Seq[Agg] = {
    val base = Seq(
      Agg(s"first($c)", Member(c, None, ignoreNulls = false)),
      Agg(s"last($c)", Member(c, None, ignoreNulls = false)),
      Agg(s"first($c, true)", Member(c, None, ignoreNulls = true)),
      Agg(s"last($c, true)", Member(c, None, ignoreNulls = true)),
      Agg(s"any_value($c)", Member(c, None, ignoreNulls = false)),
      Agg(s"first($c) FILTER (WHERE $F1)", Member(c, Some(F1), ignoreNulls = false)),
      Agg(s"last($c, true) FILTER (WHERE $F1)", Member(c, Some(F1), ignoreNulls = true)))
    val extra = Seq(
      Agg(s"first_value($c) IGNORE NULLS", Member(c, None, ignoreNulls = true)),
      Agg(s"last_value($c) IGNORE NULLS", Member(c, None, ignoreNulls = true)),
      Agg(s"any_value($c, true)", Member(c, None, ignoreNulls = true)),
      Agg(s"last($c) FILTER (WHERE $F2)", Member(c, Some(F2), ignoreNulls = false)),
      Agg(s"first($c, true) FILTER (WHERE $F2)", Member(c, Some(F2), ignoreNulls = true)))
    if (all) base ++ extra else base
  }

  private val boolAggs: Seq[Agg] = Seq(
    "max(c_bool)",
    "min(c_bool)",
    "bool_and(c_bool)",
    "bool_or(c_bool)",
    "every(c_bool)",
    "some(c_bool)",
    "any(c_bool)",
    s"max(c_bool) FILTER (WHERE $F1)",
    s"min(c_bool) FILTER (WHERE $F2)",
    s"bool_and(c_bool) FILTER (WHERE $F1)",
    s"bool_or(c_bool) FILTER (WHERE $F2)",
    "count(c_bool)").map(Agg(_, Exact))

  private def distinctAggs(native: Boolean): Seq[Agg] = {
    val (d2, s, n) = if (native) ("c_date", "c_double", "c_ts") else ("c_str", "c_str", "c_arr")
    Seq(
      Agg("count(DISTINCT c_int)", Exact),
      Agg(s"count(DISTINCT $d2)", Exact),
      Agg("first(c_long)", Member("c_long", None, ignoreNulls = false)),
      Agg(s"last($s, true)", Member(s, None, ignoreNulls = true)),
      Agg("any_value(c_date)", Member("c_date", None, ignoreNulls = false)),
      Agg(s"first(c_dec38) FILTER (WHERE $F1)", Member("c_dec38", Some(F1), ignoreNulls = false)),
      Agg(s"last($n)", Member(n, None, ignoreNulls = false)),
      Agg(s"first($n, true)", Member(n, None, ignoreNulls = true)),
      Agg("max(c_bool)", Exact),
      Agg("min(c_bool)", Exact),
      Agg(s"bool_or(c_bool) FILTER (WHERE $F2)", Exact),
      Agg("count(*)", Exact))
  }

  private val singleDistinctAggs: Seq[Agg] = Seq(
    Agg("count(DISTINCT c_int)", Exact),
    Agg("first(c_long, true)", Member("c_long", None, ignoreNulls = true)),
    Agg("last(c_dec38)", Member("c_dec38", None, ignoreNulls = false)),
    Agg(s"first(c_ts) FILTER (WHERE $F1)", Member("c_ts", Some(F1), ignoreNulls = false)),
    Agg("max(c_bool)", Exact),
    Agg("bool_and(c_bool)", Exact))

  // ---------------------------------------------------------------- shapes

  private case class Shape(
      name: String,
      multi: Boolean,
      spill: Boolean,
      confs: Seq[(String, String)]) {
    def ordered: Boolean = !multi && !spill
  }

  private def aqe(on: Boolean) = SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> on.toString
  private def partitions(n: Int) = SQLConf.SHUFFLE_PARTITIONS.key -> n.toString
  private def batch(n: Int) = CometConf.COMET_BATCH_SIZE.key -> n.toString
  private val tinyPool = CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key -> "0.00005"

  private val singleAqe =
    Shape("single_aqe", multi = false, spill = false, Seq(aqe(true), partitions(1)))
  private val singleNoAqe =
    Shape("single_noaqe", multi = false, spill = false, Seq(aqe(false), partitions(3)))
  private val singleBatch8 =
    Shape(
      "single_batch8_noaqe",
      multi = false,
      spill = false,
      Seq(aqe(false), partitions(1), batch(8)))
  private val singleSpill =
    Shape(
      "single_spill",
      multi = false,
      spill = true,
      Seq(aqe(true), partitions(1), batch(64), tinyPool))
  private val multiAqe =
    Shape("multi_aqe", multi = true, spill = false, Seq(aqe(true), partitions(7)))
  private val multiBatch8NoAqe =
    Shape(
      "multi_batch8_noaqe",
      multi = true,
      spill = false,
      Seq(aqe(false), partitions(5), batch(8)))
  private val multiSpill =
    Shape(
      "multi_spill",
      multi = true,
      spill = true,
      Seq(aqe(true), partitions(3), batch(64), tinyPool))

  private val allShapes =
    Seq(singleAqe, singleNoAqe, singleBatch8, singleSpill, multiAqe, multiBatch8NoAqe, multiSpill)

  private def layoutConfs(multi: Boolean): Seq[(String, String)] =
    if (multi) {
      Seq(
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "65536",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1")
    } else {
      Seq(
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> (1L << 30).toString,
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> (1L << 30).toString)
    }

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

  private def show(v: Any): String = v match {
    case null => "null"
    case b: Array[Byte] => b.mkString("bin[", ",", "]")
    case s: String if s.length > 60 => s"${s.take(60)}...(${s.length} chars)"
    case d: Double if d == 0.0 && 1.0 / d < 0 => "-0.0"
    case f: Float if f == 0.0f && 1.0f / f < 0 => "-0.0f"
    case other => String.valueOf(other)
  }

  private case class CaseResult(native: Boolean, aggSpills: Long, inputPartitions: Int)

  private def aggregatesNative(plan: SparkPlan): Boolean = {
    val comet = collect(plan) { case a: CometHashAggregateExec => a }
    val spark = collect(plan) { case a: BaseAggregateExec => a }
    comet.nonEmpty && spark.isEmpty
  }

  private def runCase(
      label: String,
      ds: DataSet,
      shape: Shape,
      keys: Seq[String],
      aggs: Seq[Agg],
      expectNative: Boolean): CaseResult = {
    val view = if (shape.multi) ds.multiView else ds.singleView
    val select = (keys ++ aggs.zipWithIndex.map { case (a, i) => s"${a.sql} AS a$i" })
      .mkString(", ")
    val groupBy = if (keys.isEmpty) "" else keys.mkString(" GROUP BY ", ", ", "")
    val query = s"SELECT $select FROM $view$groupBy"
    val ctx = s"[seed=$seed case=$label shape=${shape.name} data=${ds.name} " +
      s"keys=${keys.mkString(",")}]"
    val nk = keys.length

    withConfs(shape.confs ++ layoutConfs(shape.multi): _*) {
      val inputPartitions = withConfs(CometConf.COMET_ENABLED.key -> "false") {
        spark.table(view).rdd.getNumPartitions
      }
      assert(shape.multi == (inputPartitions > 1), s"$ctx input partitions: $inputPartitions")
      val expected = withConfs(CometConf.COMET_ENABLED.key -> "false") {
        sql(query).collect().toSeq
      }
      val df = sql(query)
      val actual =
        try df.collect().toSeq
        catch {
          case e: Throwable => fail(s"$ctx Comet failed for query:\n$query", e)
        }
      val plan = df.queryExecution.executedPlan
      val native = aggregatesNative(plan)
      if (expectNative) {
        assert(native, s"$ctx expected native aggregates, plan:\n$plan")
      }
      val aggSpills = collect(plan) { case a: CometHashAggregateExec => a }
        .flatMap(_.metrics.get("spill_count"))
        .map(_.value)
        .sum

      def keyed(rows: Seq[Row], what: String): Map[Any, Row] = {
        val m = rows.groupBy(r => norm(Row.fromSeq(r.toSeq.take(nk))))
        val dups = m.filter(_._2.size > 1)
        assert(dups.isEmpty, s"$ctx $what has duplicate groups: ${dups.keys.take(5)}")
        m.map { case (k, v) => k -> v.head }
      }
      val exp = keyed(expected, "Spark")
      val act = keyed(actual, "Comet")
      assert(
        exp.keySet == act.keySet,
        s"$ctx group sets differ: missing in Comet ${(exp.keySet -- act.keySet).take(5)}, " +
          s"extra in Comet ${(act.keySet -- exp.keySet).take(5)}\n$query")

      val members = aggs.map(_.check).collect { case m: Member => m }.distinct
      val candidates =
        if (members.isEmpty) Map.empty[Member, Map[Any, (Set[Any], Boolean, Long)]]
        else candidateSets(view, keys, members)

      val errors = mutable.ArrayBuffer[String]()
      for ((k, e) <- exp; a = act(k); (agg, i) <- aggs.zipWithIndex) {
        val ev = e.get(nk + i)
        val av = a.get(nk + i)
        def err(msg: String): Unit =
          errors += s"group ${show(k)} ${agg.sql}: $msg (Comet ${show(av)}, Spark ${show(ev)})"
        agg.check match {
          case Exact =>
            if (norm(ev) != norm(av)) err("differs")
          case m: Member =>
            if (shape.ordered && native && norm(ev) != norm(av)) {
              err("differs on an ordered single split")
            }
            val (vals, hasNull, n) = candidates(m)(k)
            def allowed(v: Any): Boolean =
              if (n == 0) v == null
              else if (v == null) { if (m.ignoreNulls) vals.isEmpty else hasNull }
              else vals.contains(norm(v))
            if (!allowed(av)) err("Comet value is not a value of the group")
            if (!allowed(ev)) err("Spark value is not a value of the group")
        }
      }
      if (errors.nonEmpty) {
        fail(
          s"$ctx ${errors.size} mismatches, first ones:\n" +
            errors.take(25).mkString("\n") + s"\nquery: $query\nplan:\n$plan")
      }
      CaseResult(native, aggSpills, inputPartitions)
    }
  }

  private def candidateSets(
      view: String,
      keys: Seq[String],
      members: Seq[Member]): Map[Member, Map[Any, (Set[Any], Boolean, Long)]] = {
    val nk = keys.length
    val cols = members.zipWithIndex.flatMap { case (m, i) =>
      val f = m.filter.map(c => s" FILTER (WHERE $c)").getOrElse("")
      Seq(
        s"collect_list(${m.value})$f AS v$i",
        s"count(1)$f AS n$i",
        s"count(${m.value})$f AS nn$i")
    }
    val groupBy = if (keys.isEmpty) "" else keys.mkString(" GROUP BY ", ", ", "")
    val rows = withConfs(CometConf.COMET_ENABLED.key -> "false") {
      sql(s"SELECT ${(keys ++ cols).mkString(", ")} FROM $view$groupBy").collect().toSeq
    }
    members.zipWithIndex.map { case (m, i) =>
      m -> rows.map { r =>
        val k = norm(Row.fromSeq(r.toSeq.take(nk)))
        val vals = r.getSeq[Any](nk + 3 * i).map(norm).toSet
        val n = r.getLong(nk + 3 * i + 1)
        val nn = r.getLong(nk + 3 * i + 2)
        k -> ((vals, n > nn, n))
      }.toMap
    }.toMap
  }

  // ---------------------------------------------------------------- matrix

  private val groupings: Seq[Seq[String]] = Seq(Nil, Seq("k1"), Seq("k1", "k2"))

  private val nativeTypes: Set[String] = Set(
    "c_bool",
    "c_byte",
    "c_short",
    "c_int",
    "c_long",
    "c_float",
    "c_double",
    "c_dec10",
    "c_dec38",
    "c_date",
    "c_ts",
    "c_ntz")

  private def colAggs(cols: Seq[Col], all: Boolean): Seq[Agg] =
    cols.flatMap(c => firstLastAggs(c.name, all)) ++
      (if (cols.exists(_.name == "c_bool")) boolAggs else Nil)

  private case class Query(label: String, aggs: Seq[Agg], expectNative: Boolean)

  private def queries(ds: DataSet, onlyMain: Boolean, all: Boolean): Seq[Query] = {
    val cols = ds.cols.filter(c => c.main || !onlyMain)
    val (native, other) = cols.partition(c => nativeTypes.contains(c.name))
    Query("native_types", colAggs(native, all), expectNative = true) +:
      other.map(c => Query(s"type_${c.name}", colAggs(Seq(c), all), expectNative = false))
  }

  private def runAll(
      ds: DataSet,
      shape: Shape,
      keys: Seq[String],
      qs: Seq[Query]): Seq[CaseResult] =
    qs.map(q => runCase(q.label, ds, shape, keys, q.aggs, q.expectNative))

  private def assumeFull(): Unit =
    assume(fullMatrix, "the full matrix runs only with -Dcomet.test.aggFuzz.full=true")

  // ---------------------------------------------------------------- default tests

  test("ordered single split: exact first/last, bool aggregates and FILTER") {
    val ds = dataSet("mixed")
    for (keys <- groupings) {
      runAll(ds, singleAqe, keys, queries(ds, onlyMain = true, all = keys.nonEmpty))
    }
  }

  test("small batches with partial emission, AQE off") {
    val ds = dataSet("mixed")
    runAll(ds, singleBatch8, Seq("k1", "k2"), queries(ds, onlyMain = true, all = false))
  }

  test("many partitions across a shuffle: values belong to their group") {
    val ds = dataSet("mixed")
    for (keys <- Seq(Nil, Seq("k1", "k2"))) {
      runAll(ds, multiAqe, keys, queries(ds, onlyMain = true, all = false))
    }
  }

  test("forced spill of the aggregate state") {
    val ds = dataSet("mixed")
    val native = ds.cols.filter(c => c.main && nativeTypes.contains(c.name))
    val r = runCase("spill", ds, multiSpill, Seq("k1", "k2"), colAggs(native, all = false), true)
    assert(r.aggSpills > 0, s"[seed=$seed] the aggregate did not spill")
  }

  test("COUNT(DISTINCT) rewrite: first/last/min/max with FILTER over Expand") {
    val ds = dataSet("mixed")
    for (shape <- Seq(singleAqe, multiAqe)) {
      runCase("distinct", ds, shape, Seq("k1"), distinctAggs(native = true), expectNative = true)
    }
    runCase("distinct_one", ds, singleAqe, Seq("k1"), singleDistinctAggs, expectNative = true)
  }

  test("COUNT(DISTINCT) over a spilling PartialMerge aggregate") {
    val ds = dataSet("mixed")
    val r =
      runCase("distinct_one", ds, multiSpill, Seq("k1"), singleDistinctAggs, expectNative = true)
    assert(r.aggSpills > 0, s"[seed=$seed] the aggregate did not spill")
  }

  // ---------------------------------------------------------------- full matrix

  for (shape <- allShapes; keys <- groupings) {
    test(s"full: ${shape.name}, group by [${keys.mkString(", ")}]") {
      assumeFull()
      val ds = dataSet("mixed")
      val all = queries(ds, onlyMain = false, all = true) ++ Seq(
        Query("distinct", distinctAggs(native = true), expectNative = true),
        Query("distinct_fallback_types", distinctAggs(native = false), expectNative = false),
        Query("distinct_one", singleDistinctAggs, expectNative = true))
      val runnable = if (shape.spill) all.filter(_.expectNative) else all
      runAll(ds, shape, keys, runnable)
    }
  }

  for ((shape, keys) <- Seq(
      singleAqe -> Seq("g"),
      singleSpill -> Seq("g"),
      multiAqe -> Seq("k1", "k2"),
      multiSpill -> Seq("g"))) {
    test(s"full: 200k groups, ${shape.name}, group by [${keys.mkString(", ")}]") {
      assumeFull()
      val ds = dataSet("large")
      val results = runAll(ds, shape, keys, queries(ds, onlyMain = false, all = false)) :+
        runCase("distinct", ds, shape, keys, distinctAggs(native = true), expectNative = true)
      if (shape.spill) {
        assert(results.exists(_.aggSpills > 0), s"[seed=$seed] the aggregate did not spill")
      }
    }
  }
}
