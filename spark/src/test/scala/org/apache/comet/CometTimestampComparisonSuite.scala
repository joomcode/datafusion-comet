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

package org.apache.comet

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Cast, FromUTCTimestamp, TruncTimestamp}
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DecimalType, DoubleType, FloatType, IntegerType, ShortType, TimestampType}

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.expressions.CometCast
import org.apache.comet.serde.Unsupported

/** Producer-to-producer timestamp comparisons, including the original Joom regressions. */
class CometTimestampComparisonSuite extends CometTestBase {

  private val zones = Seq("UTC", "Etc/UTC", "Europe/Moscow", "America/Los_Angeles")

  private def withEvents(f: => Unit): Unit = {
    withTempPath { dir =>
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark
          .sql("""SELECT * FROM VALUES
            |  (1710063000L, 1710063000123L, '2024-03-10 10:30:00.123456',
            |    TIMESTAMP'2024-03-10 10:30:00Z'),
            |  (1640995200L, 1640995200000L, '2022-01-01 00:00:00.000000',
            |    TIMESTAMP'2022-01-01 03:00:00Z'),
            |  (1730622600L, 1730622600999L, '2024-11-03 08:30:00.999000',
            |    TIMESTAMP'2024-11-03 01:00:00Z'),
            |  (-1L, -1L, '1969-12-31 23:59:59.999999',
            |    TIMESTAMP'1969-12-31 23:59:59.999999Z'),
            |  (100L, 100L, 'not a timestamp', TIMESTAMP'1970-01-01 00:00:00Z'),
            |  (NULL, NULL, NULL, NULL)
            |AS t(s, ms, str, ts)""".stripMargin)
          .write
          .parquet(dir.getCanonicalPath)
      }
      spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("events")
      withTempView("events")(f)
    }
  }

  private def inZones(selectedZones: Seq[String] = zones)(f: => Unit): Unit =
    for (zone <- selectedZones; ansi <- Seq("true", "false")) {
      withSQLConf("spark.sql.session.timeZone" -> zone, "spark.sql.ansi.enabled" -> ansi) {
        withClue(s"session time zone $zone, ANSI=$ansi: ")(f)
      }
    }

  test("timestamp_seconds compares natively with timestamp literals and scan columns") {
    withEvents {
      inZones() {
        val betweenQuery = "SELECT s FROM events WHERE " +
          "timestamp_seconds(s) BETWEEN TIMESTAMP '2020-01-01' AND TIMESTAMP '2030-01-01'"
        // Spark filters share this producer across BETWEEN's two comparisons.
        // Keep the original row oracle and require only the accepted evaluation-order boundary.
        val (_, plan) = checkSparkAnswerAndNativeScanFilter(
          sql(betweenQuery),
          "Filter shared-expression evaluation order requires Spark")
        checkCometOperators(plan, classOf[FilterExec])
        checkSparkAnswerAndImpl(
          sql(
            "SELECT s, timestamp_seconds(s) >= ts, timestamp_seconds(s) = ts, " +
              "timestamp_seconds(s) < ts, timestamp_seconds(s) <=> ts FROM events"),
          native = Seq("timestamp_seconds"))
        checkSparkAnswerAndImpl(
          sql(
            "SELECT s, hour(timestamp_seconds(s)), CAST(timestamp_seconds(s) AS STRING) " +
              "FROM events"),
          native = Seq("timestamp_seconds"))
      }
    }
  }

  test("native and dispatched timestamp producers mix in conditionals") {
    withEvents {
      inZones() {
        checkSparkAnswerAndImpl(
          sql("""SELECT s, COALESCE(
            |  CASE WHEN s > 1000 THEN timestamp_seconds(s) END,
            |  CASE WHEN ms < 0 THEN timestamp_millis(ms) END, ts) >= ts AS later
            |FROM events""".stripMargin),
          native = Seq("timestamp_seconds"),
          dispatched = Seq("timestamp_millis"))
        checkSparkAnswerAndImpl(
          sql("SELECT s, CASE WHEN s > 1000 THEN timestamp_seconds(s) ELSE ts END FROM events"),
          native = Seq("timestamp_seconds"))
      }
      // Retain the original malformed-string/explicit-parser regression in its legacy mode.
      // The successful native comparisons above also run with the target ANSI=true mode.
      for (zone <- zones) {
        withSQLConf("spark.sql.session.timeZone" -> zone, "spark.sql.ansi.enabled" -> "false") {
          val query = """SELECT s, COALESCE(
              |  CASE WHEN timestamp_seconds(s) BETWEEN TIMESTAMP '2020-01-01'
              |    AND TIMESTAMP '2030-01-01' THEN timestamp_seconds(s) END,
              |  CASE WHEN timestamp_millis(ms) BETWEEN TIMESTAMP '2020-01-01'
              |    AND TIMESTAMP '2030-01-01' THEN timestamp_millis(ms) END,
              |  to_timestamp(str, 'yyyy-MM-dd HH:mm:ss.SSSSSS')) AS p_time
              |FROM events""".stripMargin
          if (isSpark40Plus) {
            checkSparkAnswerAndOperator(query)
          } else {
            val reason = "Conditional shared-expression evaluation order requires Spark"
            val (_, plan) = checkSparkAnswerAndFallbackReason(query, reason)
            checkCometOperators(plan, classOf[ProjectExec])
            val projects = collect(plan) { case project: ProjectExec => project }
            assert(projects.size == 1, plan)
            assert(
              projects.head.getTagValue(CometExplainInfo.FALLBACK_REASONS).contains(Set(reason)),
              plan)
            assert(collect(plan) { case scan: CometNativeScanExec => scan }.size == 1, plan)
          }
        }
      }
    }
  }

  test("from_utc_timestamp results compare with other timestamp producers") {
    withEvents {
      withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[FromUTCTimestamp]) -> "true") {
        inZones() {
          checkSparkAnswerAndImpl(
            sql(
              "SELECT s, from_utc_timestamp(timestamp_seconds(s), 'Europe/Moscow') >= ts, " +
                "from_utc_timestamp(ts, 'Europe/Moscow') < timestamp_seconds(s) FROM events"),
            native = Seq("from_utc_timestamp", "timestamp_seconds"))
        }
        for (zone <- zones) {
          withSQLConf("spark.sql.session.timeZone" -> zone, "spark.sql.ansi.enabled" -> "false") {
            checkSparkAnswerAndOperator(
              "SELECT a.s, b.s FROM events a JOIN events b ON a.s = b.s AND " +
                "from_utc_timestamp(timestamp_millis(a.ms), 'Europe/Moscow') >= " +
                "to_timestamp(b.str, 'yyyy-MM-dd HH:mm:ss.SSSSSS')")
          }
        }
      }
    }
  }

  test("date_trunc results compare natively with scan and timestamp_seconds values") {
    withEvents {
      withSQLConf(
        CometConf.getExprEnabledConfigKey("TruncTimestamp") -> "true",
        CometConf.getExprAllowIncompatConfigKey(classOf[TruncTimestamp]) -> "true") {
        // The legacy native opt-in covered fixed-offset zones. Do not widen that opt-in to
        // known DST incompatibilities; default guarded routing is checked separately below.
        inZones(Seq("UTC", "Etc/UTC", "Europe/Moscow")) {
          checkSparkAnswerAndImpl(
            sql(
              "SELECT ts, date_trunc('HOUR', ts) < ts, date_trunc('HOUR', ts) >= ts FROM events"),
            native = Seq("date_trunc"))
          checkSparkAnswerAndImpl(
            sql("SELECT ts FROM events WHERE date_trunc('HOUR', ts) < ts"),
            native = Seq("date_trunc"))
          checkSparkAnswerAndImpl(
            sql("SELECT s FROM events WHERE date_trunc('HOUR', timestamp_seconds(s)) <= ts"),
            native = Seq("date_trunc", "timestamp_seconds"))
        }
      }
    }
  }

  test("guarded timestamp producers retain safe default dispatch and comparison semantics") {
    withEvents {
      withSQLConf(
        CometConf.getExprAllowIncompatConfigKey(classOf[TruncTimestamp]) -> "false",
        CometConf.getExprAllowIncompatConfigKey(classOf[FromUTCTimestamp]) -> "false",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
        inZones(Seq("Etc/UTC", "America/Los_Angeles")) {
          // A UTC date_trunc is native; a DST-zone date_trunc stays behind its existing guard.
          if (spark.sessionState.conf.sessionLocalTimeZone == "America/Los_Angeles") {
            checkSparkAnswerAndImpl(
              sql("SELECT s, date_trunc('DAY', ts) <= timestamp_seconds(s) FROM events"),
              native = Seq("timestamp_seconds"),
              dispatched = Seq("date_trunc"))
          }
          checkSparkAnswerAndImpl(
            sql(
              "SELECT s, from_utc_timestamp(ts, 'Europe/Moscow') < timestamp_seconds(s) " +
                "FROM events"),
            native = Seq("timestamp_seconds"),
            dispatched = Seq("from_utc_timestamp"))
        }
      }
    }
  }

  test(
    "unsupported timestamp casts retain Spark rows and mandatory errors through dispatch or fallback") {
    withEvents {
      assert(
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get(),
        "The dispatcher must be on by default")
      val targets: Seq[(String, DataType)] = Seq(
        "BOOLEAN" -> BooleanType,
        "TINYINT" -> ByteType,
        "SMALLINT" -> ShortType,
        "INT" -> IntegerType,
        "FLOAT" -> FloatType,
        "DOUBLE" -> DoubleType,
        "DECIMAL(10,2)" -> DecimalType(10, 2))
      def sameError(expected: Throwable, actual: Throwable): Unit = {
        causeChain(expected).collect { case error: SparkThrowable => error }.lastOption match {
          case Some(error) =>
            val actualError = causeChain(actual)
              .collect { case failure: SparkThrowable =>
                failure
              }
              .lastOption
              .getOrElse(fail(s"Comet lost Spark's structured error: $actual"))
            assert(actualError.getClass == error.getClass)
            assert(actualError.getErrorClass == error.getErrorClass)
            assert(actualError.getSqlState == error.getSqlState)
          case None =>
            val expectedRoot = causeChain(expected).last
            val actualRoot = causeChain(actual).last
            assert(actualRoot.getClass == expectedRoot.getClass)
            assert(actualRoot.getMessage == expectedRoot.getMessage)
        }
      }
      var checked = 0
      for ((target, dataType) <- targets; ansi <- Seq("true", "false");
        dispatch <- Seq(true, false)) {
        val configs =
          Seq("spark.sql.ansi.enabled" -> ansi, "spark.sql.session.timeZone" -> "UTC") ++
            (if (dispatch) Seq.empty
             else Seq(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false"))
        withSQLConf(configs: _*) {
          withClue(s"cast timestamp to $target, ANSI=$ansi, dispatch=$dispatch: ") {
            val query = s"SELECT s, CAST(ts AS $target) FROM events"
            def analyzed(enabled: Boolean): Try[DataFrame] =
              withSQLConfResult(CometConf.COMET_ENABLED.key -> enabled.toString) {
                Try {
                  val df = sql(query)
                  val _ = df.queryExecution.analyzed
                  df
                }
              }
            (analyzed(enabled = false), analyzed(enabled = true)) match {
              case (Failure(expected), Failure(actual)) =>
                // A Spark analysis rejection precedes physical planning and earns no native credit.
                sameError(expected, actual)
              case (Failure(expected), Success(_)) =>
                fail(s"Comet accepted a query that Spark rejected during analysis: $expected")
              case (Success(_), Failure(actual)) => throw actual
              case (Success(sparkDf), Success(df)) =>
                assert(df.schema == sparkDf.schema)
                val casts = df.queryExecution.analyzed.expressions.flatMap(_.collect {
                  case cast: Cast
                      if cast.child.dataType == TimestampType && cast.dataType == dataType =>
                    cast
                })
                assert(casts.size == 1)
                val reason = CometCast.getSupportLevel(casts.head) match {
                  case Unsupported(Some(reason)) => reason
                  case other => fail(s"Expected the existing native cast guard, found $other")
                }
                val plan = stripAQEPlan(df.queryExecution.executedPlan)
                if (dispatch) {
                  checkCometOperators(plan)
                  assertExpressionImpl(plan, native = Seq.empty, dispatched = Seq("cast"))
                } else {
                  checkCometOperators(plan, classOf[ProjectExec])
                  val projects = collect(plan) { case project: ProjectExec => project }
                  assert(projects.size == 1, plan)
                  val disabledReason = s"${CometExplainInfo.exprDisplayName(casts.head)}: " +
                    s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; " +
                    "expression has no native path so the plan falls back to Spark"
                  assert(
                    projects.head
                      .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                      .contains(Set(reason, disabledReason)),
                    plan)
                }
                assert(collect(plan) { case scan: CometNativeScanExec => scan }.size == 1, plan)
                checkSparkAnswerMaybeThrows(df) match {
                  case (None, None) =>
                    if (dispatch) {
                      checkSparkAnswerAndImpl(sql(query), dispatched = Seq("cast"))
                    }
                  case (None, Some(actual)) => throw actual
                  case (Some(expected), None) =>
                    fail(s"Comet succeeded but Spark failed: $expected")
                  case (Some(expected), Some(actual)) => sameError(expected, actual)
                }
            }
            checked += 1
          }
        }
      }
      assert(checked == 28)
    }
  }

  test("timestamp_ntz comparisons retain wall-clock semantics") {
    withEvents {
      inZones() {
        checkSparkAnswerAndImpl(
          sql(
            "SELECT s, CAST(timestamp_seconds(s) AS TIMESTAMP_NTZ) >= " +
              "TIMESTAMP_NTZ'2024-03-10 10:30:00', " +
              "CAST(ts AS TIMESTAMP_NTZ) = CAST(timestamp_seconds(s) AS TIMESTAMP_NTZ), " +
              "timestamp_seconds(s) > CAST(TIMESTAMP_NTZ'2024-03-10 10:30:00' AS TIMESTAMP) " +
              "FROM events"),
          native = Seq("timestamp_seconds"))
      }
    }
  }

  test("timestamp_seconds integral overflow must fail with ANSI enabled or disabled") {
    import testImplicits._
    withTempPath { dir =>
      Seq(0L, Long.MaxValue, Long.MinValue).toDF("seconds").write.parquet(dir.getCanonicalPath)
      val input = spark.read.parquet(dir.getCanonicalPath)
      for (ansi <- Seq("true", "false"); boundary <- Seq(Long.MaxValue, Long.MinValue)) {
        withSQLConf("spark.sql.ansi.enabled" -> ansi) {
          // Each boundary is executed separately: failure on MaxValue must not conceal whether
          // MinValue is also rejected. Execute the same native DataFrame whose plan we inspect.
          val df = input.where(s"seconds = $boundary").selectExpr("timestamp_seconds(seconds)")
          assertExpressionImpl(
            df.queryExecution.executedPlan,
            Seq("timestamp_seconds"),
            Seq.empty)
          var sparkError: Option[Throwable] = None
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            val sparkDf = spark.read
              .parquet(dir.getCanonicalPath)
              .where(s"seconds = $boundary")
              .selectExpr("timestamp_seconds(seconds)")
            sparkError = Try(sparkDf.collect()).failed.toOption
          }
          val cometError = Try(df.collect()).failed.toOption
          for ((engine, error) <- Seq("Spark" -> sparkError, "Comet" -> cometError)) {
            val failure = error.getOrElse(fail(s"$engine silently accepted timestamp overflow"))
            assert(
              causeChain(failure).exists(e =>
                Option(e.getMessage).exists(_.contains("long overflow"))),
              s"$engine: $failure")
          }
        }
      }
    }
  }
}
