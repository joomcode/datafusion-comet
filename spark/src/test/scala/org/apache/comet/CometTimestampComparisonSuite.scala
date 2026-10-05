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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{FromUTCTimestamp, TruncTimestamp}

/** Comparisons of timestamps whose native producers label their time zone differently. */
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

  private def forZones(f: => Unit): Unit = inZones(zones)(f)

  private def inZones(zones: Seq[String])(f: => Unit): Unit =
    zones.foreach { zone =>
      withSQLConf("spark.sql.session.timeZone" -> zone) {
        withClue(s"session time zone $zone: ")(f)
      }
    }

  test("timestamp_seconds compares with timestamp literals and columns") {
    withEvents {
      forZones {
        checkSparkAnswerAndOperator(
          "SELECT s FROM events WHERE " +
            "timestamp_seconds(s) BETWEEN TIMESTAMP '2020-01-01' AND TIMESTAMP '2030-01-01'")
        checkSparkAnswerAndOperator(
          "SELECT s, timestamp_seconds(s) >= ts, timestamp_seconds(s) = ts, " +
            "timestamp_seconds(s) < ts FROM events")
        checkSparkAnswerAndOperator(
          "SELECT s, hour(timestamp_seconds(s)), CAST(timestamp_seconds(s) AS STRING) " +
            "FROM events")
      }
    }
  }

  test("timestamp_seconds, timestamp_millis and to_timestamp mix in conditionals") {
    withEvents {
      forZones {
        checkSparkAnswer("""SELECT s, COALESCE(
            |  CASE WHEN timestamp_seconds(s) BETWEEN TIMESTAMP '2020-01-01'
            |    AND TIMESTAMP '2030-01-01' THEN timestamp_seconds(s) END,
            |  CASE WHEN timestamp_millis(ms) BETWEEN TIMESTAMP '2020-01-01'
            |    AND TIMESTAMP '2030-01-01' THEN timestamp_millis(ms) END,
            |  to_timestamp(str, 'yyyy-MM-dd HH:mm:ss.SSSSSS')) AS p_time
            |FROM events""".stripMargin)
        checkSparkAnswerAndOperator(
          "SELECT s, CASE WHEN s > 1000 THEN timestamp_seconds(s) ELSE ts END FROM events")
      }
    }
  }

  test("from_utc_timestamp results compare with to_timestamp results") {
    withEvents {
      withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[FromUTCTimestamp]) -> "true") {
        forZones {
          checkSparkAnswer(
            "SELECT a.s, b.s FROM events a JOIN events b ON a.s = b.s AND " +
              "from_utc_timestamp(timestamp_millis(a.ms), 'Europe/Moscow') >= " +
              "to_timestamp(b.str, 'yyyy-MM-dd HH:mm:ss.SSSSSS')")
          checkSparkAnswer(
            "SELECT s, from_utc_timestamp(timestamp_seconds(s), 'Europe/Moscow') >= ts, " +
              "from_utc_timestamp(ts, 'Europe/Moscow') < timestamp_seconds(s) FROM events")
        }
      }
    }
  }

  test("date_trunc labelled with the session time zone compares with scan timestamps") {
    withEvents {
      withSQLConf(
        CometConf.getExprEnabledConfigKey("TruncTimestamp") -> "true",
        CometConf.getExprAllowIncompatConfigKey(classOf[TruncTimestamp]) -> "true") {
        inZones(Seq("UTC", "Etc/UTC", "Europe/Moscow")) {
          checkSparkAnswerAndOperator(
            "SELECT ts, date_trunc('HOUR', ts) < ts, date_trunc('HOUR', ts) >= ts FROM events")
          checkSparkAnswerAndOperator("SELECT ts FROM events WHERE date_trunc('HOUR', ts) < ts")
          checkSparkAnswerAndOperator(
            "SELECT s FROM events WHERE date_trunc('HOUR', timestamp_seconds(s)) <= ts")
        }
      }
    }
  }

  test("timestamp_ntz comparisons keep wall-clock semantics") {
    withEvents {
      forZones {
        checkSparkAnswer(
          "SELECT s, CAST(timestamp_seconds(s) AS TIMESTAMP_NTZ) >= " +
            "TIMESTAMP_NTZ'2024-03-10 10:30:00', " +
            "CAST(ts AS TIMESTAMP_NTZ) = CAST(timestamp_seconds(s) AS TIMESTAMP_NTZ), " +
            "timestamp_seconds(s) > CAST(TIMESTAMP_NTZ'2024-03-10 10:30:00' AS TIMESTAMP) " +
            "FROM events")
      }
    }
  }
}
