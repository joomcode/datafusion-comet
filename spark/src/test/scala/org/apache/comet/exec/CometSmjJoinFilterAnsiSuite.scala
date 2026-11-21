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

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{CometTestBase, QueryTest, Row}
import org.apache.spark.sql.comet.CometSortMergeJoinExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * The target-mode differential oracle is original Spark, with complete duplicate rows retained.
 */
class CometSmjJoinFilterAnsiSuite extends CometTestBase {

  private def assertJoin(plan: SparkPlan, native: Boolean): Unit = {
    val comet = collect(plan) { case j: CometSortMergeJoinExec => j }
    val spark = collect(plan) { case j: SortMergeJoinExec => j }
    if (native) {
      assert(comet.nonEmpty && spark.isEmpty, s"Expected native filtered SMJ:\n$plan")
      assert(comet.forall(_.condition.nonEmpty), s"Expected a native join condition:\n$plan")
      // Keep the upstream AQE distribution fix while changing filter evaluation.
      comet.foreach { j =>
        assert(j.requiredChildDistribution == j.originalPlan.requiredChildDistribution)
      }
    } else {
      assert(spark.nonEmpty && comet.isEmpty, s"Expected safe Spark SMJ fallback:\n$plan")
      assert(spark.forall(_.condition.nonEmpty), s"Expected a Spark join condition:\n$plan")
    }
  }

  private def compare(
      query: String,
      native: Boolean = true,
      errorClass: Option[String] = None): SparkPlan = {
    var expected: Try[Seq[Row]] = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      expected = Try(sql(query).collect().toSeq)
    }
    val df = sql(query)
    val actual = Try(df.collect().toSeq)
    val plan = df.queryExecution.executedPlan
    (expected, actual, errorClass) match {
      case (Success(sparkRows), Success(cometRows), None) =>
        // sameRows compares the entire result sequence after sorting, including multiplicity.
        QueryTest.sameRows(sparkRows, cometRows).foreach { error => fail(s"$query\n$error") }
      case (Failure(sparkError), Failure(cometError), Some(required)) =>
        def structured(error: Throwable): SparkThrowable with Throwable =
          causeChain(error)
            .collect { case e: SparkThrowable with Throwable => e }
            .lastOption
            .getOrElse(fail(s"Expected a structured $required error: $error"))
        val se = structured(sparkError)
        val ce = structured(cometError)
        assert(se.getErrorClass == required, s"Spark must raise $required: $sparkError")
        assert(ce.getClass == se.getClass)
        assert(ce.getErrorClass == required)
        assert(ce.getSqlState == se.getSqlState)
      case _ =>
        fail(
          s"Unexpected outcome for $query; required error=$errorClass, " +
            s"Spark=$expected, Comet=$actual\n$plan")
    }
    // Diagnose semantic mismatches before checking the chosen engine. A valid fallback is not
    // evidence for an incorrect native result, and a native-plan assertion must not hide it.
    assertJoin(plan, native)
    plan
  }

  private def withJoinConfs(adaptive: Boolean, batchSize: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1",
      CometConf.COMET_FORCE_SHJ.key -> "false",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_BATCH_SIZE.key -> batchSize.toString)(f)
  }

  for (adaptive <- Seq(false, true); batchSize <- Seq(7, 64)) {
    test(
      "ANSI=true filtered SMJ retains duplicates and unmatched FULL rows: " +
        s"AQE=$adaptive batch=$batchSize") {
      withJoinConfs(adaptive, batchSize) {
        // Identical physical rows intentionally repeat. A key->row map or a set would hide losses.
        val left = Seq(
          (Some(1), Some(10), "left"),
          (Some(1), Some(10), "left"),
          (Some(1), Some(40), "rejected"),
          (Some(2), Some(20), "left-only"),
          (None, Some(30), "null-key"),
          (Some(3), None, "null-filter"))
        val right: Seq[(Option[Int], Option[Int], Option[Int], String)] = Seq(
          (Some(1), Some(0), Some(11), "rate"),
          (Some(1), Some(0), Some(11), "rate"),
          (Some(1), Some(12), Some(20), "unmatched-buffered"),
          (Some(4), Some(0), Some(99), "right-only"),
          (None, Some(0), Some(99), "null-key"),
          (Some(3), None, Some(99), "null-filter"))
        withParquetTable(left, "smj_ansi_l") {
          withParquetTable(right, "smj_ansi_r") {
            for (kind <- Seq("INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI")) {
              val selected =
                if (kind.startsWith("LEFT S") || kind == "LEFT ANTI") "l.*"
                else "l.*, r.*"
              compare(
                s"SELECT /*+ MERGE(l, r) */ $selected FROM smj_ansi_l l " +
                  s"$kind JOIN smj_ansi_r r ON l._1 = r._1 " +
                  "AND l._2 > r._2 AND l._2 <= r._3")
            }
          }
        }
      }
    }

    test(
      "ANSI=true filtered SMJ preserves lazy branches and mandatory errors: " +
        s"AQE=$adaptive batch=$batchSize") {
      withJoinConfs(adaptive, batchSize) {
        // Mixed guards force vector evaluation. An all-true/all-false batch can hide eager RHS
        // evaluation in DataFusion. Every guard reads both sides to prevent scan pushdown.
        val left = Seq(
          (Some(1), "bad", Some(0), 5, None: Option[Boolean]),
          (Some(2), "7", Some(5), 5, Some(true)),
          (Some(3), "bad", Some(0), 5, None: Option[Boolean]),
          (Some(4), "8", Some(5), 5, Some(true)),
          (Some(7), "9", None, 5, Some(false)),
          (Some(5), "bad", Some(5), 5, Some(true)),
          (None, "bad", Some(5), 5, Some(true)))
        val right = Seq(1, 2, 3, 4, 6, 7).map(k => (Option(k), 5, Option(true)))
        withParquetTable(left, "smj_ansi_guard_l") {
          withParquetTable(right, "smj_ansi_guard_r") {
            def query(kind: String, predicate: String, l: String = "smj_ansi_guard_l"): String =
              s"SELECT /*+ MERGE(l, r) */ l.*, r.* FROM $l l $kind JOIN " +
                s"smj_ansi_guard_r r ON l._1 = r._1 AND ($predicate)"
            // Equality would become an additional equijoin key, bypassing the lazy predicate.
            // Keep a genuinely mixed-side, non-equi guard in the residual ON condition.
            val guarded = Seq(
              "CASE WHEN l._3 + r._2 > l._4 THEN CAST(l._2 AS INT) > r._2 ELSE l._4 = r._2 END",
              "IF(l._3 + r._2 > l._4, CAST(l._2 AS INT) > r._2, l._4 = r._2)")
            val lazyBoolean = Seq(
              "l._3 + r._2 > l._4 AND CAST(l._2 AS INT) > r._2",
              "l._3 + r._2 <= l._4 OR CAST(l._2 AS INT) > r._2")
            for (kind <- Seq("LEFT", "RIGHT", "FULL")) {
              guarded.foreach(p => compare(query(kind, p)))
              // Until native AND/OR preserves Spark's lazy RHS contract, choose Spark before
              // execution. Scan/other native operators may remain native around this join.
              lazyBoolean.foreach(p => compare(query(kind, p), native = false))
              // TRY_CAST is nonthrowing even under ANSI=true: retain the native path and compare
              // all NULL/matching/unmatched rows, rather than falling back for every AND/OR.
              lazyBoolean.foreach(p => compare(query(kind, p.replace("CAST(", "TRY_CAST("))))
              compare(
                query(
                  kind,
                  "CASE WHEN l._3 + r._2 <= l._4 THEN CAST(l._2 AS INT) > r._2 " +
                    "ELSE l._4 = r._2 END"),
                errorClass = Some("CAST_INVALID_INPUT"))
              for (op <- Seq("AND", "OR")) {
                // Keep a mixed-side nullable LHS so it cannot move into one input scan.
                val nullableQuery = query(
                  kind,
                  "(CASE WHEN l._3 + r._2 <= l._4 THEN CAST(NULL AS BOOLEAN) " +
                    s"ELSE l._5 END) $op CAST(l._2 AS INT) > r._2")
                // Default Catalyst replaces this literal NULL with false in an ON predicate:
                // AND skips the cast, whereas OR still has to evaluate it. Check that contract.
                compare(
                  nullableQuery,
                  native = false,
                  errorClass = if (op == "OR") Some("CAST_INVALID_INPUT") else None)
                // Also exercise the actual NULL expression contract, with the same narrowly
                // excluded optimizer rule in both Spark and Comet. NULL does not short circuit
                // either operator, so the malformed matching value must now raise an error.
                withSQLConf(
                  SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
                    "org.apache.spark.sql.catalyst.optimizer.ReplaceNullWithFalseInPredicate") {
                  compare(nullableQuery, native = false, errorClass = Some("CAST_INVALID_INPUT"))
                }
              }
              // Bad values have no equal key. Never evaluate conditions on unmatched rows or
              // synthetic outer-null pairs; coalesce would expose such speculative evaluation.
              val validOrUnmatched =
                "(SELECT * FROM smj_ansi_guard_l WHERE _1 IN (2, 4, 5) OR _1 IS NULL)"
              compare(query(kind, "CAST(coalesce(l._2, 'bad') AS INT) > r._2", validOrUnmatched))
            }
          }
        }
      }
    }
  }

  test("ANSI=true filtered SMJ skips disjoint keys but rejects bad actual semi/anti matches") {
    val left = Seq((Some(2), "bad"), (Some(4), "bad"), (None, "bad"))
    withParquetTable(left, "smj_disjoint_l") {
      withParquetTable(Seq((1, 5), (3, 5)), "smj_disjoint_r") {
        withParquetTable(Seq((1, 5), (2, 5), (3, 5)), "smj_actual_r") {
          for (adaptive <- Seq(false, true); batchSize <- Seq(7, 64)) {
            withJoinConfs(adaptive, batchSize) {
              for (kind <- Seq("INNER", "LEFT SEMI", "LEFT ANTI")) {
                def query(right: String): String =
                  s"SELECT /*+ MERGE(l, r) */ l.* FROM smj_disjoint_l l $kind JOIN " +
                    s"$right r ON l._1 = r._1 AND CAST(l._2 AS INT) > r._2"
                compare(query("smj_disjoint_r"))
                compare(query("smj_actual_r"), errorClass = Some("CAST_INVALID_INPUT"))
              }
            }
          }
        }
      }
    }
  }

  test("ANSI=true filtered SMJ preserves required clustering under AQE skewed REBALANCE") {
    withJoinConfs(adaptive = true, batchSize = 64) {
      withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "200",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "2k",
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled" -> "true") {
        withTempPath { dir =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark
              .range(0, 40000, 1, 8)
              .selectExpr("CASE WHEN id % 10 = 0 THEN 0L ELSE id % 1000 END AS k", "id AS v")
              .write
              .parquet(dir.getCanonicalPath)
          }
          withParquetTable(dir.getCanonicalPath, "smj_ansi_rebalance_l") {
            withParquetTable(
              (0 until 1000).map(i => (i.toLong, i.toLong)),
              "smj_ansi_rebalance_r") {
              val left = "(SELECT /*+ REBALANCE(k) */ * FROM smj_ansi_rebalance_l)"
              val right = "(SELECT /*+ REBALANCE(k) */ _1 AS k, _2 AS w " +
                "FROM smj_ansi_rebalance_r)"
              // No aggregate above the join: only the join can reject a clustering violation.
              val plan = compare(s"SELECT /*+ MERGE(a, b) */ a.k, a.v, b.w FROM $left a " +
                s"LEFT JOIN $right b ON a.k = b.k AND a.v >= b.w")
              assert(plan.isInstanceOf[AdaptiveSparkPlanExec], plan)
              assert(
                collect(plan) {
                  case read: AQEShuffleReadExec if read.hasSkewedPartition => read
                }.isEmpty,
                plan)
            }
          }
        }
      }
    }
  }
}
