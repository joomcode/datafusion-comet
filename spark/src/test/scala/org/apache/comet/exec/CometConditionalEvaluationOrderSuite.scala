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

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{CometTestBase, QueryTest, Row}
import org.apache.spark.sql.comet.{CometFilterExec, CometProjectExec}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometCodegenAssertions, CometConf}
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

/** A task-local counter makes duplicate or branch-reordered evaluation visible in result rows. */
private class ConditionalCountingFunction extends (Int => java.lang.Integer) with Serializable {
  private var calls = 0
  override def apply(value: Int): java.lang.Integer = {
    calls += 1
    if (value % 11 == 0) null else java.lang.Integer.valueOf(value * 1000 + calls)
  }
}

/** Differential contracts are defined by original Spark, including required ANSI errors. */
class CometConditionalEvaluationOrderSuite
    extends CometTestBase
    with CometCodegenAssertions
    with AdaptiveSparkPlanHelper {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")
    .set(SQLConf.ANSI_ENABLED.key, "true")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  private def compare(query: String, error: Option[String] = None): SparkPlan =
    withClue(s"$query\n") {
      var expected: Try[Seq[Row]] = null
      var expectedSchema: org.apache.spark.sql.types.StructType = null
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val vanilla = sql(query)
        expectedSchema = vanilla.schema
        expected = Try(vanilla.collect().toSeq)
      }
      val df = sql(query)
      assert(df.schema == expectedSchema, s"Schema mismatch: ${df.schema} != $expectedSchema")
      val actual = Try(df.collect().toSeq)
      val plan = df.queryExecution.executedPlan
      (expected, actual, error) match {
        case (Success(sparkRows), Success(cometRows), None) =>
          QueryTest.sameRows(sparkRows, cometRows).foreach(fail(_))
        case (Failure(sparkError), Failure(cometError), Some(required)) =>
          def structured(e: Throwable): SparkThrowable = causeChain(e)
            .collect { case s: SparkThrowable => s }
            .lastOption
            .getOrElse(fail(s"Missing structured $required error: $e"))
          val se = structured(sparkError)
          val ce = structured(cometError)
          assert(se.getErrorClass == required, sparkError)
          assert(ce.getErrorClass == required, cometError)
          assert(se.getSqlState == ce.getSqlState)
        case _ => fail(s"Expected error=$error; Spark=$expected; Comet=$actual\n$plan")
      }
      plan
    }

  private def nativeProjection(plan: SparkPlan): Unit = {
    assert(collect(plan) { case p: CometProjectExec => p }.nonEmpty, plan)
    assert(collect(plan) { case p: ProjectExec => p }.isEmpty, plan)
  }

  for (wholeStage <- Seq(false, true); batch <- Seq(7, 64)) {
    test(
      s"native boolean guards skip only determined rows: wholeStage=$wholeStage batch=$batch") {
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
        CometConf.COMET_BATCH_SIZE.key -> batch.toString) {
        // Three quarters of the valid AND batch need RHS evaluation: above DF's old 20% mask
        // heuristic. NULL must remain in the selected mask; it does not determine AND or OR.
        val rows =
          Seq((Some(false), "bad", 0), (Some(true), "2", 1), (None, "3", 2), (Some(true), "4", 3))
        withParquetTable(rows, "boolean_order") {
          nativeProjection(compare("SELECT _3, _1 AND CAST(_2 AS INT) > 0 FROM boolean_order"))
          nativeProjection(
            compare("SELECT _3, (NOT _1) OR CAST(_2 AS INT) > 0 FROM boolean_order"))
        }
        withParquetTable(Seq((None: Option[Boolean], "bad", 0)), "boolean_null_error") {
          for (operator <- Seq("AND", "OR")) {
            nativeProjection(
              compare(
                s"SELECT _1 $operator CAST(_2 AS INT) > 0 FROM boolean_null_error",
                Some("CAST_INVALID_INPUT")))
          }
        }
      }
    }
  }

  private val filterModes = Seq(
    "whole-stage" -> Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true"),
    "interpreted" -> Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false"),
    "max-fields" -> Seq(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      "spark.sql.codegen.maxFields" -> "1"),
    "no-codegen" -> Seq(
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
      "spark.sql.codegen.factoryMode" -> "NO_CODEGEN"))

  for ((mode, conf) <- filterModes; optional <- Seq(false, true)) {
    test(
      s"native-only Filter NULL guard follows actual Spark codegen: $mode optional=$optional") {
      withSQLConf((conf :+
        (CometConf.COMET_EXEC_FILTER_SHORT_CIRCUIT_JVM_DISPATCH_ENABLED.key -> optional.toString)): _*) {
        val rows = Seq(
          (Some(true), "bad", 0),
          (Some(false), "bad", 1),
          (Some(true), "2", 2),
          (Some(true), "3", 3))
        withParquetTable(rows, "filter_order") {
          val query = "SELECT _3 FROM filter_order WHERE " +
            "(CASE WHEN _3 = 0 THEN CAST(NULL AS BOOLEAN) ELSE _1 END) " +
            "AND CAST(_2 AS INT) > 0"
          // Normal Catalyst optimization replaces literal NULL inside a predicate with FALSE.
          compare(query)
          withSQLConf(
            SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
              "org.apache.spark.sql.catalyst.optimizer.ReplaceNullWithFalseInPredicate") {
            // SPARK-44236 made NO_CODEGEN disable whole-stage codegen only from Spark 3.5.
            // Spark 3.4 still runs this fixture as an eligible whole-stage Filter in that mode.
            // Interpreted/codegen-predicate AND otherwise evaluates its RHS when LHS is NULL.
            val nullRejectedBeforeRhs =
              mode == "whole-stage" || (mode == "no-codegen" && !isSpark35Plus)
            val required = if (nullRejectedBeforeRhs) None else Some("CAST_INVALID_INPUT")
            val plan = compare(query, required)
            assert(
              collect(plan) {
                case f: CometFilterExec => f
                case f: FilterExec => f
              }.nonEmpty,
              plan)
          }
        }
      }
    }
  }

  for (eliminate <- Seq(false, true)) {
    test(s"shared native boolean RHS preserves Spark mandatory errors: CSE=$eliminate") {
      withSQLConf(
        SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> eliminate.toString,
        "spark.sql.subexpressionElimination.skipForShortcutExpr" -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        withParquetTable(Seq((false, "bad", false), (true, "2", true)), "shared_boolean") {
          for ((left, operator) <- Seq("_1" -> "AND", "NOT _1" -> "OR")) {
            val query = s"SELECT ($left) $operator CAST(_2 AS INT) > 0 AS a, " +
              s"($left) $operator CAST(_2 AS INT) > 1 AS b FROM shared_boolean"
            val plan = compare(query, if (eliminate) Some("CAST_INVALID_INPUT") else None)
            if (eliminate) {
              assert(collect(plan) { case p: CometProjectExec => p }.isEmpty, plan)
              assert(collect(plan) { case p: ProjectExec => p }.nonEmpty, plan)
            } else nativeProjection(plan)
          }
        }
      }
    }
  }

  for (wholeStage <- Seq(false, true); batch <- Seq(7, 64)) {
    test(
      s"conditional JVM occurrences preserve row-state and NULLs: wholeStage=$wholeStage batch=$batch") {
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
        CometConf.COMET_BATCH_SIZE.key -> batch.toString) {
        spark.udf.register(
          "conditional_count",
          udf(new ConditionalCountingFunction).asNondeterministic())
        withParquetTable((0 until 48).map(i => Tuple1(i)), "conditional_state") {
          val expressions = Seq(
            "COALESCE(conditional_count(_1), -7)",
            "CASE WHEN _1 % 2 = 0 THEN conditional_count(_1) " +
              "ELSE conditional_count(_1 + 100) END",
            "IF(IF(_1 % 5 = 0, NULL, _1 % 2 = 0), conditional_count(_1), " +
              "conditional_count(_1 + 100))")
          expressions.foreach { expr =>
            assertCodegenRan {
              val plan = compare(s"SELECT $expr FROM conditional_state")
              nativeProjection(plan)
              val roots = collect(plan) { case p: CometProjectExec =>
                p.nativeOp.getProjection.getProjectListList.asScala.toSeq
              }.headOption.getOrElse(Nil)
              assert(
                roots.size == 1 && roots.head.hasJvmScalarUdf,
                s"The entire conditional must own one row-oriented JVM kernel: $roots\n$plan")
            }
          }
        }
      }
    }
  }

  test(
    "pure string predicates preserve native Filter ordering around a final unknown expression") {
    val rows = Seq(
      (Some("data/versioned/a"), Some("bucket1"), None: Option[String]),
      (Some("data/versioned/b/"), Some("bucket1"), None: Option[String]),
      (Some("data/versioned/c"), Some("bucket2"), None: Option[String]),
      (Some("data/versioned/d"), None, None: Option[String]),
      (Some("other/a"), Some("bucket1"), None: Option[String]),
      (Some("data/versioned/e"), Some("bucket1"), Some("deleted")),
      (None, Some("bucket1"), None: Option[String]))
    withParquetTable(rows, "string_filter_order") {
      val query = "SELECT _1 FROM string_filter_order WHERE _1 IS NOT NULL AND _3 IS NULL " +
        "AND NOT endswith(_1, '/') AND _1 LIKE 'data/versioned/%' " +
        "AND startswith(concat('gs://', _2, '/', _1), 'gs://bucket1/data/')"
      for (optional <- Seq(false, true)) {
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          CometConf.COMET_EXEC_FILTER_SHORT_CIRCUIT_JVM_DISPATCH_ENABLED.key -> optional.toString) {
          val plan = compare(query)
          val nativeFilters = collect(plan) { case p: CometFilterExec => p }
          assert(nativeFilters.nonEmpty, plan)
          assert(
            nativeFilters.exists(_.nativeOp.getFilter.getPredicate.hasCaseWhen),
            "The final unknown CONCAT path still needs a mandatory mask, even optional=false")
          assert(collect(plan) { case p: FilterExec => p }.isEmpty, plan)
        }
      }
    }
  }

  test("whole-conditional dispatch honors disabled nested expressions") {
    spark.udf.register(
      "conditional_count",
      udf(new ConditionalCountingFunction).asNondeterministic())
    withParquetTable(Seq(Tuple1(1), Tuple1(11)), "conditional_disabled") {
      withSQLConf(CometConf.getExprEnabledConfigKey("ScalaUDF") -> "false") {
        val plan = compare("SELECT COALESCE(conditional_count(_1), -7) FROM conditional_disabled")
        assert(collect(plan) { case p: CometProjectExec => p }.isEmpty, plan)
        assert(collect(plan) { case p: ProjectExec => p }.nonEmpty, plan)
      }
    }
  }
}
