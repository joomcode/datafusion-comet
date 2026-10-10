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

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.comet.{CometFilterExec, CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometConf
import org.apache.comet.serde.ExprOuterClass.Expr

class CometFilterShortCircuitSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val backreference = "^(\\\\d)\\\\1"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.udf.register(
      "strict_even",
      (x: Int) => {
        if (x % 3 != 0) throw new IllegalStateException(s"strict_even evaluated on $x")
        x % 2 == 0
      })
    spark.udf.register("is_even", (x: Int) => x % 2 == 0)
  }

  private def withData(f: => Unit): Unit = {
    withTable("t") {
      sql("""CREATE TABLE t USING parquet AS
            |SELECT
            |  IF(id % 7 = 0, NULL, CAST(id AS INT)) AS x,
            |  IF(id % 5 = 0, NULL, id % 4 = 0) AS b,
            |  IF(id % 11 = 0, NULL, CAST(id * 37 % 1000 AS STRING)) AS s
            |FROM range(4000)""".stripMargin)
      f
    }
  }

  private def filterPredicates(df: DataFrame): Seq[Expr] =
    collect(df.queryExecution.executedPlan) { case f: CometFilterExec =>
      f.nativeOp.getFilter.getPredicate
    }

  private def caseWhenDepth(e: Expr): Int =
    if (e.hasCaseWhen) 1 + caseWhenDepth(e.getCaseWhen.getWhen(0)) else 0

  private def scanDataFilters(df: DataFrame): Seq[Expression] =
    collect(df.queryExecution.executedPlan) {
      case s: CometNativeScanExec => s.dataFilters
      case s: CometScanExec => s.dataFilters
    }.flatten

  private def checkRewritten(query: String, depth: Int = 1): Unit = {
    val df = sql(query)
    checkSparkAnswerAndOperator(df)
    val predicates = filterPredicates(df)
    assert(predicates.nonEmpty, df.queryExecution.executedPlan)
    assert(predicates.map(caseWhenDepth).max == depth, predicates)
  }

  private def checkNotRewritten(query: String): Unit = {
    val df = sql(query)
    checkSparkAnswer(df)
    assert(filterPredicates(df).forall(p => !p.hasCaseWhen), filterPredicates(df))
  }

  test("expensive conjunct is evaluated only on rows passing preceding conjuncts") {
    withData {
      checkRewritten("SELECT x FROM t WHERE x % 3 = 0 AND strict_even(x)")
      checkRewritten("SELECT x FROM t WHERE x > 10 AND x % 3 = 0 AND strict_even(x) AND x < 3000")
    }
  }

  test("disabled rewrite evaluates the expensive conjunct on every row") {
    withData {
      withSQLConf(CometConf.COMET_EXEC_FILTER_SHORT_CIRCUIT_JVM_DISPATCH_ENABLED.key -> "false") {
        val df = sql("SELECT x FROM t WHERE x % 3 = 0 AND strict_even(x)")
        assert(filterPredicates(df).forall(p => !p.hasCaseWhen))
        val e = intercept[Exception](df.collect())
        assert(
          e.toString.contains("strict_even evaluated on") ||
            Option(e.getCause).exists(_.toString.contains("strict_even evaluated on")))
      }
    }
  }

  test("three-valued logic matches Spark") {
    withData {
      checkRewritten("SELECT x, b, s FROM t WHERE b AND is_even(x)")
      checkRewritten("SELECT x, b, s FROM t WHERE NOT b AND is_even(x)")
      checkRewritten("SELECT x, b, s FROM t WHERE (b OR x IS NULL) AND is_even(x)")
      checkRewritten(s"SELECT x, b, s FROM t WHERE b AND s RLIKE '$backreference'")
      checkRewritten(s"SELECT x, b, s FROM t WHERE x % 3 = 0 AND NOT (s RLIKE '$backreference')")
      checkRewritten("SELECT x, b, s FROM t WHERE x % 2 = 0 AND (NOT is_even(x) OR b)")
    }
  }

  test("nested AND/OR and multiple expensive conjuncts") {
    withData {
      checkRewritten(
        s"SELECT x, s FROM t WHERE x % 3 = 0 AND (b OR s RLIKE '$backreference') AND x > 100")
      checkRewritten(
        s"SELECT x, s FROM t WHERE (x % 3 = 0 AND b) AND (is_even(x) AND s RLIKE '$backreference')",
        depth = 2)
      checkRewritten(
        s"SELECT x, s FROM t WHERE x % 3 = 0 AND strict_even(x) AND b AND s RLIKE '$backreference'",
        depth = 2)
      checkNotRewritten(s"SELECT x, s FROM t WHERE x % 3 = 0 OR s RLIKE '$backreference'")
      checkNotRewritten(
        s"SELECT x, s FROM t WHERE (x % 3 = 0 AND b) OR (is_even(x) AND s RLIKE '$backreference')")
    }
  }

  test("no rewrite without a preceding conjunct or JVM dispatch") {
    withData {
      checkNotRewritten("SELECT x FROM t WHERE is_even(x)")
      checkNotRewritten("SELECT x FROM t WHERE x % 3 = 0 AND b AND s IS NOT NULL")
      checkNotRewritten("SELECT x FROM t WHERE x % 3 = 0 AND s LIKE '1%'")
    }
  }

  test("nondeterministic condition is not rewritten") {
    withData {
      val df = sql(
        "SELECT x FROM t WHERE monotonically_increasing_id() % 5 < 4 AND x % 3 = 0 AND is_even(x)")
      assert(filterPredicates(df).nonEmpty, df.queryExecution.executedPlan)
      assert(
        filterPredicates(df).forall(p => !p.hasCaseWhen),
        df.queryExecution.optimizedPlan.toString + df.queryExecution.executedPlan)
      checkSparkAnswer(df)
    }
  }

  test("scan pushdown is unchanged") {
    withData {
      val query = "SELECT x FROM t WHERE x > 10 AND b AND is_even(x)"
      val rewritten = sql(query)
      var baselineFilters: Seq[Expression] = Nil
      withSQLConf(CometConf.COMET_EXEC_FILTER_SHORT_CIRCUIT_JVM_DISPATCH_ENABLED.key -> "false") {
        val df = sql(query)
        assert(filterPredicates(df).forall(p => !p.hasCaseWhen))
        baselineFilters = scanDataFilters(df)
      }
      assert(filterPredicates(rewritten).exists(_.hasCaseWhen))
      assert(scanDataFilters(rewritten).nonEmpty)
      assert(scanDataFilters(rewritten) == baselineFilters)
      checkSparkAnswerAndOperator(rewritten)
    }
  }
}
