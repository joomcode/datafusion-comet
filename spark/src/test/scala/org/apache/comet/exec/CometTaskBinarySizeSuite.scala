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

import scala.collection.mutable

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometTaskBinarySizeSuite extends CometTestBase {

  private case class StageBinary(name: String, bytes: Long)

  private def narrowLineage(rdd: RDD[_]): Seq[RDD[_]] = {
    val seen = mutable.LinkedHashMap.empty[Int, RDD[_]]
    def walk(r: RDD[_]): Unit = if (!seen.contains(r.id)) {
      seen(r.id) = r
      r.dependencies.foreach {
        case _: ShuffleDependency[_, _, _] =>
        case d => walk(d.rdd)
      }
    }
    walk(rdd)
    seen.values.toSeq
  }

  private def stageBinaries(df: DataFrame): Seq[StageBinary] = {
    if (spark.conf.get(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key).toBoolean) df.collect()
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val func = (_: TaskContext, it: Iterator[_]) => it.size
    val out = mutable.ArrayBuffer.empty[StageBinary]
    val visited = mutable.Set.empty[Int]
    def visit(rdd: RDD[_], dep: Option[ShuffleDependency[_, _, _]]): Unit = {
      val lineage = narrowLineage(rdd)
      lineage.foreach(_.partitions)
      val payload: AnyRef = dep match {
        case Some(d) => (rdd, d)
        case None => (rdd, func)
      }
      out += StageBinary(
        dep.map(d => s"shuffle ${d.shuffleId}").getOrElse("result"),
        serializer.serialize(payload).limit().toLong)
      lineage.flatMap(_.dependencies).foreach {
        case d: ShuffleDependency[_, _, _] if visited.add(d.shuffleId) => visit(d.rdd, Some(d))
        case _ =>
      }
    }
    visit(df.queryExecution.executedPlan.execute(), None)
    out.toSeq
  }

  private def compare(sql: String, aqe: Boolean)(
      check: (Seq[StageBinary], Seq[StageBinary]) => Unit): Unit = {
    def binaries(cometEnabled: Boolean): Seq[StageBinary] = {
      var result = Seq.empty[StageBinary]
      withSQLConf(
        CometConf.COMET_ENABLED.key -> cometEnabled.toString,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
        result = stageBinaries(spark.sql(sql))
      }
      result
    }
    val vanilla = binaries(cometEnabled = false)
    val comet = binaries(cometEnabled = true)
    withClue(s"aqe=$aqe vanilla=$vanilla comet=$comet") {
      check(vanilla, comet)
    }
  }

  private def writeInventory(dir: java.io.File, n: Int): Seq[String] =
    (0 until n).map { i =>
      val path = new java.io.File(dir, s"inventory$i").getCanonicalPath
      spark
        .range(50)
        .selectExpr(
          s"'bucket$i' AS bucket",
          "concat('data/versioned/', cast(id AS string)) AS name",
          "cast(id AS decimal(20, 0)) AS size",
          "IF(id % 5 = 0, timestamp'2026-01-01 00:00:00', NULL) AS timeDeleted")
        .coalesce(1)
        .write
        .parquet(path)
      path
    }

  private def inventoryUnion(paths: Seq[String]): String =
    paths
      .map { p =>
        s"SELECT concat('gs://', bucket, '/', name) AS path, size FROM parquet.`$p` " +
          "WHERE name IS NOT NULL AND timeDeleted IS NULL AND NOT endswith(name, '/') AND " +
          "name LIKE 'data/versioned/%' AND " +
          "startswith(concat('gs://', bucket, '/', name), 'gs://bucket1/data/')"
      }
      .mkString("SELECT path, count(*) AS c FROM (\n", "\nUNION ALL\n", "\n) GROUP BY path")

  test("a union of many scans carries the query text once per scan") {
    withTempDir { dir =>
      val scans = 60
      val sql = inventoryUnion(writeInventory(dir, scans))
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "200") {
        checkSparkAnswer(sql)
        Seq(false, true).foreach { aqe =>
          compare(sql, aqe) { (vanilla, comet) =>
            val bound = vanilla.map(_.bytes).max + scans * sql.length * 3 / 2
            comet.foreach(stage => assert(stage.bytes < bound, s"${stage.name} over $bound"))
          }
        }
      }
    }
  }

  test("a reduce stage does not carry the plan of the stage it reads") {
    withTempDir { dir =>
      val sql = inventoryUnion(writeInventory(dir, 30))
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "200") {
        compare(sql, aqe = false) { (vanilla, comet) =>
          val mapStage = comet.filter(_.name != "result").map(_.bytes).max
          val result = comet.find(_.name == "result").get.bytes
          val vanillaResult = vanilla.find(_.name == "result").get.bytes
          assert(result < mapStage / 2)
          assert(result < vanillaResult * 2)
        }
      }
    }
  }

  private def dbtLikeSql(events: String, dim: String, cols: Int): String = {
    val derived = (0 until cols)
      .map(i =>
        s"CASE WHEN c$i > ${i * 7} THEN c$i * ${i + 3} WHEN s${i % 4} LIKE 'x$i%' THEN " +
          s"length(s${i % 4}) ELSE coalesce(c${(i + 1) % cols}, 0) END AS d$i")
      .mkString(",\n  ")
    val aggs = (0 until cols).map(i => s"sum(d$i) AS sd$i, max(d$i) AS md$i").mkString(",\n  ")
    val outs = (0 until cols)
      .map(i => s"coalesce(sd$i, 0) + coalesce(md$i, 0) * dim.w AS o$i")
      .mkString(",\n  ")
    s"""WITH base AS (
       |  SELECT k, s0, $derived
       |  FROM parquet.`$events`
       |  WHERE c0 IS NOT NULL AND s1 NOT LIKE '%zzz%'
       |), agg AS (
       |  SELECT k, s0, $aggs
       |  FROM base GROUP BY k, s0
       |), joined AS (
       |  SELECT agg.k, agg.s0, $outs
       |  FROM agg JOIN parquet.`$dim` dim ON agg.k = dim.k
       |)
       |SELECT s0, ${(0 until cols).map(i => s"sum(o$i) AS t$i").mkString(", ")}
       |FROM joined GROUP BY s0""".stripMargin
  }

  test("stages of a multi-stage query do not grow with the query text") {
    withTempDir { dir =>
      val cols = 40
      val events = new java.io.File(dir, "events").getCanonicalPath
      spark
        .range(5000)
        .selectExpr(
          Seq("id % 97 AS k") ++ (0 until cols).map(i => s"(id * ${i + 1}) % 1013 AS c$i") ++
            (0 until 4).map(i => s"concat('x', cast(id % ${i + 5} AS string)) AS s$i"): _*)
        .repartition(4)
        .write
        .parquet(events)
      val dim = new java.io.File(dir, "dim").getCanonicalPath
      spark.range(97).selectExpr("id AS k", "id % 7 AS w").write.parquet(dim)
      val sql = dbtLikeSql(events, dim, cols)
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "50") {
        checkSparkAnswer(sql)
        Seq(false, true).foreach { aqe =>
          compare(sql, aqe) { (vanilla, comet) =>
            val bound = vanilla.map(_.bytes).max + 2 * sql.length
            comet.foreach(stage => assert(stage.bytes < bound, s"${stage.name} over $bound"))
          }
        }
      }
    }
  }
}
