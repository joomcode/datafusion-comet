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

import java.util.IdentityHashMap

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometNativeScanExec, CometPlan, CometScanExec, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.shims.CometTypeShim

/**
 * The one place where the formats of stage boundaries (shuffles and broadcasts) are decided from
 * the engines on their two sides. [[ChooseBoundaryFormats]] applies it to the engines that
 * [[CometExecRule]] chose; [[CostBasedEngineChoice]] asks it for the cost of each engine choice
 * and then applies it to the engines it chose.
 *
 * Formats, from the engine of the producer below a boundary and of the consumer above it:
 *   - Comet producer: native shuffle, or Comet broadcast for a Comet join. A Spark consumer
 *     converts once, when it reads.
 *   - Spark producer, Comet consumer: Comet's JVM columnar shuffle, which converts once, when it
 *     writes.
 *   - Spark producer, Spark consumer: Spark's shuffle (the exchange's `originalPlan` over the
 *     same producer), tagged [[CometExecRule.SKIP_COMET_SHUFFLE_TAG]] so AQE's per-stage
 *     conversion keeps it, and no conversion at all.
 *   - Spark join: Spark broadcast, tagged [[CometExecRule.SKIP_COMET_BROADCAST_TAG]].
 *
 * Infeasible: a Comet consumer reading rows, a native shuffle or Comet broadcast over a Spark
 * producer, a Comet broadcast into a Spark join, or inputs of one stage hashed by both Comet and
 * Spark when their keys may hash differently (see [[modes]]).
 *
 * Boundaries whose format is fixed: materialized or reused query stages (`QueryStageExec`,
 * `ReusedExchangeExec`, `AQEShuffleReadExec`), any other exchange implementation, and a boundary
 * with no consumer in the plan (the root of a subquery or query stage, or an exchange directly
 * over another exchange). Their consumers still pay the conversion their fixed format implies.
 *
 * Range and round-robin shuffles, and single-partition ones, are never co-partitioned with
 * another input, so only their conversions count. `shuffleOrigin` and the advisory partition size
 * are carried over by rebuilding every format from the same Spark exchange, so AQE's coalescing,
 * skew handling and rebalancing see the same shuffle whatever its format.
 */
object BoundaryFormats extends Logging with CometTypeShim {

  sealed trait Engine
  object Engine {
    case object Comet extends Engine
    case object Spark extends Engine
    val all: Seq[Engine] = Seq(Comet, Spark)
  }

  sealed abstract class Format(val arrowOutput: Boolean)
  case object NativeShuffle extends Format(true)
  case object ColumnarShuffle extends Format(true)
  case object SparkShuffle extends Format(false)
  case object CometBroadcast extends Format(true)
  case object SparkBroadcast extends Format(false)

  /** The format of a boundary that no rule may change: a materialized or reused stage. */
  case class Fixed(override val arrowOutput: Boolean) extends Format(arrowOutput)

  /** Which hash function assigns rows to partitions. */
  sealed trait HashImpl
  case object CometHash extends HashImpl
  case object SparkHash extends HashImpl

  /**
   * How the hash-partitioned inputs of one stage must agree.
   *   - [[Unconstrained]]: they may mix hash functions.
   *   - [[Uniform]]: all of them use `hash`.
   *   - [[KeepCurrent]]: each keeps the hash function it has now.
   */
  sealed trait Mode
  case object Unconstrained extends Mode
  case class Uniform(hash: HashImpl) extends Mode
  case object KeepCurrent extends Mode

  /**
   * One boundary feeding a stage.
   *
   * @param consumer
   *   engine of the operator reading the boundary, `None` when no consumer is in the plan
   * @param producer
   *   engine of the operator below the boundary; ignored for fixed boundaries
   * @param producerPlan
   *   the operator below the boundary as it would be with engine `producer`, used to check that
   *   Comet's columnar shuffle can take it
   */
  case class Input(
      boundary: SparkPlan,
      consumer: Option[Engine],
      producer: Engine,
      producerPlan: SparkPlan)

  case class Choice(format: Format, conversions: Int)

  case class Decision(choices: Seq[Choice], mode: Mode) {
    def conversions: Int = choices.map(_.conversions).sum
  }

  // ---------------------------------------------------------------------------------------------
  // Plan structure
  // ---------------------------------------------------------------------------------------------

  def isBoundary(plan: SparkPlan): Boolean = plan match {
    case _: ShuffleExchangeLike | _: BroadcastExchangeLike | _: QueryStageExec |
        _: ReusedExchangeExec | _: AQEShuffleReadExec =>
      true
    case _ => false
  }

  /** A boundary whose format this object can decide: an exchange not yet materialized. */
  def isDecidable(plan: SparkPlan): Boolean = plan match {
    case _: CometShuffleExchangeExec | _: ShuffleExchangeExec | _: CometBroadcastExchangeExec |
        _: BroadcastExchangeExec =>
      true
    case _ => false
  }

  /** The engine whose batches or rows `plan` produces, looking through stage wrappers. */
  def engineOf(plan: SparkPlan): Engine = plan match {
    case stage: QueryStageExec => engineOf(stage.plan)
    case reused: ReusedExchangeExec => engineOf(reused.child)
    case read: AQEShuffleReadExec => engineOf(read.child)
    case _: CometPlan => Engine.Comet
    case _ => Engine.Spark
  }

  /**
   * The engine of an operator as the consumer of its inputs. A row-to-columnar transition reads
   * rows, whatever it produces.
   */
  def consumerEngineOf(plan: SparkPlan): Engine = plan match {
    case _: CometSparkToColumnarExec => Engine.Spark
    case other => engineOf(other)
  }

  def currentFormat(boundary: SparkPlan): Format = boundary match {
    case e: CometShuffleExchangeExec if e.shuffleType == CometNativeShuffle => NativeShuffle
    case e: CometShuffleExchangeExec if e.shuffleType == CometColumnarShuffle => ColumnarShuffle
    case _: ShuffleExchangeExec => SparkShuffle
    case _: CometBroadcastExchangeExec => CometBroadcast
    case _: BroadcastExchangeExec => SparkBroadcast
    case other => Fixed(engineOf(other) == Engine.Comet)
  }

  private def isBroadcast(boundary: SparkPlan): Boolean = boundary match {
    case _: BroadcastExchangeLike => true
    case stage: QueryStageExec => isBroadcast(stage.plan)
    case reused: ReusedExchangeExec => isBroadcast(reused.child)
    case _ => false
  }

  /** The exchange a boundary reads, looking through stage wrappers. */
  private def exchangeOf(boundary: SparkPlan): SparkPlan = boundary match {
    case stage: QueryStageExec => exchangeOf(stage.plan)
    case reused: ReusedExchangeExec => exchangeOf(reused.child)
    case read: AQEShuffleReadExec => exchangeOf(read.child)
    case other => other
  }

  /** The Spark shuffle a Comet shuffle was converted from, or the Spark shuffle itself. */
  private def sparkShuffleOf(boundary: SparkPlan): Option[ShuffleExchangeExec] = boundary match {
    case e: CometShuffleExchangeExec =>
      e.originalPlan match {
        case s: ShuffleExchangeExec => Some(s)
        case _ => None
      }
    case s: ShuffleExchangeExec => Some(s)
    case _ => None
  }

  // ---------------------------------------------------------------------------------------------
  // Co-partitioning
  // ---------------------------------------------------------------------------------------------

  /**
   * Key types that Comet's native Murmur3 hash and Spark's `Murmur3Hash` map to the same value,
   * so native-shuffled and Spark-shuffled inputs of one join still meet in the same partitions.
   * Source: the native hasher (`native/spark-expr/src/hash_funcs/utils.rs`) and
   * `CometHashExpressionSuite`, which checks Comet's `hash` against Spark's for each of these
   * types. Decimals with precision above 18 are excluded: Spark hashes the bytes of their
   * `BigInteger` unscaled value, the native hasher their 16 little-endian bytes (apache
   * datafusion-comet#6005). Timestamps without time zone, intervals, collated strings and nested
   * types are excluded because no test compares them.
   */
  def hashesAlike(dataType: DataType): Boolean = dataType match {
    case st: StringType => !isStringCollationType(st)
    case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
        _: FloatType | _: DoubleType | _: DateType | _: TimestampType | _: BinaryType =>
      true
    case d: DecimalType => d.precision <= 18
    case _ => false
  }

  /** A hash-partitioned input of a stage: its key types and, if known, its hash function. */
  private case class HashMember(keyTypes: Seq[DataType], fixedHash: Option[Option[HashImpl]])

  private def hashPartitioningOf(plan: SparkPlan): Option[HashPartitioning] =
    plan.outputPartitioning match {
      case h: HashPartitioning => Some(h)
      case _ => None
    }

  /** The hash function of the rows a fixed or current boundary delivers. */
  private def currentHash(boundary: SparkPlan): HashImpl = exchangeOf(boundary) match {
    case e: CometShuffleExchangeExec if e.shuffleType == CometNativeShuffle => CometHash
    case _ => SparkHash
  }

  private def hashMember(boundary: SparkPlan): Option[HashMember] = {
    if (isBroadcast(boundary)) return None
    val exchange = exchangeOf(boundary)
    hashPartitioningOf(exchange).map { h =>
      val fixed = if (isDecidable(boundary)) None else Some(Some(currentHash(boundary)))
      HashMember(h.expressions.map(_.dataType), fixed)
    }
  }

  /**
   * A hash-partitioned leaf inside a stage, such as a bucketed scan, is another input the stage
   * relies on being co-partitioned with its shuffles. Spark's bucketing uses Spark's hash; for
   * any other leaf the hash function is unknown.
   */
  private def leafMember(leaf: SparkPlan): Option[HashMember] =
    hashPartitioningOf(leaf).map { h =>
      val hash = leaf match {
        case _: FileSourceScanExec | _: CometScanExec | _: CometNativeScanExec => Some(SparkHash)
        case _ => None
      }
      HashMember(h.expressions.map(_.dataType), Some(hash))
    }

  /**
   * The ways the hash-partitioned inputs of one stage may be hashed. The whole stage is one
   * co-partitioned group: its sort-merge and shuffled hash joins, cogroups, and any operator
   * relying on a union of shuffles being co-partitioned read inputs from anywhere in the stage,
   * through aggregates and sorts that preserve partitioning. AQE coalescing and skew-join
   * splitting act on partition indexes, which stay aligned as long as the hash functions agree.
   * Hash functions may mix only when at most one input is hash-partitioned or every key type
   * hashes alike ([[hashesAlike]]).
   */
  def modes(boundaries: Seq[SparkPlan], leaves: Seq[SparkPlan]): Seq[Mode] = {
    val members = boundaries.flatMap(hashMember) ++ leaves.flatMap(leafMember)
    if (members.size <= 1 || members.forall(_.keyTypes.forall(hashesAlike))) {
      Seq(Unconstrained)
    } else if (members.exists(_.fixedHash.contains(None))) {
      Seq(KeepCurrent)
    } else {
      val fixed = members.flatMap(_.fixedHash.flatten.toSeq).toSet
      val uniform =
        Seq(Uniform(SparkHash), Uniform(CometHash)).filter(m => fixed.subsetOf(Set(m.hash)))
      if (uniform.isEmpty) {
        // Already materialized with both hash functions; nothing chosen now can change that.
        logWarning("Stage inputs were materialized with both Comet's and Spark's hash functions")
        Seq(KeepCurrent)
      } else {
        uniform
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The decision
  // ---------------------------------------------------------------------------------------------

  private def conversionsInto(consumer: Option[Engine], format: Format): Int =
    consumer match {
      case Some(Engine.Spark) if format.arrowOutput => 1
      case _ => 0
    }

  private def columnarAvailable(input: Input): Boolean = input.boundary match {
    case e: CometShuffleExchangeExec
        if e.shuffleType == CometColumnarShuffle && (input.producerPlan eq e.child) =>
      true
    case b =>
      sparkShuffleOf(b).exists { s =>
        CometShuffleExchangeExec.columnarShuffleAvailable(
          s.withNewChildren(Seq(input.producerPlan)).asInstanceOf[ShuffleExchangeExec])
      }
  }

  /** The formats `input` may take, with the conversions each implies and its hash function. */
  private def options(input: Input): Seq[(Format, Int, HashImpl)] = {
    val boundary = input.boundary
    val current = currentFormat(boundary)
    val producerIsComet = input.producer == Engine.Comet
    val consumerIsComet = input.consumer.contains(Engine.Comet)

    current match {
      case fixed: Fixed =>
        val readable = input.consumer match {
          case Some(Engine.Comet) => fixed.arrowOutput
          // A Spark join cannot read Comet's broadcast.
          case Some(Engine.Spark) => !(fixed.arrowOutput && isBroadcast(boundary))
          case None => true
        }
        if (readable) Seq((fixed, conversionsInto(input.consumer, fixed), currentHash(boundary)))
        else Nil

      case CometBroadcast | SparkBroadcast =>
        if (input.consumer.isEmpty) {
          // The consumer is outside the plan: keep the format, the producer must feed it.
          if (current == CometBroadcast) {
            if (producerIsComet) Seq((CometBroadcast, 0, SparkHash)) else Nil
          } else {
            Seq((SparkBroadcast, if (producerIsComet) 1 else 0, SparkHash))
          }
        } else if (consumerIsComet) {
          if (current == CometBroadcast && producerIsComet) Seq((CometBroadcast, 0, SparkHash))
          else Nil
        } else {
          Seq((SparkBroadcast, if (producerIsComet) 1 else 0, SparkHash))
        }

      case _ =>
        val candidates = mutable.ArrayBuffer.empty[(Format, Int, HashImpl)]
        val keep = input.consumer.isEmpty
        if (current == NativeShuffle && producerIsComet &&
          !CometShuffleExchangeExec.hasWideDecimalHashKey(
            exchangeOf(boundary).outputPartitioning)) {
          candidates += ((
            NativeShuffle,
            conversionsInto(input.consumer, NativeShuffle),
            CometHash))
        }
        if ((!keep || current == ColumnarShuffle) && current != SparkShuffle &&
          columnarAvailable(input)) {
          val write = if (producerIsComet) 2 else 1
          candidates += ((
            ColumnarShuffle,
            write + conversionsInto(input.consumer, ColumnarShuffle),
            SparkHash))
        }
        if ((!keep || current == SparkShuffle) && !consumerIsComet) {
          candidates += ((SparkShuffle, if (producerIsComet) 1 else 0, SparkHash))
        }
        candidates.toSeq
    }
  }

  /**
   * The cheapest format for `input` under `mode`, preferring its current format on a tie, or
   * `None` if no format is feasible.
   */
  def choose(input: Input, mode: Mode): Option[Choice] = {
    val current = currentFormat(input.boundary)
    val allowed = optionsUnder(input, mode)
    if (allowed.isEmpty) {
      None
    } else {
      val (format, conversions) =
        allowed.minBy { case (f, c) => (c, if (f == current) 0 else 1) }
      Some(Choice(format, conversions))
    }
  }

  /** The formats `input` may take under `mode`, with the conversions each implies. */
  private def optionsUnder(input: Input, mode: Mode): Seq[(Format, Int)] = {
    val isMember = hashMember(input.boundary).isDefined
    options(input).collect {
      case (format, conversions, hash) if !isMember || (mode match {
            case Unconstrained => true
            case Uniform(h) => hash == h
            case KeepCurrent => hash == currentHash(input.boundary)
          }) =>
        (format, conversions)
    }
  }

  /**
   * The formats of all boundaries feeding one stage, deciding the co-partitioned ones together,
   * or `None` if no choice of formats is feasible for these engines.
   */
  def decide(inputs: Seq[Input], stageModes: Seq[Mode]): Option[Decision] = {
    val candidates = stageModes.flatMap { mode =>
      val choices = inputs.map(choose(_, mode))
      if (choices.forall(_.isDefined)) Some(Decision(choices.flatten, mode)) else None
    }
    def changes(d: Decision): Int =
      inputs.zip(d.choices).count { case (i, c) => c.format != currentFormat(i.boundary) }
    if (candidates.isEmpty) None
    else Some(candidates.minBy(d => (d.conversions, changes(d))))
  }

  // ---------------------------------------------------------------------------------------------
  // Applying formats
  // ---------------------------------------------------------------------------------------------

  /** `boundary` in `format` over `producer`. */
  def applyFormat(boundary: SparkPlan, format: Format, producer: SparkPlan): SparkPlan = {
    val current = currentFormat(boundary)
    if (format == current || format.isInstanceOf[Fixed]) {
      if (boundary.children.headOption.exists(_ eq producer)) boundary
      else boundary.withNewChildren(Seq(producer))
    } else {
      format match {
        case SparkShuffle =>
          val spark = sparkShuffleOf(boundary).get.withNewChildren(Seq(producer))
          spark.setTagValue(CometExecRule.SKIP_COMET_SHUFFLE_TAG, ())
          withFallbackReason(spark, "Spark shuffle: no Comet operator reads or writes it")
        case ColumnarShuffle =>
          val spark = sparkShuffleOf(boundary).get
            .withNewChildren(Seq(producer))
            .asInstanceOf[ShuffleExchangeExec]
          val columnar = CometShuffleExchangeExec(spark, shuffleType = CometColumnarShuffle)
          spark.logicalLink.foreach(columnar.setLogicalLink)
          columnar
        case SparkBroadcast =>
          val broadcast = boundary.asInstanceOf[CometBroadcastExchangeExec]
          val spark = broadcast.originalPlan.withNewChildren(Seq(producer))
          spark.setTagValue(CometExecRule.SKIP_COMET_BROADCAST_TAG, ())
          withFallbackReason(spark, "Spark broadcast: a Spark join reads it")
        case other =>
          throw new IllegalStateException(
            s"Cannot change ${boundary.nodeName} from $current to $other")
      }
    }
  }

  /** The boundaries at the edge of the stage rooted at `root`, each with its consumer. */
  def stageInputs(root: SparkPlan): Seq[(SparkPlan, SparkPlan)] =
    root.children.flatMap { child =>
      if (isBoundary(child)) Seq((root, child)) else stageInputs(child)
    }

  /** The leaves inside the stage rooted at `root`. */
  def stageLeaves(root: SparkPlan): Seq[SparkPlan] =
    if (root.children.isEmpty) Seq(root)
    else root.children.filterNot(isBoundary).flatMap(stageLeaves)

  /**
   * Sets the format of every decidable boundary in `plan` from the engines of the operators on
   * its two sides, which it does not change. Identical exchanges, which Spark would reuse, get
   * one format when one format suits all of their consumers.
   */
  def applyFormats(plan: SparkPlan): SparkPlan = {
    val decided = new IdentityHashMap[SparkPlan, (Input, Mode, Format)]()

    def visitKept(boundary: SparkPlan): Unit = {
      if (isDecidable(boundary)) {
        val producer = boundary.children.head
        visitProducer(producer)
        val input = Input(boundary, None, engineOf(producer), producer)
        choose(input, Unconstrained).foreach(c =>
          decided.put(boundary, (input, Unconstrained, c.format)))
      }
    }

    def visitProducer(producer: SparkPlan): Unit =
      if (isBoundary(producer)) visitKept(producer) else visitStage(producer)

    def visitStage(root: SparkPlan): Unit = {
      val edges = stageInputs(root)
      edges.foreach { case (_, boundary) =>
        if (isDecidable(boundary)) visitProducer(boundary.children.head)
      }
      val inputs = edges.map { case (consumer, boundary) =>
        val producer = if (isDecidable(boundary)) boundary.children.head else boundary
        Input(boundary, Some(consumerEngineOf(consumer)), engineOf(producer), producer)
      }
      val stageModes = modes(edges.map(_._2), stageLeaves(root))
      decide(inputs, stageModes) match {
        case Some(decision) =>
          inputs.zip(decision.choices).foreach { case (input, choice) =>
            if (isDecidable(input.boundary)) {
              decided.put(input.boundary, (input, decision.mode, choice.format))
            }
          }
        case None =>
          logDebug(s"No feasible boundary formats for the stage at ${root.nodeName}; kept")
      }
    }

    if (isBoundary(plan)) visitKept(plan) else visitStage(plan)
    unifyReused(decided)

    def rebuild(node: SparkPlan): SparkPlan = {
      val children = node.children.map(rebuild)
      Option(decided.get(node)) match {
        case Some((_, _, format)) => applyFormat(node, format, children.head)
        case None =>
          if (children.zip(node.children).forall { case (a, b) => a eq b }) node
          else node.withNewChildren(children)
      }
    }
    rebuild(plan)
  }

  /**
   * Spark reuses identical exchanges, so two copies given different formats would both run. When
   * a single format is feasible for every copy, under each copy's consumer and its stage's hash
   * mode, use the cheapest such format for all of them.
   */
  private def unifyReused(decided: IdentityHashMap[SparkPlan, (Input, Mode, Format)]): Unit = {
    val entries = mutable.ArrayBuffer.empty[(SparkPlan, (Input, Mode, Format))]
    val it = decided.entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      entries += ((e.getKey, e.getValue))
    }
    entries.groupBy(_._1.canonicalized).values.foreach { group =>
      if (group.size > 1 && group.map(_._2._3).distinct.size > 1) {
        val perCopy = group.map { case (_, (input, mode, _)) =>
          optionsUnder(input, mode).map { case (f, c) => f -> c }.toMap
        }
        val common = perCopy.map(_.keySet).reduce(_ intersect _)
        if (common.nonEmpty) {
          val best = common.minBy(f => perCopy.map(_(f)).sum)
          group.foreach { case (boundary, (input, mode, _)) =>
            decided.put(boundary, (input, mode, best))
          }
        } else {
          logDebug(s"Copies of ${group.head._1.nodeName} need different formats; not reused")
        }
      }
    }
  }
}
