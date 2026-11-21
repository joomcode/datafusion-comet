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

package org.apache.spark.shuffle.sort

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.comet.{CometShuffleMemoryAllocator, CometShuffleMemoryAllocatorTrait}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.comet.execution.shuffle.{NativeBatchDecoderIterator, SpillInfo => CometSpillInfo, SpillWriter}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock

import org.apache.comet.{CometConf, Native}
import org.apache.comet.vector.NativeUtil

/**
 * The sort-based JVM shuffle writer's memory is released when another consumer of the task, such
 * as a native aggregate below the writer, needs it.
 */
class CometShuffleExternalSorterSpillSuite extends CometTestBase {

  private val limit = 1024L * 1024
  private val pageSize = 4096L
  private val initialSize = 128
  private val records = 1000

  /** A consumer of the same task that cannot spill, like the native plan's. */
  private class OtherConsumer(tmm: TaskMemoryManager)
      extends MemoryConsumer(tmm, tmm.pageSizeBytes(), MemoryMode.OFF_HEAP) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  private def withSorter(
      body: (CometShuffleExternalSorter, TaskMemoryManager, TaskContext, () => Long) => Unit)
      : Unit = {
    withSQLConf(CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.key -> Int.MaxValue.toString) {
      val conf = new SparkConf(false)
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "10m")
      val memoryManager = new TestMemoryManager(conf)
      memoryManager.limit(limit)
      val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
      val allocator = CometShuffleMemoryAllocator.getInstance(taskMemoryManager, pageSize)
      val taskContext = TaskContext.empty()
      val sorter = new CometShuffleExternalSorter(
        allocator,
        SparkEnv.get.blockManager,
        taskContext,
        initialSize,
        2,
        conf,
        taskContext.taskMetrics.shuffleWriteMetrics,
        new StructType().add("id", IntegerType))
      try {
        body(sorter, taskMemoryManager, taskContext, () => allocator.getUsed)
      } finally {
        sorter.cleanupResources()
        taskMemoryManager.cleanUpAllAllocatedMemory()
      }
    }
  }

  private def insert(sorter: CometShuffleExternalSorter, value: Int): Unit = {
    val bytes = new Array[Byte](4 + 16)
    ByteBuffer.wrap(bytes).putInt(16)
    val row = new UnsafeRow(1)
    row.pointTo(bytes, (Platform.BYTE_ARRAY_OFFSET + 4).toLong, 16)
    row.setInt(0, value)
    sorter.insertRecord(bytes, Platform.BYTE_ARRAY_OFFSET.toLong, bytes.length, value % 2)
  }

  test("buffered records are spilled when another consumer of the task needs memory") {
    withSorter { (sorter, taskMemoryManager, taskContext, used) =>
      (0 until records).foreach(insert(sorter, _))
      val buffered = used()
      assert(buffered > initialSize * 8L)

      // More than is left in the task's share, as when a native aggregate starts below a writer
      // that has already buffered the task's share.
      val other = new OtherConsumer(taskMemoryManager)
      val required = limit - buffered + 1
      assert(other.acquireMemory(required) == required)
      assert(taskContext.taskMetrics.memoryBytesSpilled > 0)
      assert(used() == 0L)
      other.freeMemory(required)

      (records until 2 * records).foreach(insert(sorter, _))
      val spills = sorter.closeAndGetSpills()
      assert(spills.length == 2)
      assert(taskContext.taskMetrics.shuffleWriteMetrics.recordsWritten == 2L * records)
    }
  }

  test("buffered records are not spilled for a request made on another thread") {
    withSorter { (sorter, taskMemoryManager, taskContext, used) =>
      (0 until records).foreach(insert(sorter, _))
      val buffered = used()

      val other = new OtherConsumer(taskMemoryManager)
      val required = limit - buffered + 1
      var granted = -1L
      val thread = new Thread(() => granted = other.acquireMemory(required))
      thread.start()
      thread.join(10000L)
      assert(!thread.isAlive, "cross-thread allocation must not block")
      assert(granted == limit - buffered)
      assert(taskContext.taskMetrics.memoryBytesSpilled == 0)
      assert(used() == buffered)
      other.freeMemory(granted)

      val spills = sorter.closeAndGetSpills()
      assert(spills.length == 1)
      assert(taskContext.taskMetrics.shuffleWriteMetrics.recordsWritten == records)
    }
  }

  test("a closed sorter spills nothing for another consumer") {
    withSorter { (sorter, taskMemoryManager, taskContext, used) =>
      (0 until records).foreach(insert(sorter, _))
      sorter.closeAndGetSpills()
      val other = new OtherConsumer(taskMemoryManager)
      val available = limit - used()
      assert(other.acquireMemory(available + 1) == available)
      assert(taskContext.taskMetrics.memoryBytesSpilled == 0)
    }
  }

  /** Forwards pages to the ordinary allocator while observing allocation/callback boundaries. */
  private class ProbeAllocator(tmm: TaskMemoryManager, delegate: CometShuffleMemoryAllocatorTrait)
      extends CometShuffleMemoryAllocatorTrait(tmm, pageSize, MemoryMode.OFF_HEAP) {
    val other = new OtherConsumer(tmm)
    var inOwnerRequest = false
    var allocationsInOwnerRequest = 0
    var probeOnAllocation = false
    var busyProbes = 0
    var failArrays = 0
    var failMetadataArray = false
    var failPages = 0

    private def beforeAllocation(): Unit = {
      if (inOwnerRequest) allocationsInOwnerRequest += 1
      if (probeOnAllocation) {
        busyProbes += 1
        assert(spillOwnerFor(other) == 0L, "a busy insertion must not be reentered by spill")
      }
    }

    def requestOther(): Long = {
      inOwnerRequest = true
      try spill(Long.MaxValue, other)
      finally inOwnerRequest = false
    }

    override def spill(size: Long, trigger: MemoryConsumer): Long = spillOwnerFor(trigger)
    override def getUsed: Long = delegate.getUsed
    override def allocate(required: Long): MemoryBlock = {
      beforeAllocation()
      if (failPages > 0) {
        failPages -= 1
        throw new SparkOutOfMemoryError(
          "UNABLE_TO_ACQUIRE_MEMORY",
          Map("requestedBytes" -> required.toString, "receivedBytes" -> "0").asJava)
      }
      delegate.allocate(required)
    }
    override def allocateArray(size: Long): LongArray = {
      beforeAllocation()
      if (failArrays > 0 || (failMetadataArray && size == 1L)) {
        failArrays = math.max(0, failArrays - 1)
        failMetadataArray = false
        throw new SparkOutOfMemoryError(
          "UNABLE_TO_ACQUIRE_MEMORY",
          Map("requestedBytes" -> (size * 8L).toString, "receivedBytes" -> "0").asJava)
      }
      delegate.allocateArray(size)
    }
    override def freeArray(array: LongArray): Unit = delegate.freeArray(array)
    override def free(block: MemoryBlock): Long = delegate.free(block)
    override def getOffsetInPage(address: Long): Long = delegate.getOffsetInPage(address)
    override def encodePageNumberAndOffset(page: MemoryBlock, offset: Long): Long =
      delegate.encodePageNumberAndOffset(page, offset)
  }

  private def withProbe(
      body: (CometShuffleExternalSorter, ProbeAllocator, TaskContext) => Unit): Unit = {
    withSQLConf(CometConf.COMET_SHUFFLE_JVM_SPILL_THRESHOLD.key -> Int.MaxValue.toString) {
      val conf = new SparkConf(false)
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "10m")
      val manager = new TestMemoryManager(conf)
      manager.limit(limit)
      val tmm = new TaskMemoryManager(manager, 0)
      val allocator =
        new ProbeAllocator(tmm, CometShuffleMemoryAllocator.getInstance(tmm, pageSize))
      val context = TaskContext.empty()
      val sorter = new CometShuffleExternalSorter(
        allocator,
        SparkEnv.get.blockManager,
        context,
        initialSize,
        2,
        conf,
        context.taskMetrics.shuffleWriteMetrics,
        new StructType().add("id", IntegerType))
      try body(sorter, allocator, context)
      finally {
        sorter.cleanupResources()
        assert(allocator.getUsed == 0L)
        tmm.cleanUpAllAllocatedMemory()
      }
    }
  }

  private def assertDecodedIds(spills: Array[CometSpillInfo], count: Int): Unit = {
    val decoded = spills.flatMap { spill =>
      val bytes = Files.readAllBytes(spill.file.toPath)
      assert(spill.partitionLengths.sum == bytes.length)
      val starts = spill.partitionLengths.scanLeft(0L)(_ + _).map(_.toInt)
      (0 until 2).flatMap { partition =>
        val util = new NativeUtil
        val decoder = NativeBatchDecoderIterator(
          new ByteArrayInputStream(bytes.slice(starts(partition), starts(partition + 1))),
          new SQLMetric("decodeTime"),
          new Native,
          util,
          tracingEnabled = false)
        try {
          val ids =
            decoder.flatMap(batch => (0 until batch.numRows()).map(batch.column(0).getInt)).toList
          assert(ids.forall(_ % 2 == partition), "partition assignment must be preserved")
          ids
        } finally {
          decoder.close()
          util.close()
        }
      }
    }
    assert(decoded.sorted.toSeq == (0 until count), "every original id must occur exactly once")
  }

  test("external spill does not acquire task memory and resumes the pending record") {
    withProbe { (sorter, allocator, context) =>
      (0 until records).foreach(insert(sorter, _))
      val before = allocator.getUsed
      assert(before > 0)
      assert(allocator.spill(Long.MaxValue, allocator) == 0L)
      assert(allocator.getUsed == before)
      assert(allocator.requestOther() == before)
      assert(
        allocator.allocationsInOwnerRequest == 0,
        "Spark MemoryConsumer.spill forbids replacement pointer-array allocation")
      assert(allocator.getUsed == 0L)
      (records until 2 * records).foreach(insert(sorter, _))
      assertDecodedIds(sorter.closeAndGetSpills(), 2 * records)
      assert(context.taskMetrics.memoryBytesSpilled > 0)
      assert(context.taskMetrics.shuffleWriteMetrics.recordsWritten == 2L * records)
      assert(allocator.requestOther() == 0L)
      intercept[IllegalStateException] { insert(sorter, 2 * records) }
    }
  }

  test("an allocation inside insertion cannot reenter the owner spill callback") {
    withProbe { (sorter, allocator, context) =>
      (0 until records).foreach(insert(sorter, _))
      allocator.probeOnAllocation = true
      (records until records + 400).foreach(insert(sorter, _))
      assert(allocator.busyProbes > 0, "fixture must probe during actual page or pointer growth")
      assert(context.taskMetrics.memoryBytesSpilled == 0L)
      allocator.probeOnAllocation = false
      assertDecodedIds(sorter.closeAndGetSpills(), records + 400)
    }
  }

  test("pointer allocation refusal spills buffered rows without losing the pending record") {
    withProbe { (sorter, allocator, context) =>
      (0 until records).foreach(insert(sorter, _))
      allocator.failArrays = 1
      (records until records + 100).foreach(insert(sorter, _))
      assert(allocator.failArrays == 0, "fixture must reach pointer-array growth")
      assert(context.taskMetrics.memoryBytesSpilled > 0L)
      assertDecodedIds(sorter.closeAndGetSpills(), records + 100)
    }
  }

  test(
    "page allocation refusal regrows the lazy pointer array before inserting the pending row") {
    withProbe { (sorter, allocator, context) =>
      (0 until records).foreach(insert(sorter, _))
      allocator.failPages = 1
      (records until records + 400).foreach(insert(sorter, _))
      assert(allocator.failPages == 0, "fixture must reach a new data page")
      assert(context.taskMetrics.memoryBytesSpilled > 0L)
      assertDecodedIds(sorter.closeAndGetSpills(), records + 400)
    }
  }

  test("failure to restore an empty pointer array fails loudly after external spill") {
    withProbe { (sorter, allocator, _) =>
      (0 until records).foreach(insert(sorter, _))
      assert(allocator.requestOther() > 0L)
      allocator.failArrays = 1
      intercept[SparkOutOfMemoryError] { insert(sorter, records) }
      assertDecodedIds(sorter.closeAndGetSpills(), records)
    }
  }

  for (origin <- Seq("callback", "explicit", "final")) {
    test(s"failed $origin spill poisons later insert and close and cleans its partial file") {
      withProbe { (sorter, allocator, _) =>
        (0 until records).foreach(insert(sorter, _))
        val original = new IOException("injected partial shuffle write failure")
        var partial: java.nio.file.Path = null
        var nativeWrites = 0
        // Native is an ordinary virtual Scala class. Override only the final file write;
        // actual native pointer sorting still runs, and all original pages remain owned.
        val failing = new Native {
          override def writeSortedPartitionsNative(
              recordPointersAddr: Long,
              numRecords: Int,
              pageAddresses: Array[Long],
              uaoSize: Int,
              datatypes: Array[Array[Byte]],
              file: String,
              preferDictionaryRatio: Double,
              batchSize: Int,
              checksumAlgo: Int,
              partitionChecksums: Array[Long],
              partitionLengths: Array[Long],
              compressionCodec: String,
              compressionLevel: Int,
              offsetsAsLengths: Boolean,
              tracingEnabled: Boolean): Long = {
            nativeWrites += 1
            partial = java.nio.file.Paths.get(file)
            Files.write(partial, Array[Byte](1, 2, 3))
            if (partitionChecksums.nonEmpty) partitionChecksums(0) = 12345L
            partitionLengths(0) = 3L
            throw original
          }
        }
        val activeField =
          classOf[CometShuffleExternalSorter].getDeclaredField("activeSpillSorter")
        activeField.setAccessible(true)
        val active = activeField.get(sorter).asInstanceOf[SpillSorter]
        val nativeField = classOf[SpillWriter].getDeclaredField("nativeLib")
        nativeField.setAccessible(true)
        nativeField.set(active, failing)
        val first = intercept[IOException] {
          origin match {
            case "callback" => allocator.requestOther()
            case "explicit" => sorter.spill()
            case "final" => sorter.closeAndGetSpills()
          }
        }
        assert(first eq original)
        assert(nativeWrites == 1)
        assert(partial != null && Files.exists(partial))
        // Mimic a caller recovering from its allocation error. Once a write was partial,
        // later rows and a final close must never be accepted, even if memory is available.
        val afterInsert = intercept[IOException] { insert(sorter, records) }
        assert(afterInsert.getCause eq original)
        val afterClose = intercept[IOException] { sorter.closeAndGetSpills() }
        assert(afterClose.getCause eq original)
        assert(nativeWrites == 1, "the poisoned writer must not retry a partial checksum/file")
        assert(allocator.requestOther() == 0L)
        val ownerField = classOf[CometShuffleMemoryAllocatorTrait].getDeclaredField("ownerSpill")
        ownerField.setAccessible(true)
        assert(ownerField.get(allocator) == null, "the failed owner's callback must be detached")
        sorter.cleanupResources()
        assert(!Files.exists(partial))
        assert(allocator.getUsed == 0L)
      }
    }
  }

  test("lazy Spark sorter construction failure releases the supplied pointer array") {
    withProbe { (sorter, allocator, _) =>
      (0 until records).foreach(insert(sorter, _))
      assert(allocator.requestOther() > 0L)
      assert(allocator.getUsed == 0L)
      allocator.failMetadataArray = true
      intercept[SparkOutOfMemoryError] { insert(sorter, records) }
      assert(!allocator.failMetadataArray, "fixture must fail the Spark metadata constructor")
      assert(allocator.getUsed == 0L, "the unadopted caller array must be released")
      assertDecodedIds(sorter.closeAndGetSpills(), records)
    }
  }

}
