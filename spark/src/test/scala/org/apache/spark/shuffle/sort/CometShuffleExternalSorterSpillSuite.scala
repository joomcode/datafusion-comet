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

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.unsafe.Platform

import org.apache.comet.CometConf

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
    Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET, value)
    val row = new UnsafeRow(1)
    row.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET + 4, 16)
    row.setInt(0, value)
    sorter.insertRecord(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, value % 2)
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
      assert(used() == initialSize * 8L)
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
      thread.join()
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
}
