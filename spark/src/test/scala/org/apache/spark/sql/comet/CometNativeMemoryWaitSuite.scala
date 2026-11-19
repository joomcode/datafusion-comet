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

package org.apache.spark.sql.comet

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import org.apache.arrow.vector.BigIntVector
import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl, TaskKilledException, TaskMemoryTestUtils}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Descending, SortOrder}
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.internal.{ReadOnlySQLConf, SQLConf}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.{CometArrowAllocator, CometConf, CometExecIterator, Native}
import org.apache.comet.serde.OperatorOuterClass.{Operator, RangeScan, Scan, Sort}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.vector.CometPlainVector

/** Real JNI execution must stop even while its reservation waits inside Spark's memory pool. */
class CometNativeMemoryWaitSuite extends CometTestBase with TaskMemoryTestUtils {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "64m")

  for {
    (poolType, poolIndex) <- Seq("fair_unified", "greedy_unified").zipWithIndex
    (nativeProducer, pathIndex) <- Seq(true, false).zipWithIndex
  } {
    val path = if (nativeProducer) "native producer" else "JVM-fed execution"
    test(s"canceling $path blocked in Spark memory acquisition releases its $poolType plan") {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key -> poolType,
        CometConf.COMET_METRICS_UPDATE_INTERVAL.key -> "0") {
        assert(CometExecIterator.getMemoryConfig(spark.sparkContext.getConf).offHeapMode)
        checkKilledReservation(nativeProducer, 8900000L + poolIndex * 2 + pathIndex)
      }
    }
  }

  for ((returnBatch, index) <- Seq(true, false).zipWithIndex) {
    val result = if (returnBatch) "a batch" else "EOF"
    test(s"a task killed inside its JNI input cannot return $result successfully") {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        CometConf.COMET_METRICS_UPDATE_INTERVAL.key -> "0") {
        checkKilledNativeReturn(returnBatch, 8910000L + index)
      }
    }
  }

  private def checkKilledNativeReturn(returnBatch: Boolean, taskId: Long): Unit = {
    val pool = offHeapMemoryManager()
    val taskMemory = new TaskMemoryManager(pool, taskId)
    val context = newTaskContext(taskId, taskMemory)
    val previousContext = TaskContext.get()
    TaskContext.setTaskContext(context)
    val allocator =
      CometArrowAllocator.newChildAllocator("native-killed-result", 0, Long.MaxValue)
    val native = new Native()
    val before = native.getMemoryUsage()
    val armed = new AtomicBoolean(false)
    val killed = new AtomicBoolean(false)
    val callbackWasNative = new AtomicBoolean(false)
    var iterator: CometExecIterator = null
    try {
      val input = new Iterator[ColumnarBatch] {
        private var emitted = 0
        override def hasNext: Boolean = {
          if (armed.get() && killed.compareAndSet(false, true)) {
            callbackWasNative.set(
              Thread
                .currentThread()
                .getStackTrace
                .exists(frame =>
                  frame.getClassName == "org.apache.comet.Native" &&
                    frame.getMethodName == "executePlan"))
            context.markInterrupted("kill while the native scan polls its Arrow C stream")
          }
          emitted < (if (returnBatch) 2 else 1)
        }
        override def next(): ColumnarBatch = {
          val vector = new BigIntVector("id", allocator)
          vector.allocateNew(1)
          vector.set(0, emitted.toLong)
          vector.setValueCount(1)
          emitted += 1
          new ColumnarBatch(Array[ColumnVector](new CometPlainVector(vector)), 1)
        }
      }
      val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
      val stream = CometArrowStream.fromColumnarBatchIter(
        input,
        schema,
        CometArrowStream.NATIVE_TIMEZONE,
        "native-killed-result")
      // A root scan returns the input directly, without a sort/aggregate memory callback that
      // could detect cancellation before the JNI result crosses back into the JVM.
      iterator = new CometExecIterator(
        id = taskId + 2000L,
        inputObjects = Array(stream.asInstanceOf[Object]),
        numOutputCols = 1,
        protobufQueryPlan = scanPlan.toByteArray,
        nativeMetrics = CometMetricNode(Map.empty),
        numParts = 1,
        partitionIndex = 0)
      val manager = CometExecIterator.taskMemory(context, iterator.id).manager
      assert(iterator.hasNext)
      assert(iterator.next().getRow(0).getLong(0) == 0L)
      assert(!context.isInterrupted())
      // Schema reconciliation eagerly consumes the first input batch. Arm only after that
      // batch was executed, so the kill happens after getNextBatch's initial task check.
      armed.set(true)
      intercept[TaskKilledException](iterator.hasNext)
      assert(killed.get(), "the second JNI poll did not reach its input")
      assert(callbackWasNative.get(), "the task was not killed inside Native.executePlan")
      iterator.close()
      assert(manager.getUsed == 0L)
      assert(taskMemory.getMemoryConsumptionForThisTask == 0L)
      assert(pool.offHeapExecutionMemoryUsed == 0L)
      val after = native.getMemoryUsage()
      assert(after(3) == before(3), "the canceled result retained its native plan")
      assert(after(1) == before(1), "the canceled result retained native reservations")
      assert(allocator.getAllocatedMemory == 0L, "the canceled result retained Arrow buffers")
      iterator.close()
    } finally {
      try {
        if (iterator != null) iterator.close()
        context.markTaskCompleted(None)
        taskMemory.cleanUpAllAllocatedMemory()
        allocator.close()
      } finally {
        if (previousContext == null) TaskContext.unset()
        else TaskContext.setTaskContext(previousContext)
      }
    }
  }

  /**
   * A single descending native sort reserves memory, either from an independent RangeScan
   * producer or while the JNI caller feeds it an Arrow C stream. Both paths use the real Spark
   * execution pool, rather than a fake grant/refusal or a slow input as a proxy for memory wait.
   */
  private def checkKilledReservation(nativeProducer: Boolean, taskId: Long): Unit = {
    val expectedPoolType = CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.get()
    val pool = offHeapMemoryManager()
    val acquiringThread = new AtomicReference[Thread]()
    val taskMemory = new TaskMemoryManager(pool, taskId) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        acquiringThread.set(Thread.currentThread())
        super.acquireExecutionMemory(required, consumer)
      }
    }
    val otherTaskMemory = new TaskMemoryManager(pool, taskId + 1000L)
    val other = new OffHeapConsumer(otherTaskMemory)
    val context = newTaskContext(taskId, taskMemory)
    val previousContext = TaskContext.get()
    TaskContext.setTaskContext(context)
    val allocator = CometArrowAllocator.newChildAllocator("native-memory-wait", 0, Long.MaxValue)
    val native = new Native()
    val before = native.getMemoryUsage()
    var iterator: CometExecIterator = null
    var runner: Thread = null
    val returnedNormally = new AtomicBoolean(false)
    val failure = new AtomicReference[Throwable]()

    try {
      assert(SQLConf.get.isInstanceOf[ReadOnlySQLConf])
      assert(SQLConf.get.ansiEnabled)
      assert(context.getLocalProperty(SQLConf.ANSI_ENABLED.key) == "true")
      assert(
        context.getLocalProperty(
          CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key) == expectedPoolType)
      val memoryConfig = CometExecIterator.getMemoryConfig(spark.sparkContext.getConf)
      assert(memoryConfig.offHeapMode)
      assert(memoryConfig.memoryPoolType == expectedPoolType)
      val inputs = if (nativeProducer) {
        Array.empty[Object]
      } else {
        val vector = new BigIntVector("id", allocator)
        vector.allocateNew(128)
        (0 until 128).foreach(i => vector.set(i, i.toLong))
        vector.setValueCount(128)
        val batch = new ColumnarBatch(Array[ColumnVector](new CometPlainVector(vector)), 128)
        val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
        val stream = CometArrowStream.fromColumnarBatchIter(
          Iterator.single(batch),
          schema,
          CometArrowStream.NATIVE_TIMEZONE,
          "native-memory-wait")
        Array(stream.asInstanceOf[Object])
      }
      iterator = new CometExecIterator(
        id = taskId + 2000L,
        inputObjects = inputs,
        numOutputCols = 1,
        protobufQueryPlan = sortPlan(nativeProducer).toByteArray,
        nativeMetrics = CometMetricNode(Map.empty),
        numParts = 1,
        partitionIndex = 0)
      val manager = CometExecIterator.taskMemory(context, iterator.id).manager
      assert(native.getMemoryUsage()(3) == before(3) + 1, "the JNI plan was not created")
      assert(manager.getUsed == 0L, "construction unexpectedly reserved execution memory")
      // The competitor goes first, while it is the only task registered in this isolated pool.
      // Its 90 bytes leave the native task below Spark's minimum share and force a real wait.
      assert(other.acquireMemory(90L) == 90L)
      runner = new Thread(
        () => {
          try {
            iterator.hasNext
            // Both a batch and EOF would be an incorrect successful outcome after the kill.
            returnedNormally.set(true)
          } catch {
            case error: Throwable => failure.set(error)
          } finally {
            try iterator.close()
            catch {
              case closeError: Throwable =>
                val original = failure.get()
                if (original == null) failure.set(closeError)
                else original.addSuppressed(closeError)
            }
          }
        },
        s"memory-wait-consumer-$taskId")
      runner.setDaemon(true)
      runner.start()

      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TimeoutSeconds)
      while (Option(acquiringThread.get()).forall(t => !waitingInSpark(t)) &&
        runner.isAlive && System.nanoTime() < deadline) Thread.sleep(10)
      val waiting = acquiringThread.get()
      assert(waiting != null, s"native code did not request Spark memory: ${failure.get()}")
      assert(waitingInSpark(waiting), s"native acquire did not wait in Spark: ${failure.get()}")
      assert(
        waiting.getStackTrace.exists(_.getClassName == "org.apache.spark.CometTaskMemoryManager"),
        "the blocked reservation did not cross the real Comet JNI memory callback")
      assert(
        (waiting ne runner) == nativeProducer,
        s"the request did not use the expected execution path: ${waiting.getName}")
      assert(other.getUsed == 90L)

      // This marks only the task, exactly as interruptOnCancel=false does. The test never
      // interrupts the running plan or gives back the competing bytes to make cancellation work.
      val killedAt = System.nanoTime()
      context.markInterrupted("cancel native memory wait without interrupting its task thread")
      runner.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
      assert(!runner.isAlive, "the killed JNI plan/close waited for the other task's memory")
      val stopMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killedAt)
      assert(stopMs < 2000L, s"the killed memory reservation took $stopMs ms to stop")
      assert(!returnedNormally.get(), "a canceled reservation returned a batch or successful EOF")
      assert(
        Option(failure.get()).exists(error =>
          causeChain(error).exists(_.isInstanceOf[TaskKilledException])),
        s"the native plan did not report task cancellation: ${failure.get()}")
      assert(other.getUsed == 90L, "the competing allocation was released before cancellation")
      assert(otherTaskMemory.getMemoryConsumptionForThisTask == 90L)
      assert(manager.getUsed == 0L, "the native plan retained its Spark reservation")
      assert(taskMemory.getMemoryConsumptionForThisTask == 0L)
      assert(pool.offHeapExecutionMemoryUsed == 90L, "the isolated pool's accounting drifted")
      val after = native.getMemoryUsage()
      assert(after(3) == before(3), "releasePlan did not release the native execution context")
      assert(after(1) == before(1), "releasePlan did not return the native reservations")
      // Retrying close after cancellation must not release any block/native reservation twice.
      iterator.close()
      assert(manager.getUsed == 0L)
      assert(other.getUsed == 90L)
    } finally {
      // Also bounds the negative-control run: a broken cancellation path is unblocked only
      // after the failed assertion, so it cannot hang the Maven process or a later suite.
      other.freeMemory(other.getUsed)
      if (runner != null) {
        runner.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        if (runner.isAlive) {
          runner.interrupt()
          runner.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        }
      }
      try {
        if (runner == null || !runner.isAlive) {
          if (iterator != null) iterator.close()
          context.markTaskCompleted(None)
          taskMemory.cleanUpAllAllocatedMemory()
          otherTaskMemory.cleanUpAllAllocatedMemory()
          allocator.close()
        }
      } finally {
        if (previousContext == null) TaskContext.unset()
        else TaskContext.setTaskContext(previousContext)
      }
      assert(runner == null || !runner.isAlive, "the memory-wait worker outlived test cleanup")
    }
  }

  private def newTaskContext(taskId: Long, taskMemory: TaskMemoryManager): TaskContextImpl = {
    // Task-bound SQLConf reads the task properties, not the outer session's configuration.
    // Copy them before installing this synthetic context so both pool variants really run.
    val properties = new Properties
    SQLConf.get.getAllConfs.foreach { case (key, value) => properties.setProperty(key, value) }
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = taskId,
      attemptNumber = 0,
      taskMemoryManager = taskMemory,
      localProperties = properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)
  }

  private def sortPlan(nativeProducer: Boolean): Operator = {
    val input = if (nativeProducer) {
      Operator
        .newBuilder()
        .setRangeScan(
          RangeScan
            .newBuilder()
            .setStart(0L)
            .setStep(1L)
            .setNumElements(128L)
            .setNumSlices(1))
        .build()
    } else {
      scanPlan
    }
    val attribute = AttributeReference("id", LongType, nullable = false)()
    val descending = exprToProto(SortOrder(attribute, Descending), Seq(attribute)).get
    Operator
      .newBuilder()
      .addChildren(input)
      .setSort(Sort.newBuilder().addSortOrders(descending))
      .build()
  }

  private def scanPlan: Operator =
    Operator
      .newBuilder()
      .setScan(
        Scan
          .newBuilder()
          .setSource("MemoryWaitInput")
          .addFields(serializeDataType(LongType).get))
      .build()
}
