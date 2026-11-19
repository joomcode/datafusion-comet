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

package org.apache.spark

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager, TestMemoryManager}

class CometTaskMemoryManagerSuite extends TaskMemoryTestUtils {

  test("native memory usage is visible to Spark's memory consumer") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 0L)

      assert(manager.acquireMemory(128L) == 128L)
      assert(manager.getUsed == 128L)
      assert(consumer.getUsed == 128L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 128L)

      manager.releaseMemory(128L)
      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("partial and zero grants account only for acquired memory") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      def checkUsage(expected: Long): Unit = {
        assert(manager.getUsed == expected)
        assert(consumer.getUsed == expected)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == expected)
      }

      assert(manager.acquireMemory(768L) == 768L)
      checkUsage(768L)
      assert(manager.acquireMemory(512L) == 256L)
      checkUsage(1024L)
      assert(manager.acquireMemory(128L) == 0L)
      checkUsage(1024L)

      manager.releaseMemory(256L)
      checkUsage(768L)
      assert(manager.acquireMemory(128L) == 128L)
      checkUsage(896L)
      manager.releaseMemory(896L)
      checkUsage(0L)
    }
  }

  test("managers in the same task retain separate memory accounting") {
    withTaskMemoryManager { taskMemoryManager =>
      val first = new CometTaskMemoryManager(1L, 0L)
      val second = new CometTaskMemoryManager(2L, 0L)
      val firstConsumer = nativeMemoryConsumer(first)
      val secondConsumer = nativeMemoryConsumer(second)

      def checkUsage(firstBytes: Long, secondBytes: Long): Unit = {
        assert(first.getUsed == firstBytes)
        assert(firstConsumer.getUsed == firstBytes)
        assert(second.getUsed == secondBytes)
        assert(secondConsumer.getUsed == secondBytes)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == firstBytes + secondBytes)
      }

      checkUsage(0L, 0L)
      assert(first.acquireMemory(128L) == 128L)
      checkUsage(128L, 0L)
      assert(second.acquireMemory(256L) == 256L)
      checkUsage(128L, 256L)
      first.releaseMemory(64L)
      checkUsage(64L, 256L)
      second.releaseMemory(256L)
      checkUsage(64L, 0L)
      first.releaseMemory(64L)
      checkUsage(0L, 0L)
    }
  }

  test("partial and zero grants log nothing at INFO or above") {
    // Spark refuses native reservations routinely under memory pressure, each time with one of
    // these.
    withTaskMemoryManager { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val events = logEvents(Level.INFO) {
        assert(manager.acquireMemory(768L) == 768L)
        assert(manager.acquireMemory(512L) == 256L)
        assert(manager.acquireMemory(128L) == 0L)
      }
      assert(events.isEmpty, events.map(_.getMessage.getFormattedMessage).mkString("\n"))
      manager.releaseMemory(1024L)
    }
  }

  test("a partial grant is logged at DEBUG, without the task's memory usage dump") {
    withTaskMemoryManager { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val messages = logEvents(Level.DEBUG) {
        assert(manager.acquireMemory(768L) == 768L)
        assert(manager.acquireMemory(512L) == 256L)
      }.map(_.getMessage.getFormattedMessage)
      val log = messages.mkString("\n")
      assert(messages.exists(_.contains("requested 512 bytes but only received 256 bytes")), log)
      // TaskMemoryManager.showMemoryUsage takes the task's monitor; see acquireMemory.
      assert(!messages.exists(_.contains("Memory used in task")), log)
      manager.releaseMemory(1024L)
    }
  }

  test("an acquire waiting in Spark survives a release that empties the task's balance") {
    checkRequestWhileBalanceEmpties(loggers) { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      // Another native thread releases the task's last 10 bytes while the acquire waits.
      WaitingRequest(
        manager.acquireMemory(_),
        manager.releaseMemory(_),
        () => manager.acquireMemory(20L),
        granted => {
          assert(granted == 20L)
          assert(manager.getUsed == 20L)
          assert(taskMemoryManager.getMemoryConsumptionForThisTask == 20L)
          manager.releaseMemory(20L)
          assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        })
    }
  }

  for ((state, end) <- Seq[(String, TaskContextImpl => Unit)](
      ("completed", _.markTaskCompleted(None)),
      ("was killed", _.markInterrupted("killed")))) {
    test(s"an acquire that loses its task entry after the task $state is not made again") {
      val memoryManager = offHeapMemoryManager()
      val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))

      withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
        val manager = new CometTaskMemoryManager(1L, 0L)
        assert(otherTask.acquireMemory(90L) == 90L)
        assert(manager.acquireMemory(10L) == 10L)

        val granted = new AtomicReference[java.lang.Long]()
        val failure = new AtomicReference[Throwable]()
        val acquire = new Thread(() =>
          try granted.set(manager.acquireMemory(20L))
          catch { case t: Throwable => failure.set(t) })
        acquire.setDaemon(true)
        // Spark's executor frees what the task still holds once the task has ended, under the
        // task memory manager's monitor, which a parked acquire holds.
        val cleanUp = new Thread(() => taskMemoryManager.cleanUpAllAllocatedMemory())
        cleanUp.setDaemon(true)

        try {
          acquire.start()
          awaitWaitingInSpark(acquire)
          // The task ends and releasing its plan gives back the task's last bytes while a native
          // thread still waits in Spark, as releasePlan does when it drops a plan's stream.
          end(TaskContext.get().asInstanceOf[TaskContextImpl])
          manager.releaseMemory(10L)
          acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
          // Parked again, the acquire would wait for the other task while holding the monitor.
          assert(!acquire.isAlive, s"the acquire is ${acquire.getState} after the task $state")
          cleanUp.start()
          cleanUp.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
          assert(!cleanUp.isAlive, s"the clean up is ${cleanUp.getState}")
        } finally {
          // Free the other task's memory so that neither thread outlives a failed test.
          otherTask.freeMemory(otherTask.getUsed)
          acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
          cleanUp.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        }

        assert(failure.get == null, s"the acquire failed: ${failure.get}")
        assert(granted.get == 0L)
        assert(manager.getUsed == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      }
    }
  }

  test(
    "cancelling a real Spark memory wait leaves competing memory held and its thread reusable") {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))
    withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
      val context = TaskContext.get().asInstanceOf[TaskContextImpl]
      val manager = new CometTaskMemoryManager(1L, 0L)
      assert(otherTask.acquireMemory(90L) == 90L)
      assert(manager.acquireMemory(10L) == 10L)
      val granted = new AtomicReference[java.lang.Long]()
      val failure = new AtomicReference[Throwable]()
      val reusedThreadFailure = new AtomicReference[Throwable]()
      val interruptedAfterCancel = new AtomicBoolean(true)
      val reusedGrant = new AtomicReference[java.lang.Long]()
      val acquire = daemonThread {
        try granted.set(manager.acquireMemory(20L))
        catch { case t: Throwable => failure.set(t) }
        interruptedAfterCancel.set(Thread.currentThread().isInterrupted)
        try {
          // A Tokio worker can serve another task after the cancelled JNI call returns.
          // Exercise the same physical thread with a fresh, healthy task context.
          withTaskMemoryManager { freshTask =>
            val healthy = new CometTaskMemoryManager(2L, 0L)
            reusedGrant.set(healthy.acquireMemory(128L))
            healthy.releaseMemory(128L)
            assert(healthy.getUsed == 0L)
            assert(freshTask.getMemoryConsumptionForThisTask == 0L)
          }
        } catch { case t: Throwable => reusedThreadFailure.set(t) }
      }
      try {
        acquire.start()
        awaitWaitingInSpark(acquire)
        context.markInterrupted("cancelled while acquiring Spark memory")
        manager.cancelPendingAcquires()
        manager.cancelPendingAcquires()
        awaitFinished(acquire)
        assert(failure.get.isInstanceOf[InterruptedException], s"failure: ${failure.get}")
        assert(granted.get == null)
        assert(!interruptedAfterCancel.get, "the cancelled acquisition leaked its interrupt")
        assert(
          reusedThreadFailure.get == null,
          s"reused thread failed: ${reusedThreadFailure.get}")
        assert(reusedGrant.get == 128L)
        assert(otherTask.getUsed == 90L, "the competitor must not unblock the assertion")
        assert(manager.getUsed == 10L)
        manager.releaseMemory(10L)
        assert(manager.getUsed == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
        assert(memoryManager.offHeapExecutionMemoryUsed == 90L)
      } finally {
        otherTask.freeMemory(otherTask.getUsed)
        acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        if (manager.getUsed > 0L) manager.releaseMemory(manager.getUsed)
      }
    }
  }

  test("an acquire started after task cancellation never enters Spark") {
    val calls = new AtomicInteger()
    val taskMemoryManager = new TaskMemoryManager(offHeapMemoryManager(), 0L) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        calls.incrementAndGet()
        super.acquireExecutionMemory(required, consumer)
      }
    }
    withTaskContext(taskMemoryManager) { _ =>
      val context = TaskContext.get().asInstanceOf[TaskContextImpl]
      val manager = new CometTaskMemoryManager(1L, 0L)
      context.markInterrupted("cancelled before acquiring")
      manager.cancelPendingAcquires()
      intercept[TaskKilledException](manager.acquireMemory(20L))
      assert(calls.get == 0)
      assert(manager.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("a full Spark grant racing with cancellation remains accounted until released") {
    val grantedBySpark = new CountDownLatch(1)
    val returnGrant = new CountDownLatch(1)
    val memoryManager = offHeapMemoryManager()
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        val grant = super.acquireExecutionMemory(required, consumer)
        grantedBySpark.countDown()
        // Hold the completed Spark grant at the boundary before Comet receives it. Ignore only
        // this test barrier's interruption, restoring it before returning to the production code.
        awaitUninterruptibly(returnGrant)
        grant
      }
    }
    withTaskContext(taskMemoryManager) { _ =>
      val context = TaskContext.get().asInstanceOf[TaskContextImpl]
      val manager = new CometTaskMemoryManager(1L, 0L)
      val failure = new AtomicReference[Throwable]()
      val returned = new AtomicReference[java.lang.Long]()
      val interruptedAfter = new AtomicBoolean(true)
      val acquire = daemonThread {
        try returned.set(manager.acquireMemory(40L))
        catch { case t: Throwable => failure.set(t) }
        interruptedAfter.set(Thread.currentThread().isInterrupted)
      }
      try {
        acquire.start()
        assert(grantedBySpark.await(TimeoutSeconds, TimeUnit.SECONDS))
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 40L)
        context.markInterrupted("cancelled after Spark granted memory")
        manager.cancelPendingAcquires()
        returnGrant.countDown()
        awaitFinished(acquire)
        // The acquire already succeeded inside Spark. Cancellation must not discard that grant
        // before the native reservation receives it and later returns it during plan cleanup.
        // The outer native-execution cancellation check is responsible for failing the query.
        assert(failure.get == null, s"failure: ${failure.get}")
        assert(returned.get == 40L)
        assert(!interruptedAfter.get)
        assert(manager.getUsed == 40L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 40L)
        manager.releaseMemory(40L)
        assert(manager.getUsed == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
        assert(memoryManager.offHeapExecutionMemoryUsed == 0L)
      } finally {
        returnGrant.countDown()
        acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
      }
    }
  }

  test("an interrupted sibling spill leaves a partial Spark grant to normal task cleanup") {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))
    val enteredSpill = new CountDownLatch(1)
    val releaseSpill = new CountDownLatch(1)
    withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
      val context = TaskContext.get().asInstanceOf[TaskContextImpl]
      val sibling = new OffHeapConsumer(taskMemoryManager) {
        override def spill(size: Long, trigger: MemoryConsumer): Long = {
          enteredSpill.countDown()
          assert(releaseSpill.await(TimeoutSeconds, TimeUnit.SECONDS))
          0L
        }
      }
      assert(sibling.acquireMemory(30L) == 30L)
      assert(otherTask.acquireMemory(50L) == 50L)
      val manager = new CometTaskMemoryManager(1L, 0L)
      val failure = new AtomicReference[Throwable]()
      val returned = new AtomicReference[java.lang.Long]()
      val acquire = daemonThread {
        try returned.set(manager.acquireMemory(40L))
        catch { case t: Throwable => failure.set(t) }
      }
      try {
        acquire.start()
        assert(enteredSpill.await(TimeoutSeconds, TimeUnit.SECONDS))
        // Spark has granted 20 bytes in addition to the sibling's 30, but the call cannot
        // return that partial grant while its spill callback is running.
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 50L)
        context.markInterrupted("cancelled in sibling spill")
        manager.cancelPendingAcquires()
        awaitFinished(acquire)
        assert(failure.get.isInstanceOf[InterruptedException], s"failure: ${failure.get}")
        assert(returned.get == null)
        assert(manager.getUsed == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 50L)
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 50L)
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        assert(otherTask.getUsed == 50L)
        assert(memoryManager.offHeapExecutionMemoryUsed == 50L)
      } finally {
        releaseSpill.countDown()
        otherTask.freeMemory(otherTask.getUsed)
        acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
      }
    }
  }

  test("nested acquisitions keep the blocked thread registered until the outer acquire exits") {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))
    var manager: CometTaskMemoryManager = null
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        if (required == 20L) {
          // Finish the inner acquire successfully, then block the still-active outer scope.
          // Removing the thread when the inner scope exits would make cancellation miss it.
          assert(manager.acquireMemory(10L) == 10L)
          super.acquireExecutionMemory(30L, consumer)
        } else {
          super.acquireExecutionMemory(required, consumer)
        }
      }
    }
    withTaskContext(taskMemoryManager) { _ =>
      val context = TaskContext.get().asInstanceOf[TaskContextImpl]
      manager = new CometTaskMemoryManager(1L, 0L)
      assert(otherTask.acquireMemory(90L) == 90L)
      val failure = new AtomicReference[Throwable]()
      val returned = new AtomicReference[java.lang.Long]()
      val interruptedAfter = new AtomicBoolean(true)
      val acquire = daemonThread {
        try returned.set(manager.acquireMemory(20L))
        catch { case t: Throwable => failure.set(t) }
        interruptedAfter.set(Thread.currentThread().isInterrupted)
      }
      try {
        acquire.start()
        awaitWaitingInSpark(acquire)
        assert(manager.getUsed == 10L)
        context.markInterrupted("cancelled in nested acquire")
        manager.cancelPendingAcquires()
        awaitFinished(acquire)
        assert(failure.get.isInstanceOf[InterruptedException], s"failure: ${failure.get}")
        assert(returned.get == null)
        assert(!interruptedAfter.get)
        assert(manager.getUsed == 10L)
        manager.releaseMemory(10L)
        assert(manager.getUsed == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        assert(otherTask.getUsed == 90L)
      } finally {
        otherTask.freeMemory(otherTask.getUsed)
        acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        if (manager.getUsed > 0L) manager.releaseMemory(manager.getUsed)
      }
    }
  }

  test("a pre-existing interrupt is preserved after an immediate successful acquisition") {
    val failure = new AtomicReference[Throwable]()
    val interruptedAfter = new AtomicBoolean(false)
    val acquire = daemonThread {
      try {
        withTaskMemoryManager { taskMemoryManager =>
          val manager = new CometTaskMemoryManager(1L, 0L)
          Thread.currentThread().interrupt()
          assert(manager.acquireMemory(128L) == 128L)
          interruptedAfter.set(Thread.currentThread().isInterrupted)
          manager.releaseMemory(128L)
          assert(manager.getUsed == 0L)
          assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        }
      } catch { case t: Throwable => failure.set(t) }
      finally {
        val _ = Thread.interrupted()
      }
    }
    acquire.start()
    awaitFinished(acquire)
    assert(failure.get == null, s"failure: ${failure.get}")
    assert(
      interruptedAfter.get,
      "an acquisition must not erase its caller's pre-existing interrupt")
  }

  test("a NoSuchElementException other than Spark's missing task entry is rethrown") {
    val wrongTask = noSuchElement("key not found: 7", fromExecutionMemoryPool = true)
    val notFromPool = noSuchElement("key not found: 0", fromExecutionMemoryPool = false)
    for (error <- Seq(wrongTask, notFromPool)) {
      val taskMemoryManager = new FailingTaskMemoryManager(() => error, failures = Int.MaxValue)
      withTaskContext(taskMemoryManager) { _ =>
        val manager = new CometTaskMemoryManager(1L, 0L)
        val thrown = intercept[NoSuchElementException](manager.acquireMemory(10L))
        assert(thrown eq error)
        assert(taskMemoryManager.calls.get == 1)
        assert(manager.getUsed == 0L)
      }
    }
  }

  test("an acquire that keeps losing its task entry gives up with a zero grant") {
    // A new exception on every call, so the test can tell which one is logged.
    val taskMemoryManager = new FailingTaskMemoryManager(
      () => noSuchElement("key not found: 0", fromExecutionMemoryPool = true),
      failures = Int.MaxValue)
    withTaskContext(taskMemoryManager) { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val events = logEvents(Level.INFO) {
        // A refusal rather than an exception, so native spills as for any refused reservation.
        assert(manager.acquireMemory(10L) == 0L)
      }
      assert(events.map(_.getLevel) == Seq(Level.INFO, Level.INFO, Level.WARN), messages(events))
      assert(events.last.getThrown eq taskMemoryManager.errors.last)
      assert(taskMemoryManager.calls.get == MaxAcquireAttempts)
      assert(manager.getUsed == 0L)
      assert(nativeMemoryConsumer(manager).getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  private val loggers =
    Seq(classOf[CometTaskMemoryManager].getName, classOf[TaskMemoryManager].getName)

  /** The events that Comet's and Spark's task memory managers log at `level` or above. */
  private def logEvents(level: Level)(f: => Unit): Seq[LogEvent] = logEvents(level, loggers)(f)

  private def daemonThread(f: => Unit): Thread = {
    val thread = new Thread(() => f)
    thread.setDaemon(true)
    thread
  }

  private def awaitFinished(thread: Thread): Unit = {
    thread.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
    assert(!thread.isAlive, s"the acquisition is still ${thread.getState}")
  }

  private def awaitUninterruptibly(latch: CountDownLatch): Unit = {
    var interrupted = false
    var finished = false
    try {
      while (!finished) {
        try {
          assert(latch.await(TimeoutSeconds, TimeUnit.SECONDS))
          finished = true
        } catch {
          case _: InterruptedException => interrupted = true
        }
      }
    } finally {
      if (interrupted) Thread.currentThread().interrupt()
    }
  }

  private def withTaskMemoryManager(f: TaskMemoryManager => Unit): Unit = {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    withTaskContext(new TaskMemoryManager(memoryManager, 0L))(f)
  }

  private def nativeMemoryConsumer(manager: CometTaskMemoryManager): MemoryConsumer = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("nativeMemoryConsumer")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[MemoryConsumer]
  }
}
