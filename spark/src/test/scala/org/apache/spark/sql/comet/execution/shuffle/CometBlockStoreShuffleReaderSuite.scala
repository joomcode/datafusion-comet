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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.{ByteArrayInputStream, InputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.network.buffer.NettyManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockFetcherIterator, ShuffleBlockId}

import io.netty.buffer.{ByteBuf, Unpooled}

class CometBlockStoreShuffleReaderSuite extends CometTestBase {

  private val blockSize = 64
  private val remote = BlockManagerId("remote-exec", "remote-host", 7337)
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private class RemoteBlocks(delivered: Int => Boolean) extends BlockStoreClient {
    val buffers = new ConcurrentLinkedQueue[ByteBuf]()

    override def fetchBlocks(
        host: String,
        port: Int,
        execId: String,
        blockIds: Array[String],
        listener: BlockFetchingListener,
        downloadFileManager: DownloadFileManager): Unit = {
      blockIds.foreach { id =>
        val index = BlockId(id).asInstanceOf[ShuffleBlockId].mapId.toInt
        if (delivered(index)) {
          val buf = Unpooled.wrappedBuffer(Array.fill(blockSize)(index.toByte))
          buffers.add(buf)
          listener.onBlockFetchSuccess(id, new NettyManagedBuffer(buf))
          buf.release()
        }
      }
    }

    override def close(): Unit = {}

    def leaked: Seq[Int] = buffers.asScala.map(_.refCnt()).filter(_ != 0).toSeq
  }

  private def blockStream(
      context: TaskContext,
      client: BlockStoreClient,
      blocks: Int,
      beforeNextBlock: Int => Unit = _ => ()): InputStream = {
    val fetcher = new ShuffleBlockFetcherIterator(
      context,
      client,
      SparkEnv.get.blockManager,
      SparkEnv.get.mapOutputTracker,
      Iterator(
        (
          remote,
          (0 until blocks).map(i =>
            (ShuffleBlockId(0, i.toLong, 0): BlockId, blockSize.toLong, i)))),
      (_, in) => in,
      48L * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Long.MaxValue,
      10,
      false,
      false,
      false,
      "ADLER32",
      context.taskMetrics().createTempShuffleReadMetrics(),
      false)
    val fetched = fetcher.toCompletionIterator
    var nextBlock = 0
    val streams = new Iterator[InputStream] {
      override def hasNext: Boolean = fetched.hasNext

      override def next(): InputStream = {
        beforeNextBlock(nextBlock)
        nextBlock += 1
        fetched.next()._2
      }
    }
    CometBlockStoreShuffleReader.concatenate(context, streams)
  }

  private def readFully(in: InputStream, bytes: Int): Array[Byte] = {
    val out = new Array[Byte](bytes)
    var read = 0
    while (read < bytes) {
      val n = in.read(out, read, bytes - read)
      assert(n > 0)
      read += n
    }
    out
  }

  private def within[T](f: => T): T = Await.result(Future(f), 20.seconds)

  test("closing an unread stream never asks the fetcher for a block") {
    val context = TaskContext.empty()
    val blocks = new Iterator[InputStream] {
      override def hasNext: Boolean = fail("close must not ask whether another block exists")
      override def next(): InputStream = fail("close must not fetch a block")
    }
    val stream = CometBlockStoreShuffleReader.concatenate(context, blocks)
    stream.close()
    stream.close()
    intercept[IOException](stream.read())
  }

  test("each completed block closes once before the next block is fetched") {
    val context = TaskContext.empty()
    val closes = Array(0, 0, 0)
    var fetched = 0
    val blocks = new Iterator[InputStream] {
      override def hasNext: Boolean = fetched < closes.length

      override def next(): InputStream = {
        val index = fetched
        if (index > 0) assert(closes(index - 1) == 1)
        fetched += 1
        new ByteArrayInputStream(Array(index.toByte)) {
          override def close(): Unit = {
            closes(index) += 1
            assert(closes(index) == 1)
          }
        }
      }
    }
    val stream = CometBlockStoreShuffleReader.concatenate(context, blocks)
    assert(readFully(stream, 3).toSeq == Seq[Byte](0, 1, 2))
    assert(stream.read() == -1)
    stream.close()
    stream.close()
    assert(closes.toSeq == Seq(1, 1, 1))
  }

  test("a failed block close cannot be followed by fetching another block") {
    val context = TaskContext.empty()
    var fetched = 0
    val closeFailure = new IOException("block close failed")
    val blocks = Iterator.continually {
      fetched += 1
      new ByteArrayInputStream(Array[Byte](1)) {
        override def close(): Unit = throw closeFailure
      }
    }
    val stream = CometBlockStoreShuffleReader.concatenate(context, blocks)
    assert(stream.read() == 1)
    assert(intercept[IOException](stream.read()) eq closeFailure)
    intercept[IOException](stream.read())
    stream.close()
    assert(fetched == 1)
  }

  test("empty and invalid bulk reads do not fetch or consume a block") {
    val context = TaskContext.empty()
    var fetched = 0
    val blocks = Iterator.continually {
      fetched += 1
      new ByteArrayInputStream(Array[Byte](1, 2, 3))
    }
    val stream = CometBlockStoreShuffleReader.concatenate(context, blocks)
    assert(stream.read(new Array[Byte](2), 1, 0) == 0)
    intercept[IndexOutOfBoundsException](stream.read(new Array[Byte](2), 1, 2))
    intercept[NullPointerException](stream.read(null, 0, 0))
    assert(fetched == 0)
    val target = Array.fill[Byte](5)(-1)
    assert(stream.read(target, 1, 3) == 3)
    assert(target.toSeq == Seq[Byte](-1, 1, 2, 3, -1))
    stream.close()
  }

  test("closing after the fetcher's cleanup releases every fetched buffer once") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ => true)
    var stream: InputStream = null
    context.addTaskCompletionListener[Unit](_ => stream.close())
    stream = blockStream(context, client, 4)
    assert(stream.read() == 0)
    context.markTaskCompleted(None)
    assert(client.buffers.size == 4)
    assert(client.leaked.isEmpty)
  }

  test("closing after a failure in a later block releases every fetched buffer once") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ => true)
    var stream: InputStream = null
    context.addTaskCompletionListener[Unit](_ => stream.close())
    stream = blockStream(context, client, 5)
    assert(readFully(stream, blockSize * 2 + 3).last == 2)
    context.markTaskCompleted(Some(new RuntimeException("downstream failure")))
    assert(client.leaked.isEmpty)
  }

  test("closing does not fetch the blocks that were not read") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ == 0)
    val stream = blockStream(context, client, 3)
    assert(stream.read() == 0)
    within(stream.close())
    context.markTaskCompleted(None)
    assert(client.leaked.isEmpty)
  }

  test("a killed task stops reading at the next block") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ => true)
    val stream = blockStream(context, client, 3)
    assert(readFully(stream, blockSize).forall(_ == 0))
    context.markInterrupted("killed by test")
    intercept[TaskKilledException](stream.read())
    context.markTaskCompleted(None)
    within(stream.close())
    assert(client.leaked.isEmpty)
  }

  test("a killed task also stops in the middle of its current block") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ => true)
    val stream = blockStream(context, client, 3)
    assert(stream.read() == 0)
    context.markInterrupted("killed by test")
    intercept[TaskKilledException](stream.read(new Array[Byte](2), 0, 2))
    context.markTaskCompleted(None)
    stream.close()
    assert(client.leaked.isEmpty)
  }

  test("a killed task does not wait for a block that never arrives") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ == 0)
    val stream = blockStream(context, client, 2)
    assert(readFully(stream, blockSize).forall(_ == 0))
    context.markInterrupted("killed by test")
    within(intercept[TaskKilledException](stream.read()))
    within(stream.close())
    context.markTaskCompleted(None)
    assert(client.leaked.isEmpty)
  }

  test("interrupting a reader blocked on a missing block does not block the interrupter") {
    val context = TaskContext.empty()
    val client = new RemoteBlocks(_ == 0)
    val nextFetch = new CountDownLatch(1)
    val stream = blockStream(context, client, 3, index => if (index == 1) nextFetch.countDown())
    val channel = Channels.newChannel(stream)
    val started = new CountDownLatch(1)
    @volatile var failure: Throwable = null
    val reader = new Thread(() => {
      val buf = ByteBuffer.allocate(blockSize)
      try {
        while (buf.hasRemaining) channel.read(buf)
        started.countDown()
        buf.clear()
        channel.read(buf)
      } catch {
        case t: Throwable => failure = t
      }
    })
    reader.setDaemon(true)
    reader.start()
    assert(started.await(20, TimeUnit.SECONDS))
    assert(nextFetch.await(20, TimeUnit.SECONDS))
    within(reader.interrupt())
    reader.join(20000)
    assert(!reader.isAlive)
    assert(failure != null)
    context.markTaskCompleted(None)
    assert(client.leaked.isEmpty)
  }
}
