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

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

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
      blocks: Int): InputStream = {
    val fetcher = new ShuffleBlockFetcherIterator(
      context,
      client,
      SparkEnv.get.blockManager,
      SparkEnv.get.mapOutputTracker,
      Iterator(
        (
          remote,
          (0 until blocks).map(i => (ShuffleBlockId(0, i, 0): BlockId, blockSize.toLong, i)))),
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
    CometBlockStoreShuffleReader.concatenate(context, fetcher.toCompletionIterator.map(_._2))
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
    val stream = blockStream(context, client, 3)
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
    Thread.sleep(200)
    within(reader.interrupt())
    reader.join(20000)
    assert(!reader.isAlive)
    assert(failure != null)
    context.markTaskCompleted(None)
    assert(client.leaked.isEmpty)
  }
}
