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

package org.apache.spark.shuffle.comet;

import java.io.IOException;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;

/** The base class for Comet JVM shuffle memory allocators. */
public abstract class CometShuffleMemoryAllocatorTrait extends MemoryConsumer {
  protected CometShuffleMemoryAllocatorTrait(
      TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    super(taskMemoryManager, pageSize, mode);
  }

  /** Spills what the owner of this allocator's memory buffers, for another consumer. */
  public interface OwnerSpill {
    /** Returns the bytes released, or 0 if the owner could not spill now. */
    long spillForOtherConsumer() throws IOException;
  }

  private OwnerSpill ownerSpill;

  /**
   * Lets the task's other memory consumers make this allocator's owner spill: the sort-based JVM
   * shuffle writer's buffered records would otherwise keep the task's whole share of the pool.
   */
  public void setOwnerSpill(OwnerSpill ownerSpill) {
    this.ownerSpill = ownerSpill;
  }

  /** Asks the owner to spill for `trigger`, a consumer other than this allocator. */
  protected long spillOwnerFor(MemoryConsumer trigger) throws IOException {
    if (trigger == this || ownerSpill == null) {
      return 0;
    }
    return ownerSpill.spillForOtherConsumer();
  }

  public abstract MemoryBlock allocate(long required);

  public abstract long free(MemoryBlock block);

  public abstract long getOffsetInPage(long pagePlusOffsetAddress);

  public abstract long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage);
}
