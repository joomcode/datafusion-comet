<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# JVM Shuffle

This document describes Comet's JVM-based columnar shuffle implementation (`CometColumnarShuffle`), which
writes shuffle data in Arrow IPC format using JVM code with native encoding. For the fully native
alternative, see [Native Shuffle](native_shuffle.md).

## Overview

Comet provides two shuffle implementations:

- **CometNativeShuffle** (`CometExchange`): Fully native shuffle using Rust. Takes columnar input directly
  from Comet native operators and performs partitioning in native code.
- **CometColumnarShuffle** (`CometColumnarExchange`): JVM-based shuffle that operates on rows internally,
  buffers `UnsafeRow`s in memory pages, and uses native code (via JNI) to encode them to Arrow IPC format.
  Uses Spark's partitioner for partition assignment. Can accept either row-based or columnar input
  (columnar input is converted to rows via `ColumnarToRowExec`).

The JVM shuffle is selected via `CometShuffleDependency.shuffleType`.

## When JVM Shuffle is Used

These are initial eligibility rules. Optional whole-plan selection can subsequently
choose Spark's shuffle, retaining rows between Spark producers and consumers.
`ChooseBoundaryFormats` changes only shuffle/broadcast formats;
`CostBasedEngineChoice` jointly chooses operator engines and formats using the cost
table. Both preserve co-partitioned input hash compatibility. A Spark producer can
feed a Comet consumer through an eligible JVM columnar shuffle; format selection
does not turn that Spark producer into a native one. Materialized AQE stages keep
their existing format. See [Native Shuffle](native_shuffle.md#when-native-shuffle-is-used)
and [conversion tuning](../user-guide/latest/tuning/transitions.md).

JVM shuffle (`CometColumnarExchange`) is used instead of native shuffle (`CometExchange`) in the following cases:

1. **Shuffle mode is explicitly set to "jvm"**: When `spark.comet.shuffle.mode` is set to `jvm`.

2. **Child plan is not a Comet native operator**: When the child plan is a Spark row-based operator
   (not a `CometPlan`), JVM shuffle is used, since native shuffle requires columnar input from
   Comet operators. In `auto` mode, `spark.comet.convert.shuffleInput.enabled` can convert
   the child's rows to Arrow with `CometSparkToColumnarExec` so that native shuffle can take the
   shuffle instead. See [When Native Shuffle is Used](native_shuffle.md#when-native-shuffle-is-used).

3. **Unsupported partition key types**: `RangePartitioning` keys must be primitive, so a complex
   range key always falls back here. `HashPartitioning` keys must be primitive only by default:
   setting `spark.comet.shuffle.native.partitioning.hash.nested.enabled` to `true` keeps struct and
   array keys, and map keys on Spark 4.0 and later, on the native path. The config defaults to
   `false`, so a complex hash key falls back to JVM columnar shuffle unless it is enabled. See
   [Supported partition key types](native_shuffle.md#when-native-shuffle-is-used) for the exact
   rules. Complex types are fully supported as data columns in both implementations.

## Input Handling

### Spark Row-Based Input

When the child plan is a Spark row-based operator, `CometColumnarExchange` calls `child.execute()` which
returns an `RDD[InternalRow]`. The rows flow directly to the JVM shuffle writers.

### Comet Columnar Input

When the child plan is a Comet native operator (e.g., `CometHashAggregate`) but JVM shuffle is selected
(due to shuffle mode setting or unsupported partitioning), `CometColumnarExchange` still calls
`child.execute()`. Comet operators implement `doExecute()` by wrapping themselves with `ColumnarToRowExec`:

```scala
// In CometExec base class
override def doExecute(): RDD[InternalRow] =
  ColumnarToRowExec(this).doExecute()
```

This means the data path becomes:

```
Comet Native (columnar) → ColumnarToRowExec → rows → JVM Shuffle → Arrow IPC → columnar
```

This is less efficient than native shuffle which avoids the columnar-to-row conversion:

```
Comet Native (columnar) → Native Shuffle → Arrow IPC → columnar
```

### Why Use Spark's Partitioner?

JVM shuffle uses row-based input so it can leverage Spark's existing partitioner infrastructure
(`partitioner.getPartition(key)`). This allows Comet to support all of Spark's partitioning schemes
without reimplementing them in Rust. Native shuffle, by contrast, serializes the partitioning scheme
to protobuf and implements the partitioning logic in native code.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CometShuffleManager                             │
│  - Extends Spark's ShuffleManager                                       │
│  - Routes to appropriate writer/reader based on ShuffleHandle type      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
           ┌────────────────────────┼────────────────────────┐
           ▼                        ▼                        ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│ CometBypassMerge-   │  │ CometUnsafe-        │  │ CometNative-        │
│ SortShuffleWriter   │  │ ShuffleWriter       │  │ ShuffleWriter       │
│ (hash-based)        │  │ (sort-based)        │  │ (fully native)      │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
           │                        │
           ▼                        ▼
┌─────────────────────┐  ┌─────────────────────┐
│ CometDiskBlock-     │  │ CometShuffleExternal│
│ Writer              │  │ Sorter              │
└─────────────────────┘  └─────────────────────┘
           │                        │
           └────────────┬───────────┘
                        ▼
              ┌─────────────────────┐
              │ SpillWriter         │
              │ (native encoding    │
              │  via JNI)           │
              └─────────────────────┘
```

## Key Classes

### Shuffle Manager

| Class                    | Location                                   | Description                                                                                                                                      |
| ------------------------ | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `CometShuffleManager`    | `.../shuffle/CometShuffleManager.scala`    | Entry point. Extends Spark's `ShuffleManager`. Selects writer/reader based on handle type. Delegates non-Comet shuffles to `SortShuffleManager`. |
| `CometShuffleDependency` | `.../shuffle/CometShuffleDependency.scala` | Extends `ShuffleDependency`. Contains `shuffleType` (`CometColumnarShuffle` or `CometNativeShuffle`) and schema info.                            |

### Shuffle Handles

| Handle                              | Writer Strategy                                           |
| ----------------------------------- | --------------------------------------------------------- |
| `CometBypassMergeSortShuffleHandle` | Hash-based: one file per partition, merged at end         |
| `CometSerializedShuffleHandle`      | Sort-based: records sorted by partition ID, single output |
| `CometNativeShuffleHandle`          | Fully native shuffle                                      |

Selection logic in `CometShuffleManager.shouldBypassMergeSort()`:

- Uses bypass if partitions ≤ threshold AND partitions × cores ≤ max threads
- Otherwise uses sort-based to avoid OOM from many concurrent writers

### Writers

| Class                               | Location                                             | Description                                                                                                                                           |
| ----------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CometBypassMergeSortShuffleWriter` | `.../shuffle/CometBypassMergeSortShuffleWriter.java` | Hash-based writer. Creates one `CometDiskBlockWriter` per partition. Supports async writes.                                                           |
| `CometUnsafeShuffleWriter`          | `.../shuffle/CometUnsafeShuffleWriter.java`          | Sort-based writer. Uses `CometShuffleExternalSorter` to buffer and sort records, then merges spill files.                                             |
| `CometDiskBlockWriter`              | `.../shuffle/CometDiskBlockWriter.java`              | Buffers rows in memory pages for a single partition. Spills to disk via native encoding. Used by bypass writer.                                       |
| `CometShuffleExternalSorter`        | `.../shuffle/sort/CometShuffleExternalSorter.java`   | Buffers records across all partitions, sorts by partition ID, spills sorted data. Used by unsafe writer.                                              |
| `SpillSorter`                       | `.../shuffle/sort/SpillSorter.java`                  | Holds one spill's records for `CometShuffleExternalSorter`. Writes every partition of the spill with one `Native.writeSortedPartitionsNative()` call. |
| `SpillWriter`                       | `.../shuffle/SpillWriter.java`                       | Base class for spill logic. Manages memory pages. `doSpilling()` encodes one partition's rows with `Native.writeSortedFileNative()`.                  |

### Reader

| Class                          | Location                                         | Description                                                                                        |
| ------------------------------ | ------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `CometBlockStoreShuffleReader` | `.../shuffle/CometBlockStoreShuffleReader.scala` | Fetches shuffle blocks via `ShuffleBlockFetcherIterator`. Decodes Arrow IPC to `ColumnarBatch`.    |
| `NativeBatchDecoderIterator`   | `.../shuffle/NativeBatchDecoderIterator.scala`   | Reads compressed Arrow IPC batches from input stream. Calls `Native.decodeShuffleBlock()` via JNI. |

## Data Flow

### Write Path

1. `ShuffleWriteProcessor` calls `CometShuffleManager.getWriter()`
2. Writer receives `Iterator[Product2[K, V]]` where V is `UnsafeRow`
3. Rows are serialized and buffered in off-heap memory pages
4. When memory threshold or batch size is reached, the buffered rows are written. `CometDiskBlockWriter`
   calls `SpillWriter.doSpilling()` for its partition. `CometShuffleExternalSorter` calls
   `SpillSorter.writeSortedPartitions()` once for each spill and once more for the final file, and each
   call writes every partition
5. Native code converts the rows to Arrow arrays and writes them as Arrow IPC blocks. `doSpilling()`
   passes one partition's row addresses and sizes to `Native.writeSortedFileNative()`. `SpillSorter`
   sorts its record pointers by partition with `Native.sortRowPartitionsNative()`, a radix sort that
   uses the half of the pointer array `ShuffleInMemorySorter` keeps free as scratch space, then
   passes them, with the address of each data page, to `Native.writeSortedPartitionsNative()`. That
   call reads the rows in place, writes every partition of the spill, and returns each partition's
   length and checksum
6. For bypass writer: partition files are concatenated into final output
7. For sort writer: spill files are merged

`SortedRowEncoder` is shared by the single-partition and sorted-spill entry points. When a
string/binary dictionary is not useful, it can reuse the dictionary's values buffer as a plain
array only if every non-NULL key names a distinct non-NULL value in insertion order. Repeated or
reordered keys, unused values and NULL dictionary values retain the ordinary Arrow cast. The
result keeps its own reference to the values buffer when builders reset for the next batch.

### Read Path

1. `CometBlockStoreShuffleReader.read()` creates `ShuffleBlockFetcherIterator`.
2. With read coalescing enabled, one `NativeBatchDecoderIterator` consumes the concatenated local
   block streams. Native `ShuffleReadCoalescer` combines small decoded blocks before export.
   With coalescing disabled, each stream uses the original per-block decoder.
3. Native code decompresses and decodes the IPC data into independently owned Arrow arrays.
4. Arrow FFI imports a ready batch as `ColumnarBatch`. The iterator releases prefetched and
   delivered batches and the native coalescer on close, including error and task-completion paths.

The row target is `spark.comet.batchSize`; the byte target defaults to 64 MiB, and a single
byte-oversized block passes through unchanged. The target is not a Spark memory-pool reservation
or a process-memory limit. See [Read Coalescing](native_shuffle.md#read-coalescing) for offset
limits, ownership and configuration. Celeborn's JVM reader remains blockwise.

This is the path taken when the consumer is a Spark operator. When a Comet native operator consumes
the shuffle output and `spark.comet.shuffle.directRead.enabled` is set, steps 2 through 4 are skipped:
the raw compressed blocks are handed to native code, which decodes them inside the plan. JVM shuffle
writes the same Arrow IPC block format as native shuffle, so direct read applies to both. See
[Direct Read](native_shuffle.md#direct-read-shufflescan) for the mechanism.

## Memory Management

- `CometShuffleMemoryAllocator.getInstance` returns the allocator for the task. Pages are always
  `Unsafe`-allocated, because their addresses are handed to native code, but what bounds them
  depends on Spark's memory mode.
- Off-heap mode gets `CometUnifiedShuffleMemoryAllocator`, an ordinary Spark `MemoryConsumer`
  drawing from `spark.memory.offHeap.size`. When it cannot acquire a page the writer spills to
  disk. An allocation that loses the task's entry in Spark's execution pool while it waits
  ([SPARK-59444](https://issues.apache.org/jira/browse/SPARK-59444)) is retried a few times before
  it counts as a page that could not be acquired.
- On-heap mode gets `CometUnboundedShuffleMemoryAllocator`, which keeps no budget and so never
  refuses a page. Nothing bounds these allocations, and memory pressure never triggers a spill.
  That mode exists only so the Spark SQL tests can run against Comet. See
  [Memory Management](memory_management.md).
- Row count still triggers spilling in either mode. `CometDiskBlockWriter` spills at
  `min(spark.comet.shuffle.jvm.spillThreshold, batch size)`, where the batch size is
  `spark.comet.shuffle.jvm.batchSize` capped at `spark.comet.batchSize`.
  `CometShuffleExternalSorter` spills at `spark.comet.shuffle.jvm.spillThreshold` alone, which
  defaults to `Int.MaxValue`, so on the sort path that trigger is effectively off by default.
- `CometDiskBlockWriter` coordinates spilling across all partition writers (largest first).
- The sort-based writer registers an `OwnerSpill` callback with its allocator so a different
  consumer in the same task can release the writer's buffered memory. The callback runs only on
  the owner's thread while the sorter is idle and open; it declines self, reentrant and concurrent
  requests. A failed spill poisons the sorter so later insertions or final output cannot silently
  continue from partially written state. Cleanup detaches the callback.
- After a spill, `SpillSorter.reset()` frees and discards Spark's in-memory sorter and clears
  its cached pointer-array reference. Replacement allocation is deferred to the next insertion, outside the
  memory-manager callback, to avoid recursive acquisition during spill.
- The bypass writer's partition writers each hold a page as soon as they buffer a row. Each one
  starts a batch on a 64 KiB page and doubles each further page, up to Spark's page size
  (`spark.buffer.pageSize`, capped at 128 MB). Spark sizes its page so that a task fits only 16 to
  32 of them, so pages of that size would make the writers spill each other about one row at a
  time once a shuffle has more partitions than that. The sort-based writer has a single sorter per
  task, and uses Spark's page size from the first page.

## Configuration

| Config                                                     | Description                                                                                   |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `spark.comet.shuffle.jvm.batchSize`                        | Rows per Arrow batch                                                                          |
| `spark.comet.shuffle.jvm.spillThreshold`                   | Row count threshold for spill                                                                 |
| `spark.comet.shuffle.compression.codec`                    | Compression codec, `lz4` by default                                                           |
| `spark.comet.shuffle.compression.offsetsAsLengths.enabled` | Store offsets as lengths in zstd blocks without dictionary-encoded columns, `true` by default |
| `spark.comet.shuffle.directRead.enabled`                   | Decode blocks natively on read, `true` by default                                             |
| `spark.comet.shuffle.read.coalesce.enabled`                | Join small decoded local blocks, `true` by default                                            |
| `spark.comet.shuffle.read.coalesce.maxBytes`               | Column-data target for joined batches, 64 MiB by default; `0` disables the byte target        |
