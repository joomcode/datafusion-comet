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

package org.apache.spark.sql.benchmark;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatchRow;
import org.apache.spark.unsafe.Platform;

import com.sun.management.ThreadMXBean;

import org.apache.comet.vector.CometPlainVector;

/**
 * Isolate the non-codegen Arrow -> UnsafeRow boundary, without I/O, sorting or HLL work.
 *
 * <p>The experimental projection uses UTF8String only as a borrowed byte carrier: UnsafeWriter
 * copies its bytes without decoding text, and Binary/String have the same UnsafeRow layout. This
 * benchmark does not change the production converter or claim a whole-query speedup.
 *
 * <p>Run via the Makefile's benchmark invocation after test-compile, with BENCH_HEAP=2g. Reports
 * medians of seven alternating-order rounds; allocation counts come from the executing thread.
 */
public final class CometBinaryRowCopyBenchmark {
  private static final int ROWS = 128;
  private static volatile long blackhole;

  private CometBinaryRowCopyBenchmark() {}

  public static void main(String[] args) {
    ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    if (!bean.isThreadAllocatedMemorySupported()) {
      throw new IllegalStateException("Thread allocation accounting is not supported");
    }
    bean.setThreadAllocatedMemoryEnabled(true);
    System.out.println("width,mode,rows_per_round,median_ns_per_row,median_alloc_bytes_per_row");
    for (int width : new int[] {32, 4096, 192 * 1024}) {
      benchmark(bean, width);
    }
    System.out.println("checksum=" + blackhole);
  }

  private static void benchmark(ThreadMXBean bean, int width) {
    try (RootAllocator allocator = new RootAllocator(256L * 1024 * 1024)) {
      VarBinaryVector vector = new VarBinaryVector("payload", allocator);
      vector.allocateNew();
      for (int i = 0; i < ROWS; i++) {
        if (i % 17 == 0) {
          vector.setNull(i);
        } else {
          byte[] bytes = new byte[i % 19 == 0 ? 0 : width];
          for (int b = 0; b < bytes.length; b++) {
            bytes[b] = (byte) (b * 37 + i); // Includes arbitrary, invalid UTF-8 bytes.
          }
          vector.setSafe(i, bytes);
        }
      }
      vector.setValueCount(ROWS);
      try (CometPlainVector column = new CometPlainVector(vector, false)) {
        ColumnarBatchRow row = new ColumnarBatchRow(new ColumnVector[] {column});
        UnsafeProjection[] projections = {
          UnsafeProjection.create(new DataType[] {DataTypes.BinaryType}),
          UnsafeProjection.create(new DataType[] {DataTypes.StringType})
        };
        for (int i = 0; i < ROWS; i++) {
          row.rowId = i;
          UnsafeRow expected = projections[0].apply(row).copy();
          UnsafeRow actual = projections[1].apply(row);
          if (expected.isNullAt(0) != actual.isNullAt(0)
              || !Arrays.equals(expected.getBinary(0), actual.getBinary(0))) {
            throw new AssertionError("Binary contents differ at row " + i);
          }
        }
        int iterations = Math.max(2048, Math.min(1000000, 128 * 1024 * 1024 / width));
        for (int warmup = 0; warmup < 3; warmup++) {
          for (UnsafeProjection projection : projections) {
            consume(projection, row, iterations);
          }
        }
        double[][] nanos = new double[2][7];
        double[][] allocations = new double[2][7];
        long thread = Thread.currentThread().getId();
        for (int round = 0; round < 7; round++) {
          for (int step = 0; step < 2; step++) {
            int mode = (round + step) % 2;
            long allocated = bean.getThreadAllocatedBytes(thread);
            long start = System.nanoTime();
            consume(projections[mode], row, iterations);
            nanos[mode][round] = (System.nanoTime() - start) / (double) iterations;
            allocations[mode][round] =
                (bean.getThreadAllocatedBytes(thread) - allocated) / (double) iterations;
          }
        }
        String[] names = {"getBinary_then_write", "borrowed_bytes_then_write"};
        for (int mode = 0; mode < 2; mode++) {
          Arrays.sort(nanos[mode]);
          Arrays.sort(allocations[mode]);
          System.out.printf(
              Locale.ROOT,
              "%d,%s,%d,%.3f,%.3f%n",
              width,
              names[mode],
              iterations,
              nanos[mode][3],
              allocations[mode][3]);
        }
      }
    }
  }

  private static void consume(UnsafeProjection projection, ColumnarBatchRow row, int iterations) {
    long checksum = 0;
    for (int i = 0; i < iterations; i++) {
      row.rowId = i & (ROWS - 1);
      UnsafeRow result = projection.apply(row);
      checksum += result.getSizeInBytes();
      checksum +=
          Platform.getByte(
              result.getBaseObject(), result.getBaseOffset() + result.getSizeInBytes() - 1L);
    }
    blackhole = checksum;
  }
}
