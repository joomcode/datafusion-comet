/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.ObjectName;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/** Isolated official-Spark ownership diagnostic; never CI or release acceptance. */
public final class SparkParquetMemoryProbe {
  private static final int CAPACITY = 4096;
  private static final int READERS = 100;
  private static final long EXPECTED_LEAK_BYTES = READERS * 2L * CAPACITY * 5L;
  private static final Field COLUMN_VECTORS;
  private static final Pattern OTHER = Pattern.compile(
      "-\\s+Other\\s+\\(reserved=(\\d+)KB, committed=(\\d+)KB\\)");
  private static final MessageType FLAT = MessageTypeParser.parseMessageType(
      "message flat { optional int32 value; }");
  private static final MessageType NESTED = MessageTypeParser.parseMessageType(
      "message nested { optional group items (LIST) { repeated group list { "
          + "optional int32 element; } } }");

  static {
    try {
      // Exact official4.2.1 declares this field private. Like upstream ParquetTestAccess,
      // inspect only; never mutate the field, reader, vectors, or allocation counters.
      COLUMN_VECTORS = VectorizedParquetRecordReader.class.getDeclaredField("columnVectors");
      COLUMN_VECTORS.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final class EmptyGroups
      implements SpecificParquetRecordReaderBase.ParquetRowGroupReader {
    @Override public PageReadStore readNextRowGroup() { return null; }
    @Override public void close() { }
  }

  private static final class Capture {
    final List<OffHeapColumnVector> ordinary = new ArrayList<>();
    final List<OffHeapColumnVector> auxiliary = new ArrayList<>();
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  private static OffHeapColumnVector offHeap(WritableColumnVector vector) {
    require(vector instanceof OffHeapColumnVector, "Official reader did not allocate offheap");
    return (OffHeapColumnVector) vector;
  }

  private static Capture allocateAndClose(MessageType schema, int count,
      List<OffHeapColumnVector> rescue) throws Exception {
    Capture result = new Capture();
    Set<WritableColumnVector> unique = Collections.newSetFromMap(new IdentityHashMap<>());
    for (int i = 0; i < count; i++) {
      VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(true, CAPACITY);
      try {
        reader.initialize(schema, schema, new EmptyGroups(), 0);
        reader.resultBatch();
        ParquetColumnVector[] roots = (ParquetColumnVector[]) COLUMN_VECTORS.get(reader);
        require(roots != null && roots.length == 1, "Expected one official Parquet root");
        for (ParquetColumnVector root : roots) {
          for (ParquetColumnVector leaf : root.getLeaves()) {
            OffHeapColumnVector ordinary = offHeap(leaf.getValueVector());
            require(ordinary.valuesNativeAddress() != 0, "Leaf has no live Int values allocation");
            result.ordinary.add(ordinary);
            for (WritableColumnVector auxiliary : new WritableColumnVector[] {
                leaf.getDefinitionLevelVector(), leaf.getRepetitionLevelVector()}) {
              if (auxiliary != null && unique.add(auxiliary)) {
                OffHeapColumnVector vector = offHeap(auxiliary);
                require(vector.valuesNativeAddress() != 0, "Auxiliary not allocated");
                result.auxiliary.add(vector);
                rescue.add(vector);
              }
            }
          }
        }
      } finally {
        // Exercise the official reader's normal cleanup before inspecting/rescuing anything.
        reader.close();
      }
    }
    for (OffHeapColumnVector vector : result.ordinary) {
      require(vector.valuesNativeAddress() == 0, "Official reader failed to close ordinary leaf");
    }
    return result;
  }

  private static long nmt(Path output, String name) throws Exception {
    String summary = (String) ManagementFactory.getPlatformMBeanServer().invoke(
        new ObjectName("com.sun.management:type=DiagnosticCommand"), "vmNativeMemory",
        new Object[] {new String[] {"summary", "scale=KB"}},
        new String[] {String[].class.getName()});
    Files.writeString(output.resolve(name + ".txt"), summary);
    Matcher match = OTHER.matcher(summary);
    require(summary.contains("Native Memory Tracking:"),
        "NMT is disabled or did not return a summary");
    // HotSpot omits an all-zero category; a valid enabled summary then means Other=0.
    return match.find() ? Long.parseLong(match.group(2)) * 1024L : 0L;
  }

  private static String quote(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  public static void main(String[] args) throws Exception {
    require(args.length == 1, "Pass one explicit output directory");
    Path output = Path.of(args[0]).toAbsolutePath();
    Files.createDirectories(output);
    String sparkJar = VectorizedParquetRecordReader.class.getProtectionDomain()
        .getCodeSource().getLocation().toString();
    require(sparkJar.endsWith("/spark-sql_2.13-4.2.1.jar"), "Unexpected official Spark JAR: " + sparkJar);
    require(sparkJar.equals(OffHeapColumnVector.class.getProtectionDomain()
        .getCodeSource().getLocation().toString()), "Vector and reader origins differ");
    require(sparkJar.equals(ParquetColumnVector.class.getProtectionDomain()
        .getCodeSource().getLocation().toString()), "Auxiliary reader origin differs");
    List<OffHeapColumnVector> rescue = new ArrayList<>();
    long beforeFlat = 0, afterFlat = 0, beforeNested = 0, afterNested = 0, afterRescue = 0;
    Capture flat = null, nested = null;
    try {
      // Warm dependency/class/MBean initialization without leaving orphaned warmup allocations.
      allocateAndClose(FLAT, 1, rescue);
      allocateAndClose(NESTED, 1, rescue);
      for (OffHeapColumnVector vector : rescue) vector.close();
      rescue.clear();
      nmt(output, "warmup");
      beforeFlat = nmt(output, "before-flat");
      flat = allocateAndClose(FLAT, READERS, rescue);
      require(flat.ordinary.size() == READERS, "Wrong flat leaf count");
      require(flat.auxiliary.isEmpty(), "Flat control unexpectedly allocated level vectors");
      afterFlat = nmt(output, "after-flat");
      beforeNested = nmt(output, "before-nested");
      nested = allocateAndClose(NESTED, READERS, rescue);
      require(nested.ordinary.size() == READERS, "Wrong nested ordinary leaf count");
      require(nested.auxiliary.size() == 2 * READERS, "Expected definition+repetition per reader");
      for (OffHeapColumnVector vector : nested.auxiliary) {
        require(vector.valuesNativeAddress() != 0,
            "Auxiliary is closed: vulnerable official4.2.1 ownership defect did not reproduce");
      }
      afterNested = nmt(output, "after-nested-readers-closed");
    } finally {
      // No production fix: release our captured diagnostic allocations only AFTER measurement.
      // This is explicit rescue cleanup to avoid leaving even this bounded4MB witness allocated.
      for (OffHeapColumnVector vector : rescue) vector.close();
      for (OffHeapColumnVector vector : rescue) {
        require(vector.valuesNativeAddress() == 0, "Explicit auxiliary rescue did not free memory");
      }
      afterRescue = nmt(output, "after-explicit-auxiliary-rescue");
    }
    require(flat != null && nested != null, "Diagnostic did not complete");
    long tolerance = 128L * 1024L; // NMT rounds KB and the self-observer may allocate small buffers.
    require(Math.abs(afterFlat - beforeFlat) <= tolerance,
        "Flat reader control has unexpected Other growth");
    require(Math.abs((afterNested - beforeNested) - EXPECTED_LEAK_BYTES) <= tolerance,
        "Nested Other growth does not match the200 captured Int auxiliary vectors");
    require(Math.abs((afterNested - afterRescue) - EXPECTED_LEAK_BYTES) <= tolerance,
        "Explicit auxiliary cleanup did not release the expected Other allocation");
    String receipt = "{\n"
        + "  \"status\": \"OFFICIAL_SPARK_AUXILIARY_OWNERSHIP_DEFECT_REPRODUCED\",\n"
        + "  \"qualification_credit\": false,\n"
        + "  \"spark_context_created\": false,\n"
        + "  \"rows_read\": 0,\n"
        + "  \"spark_jar\": " + quote(sparkJar) + ",\n"
        + "  \"readers_per_arm\": " + READERS + ",\n"
        + "  \"capacity\": " + CAPACITY + ",\n"
        + "  \"ordinary_vectors_closed_per_arm\": " + READERS + ",\n"
        + "  \"flat_auxiliary_vectors\": " + flat.auxiliary.size() + ",\n"
        + "  \"nested_auxiliary_vectors_unclosed_by_reader\": " + nested.auxiliary.size() + ",\n"
        + "  \"expected_live_auxiliary_bytes\": " + EXPECTED_LEAK_BYTES + ",\n"
        + "  \"other_bytes_before_flat\": " + beforeFlat + ",\n"
        + "  \"other_bytes_after_flat\": " + afterFlat + ",\n"
        + "  \"other_bytes_before_nested\": " + beforeNested + ",\n"
        + "  \"other_bytes_after_nested\": " + afterNested + ",\n"
        + "  \"other_bytes_after_rescue\": " + afterRescue + "\n}\n";
    Files.writeString(output.resolve("ownership-witness.json"), receipt);
    System.out.print(receipt);
  }
}
