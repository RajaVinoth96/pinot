/**
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
package org.apache.pinot.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures end-to-end offline segment creation (index writers, v3 conversion, metadata) with a mix of index types
/// typical of a real table: dictionary-encoded dimensions with inverted and range indexes, raw (no-dictionary) metrics
/// and strings, and a JSON index. Each invocation builds one segment from in-memory rows into a fresh directory, so the
/// score includes every per-file close/sync the writers perform.
///
/// The rows are generated once per trial with a fixed seed and shared across invocations; the benchmark itself is
/// single-threaded and the state is not shared between threads.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
public class BenchmarkSegmentCreation {
  private static final String TABLE_NAME = "segmentCreationBenchmark";
  private static final File ROOT_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkSegmentCreation");

  private static final int NUM_DICT_INT_COLUMNS = 10;
  private static final int NUM_INVERTED_INT_COLUMNS = 5;
  private static final int NUM_RANGE_INT_COLUMNS = 2;
  private static final int NUM_RAW_LONG_COLUMNS = 5;
  private static final int NUM_DICT_STRING_COLUMNS = 5;
  private static final int NUM_INVERTED_STRING_COLUMNS = 2;
  private static final int NUM_RAW_STRING_COLUMNS = 2;
  private static final String JSON_COLUMN = "j0";

  private static final Schema SCHEMA;
  private static final TableConfig TABLE_CONFIG;

  static {
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME);
    List<String> invertedColumns = new ArrayList<>();
    List<String> rangeColumns = new ArrayList<>();
    List<String> noDictionaryColumns = new ArrayList<>();
    for (int i = 0; i < NUM_DICT_INT_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("d" + i, FieldSpec.DataType.INT);
      if (i < NUM_INVERTED_INT_COLUMNS) {
        invertedColumns.add("d" + i);
      }
    }
    for (int i = 0; i < NUM_RANGE_INT_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("r" + i, FieldSpec.DataType.INT);
      rangeColumns.add("r" + i);
    }
    for (int i = 0; i < NUM_RAW_LONG_COLUMNS; i++) {
      schemaBuilder.addMetric("m" + i, FieldSpec.DataType.LONG);
      noDictionaryColumns.add("m" + i);
    }
    for (int i = 0; i < NUM_DICT_STRING_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("s" + i, FieldSpec.DataType.STRING);
      if (i < NUM_INVERTED_STRING_COLUMNS) {
        invertedColumns.add("s" + i);
      }
    }
    for (int i = 0; i < NUM_RAW_STRING_COLUMNS; i++) {
      schemaBuilder.addSingleValueDimension("t" + i, FieldSpec.DataType.STRING);
      noDictionaryColumns.add("t" + i);
    }
    schemaBuilder.addSingleValueDimension(JSON_COLUMN, FieldSpec.DataType.JSON);
    SCHEMA = schemaBuilder.build();
    TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(invertedColumns)
        .setRangeIndexColumns(rangeColumns)
        .setNoDictionaryColumns(noDictionaryColumns)
        .setJsonIndexColumns(List.of(JSON_COLUMN))
        .build();
  }

  @Param({"50000", "500000"})
  int _numRows;

  private List<GenericRow> _rows;
  private File _outDir;

  @Setup(Level.Trial)
  public void generateRows() {
    SplittableRandom random = new SplittableRandom(42);
    _rows = new ArrayList<>(_numRows);
    for (int row = 0; row < _numRows; row++) {
      GenericRow genericRow = new GenericRow();
      for (int i = 0; i < NUM_DICT_INT_COLUMNS; i++) {
        genericRow.putValue("d" + i, random.nextInt(1_000));
      }
      for (int i = 0; i < NUM_RANGE_INT_COLUMNS; i++) {
        genericRow.putValue("r" + i, random.nextInt(100_000));
      }
      for (int i = 0; i < NUM_RAW_LONG_COLUMNS; i++) {
        genericRow.putValue("m" + i, random.nextLong(1_000_000_000L));
      }
      for (int i = 0; i < NUM_DICT_STRING_COLUMNS; i++) {
        genericRow.putValue("s" + i, "value-" + random.nextInt(10_000));
      }
      for (int i = 0; i < NUM_RAW_STRING_COLUMNS; i++) {
        genericRow.putValue("t" + i, "free text " + random.nextLong() + " some payload " + random.nextLong());
      }
      genericRow.putValue(JSON_COLUMN,
          "{\"service\":\"svc-" + random.nextInt(100) + "\",\"region\":\"r-" + random.nextInt(8) + "\",\"code\":"
              + random.nextInt(600) + "}");
      _rows.add(genericRow);
    }
  }

  @Setup(Level.Iteration)
  public void createOutDir() {
    _outDir = new File(ROOT_DIR, UUID.randomUUID().toString());
  }

  @TearDown(Level.Iteration)
  public void deleteOutDir() {
    FileUtils.deleteQuietly(_outDir);
  }

  @TearDown(Level.Trial)
  public void deleteRootDir() {
    FileUtils.deleteQuietly(ROOT_DIR);
  }

  @Benchmark
  public File buildSegment()
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(_outDir.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(TABLE_NAME + "_" + _numRows);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (GenericRowRecordReader recordReader = new GenericRowRecordReader(_rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    return _outDir;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkSegmentCreation.class.getSimpleName()).build()).run();
  }
}
