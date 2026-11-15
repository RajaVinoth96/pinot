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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SplittableRandom;


/// Standalone probe (run with `java FsyncCost.java [dir]`) that times FileChannel.force(true) by itself:
/// - right after writing files of 1 MB to 1 GB (all data still dirty), to get the per-byte cost;
/// - a second force on the same, already-synced file, to get the cost of a no-op sync;
/// - on many small 4 KB files, to get the fixed per-file cost.
/// Single-threaded; not part of Pinot.
public class FsyncCost {
  private static final int MB = 1 << 20;

  public static void main(String[] args)
      throws Exception {
    Path dir = Path.of(args.length > 0 ? args[0] : System.getProperty("java.io.tmpdir"), "fsync-cost");
    Files.createDirectories(dir);
    ByteBuffer chunk = ByteBuffer.allocateDirect(MB);
    SplittableRandom random = new SplittableRandom(42);
    while (chunk.remaining() >= Long.BYTES) {
      chunk.putLong(random.nextLong());
    }

    System.out.println("sizeMB,run,writeMs,forceMs,forceMBps,secondForceMs");
    for (int sizeMb : new int[]{1, 10, 100, 1000}) {
      for (int run = 1; run <= 3; run++) {
        Path file = dir.resolve("f-" + sizeMb + "-" + run);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
          long t0 = System.nanoTime();
          for (int i = 0; i < sizeMb; i++) {
            chunk.clear();
            while (chunk.hasRemaining()) {
              ch.write(chunk);
            }
          }
          long t1 = System.nanoTime();
          ch.force(true);
          long t2 = System.nanoTime();
          ch.force(true);
          long t3 = System.nanoTime();
          double forceMs = (t2 - t1) / 1e6;
          System.out.printf("%d,%d,%.2f,%.2f,%.0f,%.3f%n", sizeMb, run, (t1 - t0) / 1e6, forceMs,
              sizeMb / (forceMs / 1000), (t3 - t2) / 1e6);
        }
        Files.delete(file);
      }
    }

    int numSmallFiles = 200;
    ByteBuffer small = ByteBuffer.allocate(4096);
    long totalForceNs = 0;
    for (int i = 0; i < numSmallFiles; i++) {
      Path file = dir.resolve("small-" + i);
      try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE)) {
        small.clear();
        ch.write(small);
        long t0 = System.nanoTime();
        ch.force(true);
        totalForceNs += System.nanoTime() - t0;
      }
    }
    System.out.printf("smallFiles=%d, 4KB each, avgForceMs=%.3f%n", numSmallFiles, totalForceNs / 1e6 / numSmallFiles);
    for (int i = 0; i < numSmallFiles; i++) {
      Files.delete(dir.resolve("small-" + i));
    }
  }
}
