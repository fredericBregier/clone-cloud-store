/*
 * Copyright (c) 2022-2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed
 *  under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 *  OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.clonecloudstore.common.standard.guid;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@QuarkusTest
class LongUuidNativeTest {
  private static final int NB = 1000000;
  private static final int NB_THREAD = 10;

  @Test
  void testParsing() {
    final var id1 = LongUuid.getLongUuid();
    final var id2 = new LongUuid(id1);
    assertEquals(id1, id2.getLong());
  }

  @Test
  void testNonSequentialValue() {
    final var n = NB;
    final var ids = new long[n];

    for (var i = 0; i < n; i++) {
      ids[i] = LongUuid.getLongUuid();
    }

    for (var i = 1; i < n; i++) {
      assertNotEquals(ids[i - 1], ids[i]);
    }
  }

  @Test
  void testPIDField() throws Exception {
    final var id = LongUuid.getLongUuid();
    final var longUuid = new LongUuid(id);
    assertEquals(JvmProcessMacIds.getJvmByteId() >> 4 & 0x0F, longUuid.getProcessId());
  }

  @Test
  void testForDuplicates() {
    final var n = NB;
    final Set<Long> uuids = new HashSet<>(n);
    final var uuidArray = new Long[n];

    final var start = System.currentTimeMillis();
    for (var i = 0; i < n; i++) {
      uuidArray[i] = LongUuid.getLongUuid();
    }
    final var stop = System.currentTimeMillis();
    System.out.println("Time = " + (stop - start) + " so " + n / (stop - start) * 1000 + " Uuids/s");

    uuids.addAll(Arrays.asList(uuidArray));

    System.out.println("Create " + n + " and get: " + uuids.size());
    assertEquals(n, uuids.size());
    System.out.println(
        "Time elapsed: " + uuidArray[0] + " - " + uuidArray[n - 1] + " = " + (uuidArray[n - 1] - uuidArray[0]) + " & " +
            (new LongUuid(uuidArray[n - 1]).getTimestamp() - new LongUuid(uuidArray[0]).getTimestamp()));
  }

  @Test
  void concurrentGeneration() throws Exception {
    final var numThreads = NB_THREAD;
    final var threads = new Thread[numThreads];
    final var n = NB;
    final var effectiveN = n / numThreads * numThreads;
    final var uuids = new Long[effectiveN];

    final var start = System.currentTimeMillis();
    for (var i = 0; i < numThreads; i++) {
      threads[i] = new Generator(n / numThreads, uuids, i);
      threads[i].start();
    }

    for (var i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    final var stop = System.currentTimeMillis();
    System.out.println("Time = " + (stop - start) + " so " + n / (stop - start) * 1000 + " Uuids/s");

    final Set<Long> uuidSet = new HashSet<>(effectiveN);
    uuidSet.addAll(Arrays.asList(uuids));

    assertEquals(effectiveN, uuidSet.size());
  }

  @Test
  void concurrentCounterGeneration() throws Exception {
    final var numThreads = NB_THREAD;
    final var threads = new Thread[numThreads];
    final var n = NB;
    final var effectiveN = n / numThreads * numThreads;
    final var uuids = new Long[effectiveN];

    final var start = System.currentTimeMillis();
    for (var i = 0; i < numThreads; i++) {
      threads[i] = new CounterParallel(n / numThreads, uuids, i);
      threads[i].start();
    }

    for (var i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    final var stop = System.currentTimeMillis();
    System.out.println("Time = " + (stop - start) + " so " + n / (stop - start) * 1000 + " Counter/s");

    final Set<Long> uuidSet = new HashSet<>(effectiveN);
    uuidSet.addAll(Arrays.asList(uuids));

    assertEquals(effectiveN, uuidSet.size());
  }

  private static class CounterParallel extends Thread {
    final int id;
    final int n;
    private final Long[] uuids;

    private CounterParallel(final int n, final Long[] uuids, final int id) {
      this.n = n;
      this.uuids = uuids;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        uuids[id + i] = (System.currentTimeMillis() << 20) + LongUuid.getCounter();
      }
    }
  }

  private static class Generator extends Thread {
    final int id;
    final int n;
    private final Long[] uuids;

    private Generator(final int n, final Long[] uuids, final int id) {
      this.n = n;
      this.uuids = uuids;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        uuids[id + i] = LongUuid.getLongUuid();
      }
    }
  }
}
