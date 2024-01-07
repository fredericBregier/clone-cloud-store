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

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class LongUuidTest {
  private static final int NB = 1000000;
  private static final int NB_THREAD = 10;

  @Test
  void testStructure() {
    final var id = new LongUuid();
    final var str = id.toString();

    assertEquals(16, str.length());
    assertEquals(8, id.getBytes().length);
    assertEquals(8, LongUuid.getKeySize());
  }

  @Test
  void testParsing() {
    final var id1 = new LongUuid();
    final var id2 = new LongUuid(id1.toString());
    assertFalse(id1.equals(new Object()));
    assertEquals(id1, id1);
    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1.getLong(), id2.getLong());

    final var id3 = new LongUuid(id1.getBytes());
    assertEquals(id1, id3);
    final var id4 = new LongUuid(id1.getLong());
    assertEquals(id1, id4);

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new LongUuid("aaa".getBytes()));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new LongUuid("aaa"));
  }

  @Test
  void testNonSequentialValue() {
    final var n = NB;
    final var ids = new long[n];

    for (var i = 0; i < n; i++) {
      ids[i] = new LongUuid().getLong();
    }

    for (var i = 1; i < n; i++) {
      assertNotEquals(ids[i - 1], ids[i]);
    }
  }

  @Test
  void testGetBytesImmutability() {
    final var id = new LongUuid();
    final var bytes = id.getBytes();
    final var original = Arrays.copyOf(bytes, bytes.length);
    bytes[0] = 0;
    bytes[1] = 0;
    bytes[2] = 0;

    assertArrayEquals(id.getBytes(), original);
  }

  @Test
  void testConstructorImmutability() {
    final var id = new LongUuid();
    final var bytes = id.getBytes();
    final var original = Arrays.copyOf(bytes, bytes.length);

    final var id2 = new LongUuid(bytes);
    bytes[0] = 0;
    bytes[1] = 0;

    assertArrayEquals(id2.getBytes(), original);
  }

  @Test
  void testPIDField() throws Exception {
    final var id = new LongUuid();

    assertEquals(JvmProcessMacIds.getJvmByteId() >> 4 & 0x0F, id.getProcessId());
  }

  @Test
  void testForDuplicates() {
    final var n = NB;
    final Set<Long> uuids = new HashSet<>(n);
    final var uuidArray = new LongUuid[n];

    final var start = System.currentTimeMillis();
    for (var i = 0; i < n; i++) {
      uuidArray[i] = new LongUuid();
    }
    final var stop = System.currentTimeMillis();
    System.out.println("Time = " + (stop - start) + " so " + n / (stop - start) * 1000 + " Uuids/s");

    for (var i = 0; i < n; i++) {
      uuids.add(uuidArray[i].getLong());
    }

    System.out.println("Create " + n + " and get: " + uuids.size());
    assertEquals(n, uuids.size());
    var i = 1;
    var largest = 0;
    for (; i < n; i++) {
      if (uuidArray[i].getTimestamp() != uuidArray[i - 1].getTimestamp()) {
        var j = i + 1;
        final var time = uuidArray[i].getTimestamp();
        for (; j < n; j++) {
          if (uuidArray[j].getTimestamp() != time) {
            if (largest < j - i + 1) {
              largest = j - i + 1;
            }
            i = j;
            break;
          }
        }
      }
    }
    if (largest == 0) {
      largest = n;
    }
    System.out.println(
        "Time elapsed: " + uuidArray[0] + '(' + uuidArray[0].getTimestamp() + ':' + uuidArray[0].getLong() + ") - " +
            uuidArray[n - 1] + '(' + uuidArray[n - 1].getTimestamp() + ':' + uuidArray[n - 1].getLong() + ") = " +
            (uuidArray[n - 1].getLong() - uuidArray[0].getLong()) + " & " +
            (uuidArray[n - 1].getTimestamp() - uuidArray[0].getTimestamp()));
    System.out.println(largest + " different consecutive elements for same time");
  }

  @Test
  void concurrentGeneration() throws Exception {
    final var numThreads = NB_THREAD;
    final var threads = new Thread[numThreads];
    final var n = NB;
    final var effectiveN = n / numThreads * numThreads;
    final var uuids = new LongUuid[effectiveN];

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

    final Set<LongUuid> uuidSet = new HashSet<>(effectiveN);
    uuidSet.addAll(Arrays.asList(uuids));

    assertEquals(effectiveN, uuidSet.size());
  }

  @Test
  void concurrentCounterGeneration() throws Exception {
    final var numThreads = NB_THREAD;
    final var threads = new Thread[numThreads];
    final var n = NB;
    final var effectiveN = n / numThreads * numThreads;
    final var uuids = new long[effectiveN];

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
    for (final Long i : uuids) {
      uuidSet.add(i);
    }

    assertEquals(effectiveN, uuidSet.size());
  }

  private static class CounterParallel extends Thread {
    final int id;
    final int n;
    private final long[] uuids;

    private CounterParallel(final int n, final long[] uuids, final int id) {
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

  static class Generator extends Thread {
    final int id;
    final int n;
    private final LongUuid[] uuids;

    Generator(final int n, final LongUuid[] uuids, final int id) {
      this.n = n;
      this.uuids = uuids;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        uuids[id + i] = new LongUuid();
      }
    }
  }
}
