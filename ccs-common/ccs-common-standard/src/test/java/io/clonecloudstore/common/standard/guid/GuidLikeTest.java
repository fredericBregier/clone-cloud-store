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
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.standard.guid.JvmProcessMacIds.FACTOR_3_BYTES;
import static io.clonecloudstore.common.standard.guid.JvmProcessMacIds.MODULO;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class GuidLikeTest {
  private static final int NB = 1000000;
  private static final int NB_THREAD = 10;

  @Test
  void testStructure() {
    final var id = new GuidLike();
    final var str = id.toString();
    Log.debugf("ID %s (%s %s %s)", str, id.toHex(), id.toBase32(), id.toBase64());
    assertEquals(26, str.length());
    assertEquals(32, id.toHex().length());
    assertEquals(22, id.toBase64().length());
    assertEquals(16, id.getBytes().length);
    assertEquals(16, GuidLike.getKeySize());
    var mod = (JvmProcessMacIds.getMacLong() * FACTOR_3_BYTES + JvmProcessMacIds.getJvmPID()) % MODULO;
    System.out.println("Mod = " + mod);
  }

  @Test
  void testParsing() {
    final var id1 = new GuidLike();
    final var id2 = new GuidLike(id1.toString());
    assertFalse(id1.equals(new Object()));
    assertEquals(id1, id1);
    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1.getLongHigh(), id2.getLongHigh());
    assertEquals(id1.getLongLow(), id2.getLongLow());

    final var id3 = new GuidLike(id1.getBytes());
    assertEquals(id1, id3);
    final var id16 = new GuidLike(id1.toHex());
    assertEquals(id1, id16);
    final var id32 = new GuidLike(id1.toBase32());
    assertEquals(id1, id32);
    final var id64 = new GuidLike(id1.toBase64());
    assertEquals(id1, id64);
    final var idlh = new GuidLike(id1.getLongLow(), id1.getLongHigh());
    assertEquals(id1, idlh);
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new GuidLike("abc".getBytes()));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new GuidLike("abc"));
  }

  @Test
  void testNonSequentialValue() {
    final var n = NB;
    final var ids = new GuidLike[n];

    for (var i = 0; i < n; i++) {
      ids[i] = new GuidLike();
    }

    for (var i = 1; i < n; i++) {
      assertNotEquals(ids[i - 1], ids[i]);
    }
    int firstPos = 0;
    int lastPos = 0;
    int maxTimeDistance = 1;
    for (var i = 1; i < n; i++) {
      if (ids[i - 1].getTime() == ids[i].getTime()) {
        lastPos = i;
      } else {
        if (lastPos - firstPos > maxTimeDistance) {
          maxTimeDistance = lastPos - firstPos;
        }
        firstPos = i;
        lastPos = i;
      }
    }
    System.out.println("MaxDistance: " + maxTimeDistance);
  }

  @Test
  void testGetBytesImmutability() {
    final var id = new GuidLike();
    final var bytes = id.getBytes();
    final var original = Arrays.copyOf(bytes, bytes.length);
    bytes[0] = 0;
    bytes[1] = 0;
    bytes[2] = 0;

    assertArrayEquals(id.getBytes(), original);
  }

  @Test
  void testConstructorImmutability() {
    final var id = new GuidLike();
    final var bytes = id.getBytes();
    final var original = Arrays.copyOf(bytes, bytes.length);

    final var id2 = new GuidLike(bytes);
    bytes[0] = 0;
    bytes[1] = 0;

    assertArrayEquals(id2.getBytes(), original);
  }

  @Test
  void testForDuplicates() {
    final var n = NB;
    final Set<String> uuids = new HashSet<>(n);
    final var uuidArray = new String[n];

    final var start = System.currentTimeMillis();
    for (var i = 0; i < n; i++) {
      uuidArray[i] = GuidLike.getGuid();
    }
    final var stop = System.currentTimeMillis();
    System.out.println("Time = " + (stop - start) + " so " + n / (stop - start) * 1000 + " Uuids/s");

    for (var i = 0; i < n; i++) {
      uuids.add(uuidArray[i]);
    }

    System.out.println("Create " + n + " and get: " + uuids.size());
    assertEquals(n, uuids.size());
  }

  @Test
  void concurrentGeneration() throws Exception {
    final var numThreads = NB_THREAD;
    final var threads = new Thread[numThreads];
    final var n = NB;
    final var effectiveN = n / numThreads * numThreads;
    final var uuids = new String[effectiveN];

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

    final Set<String> uuidSet = new HashSet<>(effectiveN);
    uuidSet.addAll(Arrays.asList(uuids));

    assertEquals(effectiveN, uuidSet.size());
  }

  static class Generator extends Thread {
    final int id;
    final int n;
    private final String[] uuids;

    Generator(final int n, final String[] uuids, final int id) {
      this.n = n;
      this.uuids = uuids;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        uuids[id + i] = GuidLike.getGuid();
      }
    }
  }
}
