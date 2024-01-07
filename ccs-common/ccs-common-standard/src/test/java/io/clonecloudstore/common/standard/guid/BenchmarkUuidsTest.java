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

import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class BenchmarkUuidsTest {
  private static final int NB = 5000000;

  @Test
  void concurrentGeneration() throws Exception {
    final var numThreads = 10;
    final var threads = new Thread[numThreads];
    final var n = NB;
    final var effectiveN = n / numThreads * numThreads;

    // Warmup
    var timems = setGuidLikes(threads, effectiveN, n, numThreads);
    System.out.println(
        "WarmUp GuidLike: Time = " + timems + " so " + n / timems * 1000 + " Uuids/s KeySize:" + GuidLike.getKeySize());
    timems = setLongUuids(threads, effectiveN, n, numThreads);
    System.out.println(
        "WarmUp LongUuid: Time = " + timems + " so " + n / timems * 1000 + " Uuids/s KeySize:" + LongUuid.getKeySize());
    timems = setUuids(threads, effectiveN, n, numThreads);
    System.out.println("WarmUp Uuid: Time = " + timems + " so " + n / timems * 1000 + " Uuids/s KeySize:" +
        UUID.randomUUID().toString().length());


    timems = setLongUuids(threads, effectiveN, n, numThreads);
    System.out.println(
        "LongUuid: Time = " + timems + " so " + n / timems * 1000 + " Uuids/s KeySize:" + LongUuid.getKeySize());
    timems = setUuids(threads, effectiveN, n, numThreads);
    System.out.println("Uuid: Time = " + timems + " so " + n / timems * 1000 + " Uuids/s KeySize:" +
        UUID.randomUUID().toString().length());
    timems = setGuidLikes(threads, effectiveN, n, numThreads);
    GuidLike guidLike = new GuidLike();
    System.out.println(
        "GuidLike: Time = " + timems + " so " + n / timems * 1000 + " Uuids/s KeySize:" + GuidLike.getKeySize() + " " +
            "[" + guidLike.toHex().length() + " " + guidLike.toBase32().length() + " " + guidLike.toBase64().length() +
            "]");
    assertTrue(timems > 0);
  }

  private long setLongUuids(final Thread[] threads, final int effectiveN, final int n, final int numThreads)
      throws InterruptedException {
    final var start = System.currentTimeMillis();
    for (var i = 0; i < numThreads; i++) {
      threads[i] = new Generator(n / numThreads, i);
      threads[i].start();
    }
    for (var i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    final var stop = System.currentTimeMillis();
    return stop - start;
  }

  static class Generator extends Thread {
    final int id;
    final int n;

    Generator(final int n, final int id) {
      this.n = n;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        new LongUuid().toString();
      }
    }
  }

  private long setUuids(final Thread[] threads, final int effectiveN, final int n, final int numThreads)
      throws InterruptedException {
    final var start = System.currentTimeMillis();
    for (var i = 0; i < numThreads; i++) {
      threads[i] = new GeneratorUuid(n / numThreads, i);
      threads[i].start();
    }
    for (var i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    final var stop = System.currentTimeMillis();
    return stop - start;
  }

  static class GeneratorUuid extends Thread {
    final int id;
    final int n;

    GeneratorUuid(final int n, final int id) {
      this.n = n;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        UUID.randomUUID().toString();
      }
    }
  }

  private long setGuidLikes(final Thread[] threads, final int effectiveN, final int n, final int numThreads)
      throws InterruptedException {
    final var start = System.currentTimeMillis();
    for (var i = 0; i < numThreads; i++) {
      threads[i] = new GeneratorGuidLike(n / numThreads, i);
      threads[i].start();
    }
    for (var i = 0; i < numThreads; i++) {
      threads[i].join();
    }
    final var stop = System.currentTimeMillis();
    return stop - start;
  }

  static class GeneratorGuidLike extends Thread {
    final int id;
    final int n;

    GeneratorGuidLike(final int n, final int id) {
      this.n = n;
      this.id = id * n;
    }

    @Override
    public void run() {
      for (var i = 0; i < n; i++) {
        GuidLike.getGuid();
      }
    }
  }
}
