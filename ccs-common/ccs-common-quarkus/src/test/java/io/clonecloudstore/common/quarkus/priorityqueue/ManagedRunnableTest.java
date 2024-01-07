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

package io.clonecloudstore.common.quarkus.priorityqueue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.UnaryOperator;

import io.clonecloudstore.common.standard.guid.GuidLike;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ManagedRunnableTest {
  private static final Comparator<ElementTest> comparator = Comparator.comparingLong(o -> o.rank);
  private static final Comparator<ElementTest> findEquals = Comparator.comparing(o -> o.uniqueId);
  private static final UnaryOperator<ElementTest> reprioritize = e -> {
    e.rank /= 2;
    return e;
  };
  private static final int MAX = 10;
  private static final int NB = 50;
  private static final int LIMIT_CPT = 5;

  @Test
  void testRunnerWith1() {
    final var managedRunnable = new ManagedRunnable<ElementTest>(1, comparator, findEquals, reprioritize);
    assertEquals(0, managedRunnable.size());
    assertTrue(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    final var e1 = new ElementTest(10);
    final var e2 = new ElementTest(20);
    final var e3 = new ElementTest(30);
    final var e4 = new ElementTest(38);
    final var e5 = new ElementTest(50);
    final List<ElementTest> list = new ArrayList<>();
    list.add(e2);
    list.add(e3);
    list.add(e4);
    list.add(e5);
    managedRunnable.addAll(list);
    // E2 = 20, E3 = 30, E4 = 38, E5 = 50
    var e = managedRunnable.poll();
    // E2 = 10 / E3 = 15, E4 = 19, E5 = 25
    assertEquals(10, e.rank);
    assertEquals(e2.uniqueId, e.uniqueId);
    e = managedRunnable.poll();
    // E2 = 10, E3 = 7 / E4 = 9, E5 = 12
    assertEquals(e3.uniqueId, e.uniqueId);
    assertEquals(7, e.rank);
    managedRunnable.add(e1);
    // E2 = 10, E3 = 7 / E4 = 9, E5 = 12, E1 = 10
    e = managedRunnable.poll();
    // E2 = 10, E3 = 7, E4 = 4 / E5 = 6, E1 = 5
    assertEquals(e4.uniqueId, e.uniqueId);
    assertEquals(4, e.rank);
    e = managedRunnable.poll();
    // E2 = 10, E3 = 7, E4 = 4, E1 = 2 / E5 = 3
    assertEquals(e1.uniqueId, e.uniqueId);
    assertEquals(2, e.rank);
    e = managedRunnable.poll();
    // E2 = 10, E3 = 7, E4 = 4, E1 = 2, E5 = 1 /
    assertEquals(e5.uniqueId, e.uniqueId);
    assertEquals(1, e.rank);
    e = managedRunnable.poll();
    assertNull(e);
    managedRunnable.clear();
  }

  @Test
  void testPolling() {
    final var managedRunnable = new ManagedRunnable<ElementTest>(MAX, comparator, findEquals, reprioritize);
    assertEquals(0, managedRunnable.size());
    assertTrue(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    managedRunnable.add(new ElementTest(0));
    assertEquals(1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    managedRunnable.clear();
    assertEquals(0, managedRunnable.size());
    assertTrue(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    for (var i = 0; i < NB; i++) {
      final var e = new ElementTest(i);
      managedRunnable.add(e);
    }
    assertEquals(NB, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());

    Log.info("Start");
    long global = 0;
    var e = managedRunnable.poll();
    assertNotNull(e);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(0, e.rank);
    e.cpt++;
    global++;
    managedRunnable.addContinue(e);
    assertTrue(managedRunnable.isRunnerFull());
    e = managedRunnable.poll();
    assertNotNull(e);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(MAX - 1, managedRunnable.runnerSize());
    assertEquals(0, e.rank);
    e.cpt++;
    global++;
    managedRunnable.addContinue(e);
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(MAX, managedRunnable.runnerSize());
    while (true) {
      e = managedRunnable.poll();
      assertNotNull(e);
      assertEquals(NB - 1, managedRunnable.size());
      assertFalse(managedRunnable.isEmpty());
      assertFalse(managedRunnable.isRunnerFull());
      assertEquals(MAX - 1, managedRunnable.runnerSize());
      e.cpt++;
      if (e.cpt <= LIMIT_CPT) {
        global++;
        managedRunnable.addContinue(e);
      } else {
        break;
      }
    }
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(MAX - 1, managedRunnable.runnerSize());
    managedRunnable.add(e);
    assertEquals(NB, managedRunnable.size());
    assertTrue(managedRunnable.contains(e));
    assertTrue(managedRunnable.remove(e));
    assertFalse(managedRunnable.isRunnerFull());
    e = managedRunnable.poll();
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(NB - 2, managedRunnable.size());
    assertFalse(managedRunnable.contains(e));
    assertFalse(managedRunnable.remove(e));
    managedRunnable.addContinue(e);
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(NB - 1, managedRunnable.size());
    assertTrue(managedRunnable.contains(e));
    assertTrue(managedRunnable.remove(e));
    assertEquals(NB - 2, managedRunnable.size());
    assertFalse(managedRunnable.isRunnerFull());
    managedRunnable.addContinue(e);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertTrue(managedRunnable.isRunnerFull());
    while (true) {
      e = managedRunnable.poll();
      if (e == null) {
        break;
      }
      assertFalse(managedRunnable.isRunnerFull());
      e.cpt++;
      if (e.cpt <= LIMIT_CPT) {
        global++;
        managedRunnable.addContinue(e);
      }
    }
    assertTrue(managedRunnable.isEmpty());
    managedRunnable.addContinue(null);
    assertTrue(managedRunnable.isEmpty());
    Log.info("End: " + global);
    assertEquals(NB * LIMIT_CPT, global);
  }

  @Test
  void testRunnerFullBeforeContinue() {
    final var managedRunnable = new ManagedRunnable<ElementTest>(MAX, comparator, findEquals, reprioritize);
    assertEquals(0, managedRunnable.size());
    assertTrue(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    for (var i = 0; i < NB; i++) {
      final var e = new ElementTest(i);
      managedRunnable.add(e);
    }
    assertEquals(NB, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());

    Log.info("Start");
    long global = 0;
    final var e1 = managedRunnable.poll();
    assertNotNull(e1);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(MAX - 1, managedRunnable.runnerSize());
    assertEquals(0, e1.rank);
    e1.cpt++;
    global++;
    final var e2 = managedRunnable.poll();
    assertNotNull(e2);
    assertEquals(NB - 2, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(MAX - 1, managedRunnable.runnerSize());
    assertEquals(0, e2.rank);
    e2.cpt++;
    global++;
    managedRunnable.addContinue(e2);
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(MAX, managedRunnable.runnerSize());
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    managedRunnable.addContinue(e1);
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(MAX + 1, managedRunnable.runnerSize());
    assertEquals(NB, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());

    while (true) {
      final var e = managedRunnable.poll();
      if (e == null) {
        break;
      }
      e.cpt++;
      if (e.cpt <= LIMIT_CPT) {
        global++;
        managedRunnable.addContinue(e);
      }
    }
    assertTrue(managedRunnable.isEmpty());
    Log.info("End: " + global);
    assertEquals(NB * LIMIT_CPT, global);
  }

  @Test
  void testRunnerWith1FullBeforeContinue() {
    final var managedRunnable = new ManagedRunnable<ElementTest>(1, comparator, findEquals, reprioritize);
    assertEquals(0, managedRunnable.size());
    assertTrue(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    for (var i = 0; i < NB; i++) {
      final var e = new ElementTest(i);
      managedRunnable.add(e);
    }
    assertEquals(NB, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());

    Log.info("Start");
    long global = 0;
    var e1 = managedRunnable.poll();
    assertNotNull(e1);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(0, managedRunnable.runnerSize());
    assertEquals(0, e1.rank);
    e1.cpt++;
    global++;
    managedRunnable.addContinue(e1);
    assertEquals(NB, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(1, managedRunnable.runnerSize());
    e1 = managedRunnable.poll();
    assertNotNull(e1);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(0, managedRunnable.runnerSize());
    assertEquals(0, e1.rank);
    e1.cpt++;
    global++;
    var e2 = managedRunnable.poll();
    assertNotNull(e2);
    assertEquals(NB - 2, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertFalse(managedRunnable.isRunnerFull());
    assertEquals(0, managedRunnable.runnerSize());
    assertEquals(0, e2.rank);
    e2.cpt++;
    global++;
    managedRunnable.addContinue(e2);
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(1, managedRunnable.runnerSize());
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    managedRunnable.addContinue(e1);
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(NB, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertEquals(2, managedRunnable.runnerSize());
    e2 = managedRunnable.poll();
    assertNotNull(e2);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(1, managedRunnable.runnerSize());
    assertEquals(0, e2.rank);
    e2.cpt++;
    global++;
    managedRunnable.addContinue(e2);
    e1 = managedRunnable.poll();
    assertNotNull(e1);
    assertEquals(NB - 1, managedRunnable.size());
    assertFalse(managedRunnable.isEmpty());
    assertTrue(managedRunnable.isRunnerFull());
    assertEquals(1, managedRunnable.runnerSize());
    assertEquals(0, e1.rank);
    e1.cpt++;
    global++;
    managedRunnable.addContinue(e1);
    assertEquals(2, managedRunnable.runnerSize());
    while (true) {
      final var e = managedRunnable.poll();
      if (e == null) {
        break;
      }
      e.cpt++;
      if (e.cpt <= LIMIT_CPT) {
        global++;
        managedRunnable.addContinue(e);
      }
    }
    assertTrue(managedRunnable.isEmpty());
    Log.info("End: " + global);
    assertEquals(NB * LIMIT_CPT, global);
  }

  private static class ElementTest {
    final String uniqueId;
    long rank;
    int cpt = 0;

    private ElementTest(final int rank) {
      uniqueId = GuidLike.getGuid();
      this.rank = rank;
    }
  }
}
