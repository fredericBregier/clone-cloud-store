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

package io.clonecloudstore.test.stream;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class FakeIteratorTest {

  @Test
  void emptyIterator() {
    final AtomicLong cpt = new AtomicLong();
    var iterator = new FakeIterator<Long>(0, l -> l);
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> iterator.next());
    assertThrows(UnsupportedOperationException.class, () -> iterator.remove());
    iterator.forEachRemaining(l -> cpt.addAndGet(l + 1));
    assertEquals(0, cpt.get());
    iterator.close();
  }

  @Test
  void notEmptyIterator() {
    final AtomicLong cpt = new AtomicLong();
    try (var iterator = new FakeIterator<Long>(1, l -> l)) {
      assertTrue(iterator.hasNext());
      assertThrows(UnsupportedOperationException.class, () -> iterator.remove());
      iterator.forEachRemaining(l -> cpt.addAndGet(l + 1));
      assertEquals(1, cpt.get());
    }
    cpt.set(0);
    try (var iterator = new FakeIterator<Long>(10, l -> l)) {
      assertTrue(iterator.hasNext());
      iterator.forEachRemaining(l -> cpt.addAndGet(l + 1));
      assertEquals((10 * 11) / 2, cpt.get());
      assertFalse(iterator.hasNext());
      cpt.set(0);
      iterator.reset();
      assertTrue(iterator.hasNext());
      iterator.forEachRemaining(l -> cpt.addAndGet(l + 1));
      assertEquals((10 * 11) / 2, cpt.get());
      assertFalse(iterator.hasNext());
    }
  }
}
