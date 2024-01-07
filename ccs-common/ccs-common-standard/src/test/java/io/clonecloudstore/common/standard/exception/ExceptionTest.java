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

package io.clonecloudstore.common.standard.exception;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ExceptionTest {

  @Test
  void checkException() {
    final var e = new CcsWithStatusException(null, 0);
    assertNull(e.getBusinessIn());
    assertEquals(0, e.getStatus());
    assertNull(e.getCause());
    assertTrue(e.getMessage().contains("Status:"));
    assertEquals(2, e.getMessage().split("Status").length);

    final var o = new Object();
    var e2 = new CcsWithStatusException(o, 2, "mesg", e);
    assertEquals(o, e2.getBusinessIn());
    assertEquals(2, e2.getStatus());
    assertTrue(e2.getMessage().contains("mesg"));
    assertTrue(e2.getMessage().contains("Status:"));
    assertEquals(2, e2.getMessage().split("Status").length);
    assertEquals(e, e2.getCause());

    e2 = new CcsWithStatusException(o, 3, e);
    assertEquals(o, e2.getBusinessIn());
    assertEquals(3, e2.getStatus());
    assertTrue(e2.getMessage().contains(e.getMessage()));
    assertEquals(e, e2.getCause());

    final var exception = new Exception("Trace");
    e2 = new CcsWithStatusException(o, 3, exception);
    assertEquals(o, e2.getBusinessIn());
    assertEquals(3, e2.getStatus());
    assertTrue(e2.getMessage().contains(exception.getMessage()));
    assertEquals(exception, e2.getCause());
    assertTrue(e2.getMessage().contains("Status:"));
    assertEquals(2, e2.getMessage().split("Status").length);
  }

  @Test
  void testRuntimeException() {
    final var e = new CcsInvalidArgumentRuntimeException("mesg");
    assertEquals("mesg", e.getMessage());
    assertNull(e.getCause());

    final var e2 = new CcsInvalidArgumentRuntimeException("mesg", e);
    assertTrue(e2.getMessage().contains(e.getMessage()));
    assertEquals(e, e2.getCause());
  }
}
