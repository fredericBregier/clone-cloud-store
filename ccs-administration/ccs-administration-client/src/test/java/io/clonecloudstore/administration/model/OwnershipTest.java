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

package io.clonecloudstore.administration.model;

import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class OwnershipTest {
  private static final Logger logger = Logger.getLogger(OwnershipTest.class);

  @Test
  void checkConversion() {
    logger.debugf("Testing conversion...");
    assertEquals(ClientOwnership.READ, ClientOwnership.fromStatusCode(ClientOwnership.READ.getCode()));
    assertEquals(ClientOwnership.WRITE, ClientOwnership.fromStatusCode(ClientOwnership.WRITE.getCode()));
    assertEquals(ClientOwnership.READ_WRITE, ClientOwnership.fromStatusCode(ClientOwnership.READ_WRITE.getCode()));
    assertEquals(ClientOwnership.DELETE, ClientOwnership.fromStatusCode(ClientOwnership.DELETE.getCode()));
    assertEquals(ClientOwnership.DELETE_READ, ClientOwnership.fromStatusCode(ClientOwnership.DELETE_READ.getCode()));
    assertEquals(ClientOwnership.DELETE_WRITE, ClientOwnership.fromStatusCode(ClientOwnership.DELETE_WRITE.getCode()));
    assertEquals(ClientOwnership.OWNER, ClientOwnership.fromStatusCode(ClientOwnership.OWNER.getCode()));
    assertEquals(ClientOwnership.UNKNOWN, ClientOwnership.fromStatusCode(ClientOwnership.UNKNOWN.getCode()));
    assertEquals(TopologyStatus.UNKNOWN, TopologyStatus.fromStatusCode((short) -1));
  }

  @Test
  void checkFusion() {
    logger.debugf("Testing fusion...");
    assertEquals(ClientOwnership.UNKNOWN, ClientOwnership.UNKNOWN.fusion(ClientOwnership.UNKNOWN));
    assertEquals(ClientOwnership.READ, ClientOwnership.UNKNOWN.fusion(ClientOwnership.READ));
    assertEquals(ClientOwnership.WRITE, ClientOwnership.UNKNOWN.fusion(ClientOwnership.WRITE));
    assertEquals(ClientOwnership.DELETE, ClientOwnership.UNKNOWN.fusion(ClientOwnership.DELETE));

    assertEquals(ClientOwnership.READ, ClientOwnership.READ.fusion(ClientOwnership.UNKNOWN));
    assertEquals(ClientOwnership.READ, ClientOwnership.READ.fusion(ClientOwnership.READ));
    assertEquals(ClientOwnership.READ_WRITE, ClientOwnership.READ.fusion(ClientOwnership.WRITE));
    assertEquals(ClientOwnership.DELETE_READ, ClientOwnership.READ.fusion(ClientOwnership.DELETE));

    assertEquals(ClientOwnership.WRITE, ClientOwnership.WRITE.fusion(ClientOwnership.UNKNOWN));
    assertEquals(ClientOwnership.READ_WRITE, ClientOwnership.WRITE.fusion(ClientOwnership.READ));
    assertEquals(ClientOwnership.WRITE, ClientOwnership.WRITE.fusion(ClientOwnership.WRITE));
    assertEquals(ClientOwnership.DELETE_WRITE, ClientOwnership.WRITE.fusion(ClientOwnership.DELETE));

    assertEquals(ClientOwnership.DELETE, ClientOwnership.DELETE.fusion(ClientOwnership.UNKNOWN));
    assertEquals(ClientOwnership.DELETE_READ, ClientOwnership.DELETE.fusion(ClientOwnership.READ));
    assertEquals(ClientOwnership.DELETE_WRITE, ClientOwnership.DELETE.fusion(ClientOwnership.WRITE));
    assertEquals(ClientOwnership.DELETE, ClientOwnership.DELETE.fusion(ClientOwnership.DELETE));

    assertEquals(ClientOwnership.OWNER, ClientOwnership.OWNER.fusion(ClientOwnership.UNKNOWN));
    assertEquals(ClientOwnership.OWNER, ClientOwnership.OWNER.fusion(ClientOwnership.READ));
    assertEquals(ClientOwnership.OWNER, ClientOwnership.OWNER.fusion(ClientOwnership.WRITE));
    assertEquals(ClientOwnership.OWNER, ClientOwnership.OWNER.fusion(ClientOwnership.DELETE));
  }

  @Test
  void checkComparison() {
    logger.debugf("Testing comparison...");
    assertTrue(ClientOwnership.READ.include(ClientOwnership.READ));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.WRITE));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.READ_WRITE));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.DELETE));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.DELETE_READ));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.READ.include(ClientOwnership.UNKNOWN));

    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.READ));
    assertTrue(ClientOwnership.WRITE.include(ClientOwnership.WRITE));
    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.READ_WRITE));
    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.DELETE));
    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.DELETE_READ));
    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.WRITE.include(ClientOwnership.UNKNOWN));

    assertTrue(ClientOwnership.READ_WRITE.include(ClientOwnership.READ));
    assertTrue(ClientOwnership.READ_WRITE.include(ClientOwnership.WRITE));
    assertTrue(ClientOwnership.READ_WRITE.include(ClientOwnership.READ_WRITE));
    assertFalse(ClientOwnership.READ_WRITE.include(ClientOwnership.DELETE));
    assertFalse(ClientOwnership.READ_WRITE.include(ClientOwnership.DELETE_READ));
    assertFalse(ClientOwnership.READ_WRITE.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.READ_WRITE.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.READ_WRITE.include(ClientOwnership.UNKNOWN));

    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.READ));
    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.WRITE));
    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.READ_WRITE));
    assertTrue(ClientOwnership.DELETE.include(ClientOwnership.DELETE));
    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.DELETE_READ));
    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.DELETE.include(ClientOwnership.UNKNOWN));

    assertTrue(ClientOwnership.DELETE_READ.include(ClientOwnership.READ));
    assertFalse(ClientOwnership.DELETE_READ.include(ClientOwnership.WRITE));
    assertFalse(ClientOwnership.DELETE_READ.include(ClientOwnership.READ_WRITE));
    assertTrue(ClientOwnership.DELETE_READ.include(ClientOwnership.DELETE));
    assertTrue(ClientOwnership.DELETE_READ.include(ClientOwnership.DELETE_READ));
    assertFalse(ClientOwnership.DELETE_READ.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.DELETE_READ.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.DELETE_READ.include(ClientOwnership.UNKNOWN));

    assertFalse(ClientOwnership.DELETE_WRITE.include(ClientOwnership.READ));
    assertTrue(ClientOwnership.DELETE_WRITE.include(ClientOwnership.WRITE));
    assertFalse(ClientOwnership.DELETE_WRITE.include(ClientOwnership.READ_WRITE));
    assertTrue(ClientOwnership.DELETE_WRITE.include(ClientOwnership.DELETE));
    assertFalse(ClientOwnership.DELETE_WRITE.include(ClientOwnership.DELETE_READ));
    assertTrue(ClientOwnership.DELETE_WRITE.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.DELETE_WRITE.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.DELETE_WRITE.include(ClientOwnership.UNKNOWN));

    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.READ));
    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.WRITE));
    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.READ_WRITE));
    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.DELETE));
    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.DELETE_READ));
    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.DELETE_WRITE));
    assertTrue(ClientOwnership.OWNER.include(ClientOwnership.OWNER));
    assertFalse(ClientOwnership.OWNER.include(ClientOwnership.UNKNOWN));

    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.READ));
    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.WRITE));
    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.READ_WRITE));
    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.DELETE));
    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.DELETE_READ));
    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.DELETE_WRITE));
    assertFalse(ClientOwnership.UNKNOWN.include(ClientOwnership.OWNER));
    assertTrue(ClientOwnership.UNKNOWN.include(ClientOwnership.UNKNOWN));
  }
}
