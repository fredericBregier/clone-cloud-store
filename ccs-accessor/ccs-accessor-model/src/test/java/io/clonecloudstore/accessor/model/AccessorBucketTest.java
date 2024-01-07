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

package io.clonecloudstore.accessor.model;

import java.time.Instant;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class AccessorBucketTest {

  public static final String WRONG_CHAR = "wrong\b";
  public static final String WRONG_CDATA = "wrong<![CDATA[]]/>";

  @Test
  void converter() {
    final var accessorBucket = new AccessorBucket();
    accessorBucket.setName("bucket").setStatus(AccessorStatus.READY).setCreation(Instant.now())
        .setExpires(Instant.now().plusSeconds(60)).setSite("site");
    accessorBucket.setId("id-" + accessorBucket.getName());
    final var accessorBucket1 = accessorBucket.cloneInstance();
    assertEquals(accessorBucket, accessorBucket1);
    assertEquals(accessorBucket, accessorBucket);
    Assertions.assertTrue(accessorBucket.toString().contains("site"));
    accessorBucket1.setSite("newsite");
    Assertions.assertNotEquals(accessorBucket, accessorBucket1);
  }

  @Test
  void statusTest() {
    assertEquals(AccessorStatus.ERR_DEL.name(), AccessorStatus.toString(AccessorStatus.ERR_DEL));
    assertEquals(AccessorStatus.UNKNOWN.name(), AccessorStatus.toString(AccessorStatus.UNKNOWN));
    assertEquals(AccessorStatus.UPLOAD, AccessorStatus.fromStatusCode(AccessorStatus.UPLOAD.getStatus()));
    assertEquals(AccessorStatus.READY, AccessorStatus.fromStatusCode(AccessorStatus.READY.getStatus()));
    assertEquals(AccessorStatus.DELETING, AccessorStatus.fromStatusCode(AccessorStatus.DELETING.getStatus()));
    assertEquals(AccessorStatus.DELETED, AccessorStatus.fromStatusCode(AccessorStatus.DELETED.getStatus()));
    assertEquals(AccessorStatus.ERR_UPL, AccessorStatus.fromStatusCode(AccessorStatus.ERR_UPL.getStatus()));
    assertEquals(AccessorStatus.ERR_DEL, AccessorStatus.fromStatusCode(AccessorStatus.ERR_DEL.getStatus()));
    assertEquals(AccessorStatus.UNKNOWN, AccessorStatus.fromStatusCode(AccessorStatus.UNKNOWN.getStatus()));
    assertEquals(AccessorStatus.UNKNOWN, AccessorStatus.fromStatusCode((short) -1));
  }

  @Test
  void wrongValues() {
    final var accessorBucket = new AccessorBucket();
    accessorBucket.setSite("correctSite");
    try {
      accessorBucket.setName(WRONG_CHAR);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorBucket.setName(WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorBucket.setName("BadUpperCase");
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorBucket.setName("toolong".repeat(10));
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    accessorBucket.setName("correct-bucket-0");
  }

  @Test
  void checkEquals() {
    final var accessorBucket = new AccessorBucket();
    final var accessorBucket2 = new AccessorBucket();
    final var accessorObject = new AccessorObject();
    assertFalse(accessorBucket.equals(accessorObject));
    assertNotEquals(accessorBucket.hashCode(), accessorObject.hashCode());
    assertTrue(accessorBucket.equals(accessorBucket));
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setId("idbucket");
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setId("idbucket");
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setSite("id");
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setSite("id");
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setName("idbucket");
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setName("idbucket");
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setStatus(AccessorStatus.READY);
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setStatus(AccessorStatus.READY);
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setCreation(Instant.now());
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setCreation(accessorBucket.getCreation());
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setExpires(Instant.now());
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setExpires(accessorBucket.getExpires());
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
  }
}
