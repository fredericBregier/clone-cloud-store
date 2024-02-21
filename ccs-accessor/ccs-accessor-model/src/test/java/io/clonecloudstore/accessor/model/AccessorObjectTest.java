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
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class AccessorObjectTest {

  public static final String WRONG_CHAR = "wrong\b";
  public static final String WRONG_CDATA = "wrong<![CDATA[]]/>";

  @Test
  void converter() {
    final var accessorObject = new AccessorObject();
    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    accessorObject.setId(GuidLike.getGuid()).setBucket("bucket").setName("name").setStatus(AccessorStatus.READY)
        .setSize(100).setHash("hash").setCreation(Instant.now()).setExpires(Instant.now().plusSeconds(60))
        .setSite("site").setMetadata(map);
    final var accessorObjectClone = accessorObject.cloneInstance();
    assertEquals(accessorObject, accessorObjectClone);
    assertEquals(accessorObject, accessorObject);
    Assertions.assertTrue(accessorObject.toString().contains("value2"));
    accessorObjectClone.addMetadata("key2", "value3");
    Assertions.assertNotEquals(accessorObject, accessorObjectClone);
  }

  @Test
  void wrongValues() {
    final var accessorObject = new AccessorObject();
    try {
      accessorObject.setSite(WRONG_CHAR);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setSite(WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    accessorObject.setSite("correctSite");
    try {
      accessorObject.setBucket(WRONG_CHAR);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setBucket(WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setBucket("BadUpperCase");
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setBucket("toolong".repeat(10));
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    accessorObject.setBucket("correct-name");
    accessorObject.setBucket("correctbucket");
    try {
      accessorObject.setName(WRONG_CHAR);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setName(WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setName("BadChars;");
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.setName("toolong".repeat(200));
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    accessorObject.setName("correct-Name._123-valid.ext");
    final Map<String, String> map = new HashMap<>();
    map.put(WRONG_CHAR, "value");
    try {
      accessorObject.setMetadata(map);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    map.clear();
    map.put("key", WRONG_CDATA);
    try {
      accessorObject.setMetadata(map);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.getMetadata(WRONG_CHAR);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorObject.getMetadata(WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    map.clear();
    map.put("key1", "value1");
    accessorObject.setMetadata(map);
    assertEquals("value1", accessorObject.getMetadata("key1"));
    Assertions.assertNull(accessorObject.getMetadata("key2"));
    assertEquals(1, accessorObject.getMetadata().size());
    accessorObject.addMetadata("key2", "value2");
    assertEquals(2, accessorObject.getMetadata().size());
    assertEquals("value2", accessorObject.getMetadata("key2"));
  }

  @Test
  void checkEquals() {
    final var accessorObject = new AccessorObject();
    final var accessorObject1 = new AccessorObject();
    final var accessorBucket = new AccessorBucket();
    assertFalse(accessorObject.equals(accessorBucket));
    assertNotEquals(accessorObject.hashCode(), accessorBucket.hashCode());
    assertTrue(accessorObject.equals(accessorObject));
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setId("idbucket");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setId("idbucket");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setSite("id");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setSite("id");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setName("idbucket");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setName("idbucket");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setStatus(AccessorStatus.READY);
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setStatus(AccessorStatus.READY);
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setCreation(Instant.now());
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setCreation(accessorObject.getCreation());
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setExpires(Instant.now());
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setExpires(accessorObject.getExpires());
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setSize(1);
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setSize(1);
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setBucket("idbucket");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setBucket("idbucket");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setHash("hash");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setHash("hash");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    final var map = new HashMap<String, String>();
    map.put("key", "val");
    accessorObject.setMetadata(map);
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setMetadata(map);
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
  }
}
