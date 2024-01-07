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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class AccessorFilterTest {

  public static final String WRONG_CHAR = "wrong\b";
  public static final String WRONG_CDATA = "wrong<![CDATA[]]/>";

  @Test
  void converter() throws JsonProcessingException {
    final var accessorFilter = new AccessorFilter();
    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    accessorFilter.setNamePrefix("name").setStatuses(new AccessorStatus[]{AccessorStatus.READY}).setSizeLessThan(100)
        .setSizeGreaterThan(200).setCreationBefore(Instant.now()).setCreationAfter(Instant.now())
        .setExpiresAfter(Instant.now().plusSeconds(60)).setExpiresBefore(Instant.now().plusSeconds(60))
        .setMetadataFilter(map);
    Assertions.assertTrue(accessorFilter.toString().contains("value2"));
    Log.infof(accessorFilter.toString());
    final var accessorFilter1 =
        StandardProperties.getObjectMapper().readValue(accessorFilter.toString(), AccessorFilter.class);
    assertEquals(accessorFilter, accessorFilter1);
  }

  @Test
  void wrongValues() {
    final var accessorFilter = new AccessorFilter();
    try {
      accessorFilter.setNamePrefix(WRONG_CHAR);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorFilter.setNamePrefix(WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    accessorFilter.setNamePrefix("correctSite");
    final Map<String, String> map = new HashMap<>();
    map.put(WRONG_CHAR, "value");
    try {
      accessorFilter.setMetadataFilter(map);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    map.clear();
    map.put("key", WRONG_CDATA);
    try {
      accessorFilter.setMetadataFilter(map);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    map.clear();
    map.put("key1", "value1");
    accessorFilter.setMetadataFilter(map);
    try {
      accessorFilter.addMetadata(WRONG_CHAR, "value");
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    try {
      accessorFilter.addMetadata("key", WRONG_CDATA);
      Assertions.fail("Should failed");
    } catch (final CcsInvalidArgumentRuntimeException ignored) {
    }
    accessorFilter.addMetadata("key2", "value2");
    assertEquals("value1", accessorFilter.getMetadataFilter().get("key1"));
    assertEquals("value2", accessorFilter.getMetadataFilter().get("key2"));
  }

  @Test
  void checkEquals() {
    final var accessorFilter = new AccessorFilter();
    final var accessorFilter1 = new AccessorFilter();
    final var accessorObject = new AccessorObject();
    assertFalse(accessorFilter.equals(accessorObject));
    assertNotEquals(accessorFilter.hashCode(), accessorObject.hashCode());
    assertTrue(accessorFilter.equals(accessorFilter));
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setNamePrefix("idbucket");
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setNamePrefix("idbucket");
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setStatuses(new AccessorStatus[]{AccessorStatus.READY});
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setStatuses(new AccessorStatus[]{AccessorStatus.READY});
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setSizeGreaterThan(10);
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setSizeGreaterThan(10);
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setSizeLessThan(10);
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setSizeLessThan(10);
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setCreationAfter(Instant.now());
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setCreationAfter(accessorFilter.getCreationAfter());
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setCreationBefore(Instant.now());
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setCreationBefore(accessorFilter.getCreationBefore());
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setExpiresAfter(Instant.now());
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setExpiresAfter(accessorFilter.getExpiresAfter());
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter.setExpiresBefore(Instant.now());
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setExpiresBefore(accessorFilter.getExpiresBefore());
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    final var map = new HashMap<String, String>();
    map.put("key", "val");
    accessorFilter.setMetadataFilter(map);
    assertFalse(accessorFilter.equals(accessorFilter1));
    assertNotEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    accessorFilter1.setMetadataFilter(map);
    assertTrue(accessorFilter.equals(accessorFilter1));
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
  }
}
