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

package io.clonecloudstore.driver.api;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class StorageObjectTest {

  @Test
  void checkBean() {
    final var object = new StorageObject("name", "objectName", "hash", 1024, Instant.now());
    final var object1 =
        new StorageObject(object.bucket(), object.name(), object.hash(), object.size(), object.creationDate());
    assertEquals(object1, object);
    assertEquals(object1, object);
    assertEquals(object1.hashCode(), object.hashCode());
    assertEquals(object1, object1);
    assertTrue(object1.toString().contains(object1.bucket()));
    assertTrue(object1.toString().contains(object1.creationDate().toString()));
    final Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    final var object2 =
        new StorageObject(object.bucket(), object.name(), object.hash(), object.size(), object.creationDate(), null,
            map);
    assertEquals(1, object2.metadata().size());
    Log.info(object2);
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new StorageObject("test@test", object.name(), object.hash(), object.size(), object.creationDate()));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new StorageObject(object.bucket(), "test@test", object.hash(), object.size(), object.creationDate()));

    StorageObject object3 = new StorageObject(null, null, null, 0, null);
    assertNotEquals(object, object3);
    assertEquals(-1, object3.hashCode());
    object3 = new StorageObject(object.bucket(), null, null, 0, null);
    assertNotEquals(object, object3);
    object3 = new StorageObject(null, object.name(), null, 0, null);
    assertNotEquals(object, object3);
    assertFalse(object.equals(new Object()));
  }

  @Test
  void checkTypeBean() throws JsonProcessingException {
    assertTrue(StorageType.NONE.ordinal() < StorageType.BUCKET.ordinal());
    assertTrue(StorageType.BUCKET.ordinal() < StorageType.DIRECTORY.ordinal());
    assertTrue(StorageType.DIRECTORY.ordinal() < StorageType.OBJECT.ordinal());
    assertNotNull(JsonUtil.getInstance().writeValueAsString(StorageType.OBJECT));
  }
}
