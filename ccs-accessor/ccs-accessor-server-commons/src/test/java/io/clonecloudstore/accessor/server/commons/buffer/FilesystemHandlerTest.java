/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.accessor.server.commons.buffer;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.accessor.server.commons.buffer.FilesystemHandler.X_EXPIRES;
import static io.clonecloudstore.accessor.server.commons.buffer.FilesystemHandler.X_HASH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class FilesystemHandlerTest {
  @Inject
  FilesystemHandler inputStreamHandler;

  @BeforeEach
  void cleanUp() throws IOException {
    FsTestUtil.cleanUp();
    inputStreamHandler.changeHasDatabase(false);
  }

  @Test
  void checkUsingFsInputHandlerEmpty() {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    final var now = Instant.now();
    assertFalse(inputStreamHandler.check(bucket, object));
    assertFalse(assertDoesNotThrow(() -> inputStreamHandler.delete(bucket, object)));
    assertThrows(IOException.class, () -> inputStreamHandler.readContent(bucket, object));
    assertThrows(IOException.class, () -> inputStreamHandler.readMetadata(bucket, object));
    assertThrows(IOException.class, () -> inputStreamHandler.update(bucket, object, null, null));
    assertDoesNotThrow(() -> inputStreamHandler.checkFreeSpaceGb(0));
    assertThrows(IOException.class, () -> inputStreamHandler.checkFreeSpaceGb(Long.MAX_VALUE));
    assertThrows(IOException.class, () -> inputStreamHandler.readStorageObject(bucket, object));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.deleteOlderThan(now)));
  }

  @Test
  void checkUsingFsInputHandlerNotEmpty() {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    assertEquals(100,
        assertDoesNotThrow(() -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), map, null)));

    assertTrue(inputStreamHandler.check(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
    assertEquals(100, assertDoesNotThrow(
        () -> FakeInputStream.consumeAll(assertDoesNotThrow(() -> inputStreamHandler.readContent(bucket, object)))));
    var map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertEquals(map, map2);
    final var map3 = Map.of("key1", "value1", "key2", "value2");
    assertDoesNotThrow(() -> inputStreamHandler.update(bucket, object, map3, null));
    map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertEquals(map3, map2);
    var sto = assertDoesNotThrow(() -> inputStreamHandler.readStorageObject(bucket, object));
    assertEquals(bucket, sto.bucket());
    assertEquals(object, sto.name());
    assertEquals(100, sto.size());
    assertEquals(map3, sto.metadata());
    assertNull(sto.expiresDate());
    assertNull(sto.hash());
    assertTrue(Instant.now().isAfter(sto.creationDate()));
    assertTrue(assertDoesNotThrow(() -> inputStreamHandler.delete(bucket, object)));
    checkUsingFsInputHandlerEmpty();
  }

  @Test
  void checkUsingFsInputHandlerAlreadyExists() {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    assertEquals(100, assertDoesNotThrow(
        () -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), map, Instant.now())));
    var map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertNotEquals(map, map2);
    map2.remove(X_EXPIRES);
    assertEquals(map, map2);
    var sto = assertDoesNotThrow(() -> inputStreamHandler.readStorageObject(bucket, object));
    assertEquals(bucket, sto.bucket());
    assertEquals(object, sto.name());
    assertEquals(100, sto.size());
    assertEquals(map, sto.metadata());
    assertTrue(Instant.now().isAfter(sto.expiresDate()));
    assertNull(sto.hash());
    assertTrue(Instant.now().isAfter(sto.creationDate()));

    assertTrue(inputStreamHandler.check(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));

    assertDoesNotThrow(() -> inputStreamHandler.update(bucket, object, map, "hash"));
    sto = assertDoesNotThrow(() -> inputStreamHandler.readStorageObject(bucket, object));
    assertEquals(bucket, sto.bucket());
    assertEquals(object, sto.name());
    assertEquals(100, sto.size());
    assertEquals(map, sto.metadata());
    assertTrue(Instant.now().isAfter(sto.expiresDate()));
    assertEquals("hash", sto.hash());
    assertTrue(Instant.now().isAfter(sto.creationDate()));

    assertThrows(IOException.class, () -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), map, null));

    assertDoesNotThrow(() -> inputStreamHandler.delete(bucket, object));
    checkUsingFsInputHandlerEmpty();
  }

  @Test
  void checkUsingFsInputHandlerEmptyMetadata() {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    final var map = new HashMap<String, String>();
    assertEquals(100,
        assertDoesNotThrow(() -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), map, null)));

    assertTrue(inputStreamHandler.check(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
    var map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertTrue(map2.isEmpty());
    final var map3 = Map.of("key1", "value1", "key2", "value2");
    assertDoesNotThrow(() -> inputStreamHandler.update(bucket, object, map3, "hash"));
    map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertNotEquals(map3, map2);
    map2.remove(X_HASH);
    assertEquals(map3, map2);
    assertDoesNotThrow(() -> inputStreamHandler.update(bucket, object, map, null));
    map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertNotEquals(map3, map2);
    map2.remove(X_HASH);
    assertEquals(map3, map2);

    assertDoesNotThrow(() -> inputStreamHandler.delete(bucket, object));
    checkUsingFsInputHandlerEmpty();
  }

  @Test
  void checkUsingFsInputHandlerNullMetadata() {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    assertEquals(100,
        assertDoesNotThrow(() -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), null, null)));

    assertTrue(inputStreamHandler.check(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
    var map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertTrue(map2.isEmpty());
    final var map3 = Map.of("key1", "value1", "key2", "value2");
    assertDoesNotThrow(() -> inputStreamHandler.update(bucket, object, map3, null));
    map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertEquals(map3, map2);
    assertDoesNotThrow(() -> inputStreamHandler.update(bucket, object, null, null));
    map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertFalse(map2.isEmpty());
    assertEquals(map3, map2);

    assertDoesNotThrow(() -> inputStreamHandler.delete(bucket, object));
    checkUsingFsInputHandlerEmpty();
  }

  @Test
  void checkUsingFsInputHandlerNotEmptyButDeleteOld() throws InterruptedException {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    assertEquals(100,
        assertDoesNotThrow(() -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), map, null)));
    Thread.sleep(10);
    final var now = Instant.now();

    assertTrue(inputStreamHandler.check(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
    assertEquals(100, assertDoesNotThrow(
        () -> FakeInputStream.consumeAll(assertDoesNotThrow(() -> inputStreamHandler.readContent(bucket, object)))));
    var map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertEquals(map, map2);
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.deleteOlderThan(now)));
    checkUsingFsInputHandlerEmpty();
  }

  @Test
  void checkUsingFsInputHandlerNotEmptyButDeleteOldButNotNewer() throws InterruptedException {
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.size()));
    final var bucket = "name";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    assertEquals(100,
        assertDoesNotThrow(() -> inputStreamHandler.save(bucket, object, new FakeInputStream(100), map, null)));
    Thread.sleep(10);
    final var now = Instant.now();
    Thread.sleep(10);
    assertEquals(100,
        assertDoesNotThrow(() -> inputStreamHandler.save(bucket, object + 2, new FakeInputStream(100), map, null)));

    assertTrue(inputStreamHandler.check(bucket, object));
    assertTrue(inputStreamHandler.check(bucket, object + 2));
    assertEquals(2, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(200, assertDoesNotThrow(() -> inputStreamHandler.size()));
    assertEquals(100, assertDoesNotThrow(
        () -> FakeInputStream.consumeAll(assertDoesNotThrow(() -> inputStreamHandler.readContent(bucket, object)))));
    var map2 = assertDoesNotThrow(() -> inputStreamHandler.readMetadata(bucket, object));
    assertEquals(map, map2);
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.deleteOlderThan(now)));

    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
    Thread.sleep(10);
    final var now2 = Instant.now();
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.deleteOlderThan(now2)));

    checkUsingFsInputHandlerEmpty();
  }

  @Test
  void checkRegisterUnregister() {
    assertDoesNotThrow(() -> inputStreamHandler.registerItem("name", "dir/object"));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.getCurrentRegisteredTasks()).size());
    assertDoesNotThrow(() -> inputStreamHandler.unregisterItem("name", "dir/object"));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.getCurrentRegisteredTasks()).size());
    assertDoesNotThrow(() -> inputStreamHandler.registerItem("name", "dir/object"));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.getCurrentRegisteredTasks()).size());
    List<BufferedItem> validated = List.of(new BufferedItem("name", "dir/object"));
    assertDoesNotThrow(() -> inputStreamHandler.removedValidatedTasks(validated));
    assertEquals(0, assertDoesNotThrow(() -> inputStreamHandler.getCurrentRegisteredTasks()).size());
    assertDoesNotThrow(() -> inputStreamHandler.unregisterItem("name", "dir/object"));
  }
}
