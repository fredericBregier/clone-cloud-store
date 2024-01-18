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
import java.util.Map;

import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class FilesystemHandlerWithDbTest {
  @Inject
  FilesystemHandler inputStreamHandler;

  @BeforeEach
  void cleanUp() throws IOException {
    FsTestUtil.cleanUp();
    inputStreamHandler.changeHasDatabase(true);
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
    assertThrows(IOException.class, () -> inputStreamHandler.readMetadata(bucket, object));
    assertThrows(IOException.class, () -> inputStreamHandler.update(bucket, object, map, null));
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

    assertTrue(inputStreamHandler.check(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
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
    assertThrows(IOException.class, () -> inputStreamHandler.readMetadata(bucket, object));
    assertThrows(IOException.class, () -> inputStreamHandler.update(bucket, object, map, null));
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
    assertThrows(IOException.class, () -> inputStreamHandler.readMetadata(bucket, object));
    assertThrows(IOException.class, () -> inputStreamHandler.update(bucket, object, null, null));
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
    assertThrows(IOException.class, () -> inputStreamHandler.readMetadata(bucket, object));
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
    assertThrows(IOException.class, () -> inputStreamHandler.readMetadata(bucket, object));
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.deleteOlderThan(now)));

    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.count()));
    assertEquals(100, assertDoesNotThrow(() -> inputStreamHandler.size()));
    Thread.sleep(10);
    final var now2 = Instant.now();
    assertEquals(1, assertDoesNotThrow(() -> inputStreamHandler.deleteOlderThan(now2)));

    checkUsingFsInputHandlerEmpty();
  }
}
