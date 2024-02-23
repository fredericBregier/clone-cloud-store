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

package io.clonecloudstore.common.standard.inputstream;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class PipedInputOutputStreamTest {
  private static final long LEN = 10 * 1024 * 1024 * 1024L + 1024L;

  @Test
  void constructor() throws IOException {
    assertThrows(IllegalArgumentException.class, () -> new PipedInputOutputStream(null, 0));
    var piped = new PipedInputOutputStream();
    assertThrows(NullPointerException.class, () -> piped.read(null));
    assertThrows(NullPointerException.class, () -> piped.write(null));
    assertThrows(NullPointerException.class, () -> piped.write(null, 0, 0));
    assertThrows(NullPointerException.class, () -> piped.read(null, 0, 0));
    byte[] bytes = new byte[1];
    assertThrows(IndexOutOfBoundsException.class, () -> piped.read(bytes, 0, 2));
    assertEquals(0, piped.read(bytes, 0, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> piped.write(bytes, 0, 2));
    assertDoesNotThrow(() -> piped.write(bytes, 0, 0));
  }

  @Test
  void compareNative() throws IOException {
    testNative();
    testRewritten();
    testRewritten2();
    testNative();
    testRewritten();
    testRewritten2();
  }

  private void testNative() throws IOException {
    try (final var inputStream = new FakeInputStream(LEN, (byte) 'A')) {
      final var pipedOutputStream = new PipedOutputStream();
      final var pipedInputStream = new PipedInputStream(pipedOutputStream, 3 * StandardProperties.getBufSize());
      var start = System.nanoTime();
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          SystemTools.transferTo(inputStream, pipedOutputStream);
          pipedOutputStream.close();
        } catch (final IOException ignore) {
          // Ignore
        }
      });
      assertEquals(LEN, FakeInputStream.consumeAll(pipedInputStream));
      var stop = System.nanoTime();
      Log.infof("Native in %d ms", ((stop - start) / 1000000));
    }
  }

  private void testRewritten() throws IOException {
    try (final var inputStream = new FakeInputStream(LEN, (byte) 'A')) {
      final var exceptionRef = new AtomicReference<Exception>();
      final var newPiped = new PipedInputOutputStream(exceptionRef);
      var start = System.nanoTime();
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          assertEquals(LEN, newPiped.transferFrom(inputStream));
          newPiped.closeOutput();
        } catch (final IOException ignore) {
          // Ignore
        }
      });
      assertEquals(LEN, FakeInputStream.consumeAll(newPiped));
      var stop = System.nanoTime();
      Log.infof("Rewritten in %d ms", ((stop - start) / 1000000));
    }
  }

  private void testRewritten2() throws IOException {
    try (final var inputStream = new FakeInputStream(LEN, (byte) 'A')) {
      final var exceptionRef = new AtomicReference<Exception>();
      final var newPiped = new PipedInputOutputStream(exceptionRef, 3);
      var start = System.nanoTime();
      newPiped.transferFromAsync(inputStream);
      var len = FakeInputStream.consumeAll(newPiped);
      assertEquals(LEN, len);
      var stop = System.nanoTime();
      Log.infof("Rewritten2 in %d ms", ((stop - start) / 1000000));
    }
  }
}
