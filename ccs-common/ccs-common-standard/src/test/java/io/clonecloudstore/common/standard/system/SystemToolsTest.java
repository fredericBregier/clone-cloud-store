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

package io.clonecloudstore.common.standard.system;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class SystemToolsTest {
  private static final Logger LOG = Logger.getLogger(SystemToolsTest.class);

  @Test
  void testSleep() throws InterruptedException {
    {
      var start = System.nanoTime();
      Thread.sleep(1);
      var stop = System.nanoTime();
      LOG.infof("Sleep 1 %d", stop - start);
      assertTrue(stop - start > 500);
    }
    {
      var start = System.nanoTime();
      for (int i = 0; i < 100; i++) {
        Thread.yield();
      }
      var stop = System.nanoTime();
      LOG.infof("Yield 100 %d", stop - start);
      assertTrue(stop - start > 500);
    }
    {
      var start = System.nanoTime();
      SystemTools.wait1ms();
      var stop = System.nanoTime();
      LOG.infof("Wait1ms %d", stop - start);
      assertTrue(stop - start > 500);
    }
  }

  @Test
  void testSilentlyCloseInputStream() throws IOException {
    final var inputStream = new FakeInputStream(100);
    assertNotEquals(-1, inputStream.available());
    assertNull(assertDoesNotThrow(() -> SystemTools.silentlyClose(inputStream)));
    assertEquals(0, inputStream.available());
    assertNull(assertDoesNotThrow(() -> SystemTools.silentlyClose(inputStream)));
    assertNull(assertDoesNotThrow(() -> SystemTools.silentlyClose((InputStream) null)));
    assertNotNull(assertDoesNotThrow(() -> SystemTools.silentlyClose(new InputStreamCloseException())));

    final var ouputStream = new VoidOutputStream();
    assertNull(assertDoesNotThrow(() -> SystemTools.silentlyClose(ouputStream)));
    assertNull(assertDoesNotThrow(() -> SystemTools.silentlyClose((OutputStream) null)));
    assertNotNull(assertDoesNotThrow(() -> SystemTools.silentlyClose(new OutputStreamCloseException())));
  }

  private static class InputStreamCloseException extends InputStream {
    @Override
    public void close() throws IOException {
      throw new IOException();
    }

    @Override
    public int read() throws IOException {
      return -1;
    }
  }

  private static class OutputStreamCloseException extends OutputStream {
    @Override
    public void write(final int b) throws IOException {

    }

    @Override
    public void close() throws IOException {
      throw new IOException();
    }
  }

  @Test
  void testConsumeIterator() {
    final var iterator = Stream.empty().iterator();
    var cpt = SystemTools.consumeAll(iterator);
    assertEquals(0, cpt);
    cpt = SystemTools.consumeAll((Iterator<?>) null);
    assertEquals(0, cpt);
    final var iterator2 = Stream.generate(() -> new Object()).limit(10).iterator();
    cpt = SystemTools.consumeAll(iterator2);
    assertEquals(10, cpt);
  }

  @Test
  void testConsumeStream() {
    final var stream = Stream.empty();
    var cpt = SystemTools.consumeAll(stream);
    assertEquals(0, cpt);
    cpt = SystemTools.consumeAll((Stream<?>) null);
    assertEquals(0, cpt);
    final var stream1 = Stream.generate(() -> new Object()).limit(10);
    cpt = SystemTools.consumeAll(stream1);
    assertEquals(10, cpt);
  }

  @Test
  final void testInstant() {
    final var instant = Instant.now();
    final var instant1 = SystemTools.toMillis(instant);
    assertTrue(instant1.isBefore(instant));
    assertNull(SystemTools.toMillis(null));
  }

  @Test
  void testGetField() throws NoSuchFieldException, IllegalAccessException {
    final var guid = new GuidLike();
    assertArrayEquals(guid.getBytes(), (byte[]) SystemTools.getField(GuidLike.class, "uuid", guid));
    assertThrows(NoSuchFieldException.class, () -> SystemTools.getField(GuidLike.class, "nofield", guid));
  }
}
