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

package io.clonecloudstore.test.stream;

import java.io.IOException;
import java.io.OutputStream;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.test.stream.FakeInputStream.DEFAULT_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class FakeInputStreamTest {

  @Test
  void testNullBytes() throws IOException {
    final var len = 1024 * 1024 * 1024L;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      assertThrows(IllegalArgumentException.class, () -> inputStream.read(null));
      assertThrows(IllegalArgumentException.class, () -> inputStream.read(null, 0, 10));
    }
  }

  @Test
  void testFakeInputStream() {
    final var len = 1024 * 1024 * 1024L;
    long read = 0;
    final var bytes = new byte[DEFAULT_BUFFER_SIZE];
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      long subread;
      assertTrue(inputStream.available() > 0);
      assertEquals('A', inputStream.read());
      read++;
      while ((subread = inputStream.read(bytes)) >= 0) {
        read += subread;
      }
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      assertTrue(inputStream.available() > 0);
      assertEquals(len, FakeInputStream.consumeAll(inputStream));
      assertEquals(-1, inputStream.read());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      long subread;
      assertTrue(inputStream.available() > 0);
      assertEquals('A', inputStream.read());
      read++;
      while ((subread = inputStream.skip(len)) > 0) {
        read += subread;
      }
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      assertTrue(inputStream.available() > 0);
      assertEquals(len, inputStream.transferTo(new VoidOutputStream()));
      assertEquals(-1, inputStream.read());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len)) {
      long subread;
      assertTrue(inputStream.available() > 0);
      assertTrue(inputStream.read() >= 0);
      read++;
      while ((subread = inputStream.read(bytes)) >= 0) {
        read += subread;
      }
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    try (final var inputStream = new FakeInputStream(len); final OutputStream outputStream = new VoidOutputStream()) {
      assertTrue(inputStream.available() > 0);
      assertEquals(len, inputStream.transferTo(outputStream));
      assertEquals(-1, inputStream.read());
      // Extra test on OutputStream
      outputStream.write(1);
      outputStream.write(bytes);
      outputStream.flush();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
