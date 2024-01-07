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

package io.clonecloudstore.common.standard.inputstream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.common.standard.system.SystemTools.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestMethodOrder(MethodOrderer.MethodName.class)
class ZstdStreamTest {
  private static final Logger LOGGER = Logger.getLogger(ZstdStreamTest.class.getName());
  private static final int LEN = 1024 * 1024;
  private static final int BIG_LEN = 100 * 1024 * 1024;

  @Test
  void test01FromNormalZstd() throws IOException {
    // For reference compared to this implementation (encapsulation)
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var random = new Random();
    for (var level = -1; level < 10; level++) {
      final var outputStream = new ByteArrayOutputStream(LEN);
      final var start = System.nanoTime();
      final var zstdOutputStream = new ZstdOutputStream(outputStream, RecyclingBufferPool.INSTANCE).setLevel(level);
      for (var i = 0; i < 8; i++) {
        random.nextBytes(bytes);
        zstdOutputStream.write(bytes, 0, bytes.length);
        zstdOutputStream.flush();
      }
      zstdOutputStream.close();
      final var compressed = outputStream.toByteArray();
      outputStream.close();
      final var inputStream = new ByteArrayInputStream(compressed);
      final var zstdInputStream = new ZstdInputStream(inputStream, RecyclingBufferPool.INSTANCE);
      int read;
      var computedLen = 0;
      while ((read = zstdInputStream.read(bytes, 0, bytes.length)) >= 0) {
        computedLen += read;
      }
      zstdInputStream.close();
      inputStream.close();
      final var stop = System.nanoTime();
      assertEquals(LEN, computedLen);
      LOGGER.fine("Compute " + level + " in " + ((stop - start) / 1000) + " so speed " +
          (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s for " +
          ((LEN - compressed.length) / ((double) LEN) * 100) + " compression (" + compressed.length + " vs " + LEN +
          ")");
    }
  }

  @Test
  void test02FromZstdDecompress() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var random = new Random();
    for (var level = -1; level < 10; level++) {
      final var outputStream = new ByteArrayOutputStream(LEN);
      final var start = System.nanoTime();
      final var zstdOutputStream = new ZstdOutputStream(outputStream, RecyclingBufferPool.INSTANCE).setLevel(level);
      for (var i = 0; i < 8; i++) {
        random.nextBytes(bytes);
        zstdOutputStream.write(bytes, 0, bytes.length);
        zstdOutputStream.flush();
      }
      zstdOutputStream.close();
      final var compressed = outputStream.toByteArray();
      outputStream.close();
      final var inputStream = new ByteArrayInputStream(compressed);
      final var zstdDecompressInputStream = new ZstdDecompressInputStream(inputStream);
      int read;
      var computedLen = 0;
      while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
        computedLen += read;
      }
      zstdDecompressInputStream.close();
      inputStream.close();
      final var stop = System.nanoTime();
      assertEquals(-1, zstdDecompressInputStream.read());
      assertEquals(0, zstdDecompressInputStream.skip(10));
      assertEquals(0, zstdDecompressInputStream.transferTo(new VoidOutputStream()));
      assertThrows(CcsInvalidArgumentRuntimeException.class, () -> zstdDecompressInputStream.read(null));
      assertThrows(CcsInvalidArgumentRuntimeException.class, () -> zstdDecompressInputStream.read(null, 0, 10));
      assertTrue(zstdDecompressInputStream.toString().contains("" + zstdDecompressInputStream.getSizeDecompressed()));
      assertEquals(LEN, computedLen);
      LOGGER.fine("Compute " + level + " in " + ((stop - start) / 1000) + " so speed " +
          (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s for " +
          ((LEN - compressed.length) / ((double) LEN) * 100) + " compression (" + compressed.length + " vs " + LEN +
          ")");
    }
  }

  @Test
  void test03FromZstdCompressNoDecompressSkip() throws IOException {
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    int read;
    var computedLen = 0;
    while ((read = (int) zstdCompressInputStream.skip(StandardProperties.getBufSize())) > 0) {
      computedLen += read;
    }
    assertEquals(-1, zstdCompressInputStream.read());
    assertEquals(0, zstdCompressInputStream.skip(10));
    assertEquals(0, zstdCompressInputStream.transferTo(new VoidOutputStream()));
    zstdCompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(-1, zstdCompressInputStream.read());
    assertEquals(0, zstdCompressInputStream.skip(10));
    assertEquals(0, zstdCompressInputStream.transferTo(new VoidOutputStream()));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> zstdCompressInputStream.read(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> zstdCompressInputStream.read(null, 0, 10));
    assertEquals(BIG_LEN, zstdCompressInputStream.getSizeRead());
    assertTrue(zstdCompressInputStream.toString().contains("" + zstdCompressInputStream.getSizeCompressed()));
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressNoDecompressTransferTo() throws IOException {
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    var computedLen = zstdCompressInputStream.transferTo(new VoidOutputStream());
    assertEquals(-1, zstdCompressInputStream.read());
    assertEquals(0, zstdCompressInputStream.skip(10));
    assertEquals(0, zstdCompressInputStream.transferTo(new VoidOutputStream()));
    zstdCompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(BIG_LEN, zstdCompressInputStream.getSizeRead());
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompress() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdInputStream = new ZstdInputStream(zstdCompressInputStream, RecyclingBufferPool.INSTANCE);
    int read;
    var computedLen = 0;
    while ((read = zstdInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
    }
    assertEquals(-1, zstdInputStream.read());
    assertEquals(0, zstdInputStream.skip(10));
    assertEquals(0, zstdInputStream.transferTo(new VoidOutputStream()));
    LOGGER.finest("ZSTD: " + zstdInputStream);
    LOGGER.finest("ZSTD: " + zstdCompressInputStream);
    zstdInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(BIG_LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressReadByte() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    zstdCompressInputStream.available();
    final var outputStream = new ByteArrayOutputStream();
    int val;
    while ((val = zstdCompressInputStream.read()) >= 0) {
      outputStream.write(val);
    }
    outputStream.flush();
    outputStream.close();
    final var compressed = outputStream.toByteArray();
    final var byteArrayInputStream = new ByteArrayInputStream(compressed);
    final var zstdInputStream = new ZstdInputStream(byteArrayInputStream, RecyclingBufferPool.INSTANCE);
    int read;
    var computedLen = 0;
    while ((read = zstdInputStream.read(bytes)) >= 0) {
      computedLen += read;
    }
    zstdInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressReadBytes() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var outputStream = new ByteArrayOutputStream();
    var read = 0;
    while ((read = zstdCompressInputStream.read(bytes)) >= 0) {
      outputStream.write(bytes, 0, read);
    }
    outputStream.flush();
    outputStream.close();
    final var compressed = outputStream.toByteArray();
    final var byteArrayInputStream = new ByteArrayInputStream(compressed);
    final var zstdInputStream = new ZstdInputStream(byteArrayInputStream, RecyclingBufferPool.INSTANCE);
    var computedLen = 0;
    while ((read = zstdInputStream.read(bytes)) >= 0) {
      computedLen += read;
    }
    zstdInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressTransferTo() throws IOException {
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var outputStream = new ByteArrayOutputStream();
    zstdCompressInputStream.transferTo(outputStream);
    outputStream.flush();
    outputStream.close();
    final var compressed = outputStream.toByteArray();
    final var byteArrayInputStream = new ByteArrayInputStream(compressed);
    final var zstdInputStream = new ZstdInputStream(byteArrayInputStream, RecyclingBufferPool.INSTANCE);
    int read;
    var computedLen = 0;
    while ((read = (int) zstdInputStream.skip(LEN / 8)) > 0) {
      computedLen += read;
    }
    zstdInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressWithError() throws IOException, NoSuchFieldException, IllegalAccessException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var outputStream = new ByteArrayOutputStream();
    var read = 0;
    final var atomicReference =
        (AtomicReference<IOException>) getField(ZstdCompressInputStream.class, "exceptionAtomicReference",
            zstdCompressInputStream);
    try {
      while ((read = zstdCompressInputStream.read(bytes)) >= 0) {
        outputStream.write(bytes, 0, read);
        atomicReference.set(new IOException("Test"));
      }
      fail("Should raised an exception");
    } catch (final IOException e) {
      // OK
    }
    outputStream.flush();
    outputStream.close();
    final var compressed = outputStream.toByteArray();
    final var byteArrayInputStream = new ByteArrayInputStream(compressed);
    final var zstdInputStream = new ZstdInputStream(byteArrayInputStream, RecyclingBufferPool.INSTANCE);
    var computedLen = 0;
    try {
      while ((read = zstdInputStream.read(bytes)) >= 0) {
        computedLen += read;
      }
      fail("Should raised an exception");
    } catch (final IOException e) {
      // OK
    }
    zstdInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertNotEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  void zstdCompressDecompressNoFlush(final boolean flush, final boolean highlyCompressed) throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = highlyCompressed ? new FakeInputStream(BIG_LEN, (byte) 'A') : new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream, flush);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    int read;
    var computedLen = 0L;
    while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(BIG_LEN, computedLen);
    LOGGER.fine(
        "Resp. length: " + zstdCompressInputStream.getSizeRead() + " " + zstdCompressInputStream.getSizeCompressed());
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompress() throws IOException {
    zstdCompressDecompressNoFlush(true, false);
    zstdCompressDecompressNoFlush(true, false);
    zstdCompressDecompressNoFlush(true, true);
    zstdCompressDecompressNoFlush(false, false);
    zstdCompressDecompressNoFlush(false, true);
  }

  @Test
  void test04FromZstdCompressDecompressReadBytes() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    int read;
    var computedLen = 0L;
    zstdDecompressInputStream.available();
    while ((read = zstdDecompressInputStream.read(bytes)) >= 0) {
      computedLen += read;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompressSkipBytes() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    long read;
    var computedLen = 0L;
    while ((read = zstdDecompressInputStream.skip(LEN / 8)) > 0) {
      computedLen += read;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompressTransferTo() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    final var computedLen = zstdDecompressInputStream.transferTo(new VoidOutputStream());
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompressReadByte() throws IOException {
    final var inputStream = new FakeInputStream(LEN / 10);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    var computedLen = 0L;
    assertFalse(zstdCompressInputStream.markSupported());
    assertFalse(zstdDecompressInputStream.markSupported());
    while (zstdDecompressInputStream.read() >= 0) {
      computedLen++;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN / 10, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 10 / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test05FromZstdCompressDecompressWithWaitRead() throws IOException, InterruptedException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    int read;
    var computedLen = 0L;
    while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
      Thread.yield();
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test06FromZstdCompressDecompressWithWaitWrite() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    int read;
    var computedLen = 0L;
    while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(BIG_LEN, computedLen);
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  void zstdCompleteExample(final boolean flush, final boolean highlyCompressed) throws IOException {
    final long readLimit = 400 * 1024 * 1024;
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = highlyCompressed ? new FakeInputStream(BIG_LEN, (byte) 'A') : new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new ZstdCompressInputStream(inputStream, flush);
    final var zstdDecompressInputStream = new ZstdDecompressInputStream(zstdCompressInputStream);
    int read;
    var computedLen = 0L;
    while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    assertEquals(BIG_LEN, computedLen);
    final var stop = System.nanoTime();
    LOGGER.fine("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test10FromZstdCompleteExample() throws IOException {
    zstdCompleteExample(true, false);
    zstdCompleteExample(true, false);
    zstdCompleteExample(true, true);
    zstdCompleteExample(false, false);
    zstdCompleteExample(false, true);
  }
}
