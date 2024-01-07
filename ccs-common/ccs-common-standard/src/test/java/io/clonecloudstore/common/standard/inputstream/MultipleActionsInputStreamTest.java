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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.BaseXx;
import io.clonecloudstore.common.standard.system.SysErrLogger;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestMethodOrder(MethodOrderer.MethodName.class)
class MultipleActionsInputStreamTest {
  private static final Logger LOGGER = Logger.getLogger(MultipleActionsInputStreamTest.class.getName());
  private static final int LEN = 1024 * 1024;
  private static final int BIG_LEN = 100 * 1024 * 1024;

  @Test
  void testDigestAlgo() {
    for (final var digest : DigestAlgo.values()) {
      assertEquals(digest, DigestAlgo.getFromName(digest.algoName));
    }
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> DigestAlgo.getFromName("nameFake"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new MultipleActionsInputStream(null).computeDigest(DigestAlgo.SHA256));
    final var inputstream = new FakeInputStream(10);
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new MultipleActionsInputStream(inputstream).computeDigest(null));
    try (final var digestInputStream = new MultipleActionsInputStream(inputstream)) {
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      assertThrows(CcsInvalidArgumentRuntimeException.class, () -> digestInputStream.read(null));
      assertThrows(CcsInvalidArgumentRuntimeException.class, () -> digestInputStream.read(null, 0, 10));
    } catch (IOException e) {
      fail(e);
    } catch (NoSuchAlgorithmException e) {
      fail(e);
    }
  }

  @Test
  void test5DigestInputStreamSha256() {
    assertEquals(DigestAlgo.SHA256.getByteSize() * 2, DigestAlgo.SHA256.getHexSize());

    test5DigestInputStreamDigest(DigestAlgo.MD5);
    test5DigestInputStreamDigest(DigestAlgo.SHA256);
    test5DigestInputStreamDigest(DigestAlgo.SHA512);
  }

  void test5DigestInputStreamDigest(final DigestAlgo digestAlgo) {
    final var len = BIG_LEN;
    long read = 0;
    final var bytes = new byte[StandardProperties.getBufSize()];
    final String hash;
    final String hash32;
    try (final var inputStream0 = new FakeInputStream(len, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0, StandardProperties.getMaxWaitMs())) {
      inputStream.computeDigest(digestAlgo);
      long subread;
      assertTrue(inputStream.available() > 0);
      final var start = System.nanoTime();
      while ((subread = inputStream.read(bytes)) >= 0) {
        read += subread;
      }
      final var stop = System.nanoTime();
      LOGGER.info(digestAlgo.algoName + " Time: " + (stop - start) / 1000000 + " MB/s: " +
          (len / 1024 / 1024.0) / ((stop - start) / 1000000000.0));
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
      hash32 = inputStream.getDigestBase32();
      hash = inputStream.getDigestBase64();
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    var lenShort = 128 * 1024L;
    final String hash32Short;
    final String hashShort;
    try (final var inputStream0 = new FakeInputStream(lenShort, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0, StandardProperties.getMaxWaitMs())) {
      inputStream.computeDigest(digestAlgo);
      long subread;
      assertTrue(inputStream.available() > 0);
      final var start = System.nanoTime();
      while ((subread = inputStream.read(bytes)) >= 0) {
        read += subread;
      }
      final var stop = System.nanoTime();
      LOGGER.info(digestAlgo.algoName + " Time: " + (stop - start) / 1000000 + " MB/s: " +
          (lenShort / 1024 / 1024.0) / ((stop - start) / 1000000000.0));
      assertEquals(lenShort, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
      hash32Short = inputStream.getDigestBase32();
      hashShort = inputStream.getDigestBase64();
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream0 = new FakeInputStream(lenShort, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0, StandardProperties.getMaxWaitMs())) {
      inputStream.computeDigest(digestAlgo);
      assertTrue(inputStream.available() > 0);
      final var start = System.nanoTime();
      while (inputStream.read() >= 0) {
        read++;
      }
      final var stop = System.nanoTime();
      SysErrLogger.FAKE_LOGGER.sysout(
          digestAlgo.algoName + " Read unitary Time: " + (stop - start) / 1000000 + " MB/s: " +
              (lenShort / 1024 / 1024.0) / ((stop - start) / 1000000000.0));
      assertEquals(lenShort, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
      final var hash2 = inputStream.getDigestBase64();
      final var hash322 = inputStream.getDigestBase32();
      final var defaultHash = inputStream.getDigest();
      assertEquals(hashShort, hash2);
      assertEquals(hash32Short, hash322);
      assertEquals(hash32Short, defaultHash);
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    try (final var inputStream0 = new FakeInputStream(len, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0, StandardProperties.getMaxWaitMs())) {
      inputStream.computeDigest(digestAlgo);
      assertTrue(inputStream.available() > 0);
      final var start = System.nanoTime();
      read = FakeInputStream.consumeAll(inputStream);
      final var stop = System.nanoTime();
      LOGGER.info(digestAlgo.algoName + " Time: " + (stop - start) / 1000000 + " MB/s: " +
          (len / 1024 / 1024.0) / ((stop - start) / 1000000000.0));
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
      final var byteHash = inputStream.getDigestValue();
      final var hash2 = inputStream.getDigestBase64();
      final var hash322 = inputStream.getDigestBase32();
      assertEquals(hash, hash2);
      assertEquals(hash32, hash322);
      assertEquals(hash, BaseXx.getBase64Padding(byteHash));
      assertEquals(hash32, BaseXx.getBase32(byteHash));
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

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
      LOGGER.debug("Compute " + level + " in " + ((stop - start) / 1000) + " so speed " +
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
      final var zstdDecompressInputStream = new MultipleActionsInputStream(inputStream);
      zstdDecompressInputStream.decompress();
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
      assertTrue(zstdDecompressInputStream.toString().contains("" + zstdDecompressInputStream.waitForAllRead(100)));
      assertEquals(LEN, computedLen);
      LOGGER.debug("Compute " + level + " in " + ((stop - start) / 1000) + " so speed " +
          (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s for " +
          ((LEN - compressed.length) / ((double) LEN) * 100) + " compression (" + compressed.length + " vs " + LEN +
          ")");
    }
  }

  @Test
  void test03FromZstdCompressNoDecompressSkip() throws IOException {
    final var inputStream = new FakeInputStream(BIG_LEN, (byte) 'A');
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    //zstdCompressInputStream.computeOriginalSize();
    zstdCompressInputStream.compress();
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
    assertEquals(BIG_LEN, zstdCompressInputStream.getSourceRead());
    assertTrue(zstdCompressInputStream.toString().contains("" + zstdCompressInputStream.waitForAllRead(100)));
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressNoDecompressTransferTo() throws IOException {
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    //zstdCompressInputStream.computeOriginalSize();
    zstdCompressInputStream.compress();
    var computedLen = zstdCompressInputStream.transferTo(new VoidOutputStream());
    assertEquals(-1, zstdCompressInputStream.read());
    assertEquals(0, zstdCompressInputStream.skip(10));
    assertEquals(0, zstdCompressInputStream.transferTo(new VoidOutputStream()));
    zstdCompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(BIG_LEN, zstdCompressInputStream.getSourceRead());
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompress() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdInputStream = new ZstdInputStream(zstdCompressInputStream, RecyclingBufferPool.INSTANCE);
    int read;
    var computedLen = 0;
    while ((read = zstdInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
    }
    assertEquals(-1, zstdInputStream.read());
    assertEquals(0, zstdInputStream.skip(10));
    assertEquals(0, zstdInputStream.transferTo(new VoidOutputStream()));
    LOGGER.debug("ZSTD: " + zstdInputStream);
    LOGGER.debug("ZSTD: " + zstdCompressInputStream);
    zstdInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(BIG_LEN, computedLen);
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressReadByte() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressReadBytes() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test03FromZstdCompressTransferTo() throws IOException {
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  void zstdCompressDecompressNoFlush(final boolean highlyCompressed, final int level) throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = highlyCompressed ? new FakeInputStream(BIG_LEN, (byte) 'A') : new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress(level);
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
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
    LOGGER.info("Resp. length: " + zstdCompressInputStream.waitForAllRead(100) + " " +
        zstdDecompressInputStream.waitForAllRead(100));
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompress() throws IOException {
    zstdCompressDecompressNoFlush(false, 2);
    zstdCompressDecompressNoFlush(false, 1);
    zstdCompressDecompressNoFlush(false, 0);
    zstdCompressDecompressNoFlush(false, -1);
    zstdCompressDecompressNoFlush(false, -2);
    zstdCompressDecompressNoFlush(true, 2);
    zstdCompressDecompressNoFlush(true, 1);
    zstdCompressDecompressNoFlush(true, 0);
    zstdCompressDecompressNoFlush(true, -1);
    zstdCompressDecompressNoFlush(true, -2);
  }

  @Test
  void test04FromZstdCompressDecompressReadBytes() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompressSkipBytes() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompressTransferTo() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
    final var computedLen = zstdDecompressInputStream.transferTo(new VoidOutputStream());
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    final var stop = System.nanoTime();
    assertEquals(LEN, computedLen);
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test04FromZstdCompressDecompressReadByte() throws IOException {
    final var inputStream = new FakeInputStream(LEN / 10);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 10 / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test05FromZstdCompressDecompressWithWaitRead() throws IOException, InterruptedException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
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
    LOGGER.debug("Compute in " + ((stop - start) / 1000) + " so speed " +
        (LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  @Test
  void test06FromZstdCompressDecompressWithWaitWrite() throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = new FakeInputStream(BIG_LEN);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    zstdCompressInputStream.compress();
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    zstdDecompressInputStream.decompress();
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
    LOGGER.info("Compute in " + ((stop - start) / 1000) + " so speed " +
        (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
  }

  double zstdCompleteExample(final boolean highlyCompressed, final boolean digest, final boolean fromServerToClient)
      throws IOException, NoSuchAlgorithmException {
    final long LENGB = 100 * 1024 * 1024;
    final var bytes = new byte[StandardProperties.getBufSize()];
    final var inputStream = highlyCompressed ? new FakeInputStream(LENGB, (byte) 'A') : new FakeInputStream(LENGB);
    final var start = System.nanoTime();
    final var zstdCompressInputStream = new MultipleActionsInputStream(inputStream);
    if (digest) {
      zstdCompressInputStream.computeDigest(DigestAlgo.SHA256);
    }
    zstdCompressInputStream.compress();
    if (fromServerToClient) {
      zstdCompressInputStream.asyncPipedInputStream(null);
    }
    final var zstdDecompressInputStream = new MultipleActionsInputStream(zstdCompressInputStream);
    if (digest) {
      zstdDecompressInputStream.computeDigest(DigestAlgo.SHA256);
    }
    zstdDecompressInputStream.decompress();
    int read;
    var computedLen = 0L;
    while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
      computedLen += read;
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();
    inputStream.close();
    assertEquals(LENGB, computedLen);
    if (digest) {
      assertNotNull(zstdCompressInputStream.getDigest());
      assertNotNull(zstdCompressInputStream.getDigest());
    } else {
      assertNull(zstdCompressInputStream.getDigest());
      assertNull(zstdDecompressInputStream.getDigest());
    }
    final var stop = System.nanoTime();
    assertEquals(LENGB, zstdDecompressInputStream.waitForAllRead(100));
    LOGGER.info("Stream Dg " + digest + " HC " + highlyCompressed + " S2C: " + fromServerToClient + " in " +
        ((stop - start) / 1000) + " so speed " + (LENGB / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
    System.gc();
    return (LENGB / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  Runtime runtime = Runtime.getRuntime();

  private long getCurrentlyAllocatedMemory() {
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
  }

  @Test
  void test10FromZstdCompleteExample() throws IOException, NoSuchAlgorithmException, CcsWithStatusException {
    final int NB_TRY = 3;
    double max1 = 0;
    long memMax1 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max1 = Math.max(max1, zstdCompleteExample(false, false, false));
      var after = getCurrentlyAllocatedMemory();
      memMax1 = Math.max(after - before, memMax1);
    }
    double max2 = 0;
    long memMax2 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max2 = Math.max(max2, zstdCompleteExample(false, false, true));
      var after = getCurrentlyAllocatedMemory();
      memMax2 = Math.max(after - before, memMax2);
    }
    LOGGER.infof("### Max Strm %f (%d) StrmBef %f (%d)", max1, memMax1, max2, memMax2);

    max1 = 0;
    memMax1 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max1 = Math.max(max1, zstdCompleteExample(true, false, false));
      var after = getCurrentlyAllocatedMemory();
      memMax1 = Math.max(after - before, memMax1);
    }
    max2 = 0;
    memMax2 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max2 = Math.max(max2, zstdCompleteExample(true, false, true));
      var after = getCurrentlyAllocatedMemory();
      memMax2 = Math.max(after - before, memMax2);
    }
    LOGGER.infof("### Max Strm %f (%d) StrmBef %f (%d)", max1, memMax1, max2, memMax2);

    max1 = 0;
    memMax1 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max1 = Math.max(max1, zstdCompleteExample(false, true, false));
      var after = getCurrentlyAllocatedMemory();
      memMax1 = Math.max(after - before, memMax1);
    }
    max2 = 0;
    memMax2 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max2 = Math.max(max2, zstdCompleteExample(false, true, true));
      var after = getCurrentlyAllocatedMemory();
      memMax2 = Math.max(after - before, memMax2);
    }
    LOGGER.infof("### Max Strm %f (%d) StrmBef %f (%d)", max1, memMax1, max2, memMax2);

    max1 = 0;
    memMax1 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max1 = Math.max(max1, zstdCompleteExample(true, true, false));
      var after = getCurrentlyAllocatedMemory();
      memMax1 = Math.max(after - before, memMax1);
    }
    max2 = 0;
    memMax2 = 0;
    for (int i = 0; i < NB_TRY; i++) {
      var before = getCurrentlyAllocatedMemory();
      max2 = Math.max(max2, zstdCompleteExample(true, true, true));
      var after = getCurrentlyAllocatedMemory();
      memMax2 = Math.max(after - before, memMax2);
    }
    LOGGER.infof("### Max Strm %f (%d) StrmBef %f (%d)", max1, memMax1, max2, memMax2);
  }

  @Test
  void test20Piped() throws IOException {
    for (int z = 0; z < 3; z++) {
      for (int i = 4; i < 34; i += 2) {
        int bufsize = 16 * 1024 * i;
        var start = System.nanoTime();
        for (int j = 0; j < 5; j++) {
          final FakeInputStream fakeInputStream2 = new FakeInputStream(BIG_LEN);
          PipedOutputStream out2 = new PipedOutputStream();
          PipedInputStream in2 = new PipedInputStream(out2, bufsize);
          start = System.nanoTime();
          try (in2) {
            new Thread(() -> {
              try (out2) {
                fakeInputStream2.transferTo(out2);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }).start();
            var len = FakeInputStream.consumeAll(in2);
            assertEquals(BIG_LEN, len);
          }
        }
        var stop = System.nanoTime();
        LOGGER.info("Compute " + bufsize + " in " + ((stop - start) / 1000) + " so speed " +
            (BIG_LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0)) + " MB/s ");
      }
      LOGGER.infof("ZZZ");
    }
  }
}
