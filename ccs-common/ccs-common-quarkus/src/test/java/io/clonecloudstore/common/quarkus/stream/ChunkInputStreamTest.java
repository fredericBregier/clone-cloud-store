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

package io.clonecloudstore.common.quarkus.stream;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class ChunkInputStreamTest {
  private static final Logger LOGGER = Logger.getLogger(ChunkInputStreamTest.class);

  @Test
  void test0ErrorChunkInputStream() {
    final var len = 1000 * 1024 * 1024L;
    final var chunk = 10 * 1024 * 1024;
    long read = 0;
    final var bytes = new byte[QuarkusProperties.getBufSize()];
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      try (final var chunkInputStream = new ChunkInputStream(inputStream, 0, chunk)) {
        assertThrows(CcsInvalidArgumentRuntimeException.class, () -> chunkInputStream.read(null, 0, 10));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void test1ChunkInputStream() {
    final var len = 1000 * 1024 * 1024L;
    final var chunk = 10 * 1024 * 1024;
    long read = 0;
    final var bytes = new byte[QuarkusProperties.getBufSize()];
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      try (final var chunkInputStream = new ChunkInputStream(inputStream, 0, chunk)) {
        assertFalse(chunkInputStream.isChunksDone());
        while (chunkInputStream.nextChunk()) {
          long subread;
          long chunkRead = 0;
          assertEquals(chunk, chunkInputStream.getChunkSize());
          assertEquals(chunk, chunkInputStream.getBufferSize());
          assertEquals(0, chunkInputStream.getCurrentPos());
          assertTrue(chunkInputStream.available() > 0);
          assertEquals('A', chunkInputStream.read());
          chunkRead++;
          while ((subread = chunkInputStream.read(bytes)) >= 0) {
            chunkRead += subread;
          }
          assertEquals(chunk, chunkRead);
          assertEquals(-1, chunkInputStream.read());
          assertEquals(0, chunkInputStream.available());
          assertEquals(chunk, chunkInputStream.getCurrentPos());
          assertFalse(chunkInputStream.markSupported());
          read += chunkRead;
        }
        assertTrue(chunkInputStream.isChunksDone());
        assertEquals(len, chunkInputStream.getCurrentTotalRead());
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      try (final var chunkInputStream = new ChunkInputStream(inputStream, len, chunk)) {
        while (chunkInputStream.nextChunk()) {
          long subread;
          long chunkRead = 0;
          assertEquals(chunk, chunkInputStream.getChunkSize());
          assertTrue(chunkInputStream.available() > 0);
          assertEquals('A', chunkInputStream.read());
          chunkRead++;
          while ((subread = chunkInputStream.read(bytes)) >= 0) {
            chunkRead += subread;
          }
          assertEquals(chunk, chunkRead);
          assertEquals(-1, chunkInputStream.read());
          assertEquals(0, chunkInputStream.available());
          assertFalse(chunkInputStream.markSupported());
          read += chunkRead;
        }
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      // Using wrong huge chunk size
      var first = true;
      try (final var chunkInputStream = new ChunkInputStream(inputStream, 0,
          QuarkusProperties.getDriverMaxChunkSize() * 2)) {
        while (chunkInputStream.nextChunk()) {
          long subread;
          long chunkRead = 0;
          if (first) {
            assertEquals(QuarkusProperties.getDriverMaxChunkSize(), chunkInputStream.getChunkSize());
            first = false;
          }
          assertTrue(chunkInputStream.available() > 0);
          assertEquals('A', chunkInputStream.read());
          chunkRead++;
          while ((subread = chunkInputStream.read(bytes)) >= 0) {
            chunkRead += subread;
          }
          assertEquals(-1, chunkInputStream.read());
          assertEquals(0, chunkInputStream.available());
          assertFalse(chunkInputStream.markSupported());
          read += chunkRead;
        }
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void test2ChunkInputStreamConsumeAll() {
    final var len = 1000 * 1024 * 1024L;
    final var chunk = 10 * 1024 * 1024;
    long read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      try (final var chunkInputStream = new ChunkInputStream(inputStream, 0, chunk)) {
        while (chunkInputStream.nextChunk()) {
          final var chunkRead = FakeInputStream.consumeAll(chunkInputStream);
          assertEquals(chunk, chunkRead);
          read += chunkRead;
        }
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      try (final var chunkInputStream = new ChunkInputStream(inputStream, len, chunk)) {
        while (chunkInputStream.nextChunk()) {
          final var chunkRead = chunkInputStream.transferTo(new VoidOutputStream());
          assertEquals(chunk, chunkRead);
          read += chunkRead;
        }
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      var first = true;
      try (final var chunkInputStream = new ChunkInputStream(inputStream, 0,
          QuarkusProperties.getDriverMaxChunkSize() * 2)) {
        while (chunkInputStream.nextChunk()) {
          final var chunkRead = chunkInputStream.transferTo(new VoidOutputStream());
          if (first) {
            assertEquals(QuarkusProperties.getDriverMaxChunkSize(), chunkRead);
            first = false;
          }
          read += chunkRead;
        }
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream = new FakeInputStream(len, (byte) 'A')) {
      try (final var chunkInputStream = new ChunkInputStream(inputStream, len, chunk)) {
        while (chunkInputStream.nextChunk()) {
          while (true) {
            final var chunkRead = chunkInputStream.skip(chunk / 10);
            read += chunkRead;
            if (chunkRead <= 0) {
              break;
            }
          }
        }
      }
      assertEquals(len, read);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void test5DigestInputStreamSha256() {
    assertEquals(DigestAlgo.SHA256.getByteSize() * 2, DigestAlgo.SHA256.getHexSize());

    test5DigestInputStreamDigest(DigestAlgo.MD5);
    test5DigestInputStreamDigest(DigestAlgo.SHA256);
    test5DigestInputStreamDigest(DigestAlgo.SHA512);
  }

  public void test5DigestInputStreamDigest(final DigestAlgo digestAlgo) {
    final var len = 10 * 1024 * 1024L;
    long read = 0;
    final var bytes = new byte[QuarkusProperties.getBufSize()];
    final String hash;
    final String hash32;
    try (final var inputStream0 = new FakeInputStream(len, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0)) {
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
      hash = inputStream.getDigestBase64();
      hash32 = inputStream.getDigestBase32();
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    try (final var inputStream0 = new FakeInputStream(len, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0)) {
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
      final var hash2 = inputStream.getDigestBase64();
      final var hash322 = inputStream.getDigestBase32();
      assertEquals(hash, hash2);
      assertEquals(hash32, hash322);
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    try (final var inputStream0 = new FakeInputStream(len, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0)) {
      inputStream.computeDigest(digestAlgo);
      assertTrue(inputStream.available() > 0);
      final var start = System.nanoTime();
      read = inputStream.transferTo(new VoidOutputStream());
      final var stop = System.nanoTime();
      LOGGER.info(digestAlgo.algoName + " Time: " + (stop - start) / 1000000 + " MB/s: " +
          (len / 1024 / 1024.0) / ((stop - start) / 1000000000.0));
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
      final var hash2 = inputStream.getDigestBase64();
      assertEquals(hash, hash2);
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    read = 0;
    try (final var inputStream0 = new FakeInputStream(len, (byte) 'A');
         final var inputStream = new MultipleActionsInputStream(inputStream0)) {
      inputStream.computeDigest(digestAlgo);
      long subread;
      assertTrue(inputStream.available() > 0);
      final var start = System.nanoTime();
      while ((subread = inputStream.skip(QuarkusProperties.getBufSize())) > 0) {
        read += subread;
      }
      final var stop = System.nanoTime();
      LOGGER.info(digestAlgo.algoName + " Time: " + (stop - start) / 1000000 + " MB/s: " +
          (len / 1024 / 1024.0) / ((stop - start) / 1000000000.0));
      assertEquals(len, read);
      assertEquals(-1, inputStream.read());
      assertEquals(0, inputStream.available());
      assertFalse(inputStream.markSupported());
      final var hashB = inputStream.getDigestValue();
      assertNotNull(hashB);
    } catch (final IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
