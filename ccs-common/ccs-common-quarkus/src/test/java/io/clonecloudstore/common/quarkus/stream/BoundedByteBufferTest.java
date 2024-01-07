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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.commons.io.input.BrokenInputStream;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.apache.commons.io.IOUtils.EOF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class BoundedByteBufferTest {

  private static final Logger LOGGER = Logger.getLogger(BoundedByteBufferTest.class);

  private static final int BUFFER_SIZE = 4094;

  @Test
  void testSmallReadByteReader() throws ExecutionException, InterruptedException, TimeoutException {
    final var readerCount = 1;
    final var size = 1000;
    // Given
    final var executorService = Executors.newCachedThreadPool();
    final var instance = new BoundedByteBuffer(BUFFER_SIZE, readerCount);

    // When
    final var writtenDigestFuture =
        CompletableFuture.supplyAsync(() -> writeRandomData(size, instance), executorService);
    final List<CompletableFuture<String>> readDigestFutures = new ArrayList<>();
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> instance.getReader(-1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> instance.getReader(readerCount));
    for (var i = 0; i < readerCount; i++) {
      final var reader = instance.getReader(i);
      readDigestFutures.add(CompletableFuture.supplyAsync(() -> readStreamBy1Byte(reader), executorService));
    }
    // Then
    final var writtenDigest = writtenDigestFuture.get(1, TimeUnit.MINUTES);
    for (final var readDigestFuture : readDigestFutures) {
      final var readDigest = readDigestFuture.get(1, TimeUnit.MINUTES);
      assertEquals(writtenDigest, readDigest);
    }

    instance.close();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  private String readStreamBy1Byte(final InputStream inputStream) {
    try {
      final var digestInputStream = new MultipleActionsInputStream(inputStream);
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      final OutputStream os = new VoidOutputStream();
      while (digestInputStream.read() >= 0) {
        // Nothing
      }
      digestInputStream.close();
      return digestInputStream.getDigestBase64();
    } catch (final IOException | NoSuchAlgorithmException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  @Test
  void testSingleReader() throws Exception {
    simpleTest(1, 0);
    simpleTest(1, 100);
    simpleTest(1, 1024);
    simpleTest(1, 4094);
    simpleTest(1, 4095);
    simpleTest(1, 10 * 1024 * 1024);
  }

  private void simpleTest(final int readerCount, final int size) throws Exception {

    // Given
    final var executorService = Executors.newCachedThreadPool();
    final var instance = new BoundedByteBuffer(BUFFER_SIZE, readerCount);

    // When
    final var writtenDigestFuture =
        CompletableFuture.supplyAsync(() -> writeRandomData(size, instance), executorService);
    final List<CompletableFuture<String>> readDigestFutures = new ArrayList<>();
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> instance.getReader(-1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> instance.getReader(readerCount));
    for (var i = 0; i < readerCount; i++) {
      final var reader = instance.getReader(i);
      readDigestFutures.add(CompletableFuture.supplyAsync(() -> readStream(reader), executorService));
    }

    // Then
    final var writtenDigest = writtenDigestFuture.get(1, TimeUnit.MINUTES);
    for (final var readDigestFuture : readDigestFutures) {
      final var readDigest = readDigestFuture.get(1, TimeUnit.MINUTES);
      assertEquals(writtenDigest, readDigest);
    }

    instance.close();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  private String writeRandomData(final int size, final BoundedByteBuffer boundedByteBuffer) {
    try (final var writer = boundedByteBuffer.getWriter()) {
      final var fis = new FakeInputStream(size, (byte) 'a');
      return writeToStream(writer, fis);
    }
  }

  private String readStream(final InputStream inputStream) {
    try {
      final var digestInputStream = new MultipleActionsInputStream(inputStream);
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      final OutputStream os = new VoidOutputStream();
      digestInputStream.transferTo(os);
      digestInputStream.close();
      return digestInputStream.getDigestBase64();
    } catch (final IOException | NoSuchAlgorithmException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  private String writeToStream(final BoundedByteBuffer.Writer writer, final InputStream inputStream) {
    try {
      final var buffer = new byte[1024];
      final var digestInputStream = new MultipleActionsInputStream(inputStream);
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      int n;
      while (EOF != (n = digestInputStream.read(buffer))) {
        writer.write(buffer, 0, n);
      }
      writer.writeEOF();
      return digestInputStream.getDigestBase64();
    } catch (final InterruptedException | IOException | NoSuchAlgorithmException e) {
      LOGGER.error(e);
      throw new RuntimeException("Writer error", e);
    } finally {
      writer.close();
    }
  }

  @Test
  void testMultipleReaders() throws Exception {
    simpleTest(10, 0);
    simpleTest(10, 100);
    simpleTest(10, 1024);
    simpleTest(10, 4096);
    simpleTest(10, 4097);
    simpleTest(10, 10 * 1024 * 1024);
  }

  @Test
  void testBrokenWriter() throws Exception {

    testBrokenWriter(1, 0);
    testBrokenWriter(1, 100);
    testBrokenWriter(1, 1024);
    testBrokenWriter(10, 4096);
    testBrokenWriter(10, 4097);
    testBrokenWriter(10, 10 * 1024 * 1024);
  }

  private void testBrokenWriter(final int readerCount, final int sizeBeforeBrokenStream) throws InterruptedException {

    // Given
    final var instance = new BoundedByteBuffer(BUFFER_SIZE, readerCount);
    final var executorService = Executors.newCachedThreadPool();

    // When
    final var writtenDigestFuture =
        CompletableFuture.supplyAsync(() -> writeBrokenData(sizeBeforeBrokenStream, instance), executorService);
    final List<CompletableFuture<String>> readDigestFutures = new ArrayList<>();
    for (var i = 0; i < readerCount; i++) {
      final var reader = instance.getReader(i);
      readDigestFutures.add(CompletableFuture.supplyAsync(() -> readStream(reader), executorService));
    }

    // Then
    assertThrows(ExecutionException.class, () -> writtenDigestFuture.get(1, TimeUnit.MINUTES));

    for (final var readDigestFuture : readDigestFutures) {
      assertThrows(ExecutionException.class, () -> readDigestFuture.get(1, TimeUnit.MINUTES));
    }

    instance.close();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  private String writeBrokenData(final int sizeBeforeBrokenStream, final BoundedByteBuffer boundedByteBuffer) {
    try (final var writer = boundedByteBuffer.getWriter()) {
      final var fis = new FakeInputStream(sizeBeforeBrokenStream, (byte) 'a');
      final var brokenInputStream = new BrokenInputStream();
      final var sequenceInputStream = new SequenceInputStream(fis, brokenInputStream);
      return writeToStream(writer, sequenceInputStream);
    }
  }

  @Test
  void givenPartialReaderFailureThenOtherReadersShouldCompleteSuccessfully() throws Exception {

    final var size = 1_000_000;
    final var readerCount = 3;
    final var failingReaderIndex = 1;

    // Given
    final var executorService = Executors.newCachedThreadPool();
    final var instance = new BoundedByteBuffer(BUFFER_SIZE, readerCount);

    instance.getReader(failingReaderIndex).close();

    // When
    final var writtenDigestFuture =
        CompletableFuture.supplyAsync(() -> writeRandomData(size, instance), executorService);
    final List<CompletableFuture<String>> readDigestFutures = new ArrayList<>();
    for (var i = 0; i < readerCount; i++) {
      final var reader = instance.getReader(i);
      readDigestFutures.add(CompletableFuture.supplyAsync(() -> readStream(reader), executorService));
    }

    // Then
    final var writtenDigest = writtenDigestFuture.get(1, TimeUnit.MINUTES);
    for (var i = 0; i < readerCount; i++) {
      if (i == failingReaderIndex) {
        assertThrows(ExecutionException.class,
            () -> readDigestFutures.get(failingReaderIndex).get(1, TimeUnit.MINUTES));
      } else {
        final var readDigest = readDigestFutures.get(i).get(1, TimeUnit.MINUTES);
        assertEquals(writtenDigest, readDigest);
      }
    }

    instance.close();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  void givenAllReadersFailThenWriterShouldFail() throws Exception {

    final var size = 1_000_000_000;
    final var readerCount = 3;

    // Given
    final var executorService = Executors.newCachedThreadPool();
    final var instance = new BoundedByteBuffer(BUFFER_SIZE, readerCount);

    // When
    final var writtenDigestFuture =
        CompletableFuture.supplyAsync(() -> writeRandomData(size, instance), executorService);
    for (var i = 0; i < readerCount; i++) {
      final var reader = instance.getReader(i);
      CompletableFuture.supplyAsync(() -> failReadStream(reader), executorService);
    }

    // Then
    assertThrows(ExecutionException.class, () -> writtenDigestFuture.get(1, TimeUnit.MINUTES));

    instance.close();

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  private String failReadStream(final InputStream reader) {
    try {
      reader.close();
      return null;
    } catch (final IOException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  @Test
  void testWrongWriter() {
    BoundedByteBuffer boundedByteBuffer = new BoundedByteBuffer(100, 10);
    byte[] bytes = new byte[101];
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getWriter().write(bytes, -1, 1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getWriter().write(bytes, 0, -1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getWriter().write(bytes, 100, 100));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getWriter().write(bytes, 0, 101));
    boundedByteBuffer.close();
  }

  @Test
  void testWrongReader() throws IOException {
    BoundedByteBuffer boundedByteBuffer = new BoundedByteBuffer(100, 10);
    byte[] bytes = new byte[101];
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getReader(0).read(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getReader(1).read(null, 0, 1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getReader(1).read(bytes, -1, 1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getReader(2).read(bytes, 0, -1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> boundedByteBuffer.getReader(3).read(bytes, 100, 100));
    assertEquals(0, boundedByteBuffer.getReader(3).read(bytes, 100, 0));
    boundedByteBuffer.close();
  }
}
