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
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tes for TeeInputStream
 */
@QuarkusTest
class TeeInputStreamHandlerTest {
  private static final Logger LOGGER = Logger.getLogger(TeeInputStreamHandlerTest.class);
  private static final int INPUTSTREAM_SIZE = 65536 * 4 * 10;
  private static final TreeMap<Double, String> TIMES = new TreeMap<>();
  private static long SUPPOSED_LOW_MEM = 100;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    final var runtime = Runtime.getRuntime();
    SUPPOSED_LOW_MEM = (long) ((runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) * 1.2);
  }

  @AfterAll
  static void endingClass() {
    final var builder = new StringBuilder("Time Results:");
    TIMES.forEach((t, u) -> builder.append("\n\t").append(u));
    LOGGER.warn(builder.toString());
  }

  final Runtime runtime = Runtime.getRuntime();
  long preMemory;
  long postMemory;

  private long getGcCount() {
    var sum = 0L;
    for (final var b : ManagementFactory.getGarbageCollectorMXBeans()) {
      final var count = b.getCollectionCount();
      if (count != -1) {
        sum += count;
      }
    }
    return sum;
  }

  private long getReallyUsedMemory() throws InterruptedException {
    final var before = getGcCount();
    System.gc();
    while (getGcCount() == before) {
      System.gc();
      Thread.sleep(10);
    }
    return getCurrentlyAllocatedMemory();
  }

  private long getReallyUsedMemoryConstraint(final long mem) throws InterruptedException {
    for (var i = 0; i < 20; i++) {
      if (getReallyUsedMemory() > mem) {
        Thread.sleep(10);
      } else {
        break;
      }
    }
    return getReallyUsedMemory();
  }

  private long getCurrentlyAllocatedMemory() {
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
  }

  @BeforeEach
  void memoryConsumptionBefore() throws InterruptedException {
    getReallyUsedMemoryConstraint(SUPPOSED_LOW_MEM);
    System.gc();
    preMemory = getReallyUsedMemory();
    LOGGER.info(preMemory + " MB");
  }

  @AfterEach
  void memoryConsumptionAfter() {
    postMemory = getCurrentlyAllocatedMemory();
    LOGGER.info(postMemory + " MB so real consumption: " + (postMemory - preMemory) + " MB");
  }

  @Test
  void badInitialization() {

    try (final var teeInputStream = new TeeInputStream(null, 1)) {
      fail("Should raised illegal argument");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      // nothing
    }
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, 0)) {
      fail("Should raised illegal argument");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      // nothing
    } catch (final IOException e) {
      fail(e);
    }
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, 1)) {
      teeInputStream.getInputStream(-1);
      fail("Should raised illegal argument");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      // nothing
    } catch (final IOException e) {
      fail(e);
    }
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, 1)) {
      teeInputStream.getInputStream(1);
      fail("Should raised illegal argument");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      // ignore
    } catch (final IOException e) {
      fail(e);
    }
    try (final var wrongInputStream = new WrongInputStream(INPUTSTREAM_SIZE);
         final var teeInputStream = new TeeInputStream(wrongInputStream, 1)) {
      Thread.sleep(10);
      assertThrows(IOException.class, () -> teeInputStream.throwLastException());
    } catch (final CcsInvalidArgumentRuntimeException | InterruptedException e) {
      // ignore
    } catch (final IOException e) {
      fail(e);
    }
  }

  private static class WrongInputStream extends FakeInputStream {

    public WrongInputStream(final long len) {
      super(len);
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
      throw new IOException("test");
    }
  }

  @Test
  void testTeeInputStreamSingle() {
    final var start = System.nanoTime();

    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, 1)) {
      assertNotNull(teeInputStream.toString());
      final var is = teeInputStream.getInputStream(0);
      checkSize(INPUTSTREAM_SIZE, is);
      teeInputStream.throwLastException();
    } catch (final IOException e) {
      fail("Should not raised an exception: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOGGER.debugf("Read 1: \t%d ns", stop - start);

    addTimer(INPUTSTREAM_SIZE / ((stop - start) / 1000.0), "SINGLE_BYTE :\t" + (stop - start));
  }

  private void checkSize(final int size, final InputStream is) throws IOException {
    final var cos = new CountingOutputStream(new VoidOutputStream());
    IOUtils.copy(is, cos);
    assertEquals(size, cos.getCount());
  }

  private void addTimer(double speed, final String result) {
    while (TIMES.containsKey(speed)) {
      speed += 0.01;
    }
    TIMES.put(speed, speed + " = " + result);
  }

  @Test
  void testTeeInputStreamBlock() {
    testTeeInputStreamBlock(100);
    testTeeInputStreamBlock(512);
    testTeeInputStreamBlock(1024);
    testTeeInputStreamBlock(4000);
    testTeeInputStreamBlock(8192);
    testTeeInputStreamBlock(40000);
    testTeeInputStreamBlock(65536);
    testTeeInputStreamBlock(80000);
    testTeeInputStreamBlock(100000);
  }

  private void testTeeInputStreamBlock(final int size) {
    final var start = System.nanoTime();
    try (final var fakeInputStream = new FakeInputStream(size, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, 1)) {
      final var is = teeInputStream.getInputStream(0);
      checkSize(size, is);
      teeInputStream.throwLastException();
    } catch (final IOException e) {
      fail("Should not raised an exception: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOGGER.debugf("Read %d: \t%d ns", size, stop - start);
    addTimer(size / ((stop - start) / 1000.0),
        "SINGLE_BLOCK_" + size + "  :\t" + (stop - start) + "\t" + (size / ((stop - start) / 1000.0)));
  }

  @Test
  void testTeeInputStreamMultipleShift8K() {
    final var start = System.nanoTime();
    final var size = 8192;
    final var nb = 10;
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, nb)) {
      final var is = new InputStream[nb];
      final var total = new long[nb];
      for (var i = 0; i < nb; i++) {
        is[i] = teeInputStream.getInputStream(i);
        total[i] = 0;
      }
      int read;
      final var buffer = new byte[size];
      var rank = 1;
      while (total[0] < INPUTSTREAM_SIZE) {
        rank = (rank + 1) % nb;
        if ((read = is[rank].read(buffer)) > 0) {
          total[rank] += read;
        }
      }
      for (var i = 0; i < nb; i++) {
        while ((read = is[i].read(buffer)) > 0) {
          total[i] += read;
        }
        assertEquals(INPUTSTREAM_SIZE, total[i], "rank: " + i);
      }
      teeInputStream.throwLastException();
    } catch (final IOException e) {
      System.gc();
      LOGGER.error(e);
      fail("Should not raised an exception: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOGGER.debugf("Read %d: \t%d ns", size, stop - start);
    addTimer(INPUTSTREAM_SIZE / ((stop - start) / 1000.0),
        "MULTIPLE_BLOCK_" + size + "_" + nb + " :\t" + (stop - start) + "  \t" + (stop - start) / nb + "\t" +
            (INPUTSTREAM_SIZE / ((stop - start) / 1000.0)));
  }

  @Test
  void testTeeInputStreamMultipleShift64K() {
    final var size = 64 * 1024;
    testTeeInputStreamMultipleShiftBufferSize(size);
  }

  void testTeeInputStreamMultipleShiftBufferSize(final int size) {
    final var start = System.nanoTime();
    final long lsize = 1024 * 1024 * 1024;
    final var nb = 10;
    try (final var fakeInputStream = new FakeInputStream(lsize, (byte) 'A');
         final var teeInputStream = new TeeInputStream(fakeInputStream, nb)) {
      final var is = new InputStream[nb];
      final var total = new long[nb];
      for (var i = 0; i < nb; i++) {
        is[i] = teeInputStream.getInputStream(i);
        total[i] = 0;
      }
      int read;
      final var buffer = new byte[size];
      var rank = 1;
      while (total[0] < lsize) {
        rank = (rank + 1) % nb;
        if ((read = is[rank].read(buffer)) > 0) {
          total[rank] += read;
        }
      }
      for (var i = 0; i < nb; i++) {
        while ((read = is[i].read(buffer)) > 0) {
          total[i] += read;
        }
        assertEquals(lsize, total[i], "rank: " + i);
      }
      teeInputStream.throwLastException();
    } catch (final IOException e) {
      System.gc();
      LOGGER.error(e);
      fail("Should not raised an exception: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOGGER.debugf("Read %d: \t%d ns", size, stop - start);
    addTimer(lsize / ((stop - start) / 1000.0),
        "MULTIPLE_BIG_BLOCK_" + size + "_" + nb + " :\t" + (stop - start) + "  \t" + (stop - start) / nb + "\t" +
            (lsize / ((stop - start) / 1000.0)));
  }

  @Test
  void testTeeInputStreamMultipleShiftBufSize() {
    final var size = QuarkusProperties.getBufSize();
    testTeeInputStreamMultipleShiftBufferSize(size);
  }

  @Test
  void testTeeInputStreamMultipleMultiThread() {
    testTeeInputStreamMultipleMultiThread(1, 8192, true, true);
    testTeeInputStreamMultipleMultiThread(1, 8192, true, true);
    testTeeInputStreamMultipleMultiThread(10, 8192, true, true);
    testTeeInputStreamMultipleMultiThread(1, 65536, true, true);
    testTeeInputStreamMultipleMultiThread(10, 65536, true, true);

    testTeeInputStreamMultipleMultiThread(1, 8192, false, true);
    testTeeInputStreamMultipleMultiThread(10, 8192, false, true);
    testTeeInputStreamMultipleMultiThread(1, 65536, false, true);
    testTeeInputStreamMultipleMultiThread(10, 65536, false, true);
  }

  private void testTeeInputStreamMultipleMultiThread(final int nb, final int size, final boolean block,
                                                     final boolean timer) {
    final var start = System.nanoTime();
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a');
         final var teeInputStream = new TeeInputStream(fakeInputStream, nb)) {
      InputStream is;
      @SuppressWarnings("unchecked") final Future<Integer>[] total = new Future[nb];
      final var executor = Executors.newCachedThreadPool();
      for (var i = 0; i < nb; i++) {
        is = teeInputStream.getInputStream(i);
        final var threadReader = new ThreadReader(i, is, size);
        total[i] = executor.submit(threadReader);
      }
      executor.shutdown();
      while (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
        // Empty
      }
      for (var i = 0; i < nb; i++) {
        assertEquals(INPUTSTREAM_SIZE, (int) total[i].get());
      }
      teeInputStream.throwLastException();
    } catch (final InterruptedException | ExecutionException | IOException e) {
      LOGGER.error(e);
      fail("Should not raised an exception: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOGGER.debugf("Read %d: \t%d ns", size, stop - start);
    if (timer) {
      addTimer(INPUTSTREAM_SIZE / ((stop - start) / 1000.0),
          "PARALLEL_VAR_SIZE_" + (block ? "BLOCK_" : "BYTE_") + size + "_" + nb + " :\t" + (stop - start) + "  \t" +
              (stop - start) / nb + "\t" + (INPUTSTREAM_SIZE / ((stop - start) / 1000.0)));
    }
  }

  @Test
  void testTeeInputStreamMultiRead() {
    try {
      for (var i = 0; i < 1002; i++) {
        testTeeInputStreamMultipleMultiThread(1, 1024, true, false);
      }
    } catch (final OutOfMemoryError e) {
      System.gc();
      LOGGER.error(e);
    }
  }

  @Test
  void testTeeInputStreamMultipleMultiThreadWithVariableSizes() {
    for (var len = 100; len < 2200; len += 500) {
      testTeeInputStreamMultipleMultiThread(1, len, true, false);
      testTeeInputStreamMultipleMultiThread(10, len, true, false);

      testTeeInputStreamMultipleMultiThread(1, len, false, false);
      testTeeInputStreamMultipleMultiThread(10, len, false, false);
    }
    for (var len = 100; len < 80000; len += 10000) {
      testTeeInputStreamMultipleMultiThread(1, len, true, false);
      testTeeInputStreamMultipleMultiThread(10, len, true, false);

      testTeeInputStreamMultipleMultiThread(1, len, false, false);
      testTeeInputStreamMultipleMultiThread(10, len, false, false);
    }
  }

  @Test
  void testCloseEarly() {
    final var size = 8192;
    final var nb = 10;
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a');
         final var teeInputStream = new TeeInputStream(fakeInputStream, nb)) {
      final var is = new InputStream[nb];
      for (var i = 0; i < nb; i++) {
        is[i] = teeInputStream.getInputStream(i);
      }
      final var buffer = new byte[size];
      var rank = 1;
      for (var i = 0; i < 100; i++) {
        rank = (rank + 1) % nb;
        if (is[rank].read(buffer) < 0) {
          break;
        }
      }
      teeInputStream.throwLastException();
      teeInputStream.close();
      for (var i = 0; i < nb; i++) {
        assertEquals(0, is[i].available(), "rank: " + i);
      }
    } catch (final IOException e) {
      fail("Should not raised an exception: " + e.getMessage());
    }
  }

  @Test
  void testMultipleCloseWithoutConsumingAll() {
    final var size = 8192;
    final var nb = 10;
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a');
         final var teeInputStream = new TeeInputStream(fakeInputStream, nb)) {
      final var is = new InputStream[nb];
      for (var i = 0; i < nb; i++) {
        is[i] = teeInputStream.getInputStream(i);
      }
      final var buffer = new byte[size];
      var rank = 1;
      var r = 0;
      for (var i = 0; i < 100; i++) {
        rank = (rank + 1) % nb;
        r = is[rank].read(buffer);
      }
      for (var i = 0; i < nb; i++) {
        is[i].close();
        assertEquals(0, is[i].available(), "rank: " + i);
      }
      teeInputStream.throwLastException();
    } catch (final IOException e) {
      fail("Should not raised an exception: " + e.getMessage());
    }
  }

  @Test
  void testMultipleCloseExceptOne() {
    final var size = 8192;
    final var nb = 10;
    try (final var fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a');
         final var teeInputStream = new TeeInputStream(fakeInputStream, nb)) {
      final var is = new InputStream[nb];
      for (var i = 0; i < nb; i++) {
        is[i] = teeInputStream.getInputStream(i);
      }
      final var buffer = new byte[size];
      var rank = 1;
      var r = 0;
      for (var i = 0; i < 100; i++) {
        rank = (rank + 1) % nb;
        r = is[rank].read(buffer);
        assertNotEquals(0, is[rank].available(), "rank: " + rank + " for " + i);
      }
      for (var i = 0; i < nb - 1; i++) {
        assertNotEquals(0, is[i].available(), "rank: " + i);
        is[i].close();
        assertEquals(0, is[i].available(), "rank: " + i);
      }
      assertNotEquals(0, is[nb - 1].available(), "rank: " + (nb - 1));
      while ((r = is[nb - 1].read(buffer)) >= 0) {
        // Continue
      }
      assertEquals(0, is[nb - 1].available(), "rank: " + (nb - 1));
      is[nb - 1].close();
      teeInputStream.throwLastException();
    } catch (final IOException e) {
      fail("Should not raised an exception: " + e.getMessage());
    }
  }

  @Test
  void testConcurrentBrokenMultipleTeeInputStreamHandler() {
    final List<FakeInputStream> listStream = new ArrayList<>(10 + 1);
    try {
      final List<TeeInputStream> list = new ArrayList<>(10);
      for (var i = 0; i < 10 + 1; i++) {
        listStream.add(new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a'));
      }
      for (var i = 0; i < 10; i++) {
        try {
          final var teeInputStream = new TeeInputStream(listStream.get(i), 1);
          list.add(teeInputStream);
          LOGGER.debug(teeInputStream.toString());
        } catch (final IllegalArgumentException e) {
          LOGGER.error(e);
          fail("Should not be interrupted");
        }
      }
      // Try to allocate once and possible
      try {
        list.add(new TeeInputStream(listStream.get(10), 1));
      } catch (final IllegalArgumentException e) {
        fail("Should be interrupted");
      }
      // Now free half of the list
      for (var i = 10 - 1; i >= 5; i--) {
        final var teeInputStream = list.remove(i);
        try {
          teeInputStream.throwLastException();
        } catch (final IOException e) {
          LOGGER.error(e);
          fail("Should not have an exception");
        }
        teeInputStream.close();
        LOGGER.debug(teeInputStream.toString());
        SystemTools.silentlyCloseNoException(listStream.remove(i));
        listStream.add(new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a'));
      }
      // Now reallocate 5
      for (var i = 5; i < 10; i++) {
        try {
          final var teeInputStream = new TeeInputStream(listStream.get(i), 1);
          list.add(teeInputStream);
          LOGGER.debug(teeInputStream.toString());
        } catch (final IllegalArgumentException e) {
          LOGGER.error(e);
          fail("Should not be interrupted");
        }
      }
      for (final var teeInputStream : list) {
        try {
          teeInputStream.throwLastException();
        } catch (final IOException e) {
          LOGGER.error(e);
          fail("Should not have an exception");
        }
        teeInputStream.close();
        LOGGER.debug(teeInputStream.toString());
      }
      for (final var fakeInputStream : listStream) {
        SystemTools.silentlyCloseNoException(fakeInputStream);
      }
      for (var i = 0; i < 10; i++) {
        listStream.add(new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a'));
      }
      for (var i = 0; i < 10; i++) {
        try {
          final var teeInputStream = new TeeInputStream(listStream.get(i), 1);
          LOGGER.debug(teeInputStream.toString());
          final var stream = teeInputStream.getInputStream(0);
          SystemTools.silentlyCloseNoException(stream);
          teeInputStream.throwLastException();
          teeInputStream.close();
          LOGGER.debug(teeInputStream.toString());
        } catch (final IllegalArgumentException | IOException e) {
          LOGGER.error(e);
          fail("Should not be interrupted");
        }
      }
    } finally {
      for (final var fakeInputStream : listStream) {
        SystemTools.silentlyCloseNoException(fakeInputStream);
      }
    }
  }

  @Test
  void testConcurrentMultipleThreadTeeInputStreamHandler() {
    final var nb = 10;
    final List<FakeInputStream> listStream = new ArrayList<>(nb + 1);
    try {
      final List<TeeInputStream> list = new ArrayList<>(nb);
      for (var i = 0; i < nb; i++) {
        listStream.add(new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a'));
      }
      final var executor = Executors.newCachedThreadPool();
      @SuppressWarnings("unchecked") final Future<Integer>[] total = new Future[nb];
      for (var i = 0; i < nb; i++) {
        final var teeInputStream = new TeeInputStream(listStream.get(i), 1);
        list.add(teeInputStream);
        LOGGER.debug(teeInputStream.toString());
        final var stream = teeInputStream.getInputStream(0);
        final var threadReader = new ThreadReader(i, stream, QuarkusProperties.getBufSize());
        total[i] = executor.submit(threadReader);
      }
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e1) {
        // ignore
      }
      executor.shutdown();
      try {
        while (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
          // Empty
        }
      } catch (final InterruptedException e) {
        LOGGER.error(e);
        fail("Should not failed");
      }
      for (var i = 0; i < nb; i++) {
        try {
          assertEquals(INPUTSTREAM_SIZE, (int) total[i].get(), "Rank not equal: " + i);
        } catch (final InterruptedException | ExecutionException e) {
          LOGGER.error(e);
          fail("Should not failed");
        }
      }
      for (final var teeInputStream : list) {
        teeInputStream.throwLastException();
        teeInputStream.close();
      }

    } catch (final IOException e) {
      LOGGER.error(e);
      fail("Should not have an exception");
    } finally {
      for (final var fakeInputStream : listStream) {
        SystemTools.silentlyCloseNoException(fakeInputStream);
      }
    }
  }

  private record ThreadReader(int rank, InputStream is, int size) implements Callable<Integer> {

    @Override
    public Integer call() {
      int read;
      var total = 0;
      final var buffer = new byte[size];
      try {
        while ((read = is.read(buffer)) >= 0) {
          LOGGER.debugf("%d Read: %d", rank, read);
          total += read;
        }
        LOGGER.debugf("%d Read: %d Total: %d", rank, read, total);
        return total;
      } catch (final IOException e) {
        LOGGER.error(e);
        return total;
      }
    }

  }
}
