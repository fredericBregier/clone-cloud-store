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

package io.clonecloudstore.common.standard.stream;

import java.io.IOException;
import java.time.Instant;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.FakeIterator;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestMethodOrder(MethodOrderer.MethodName.class)
class StreamIteratorUtilsTest {
  private static final int NB = 1000000;

  @RegisterForReflection
  static class Dto {
    int value;
    String svalue;

    Dto() {

    }

    Dto(final AtomicInteger cpt) {
      this.value = cpt.incrementAndGet();
      this.svalue = value + "A";
    }

    public int getValue() {
      return value;
    }

    public void setValue(final int value) {
      this.value = value;
    }

    public String getSvalue() {
      return svalue;
    }

    public void setSvalue(final String svalue) {
      this.svalue = svalue;
    }
  }

  @RegisterForReflection
  static class WrongDto {
    int value;
    Instant svalue;

    WrongDto() {

    }

    WrongDto(final AtomicInteger cpt) {
      this.value = cpt.incrementAndGet();
      this.svalue = Instant.now();
    }

    public int getValue() {
      return value;
    }

    public void setValue(final int value) {
      this.value = value;
    }

    public Instant getSvalue() {
      return svalue;
    }

    public void setSvalue(final Instant instant) {
      this.svalue = instant;
    }
  }

  @Test
  void test01StreamToJsonInputStreamFailedOnConsumeAndInputStream() throws CcsWithStatusException, IOException {
    final var stream = Stream.generate(Object::new).limit(NB);
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, Dto.class);
    Log.info("Caught", assertThrows(Exception.class, () -> {
      streamFinal.count();
      inputStream.close();
    }));

    final var iterator = Stream.generate(Object::new).limit(NB).iterator();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, Dto.class);
    final var iteratorFromInputStream = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, Dto.class);
    Log.info("Caught", assertThrows(Exception.class, () -> {
      SystemTools.consumeAll(iteratorFromInputStream);
      inputStream2.close();
    }));
  }

  @Test
  void test02StreamToJsonInputStreamFailedOnInputStream() throws IOException {
    final var stream = Stream.generate(Object::new).limit(NB);
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, Dto.class);
    Log.info("Caught", assertThrows(IOException.class, () -> {
      FakeInputStream.consumeAll(inputStream);
      inputStream.close();
    }));

    final var iterator = Stream.generate(Object::new).limit(NB).iterator();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, Dto.class);
    Log.info("Caught", assertThrows(IOException.class, () -> {
      FakeInputStream.consumeAll(inputStream2);
      inputStream2.close();
    }));
  }

  @Test
  void test03StreamToJsonInputStreamFailedOnConsumeWithWrongDto() throws CcsWithStatusException, IOException {
    final var cpt = new AtomicInteger(0);
    final var stream = Stream.generate(() -> new Dto(cpt)).limit(10);
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, WrongDto.class);
    assertThrows(CcsWithStatusException.class, () -> streamFinal.count());
    inputStream.close();

    final var iterator = Stream.generate(() -> new Dto(cpt)).limit(10).iterator();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, Dto.class);
    final var iterator1 = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, WrongDto.class);
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemTools.consumeAll(iterator1));
    inputStream2.close();
  }

  @Test
  void test04StreamToJsonInputStreamFailedOnStream() throws IOException, CcsWithStatusException {
    final var cpt = new AtomicInteger(0);
    final var stream = Stream.generate(() -> {
      if (cpt.get() > 5) {
        throw new CcsInvalidArgumentRuntimeException("Failed item");
      }
      return new Dto(cpt);
    }).limit(NB);
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, Dto.class);
    Log.info("Caught", assertThrows(IOException.class, () -> {
      FakeInputStream.consumeAll(inputStream);
      inputStream.close();
    }));

    cpt.set(0);
    final var iterator = Stream.generate(() -> {
      if (cpt.get() > 5) {
        throw new CcsInvalidArgumentRuntimeException("Failed item");
      }
      return new Dto(cpt);
    }).limit(NB).iterator();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, Dto.class);
    final var iteratorFromInputStream = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, Dto.class);
    Log.info("Caught", assertThrows(IOException.class, () -> {
      FakeInputStream.consumeAll(inputStream2);
      inputStream2.close();
    }));
  }

  @Test
  void test05StreamToJsonInputStreamFailedOnStream() throws IOException, CcsWithStatusException {
    final var cpt = new AtomicInteger(0);
    final var stream = Stream.generate(() -> {
      if (cpt.get() > 5) {
        Thread.currentThread().interrupt();
      }
      return new Dto(cpt);
    }).limit(NB);
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, Dto.class);
    Log.info("Caught", assertThrows(Exception.class, () -> {
      streamFinal.count();
      inputStream.close();
    }));

    cpt.set(0);
    final var iterator = Stream.generate(() -> {
      if (cpt.get() > 5) {
        Thread.currentThread().interrupt();
      }
      return new Dto(cpt);
    }).limit(NB).iterator();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, Dto.class);
    final var iteratorFromInputStream = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, Dto.class);
    Log.info("Caught", assertThrows(Exception.class, () -> {
      SystemTools.consumeAll(iteratorFromInputStream);
      inputStream2.close();
    }));
  }

  @Test
  void test10StreamToJsonInputStream() throws CcsWithStatusException, IOException {
    final var cpt = new AtomicInteger(0);
    final var stream = Stream.generate(() -> new Dto(cpt)).limit(NB);
    final var start = System.nanoTime();
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, Dto.class);
    assertEquals(NB, streamFinal.count());
    inputStream.close();
    final var stop = System.nanoTime();
    Log.infof("MicroBenchmark on Stream: %f ms so %f objects/s", ((stop - start) / 1000000.0),
        NB * 1000.0 / ((stop - start) / 1000000.0));

    final var iterator = Stream.generate(() -> new Dto(cpt)).limit(NB).iterator();
    final var start2 = System.nanoTime();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, Dto.class);
    final var iteratorFromInputStream = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, Dto.class);
    assertEquals(NB, SystemTools.consumeAll(iteratorFromInputStream));
    inputStream2.close();
    final var stop2 = System.nanoTime();
    Log.infof("MicroBenchmark on Iterator: %f ms so %f objects/s", ((stop2 - start2) / 1000000.0),
        NB * 1000.0 / ((stop2 - start2) / 1000000.0));
  }

  @Test
  void test11StreamToJsonInputStreamConvert() throws CcsWithStatusException, IOException {
    final var cpt = new AtomicInteger(0);
    final var stream = Stream.generate(() -> new Dto(cpt)).limit(NB);
    final var start = System.nanoTime();
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, source -> {
      ((Dto) source).value++;
      return source;
    }, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, Dto.class);
    assertEquals(NB, streamFinal.count());
    inputStream.close();
    final var stop = System.nanoTime();
    Log.infof("MicroBenchmark on Stream Transformed: %f ms so %f objects/s", ((stop - start) / 1000000.0),
        NB * 1000.0 / ((stop - start) / 1000000.0));

    final var iterator = Stream.generate(() -> new Dto(cpt)).limit(NB).iterator();
    final var start2 = System.nanoTime();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, source -> {
      ((Dto) source).value++;
      return source;
    }, Dto.class);
    final var iteratorFromInputStream = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, Dto.class);
    assertEquals(NB, SystemTools.consumeAll(iteratorFromInputStream));
    inputStream2.close();
    final var stop2 = System.nanoTime();
    Log.infof("MicroBenchmark on Iterator Transformed: %f ms so %f objects/s", ((stop2 - start2) / 1000000.0),
        NB * 1000.0 / ((stop2 - start2) / 1000000.0));

    cpt.set(0);
    final var iterator2 = Stream.generate(() -> new Dto(cpt)).limit(NB).iterator();
    final var start3 = System.nanoTime();
    final var inputStream3 = StreamIteratorUtils.getInputStreamFromIterator(iterator2, source -> {
      if (((Dto) source).value == 1) {
        return null;
      }
      ((Dto) source).value++;
      return source;
    }, Dto.class);
    final var iteratorFromInputStream3 = StreamIteratorUtils.getIteratorFromInputStream(inputStream3, Dto.class);
    assertEquals(NB - 1, SystemTools.consumeAll(iteratorFromInputStream3));
    inputStream3.close();
    final var stop3 = System.nanoTime();
    Log.infof("MicroBenchmark on Iterator TransformedNull: %f ms so %f objects/s", ((stop3 - start3) / 1000000.0),
        NB * 1000.0 / ((stop3 - start3) / 1000000.0));
  }

  @Test
  void test12EmptyStreamToJsonInputStreamConvert() throws CcsWithStatusException, IOException {
    final var stream = Stream.empty();
    final var start = System.nanoTime();
    final var inputStream = StreamIteratorUtils.getInputStreamFromStream(stream, source -> {
      ((Dto) source).value++;
      return source;
    }, Dto.class);
    final var streamFinal = StreamIteratorUtils.getStreamFromInputStream(inputStream, Dto.class);
    assertEquals(0, streamFinal.count());
    inputStream.close();
    final var stop = System.nanoTime();
    Log.infof("MicroBenchmark on empty Stream Transformed: %f ms", ((stop - start) / 1000000.0));

    final var iterator = Stream.empty().iterator();
    final var start2 = System.nanoTime();
    final var inputStream2 = StreamIteratorUtils.getInputStreamFromIterator(iterator, source -> {
      ((Dto) source).value++;
      return source;
    }, Dto.class);
    final var iteratorFromInputStream = StreamIteratorUtils.getIteratorFromInputStream(inputStream2, Dto.class);
    assertEquals(0, SystemTools.consumeAll(iteratorFromInputStream));
    inputStream2.close();
    final var stop2 = System.nanoTime();
    Log.infof("MicroBenchmark on empty Iterator Transformed: %f ms", ((stop2 - start2) / 1000000.0));
  }

  @Test
  void test13IteratorToList() {
    final var iterator = Stream.empty().iterator();
    var list = StreamIteratorUtils.getListFromIterator(iterator);
    assertTrue(list.isEmpty());
    list = StreamIteratorUtils.getListFromIterator(null);
    assertTrue(list.isEmpty());
    final var cpt = new AtomicInteger(0);
    final var iterator2 = Stream.generate(() -> new Dto(cpt)).limit(NB).iterator();
    var list2 = StreamIteratorUtils.getListFromIterator(iterator2);
    assertFalse(list2.isEmpty());
    assertEquals(NB, list2.size());
  }

  @Test
  void test99MicroBenchmarks() throws IOException, CcsWithStatusException {
    final FakeIterator<Long> fakeIterator = new FakeIterator<>(100000, l -> l);
    // warmup
    microBenchmark(fakeIterator, false);
    // real
    microBenchmark(fakeIterator, true);
  }

  void microBenchmark(final FakeIterator<Long> fakeIterator, final boolean log)
      throws IOException, CcsWithStatusException {
    final int NB = 10;
    final var results = log ? new TreeMap<Double, String>() : null;
    {
      var start = System.nanoTime();
      for (int i = 0; i < NB; i++) {
        fakeIterator.reset();
        FakeInputStream.consumeAll(StreamIteratorUtils.getInputStreamFromIterator(fakeIterator, Long.class));
      }
      var stop = System.nanoTime();
      if (log) {
        results.put((stop - start) / 1000000.0, "InputStreamFromIterator");
      }
    }
    {
      var start = System.nanoTime();
      for (int i = 0; i < NB; i++) {
        fakeIterator.reset();
        FakeInputStream.consumeAll(StreamIteratorUtils.getInputStreamFromIterator(fakeIterator, l -> l, Long.class));
      }
      var stop = System.nanoTime();
      if (log) {
        results.put((stop - start) / 1000000.0, "InputStreamTransformFromIterator");
      }
    }
    {
      var start = System.nanoTime();
      for (int i = 0; i < NB; i++) {
        fakeIterator.reset();
        FakeInputStream.consumeAll(
            StreamIteratorUtils.getInputStreamFromStream(StreamIteratorUtils.getStreamFromIterator(fakeIterator),
                Long.class));
      }
      var stop = System.nanoTime();
      if (log) {
        results.put((stop - start) / 1000000.0, "InputStreamFromStream");
      }
    }
    {
      var start = System.nanoTime();
      for (int i = 0; i < NB; i++) {
        fakeIterator.reset();
        FakeInputStream.consumeAll(
            StreamIteratorUtils.getInputStreamFromStream(StreamIteratorUtils.getStreamFromIterator(fakeIterator),
                l -> l, Long.class));
      }
      var stop = System.nanoTime();
      if (log) {
        results.put((stop - start) / 1000000.0, "InputStreamTransformFromStream");
      }
    }
    {
      var start = System.nanoTime();
      for (int i = 0; i < NB; i++) {
        fakeIterator.reset();
        SystemTools.consumeAll(StreamIteratorUtils.getIteratorFromInputStream(
            StreamIteratorUtils.getInputStreamFromIterator(fakeIterator, Long.class), Long.class));
      }
      var stop = System.nanoTime();
      if (log) {
        results.put((stop - start) / 1000000.0, "IteratorFromIterator");
      }
    }
    {
      var start = System.nanoTime();
      for (int i = 0; i < NB; i++) {
        fakeIterator.reset();
        SystemTools.consumeAll(StreamIteratorUtils.getStreamFromInputStream(
            StreamIteratorUtils.getInputStreamFromStream(StreamIteratorUtils.getStreamFromIterator(fakeIterator),
                Long.class), Long.class));
      }
      var stop = System.nanoTime();
      if (log) {
        results.put((stop - start) / 1000000.0, "StreamFromStream");
      }
    }
    if (log) {
      Log.infof("Results: %s", results);
      assertFalse(results.isEmpty());
    }
  }
}
