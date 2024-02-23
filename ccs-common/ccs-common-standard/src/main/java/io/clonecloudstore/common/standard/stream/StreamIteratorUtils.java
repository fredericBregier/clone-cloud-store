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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.primitives.Bytes;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.PipedInputOutputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;

/**
 * Utility class to help to create an InputStream of serialized objects from a Stream of Objects and reverse.
 * Note that DTO class must have an empty constructor or being a record.
 */
public class StreamIteratorUtils {
  private static final byte[] END_OF_LINE = {'\n'};

  /**
   * Transform interface
   */
  public interface Transform {
    Object transform(Object source);
  }

  /**
   * @param stream   The Stream to transform to InputStream of Json serialized Objects
   * @param forClass the object Class
   * @return the InputStream usable in REST API
   */
  public static InputStream getInputStreamFromStream(final Stream<?> stream, final Class<?> forClass) {
    return getInputStreamFromStream(stream, null, forClass);
  }

  /**
   * @param stream    The Stream to transform to InputStream of Json serialized Objects
   * @param transform function to convert source Stream object to another one
   * @param forClass  the object Class
   * @return the InputStream usable in REST API
   */
  public static InputStream getInputStreamFromStream(final Stream<?> stream, final Transform transform,
                                                     final Class<?> forClass) {
    final var reader = new InternalPipedInputOutputStream();
    final var objectWriter = StandardProperties.getObjectMapper().writerFor(forClass);
    SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        stream.sequential().forEach(item -> {
          final var exc = reader.exception;
          if (exc != null) {
            throw new IllegalStateException(exc);
          }
          internalWriteItemToInputStream(transform, item, objectWriter, reader);
        });
        reader.flush();
        stream.close();
        Thread.yield();
      } catch (final Throwable e) { // NOSONAR exception caught in reader
        errorFromStreamIterator(e, reader);
      } finally {
        reader.closeOutput();
      }
    });
    Thread.yield();
    return reader;
  }

  private static void errorFromStreamIterator(final Throwable e, final InternalPipedInputOutputStream reader) {
    final var e2 = e instanceof IllegalStateException ise ? ise.getCause() : e;
    reader.setException(e2);
    SystemTools.silentlyCloseNoException(reader);
  }

  private static void writeItemTransformedToInputStream(final Transform transform, final Object item,
                                                        final ObjectWriter objectWriter,
                                                        final InternalPipedInputOutputStream writer)
      throws IOException {
    final var transformed = transform.transform(item);
    if (transformed == null) {
      return;
    }
    writeItemToInputStream(transformed, objectWriter, writer);
  }

  private static void writeItemToInputStream(final Object item, final ObjectWriter objectWriter,
                                             final InternalPipedInputOutputStream writer) throws IOException {
    try {
      writer.write(Bytes.concat(objectWriter.writeValueAsBytes(item), END_OF_LINE));
    } catch (final IOException e) {
      // Fake write
      try {
        writer.write(END_OF_LINE);
      } catch (final IOException ignore) {
        // Ignore
      }
      writer.setException(e);
      throw e;
    }
  }

  /**
   * @param iterator The Iterator to transform to InputStream of Json serialized Objects
   * @param forClass the object Class
   * @return the InputStream usable in REST API
   */
  public static InputStream getInputStreamFromIterator(final Iterator<?> iterator, final Class<?> forClass) {
    return getInputStreamFromIterator(iterator, null, forClass);
  }

  /**
   * @param iterator  The Iterator to transform to InputStream of Json serialized Objects
   * @param transform function to convert source Stream object to another one
   * @param forClass  the object Class
   * @return the InputStream usable in REST API
   */
  public static InputStream getInputStreamFromIterator(final Iterator<?> iterator, final Transform transform,
                                                       final Class<?> forClass) {
    final var reader = new InternalPipedInputOutputStream();
    final var objectWriter = StandardProperties.getObjectMapper().writerFor(forClass);
    SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        while (iterator.hasNext()) {
          final var item = iterator.next();
          final var exc = reader.exception;
          if (exc != null) {
            throw new IllegalStateException(exc);
          }
          internalWriteItemToInputStream(transform, item, objectWriter, reader);
        }
        reader.flush();
        Thread.yield();
      } catch (final Throwable e) { // NOSONAR exception caught in reader
        errorFromStreamIterator(e, reader);
      } finally {
        finalizeWriteIterator(iterator, reader);
      }
    });
    Thread.yield();
    return reader;
  }

  private static void internalWriteItemToInputStream(final Transform transform, final Object item,
                                                     final ObjectWriter objectWriter,
                                                     final InternalPipedInputOutputStream writer) {
    try {
      if (transform == null) {
        writeItemToInputStream(item, objectWriter, writer);
      } else {
        writeItemTransformedToInputStream(transform, item, objectWriter, writer);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void finalizeWriteIterator(final Iterator<?> iterator, final InternalPipedInputOutputStream writer) {
    if (iterator instanceof final Closeable closingIterator) {
      try {
        closingIterator.close();
      } catch (final IOException ignore) {
        // Nothing
      }
    }
    writer.closeOutput();
  }

  /**
   * @param inputStream the InputStream containing Json Objects
   * @param forClass    the object Class
   * @return The Stream of deserialized Objects
   * @throws CcsWithStatusException if an issue occurs during streaming
   */
  public static <E> Stream<E> getStreamFromInputStream(final InputStream inputStream, final Class<E> forClass)
      throws CcsWithStatusException { // NOSONAR informative exception
    final var objectReader = StandardProperties.getObjectMapper().readerFor(forClass);
    return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().sequential()
        .map(Throwing.rethrow(s -> {
          try {
            return objectReader.readValue(s);
          } catch (final JsonProcessingException | UncheckedIOException e) {
            throw new CcsWithStatusException(null, 500, e);
          }
        }));
  }

  /**
   * @param inputStream the InputStream containing Json Objects
   * @param forClass    the object Class
   * @return The Stream of deserialized Objects
   * @throws CcsInvalidArgumentRuntimeException if an issue occurs during iterating
   */
  public static <E> ClosingIterator<E> getIteratorFromInputStream(final InputStream inputStream,
                                                                  final Class<E> forClass) {
    final var objectReader = StandardProperties.getObjectMapper().readerFor(forClass);
    return new LineIterator<>(new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)),
        objectReader);
  }

  private static class LineIterator<E> implements ClosingIterator<E> {
    private final BufferedReader reader;
    private final ObjectReader objectReader;
    private String nextLine;

    private LineIterator(final BufferedReader reader, final ObjectReader objectReader) {
      this.reader = reader;
      this.objectReader = objectReader;
    }

    private String bufferNext() {
      try {
        nextLine = reader.readLine();
        return nextLine;
      } catch (final IOException ignore) {
        // Ignore
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = nextLine != null || bufferNext() != null;
      if (!hasNext) {
        try {
          reader.close();
        } catch (final IOException ignore) {
          // Ignore
        }
      }
      return hasNext;
    }

    @Override
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        return objectReader.readValue(nextLine);
      } catch (final JsonProcessingException | UncheckedIOException e) {
        throw new CcsInvalidArgumentRuntimeException(e.getMessage(), e);
      } finally {
        nextLine = null;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (final IOException ignore) {
        // Ignore
      }
    }
  }

  /**
   * Transform Iterator to List (all in memory)
   */
  public static <E> List<E> getListFromIterator(final Iterator<E> iterator) {
    final List<E> list = new ArrayList<>();
    if (iterator != null) {
      while (iterator.hasNext()) {
        final var item = iterator.next();
        list.add(item);
      }
    }
    return list;
  }

  /**
   * Transform Iterator to Stream
   */
  public static <E> Stream<E> getStreamFromIterator(final Iterator<E> iterator) {
    final var spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL);
    return StreamSupport.stream(spliterator, false);
  }

  /**
   * To capture Exception from Stream
   */
  private static class InternalPipedInputOutputStream extends PipedInputOutputStream {
    private IOException exception = null;

    private InternalPipedInputOutputStream() {
      super(null, 10000);
    }

    void setException(final Throwable e) {
      if (exception != null) {
        return;
      }
      exception = e instanceof IOException io ? io : new IOException(e);
    }

    private void checkException() throws IOException {
      if (exception != null) {
        throw exception;
      }
    }

    @Override
    public int read() throws IOException {
      checkException();
      return super.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      checkException();
      return super.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
      checkException();
      return super.available();
    }

    @Override
    public void close() throws IOException {
      super.close();
      checkException();
    }
  }

  /**
   * Map interface to re-trow exception
   */
  @FunctionalInterface
  private interface ThrowingMap<T, R> extends Function<T, R> {
    @Override
    default R apply(T t) {
      try {
        return applyThrows(t);
      } catch (final Throwable e) {
        Throwing.sneakyThrow(e);
        // Return but not really
        return null;
      }
    }

    R applyThrows(T t) throws Throwable; // NOSONAR intentional
  }

  /**
   * Throwing class
   */
  private static final class Throwing {
    private static <T, R> Function<T, R> rethrow(final ThrowingMap<T, R> map) {
      return map;
    }

    /**
     * The compiler sees the signature with the throws T inferred to a RuntimeException type, so it
     * allows the unchecked exception to propagate.
     * <p>
     * <a href="http://www.baeldung.com/java-sneaky-throws">Baeldung Java Sneaky Throws</a>
     */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(final Throwable ex) throws E {
      throw (E) ex;
    }
  }
}
