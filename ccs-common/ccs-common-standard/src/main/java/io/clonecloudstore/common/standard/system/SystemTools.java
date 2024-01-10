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
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;

public class SystemTools {
  /**
   * Global Thread pool for Internal actions such as in TeeInputStream, ZstdCompressedInputStream, S3
   */
  public static final ExecutorService VIRTUAL_EXECUTOR_SERVICE = Executors.newVirtualThreadPerTaskExecutor();
  public static final ThreadFactory DAEMON_THREAD_FACTORY = new DaemonThreadFactory();
  public static final ExecutorService STANDARD_EXECUTOR_SERVICE = Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);

  private SystemTools() {
    // Nothing
  }

  /**
   * @param instant instant to truncate
   * @return an Instant truncated to Millisecond (since most of the DB truncated it)
   */
  public static Instant toMillis(final Instant instant) {
    if (instant != null) {
      return instant.truncatedTo(ChronoUnit.MILLIS);
    }
    return null;
  }

  /**
   * Silently close InputStream (no exception)
   */
  public static void silentlyCloseNoException(final InputStream inputStream) {
    silentlyClose(inputStream); // NOSONAR intentional
  }

  /**
   * Silently close InputStream (exception is returned if any)
   */
  public static IOException silentlyClose(final InputStream inputStream) {
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (final IOException e) {
        return e;
      }
    }
    return null;
  }

  /**
   * Silently close OutputStream (no exception)
   */
  public static void silentlyCloseNoException(final OutputStream outputStream) {
    silentlyClose(outputStream); // NOSONAR intentional
  }

  /**
   * Silently close OutputStream (exception is returned if any)
   */
  public static IOException silentlyClose(final OutputStream outputStream) {
    if (outputStream != null) {
      try {
        outputStream.close();
      } catch (final IOException e) {
        return e;
      }
    }
    return null;
  }

  /**
   * @param iterator to consume fully
   * @return the number of items consumed
   */
  public static long consumeAll(final Iterator<?> iterator) {
    long cpt = 0;
    if (iterator != null) {
      while (iterator.hasNext()) {
        iterator.next();
        cpt++;
      }
    }
    return cpt;
  }

  /**
   * @param stream to consume fully
   * @return the number of items consumed
   */
  public static long consumeAll(final Stream<?> stream) {
    long cpt = 0;
    if (stream != null) {
      cpt = stream.count();
    }
    return cpt;
  }

  /**
   * Get the field from the object named fieldName from Class
   *
   * @throws NoSuchFieldException   field does not exist
   * @throws IllegalAccessException issue on access
   */
  public static Object getField(final Class<?> clasz, final String fieldName, final Object object)
      throws NoSuchFieldException, IllegalAccessException {
    try {
      final var field = clasz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(object);
    } catch (final NoSuchFieldException e) {
      final var superClass = clasz.getSuperclass();
      if (superClass == null) {
        throw e;
      } else {
        return getField(superClass, fieldName, object);
      }
    }
  }

  /**
   * Thread.sleep(1 ms)
   */
  public static void wait1ms() {
    try {
      Thread.sleep(1); //NOSONAR intentional
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Consume all InputStream in error case with no error
   */
  public static void consumeWhileErrorInputStream(final InputStream inputStream) {
    if (inputStream == null) {
      return;
    }
    MultipleActionsInputStream mi = null;
    if (inputStream instanceof MultipleActionsInputStream mi1) {
      mi = mi1;
      mi.invalidExceptionDuringConsumeWhileErrorInputStream(Boolean.FALSE);
    }
    try {
      long currentTime = System.currentTimeMillis();
      long len;
      final var bytes = new byte[StandardProperties.getBufSize()];
      do {
        long newTime = System.currentTimeMillis();
        if (newTime - currentTime > StandardProperties.getMaxWaitMs()) {
          break;
        }
        currentTime = newTime;
        try {
          len = inputStream.read(bytes, 0, bytes.length);
        } catch (final IOException ignore) {
          return;
        }
        Thread.yield();
      } while (len >= 0);
    } finally {
      silentlyCloseNoException(inputStream);
      if (mi != null) {
        mi.invalidExceptionDuringConsumeWhileErrorInputStream(Boolean.TRUE);
      }
    }
  }

  /**
   * Consume all InputStream in error case with no error with a timeout
   */
  public static void consumeWhileErrorInputStream(final InputStream inputStream, long timeout) {
    try {
      final var wait = STANDARD_EXECUTOR_SERVICE.submit(() -> consumeWhileErrorInputStream(inputStream));
      Thread.yield();
      wait.get(timeout, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final ExecutionException | TimeoutException ignore) {
      // ignore
    }
  }

  /**
   * Transfer all inputStream to outputStream
   *
   * @return length of transferred bytes
   */
  public static long transferTo(final InputStream inputStream, final OutputStream outputStream) throws IOException {
    return transferTo(inputStream, outputStream, false);
  }

  /**
   * Transfer all inputStream to outputStream
   *
   * @return length of transferred bytes
   */
  public static long transferTo(final InputStream inputStream, final OutputStream outputStream,
                                final boolean flushOnChunk) throws IOException {
    ParametersChecker.checkParameter("OutputStream and InputStream cannot be null", outputStream, inputStream);
    var transferred = 0L;
    var read = 0;
    final var buf = new byte[StandardProperties.getBufSize()];
    while ((read = inputStream.read(buf, 0, buf.length)) >= 0) {
      outputStream.write(buf, 0, read);
      transferred += read;
      if (flushOnChunk) {
        outputStream.flush();
      }
      Thread.yield();
    }
    outputStream.flush();
    inputStream.close();
    return transferred;
  }

  /**
   * Skip bytes from InputStream
   *
   * @return the number of really skipped bytes
   */
  public static long skip(final InputStream inputStream, final long skip) throws IOException {
    final var bytes = new byte[StandardProperties.getBufSize()];
    var still = skip;
    var total = 0L;
    var max = (int) Math.min(StandardProperties.getBufSize(), still);
    var read = 0;
    while (max > 0 && (read = inputStream.read(bytes, 0, max)) >= 0) {
      still -= read;
      total += read;
      max = (int) Math.min(StandardProperties.getBufSize(), still);
      if (max <= 0) {
        return total;
      }
    }
    if (read < 0) {
      inputStream.close();
    }
    return total;
  }
}
