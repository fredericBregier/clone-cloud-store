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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import org.jboss.logging.Logger;

/**
 * Generate multiples InputStreams from one to many using Pipe
 */
public class TeeInputStream implements Closeable {
  private static final Logger LOGGER = Logger.getLogger(TeeInputStream.class);
  private final int nbCopy;
  private final BoundedByteBuffer boundedByteBuffer;
  private final InputStream source;
  private AtomicReference<IOException> lastException = new AtomicReference<>();

  /**
   * Create one MultipleInputStreamHandler from one InputStream and make nbCopy linked InputStreams
   *
   * @param source InputStream source
   * @param nbCopy number of copied InputStream
   * @throws CcsInvalidArgumentRuntimeException if source is null or nbCopy &lt;= 0 or global service is down
   */
  public TeeInputStream(final InputStream source, final int nbCopy) {
    ParametersChecker.checkParameter("InputStream cannot be null", source);
    ParametersChecker.checkValue("nbCopy", nbCopy, 1);

    this.nbCopy = nbCopy;
    this.source = source;

    final var bufferSize = StandardProperties.getBufSize() * 10;
    this.boundedByteBuffer = new BoundedByteBuffer(bufferSize, nbCopy);

    StandardProperties.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        copy();
      } catch (final IOException e) {
        LOGGER.error(e);
        lastException.set(e);
      }
    });
    Thread.yield();
  }

  protected final void copy() throws IOException {

    try (final var writer = boundedByteBuffer.getWriter()) {

      // Buffer size should not be greater than buffer size.
      final var buffer = new byte[StandardProperties.getBufSize()];

      int n;
      while (-1 != (n = source.read(buffer))) {
        writer.write(buffer, 0, n);
      }
      writer.writeEOF();
      Thread.yield();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted thread", e);
    }
  }

  /**
   * Get the rank-th linked InputStream
   *
   * @param rank between 0 and nbCopy-1
   * @return the rank-th linked InputStream
   * @throws CcsInvalidArgumentRuntimeException if rank &lt; 0 or rank &gt;= nbCopy
   */
  public InputStream getInputStream(final int rank) {
    if (rank < 0 || rank >= nbCopy) {
      throw new CcsInvalidArgumentRuntimeException("Rank is invalid");
    }
    return this.boundedByteBuffer.getReader(rank);
  }

  /**
   * @throws IOException if any exception is found during multiple streams
   */
  public void throwLastException() throws IOException {
    if (lastException.get() != null) {
      throw lastException.get();
    }
  }

  @Override
  public void close() {
    boundedByteBuffer.close();
    SystemTools.silentlyCloseNoException(source);
  }
}
