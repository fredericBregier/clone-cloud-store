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

package io.clonecloudstore.common.standard.inputstream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;

import static io.clonecloudstore.common.standard.properties.StandardProperties.DEFAULT_PIPED_BUFFER_SIZE;
import static io.clonecloudstore.common.standard.properties.StandardProperties.VIRTUAL_EXECUTOR_SERVICE;

public class TransferInputStream extends InputStream {
  private static final Logger LOGGER = Logger.getLogger(TransferInputStream.class.getName());
  private final InputStream inputStream;
  private final PipedInputStream pipedInputStream;
  private final PipedOutputStream pipedOutputStream;
  private final AtomicReference<Exception> exceptionAtomicReference;
  private final AtomicBoolean done = new AtomicBoolean(false);

  public TransferInputStream(final InputStream inputStream, final AtomicReference<Exception> exceptionAtomicReference)
      throws IOException {
    this.inputStream = inputStream;
    pipedInputStream = new PipedInputStream(DEFAULT_PIPED_BUFFER_SIZE);
    pipedOutputStream = new PipedOutputStream(pipedInputStream);
    this.exceptionAtomicReference =
        exceptionAtomicReference != null ? exceptionAtomicReference : new AtomicReference<>();
  }

  public void startCopyAsync() {
    StandardProperties.STANDARD_EXECUTOR_SERVICE.execute(this::transferFromSource);
    Thread.yield();
  }

  private void transferFromSource() {
    try {
      SystemTools.transferTo(this.inputStream, this.pipedOutputStream);
    } catch (final IOException e) {
      LOGGER.log(Level.SEVERE, e.getMessage());
      exceptionAtomicReference.compareAndSet(null, e);
      VIRTUAL_EXECUTOR_SERVICE.execute(() -> {
        try {
          close();
        } catch (final IOException ignore) {
          // Ignore
        }
      });
      Thread.yield();
    } finally {
      VIRTUAL_EXECUTOR_SERVICE.execute(() -> {
        SystemTools.silentlyCloseNoException(pipedOutputStream);
        SystemTools.silentlyCloseNoException(this.inputStream);
      });
      Thread.yield();
    }
  }

  private void checkException() throws IOException {
    if (exceptionAtomicReference.get() != null) {
      final var exc = exceptionAtomicReference.get();
      if (exc instanceof IOException e) {
        throw e;
      }
      throw new IOException(exc);
    }
  }

  @Override
  public int read() throws IOException {
    checkException();
    if (done.get()) {
      return -1;
    }
    final var read = pipedInputStream.read();
    if (read < 0) {
      done.set(true);
    }
    return read;
  }

  @Override
  public int read(final byte[] b) throws IOException {
    if (b == null) {
      throw new CcsInvalidArgumentRuntimeException("buffer cannot be null");
    }
    return read(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (b == null || off + len > b.length) {
      throw new CcsInvalidArgumentRuntimeException("buffer cannot be null or offset / length > real length");
    }
    checkException();
    if (done.get()) {
      return -1;
    }
    final var read = pipedInputStream.read(b, off, len);
    if (read < 0) {
      done.set(true);
    }
    return read;
  }

  @Override
  public long skip(final long n) throws IOException {
    checkException();
    if (done.get()) {
      return 0;
    }
    return SystemTools.skip(this, n);
  }

  @Override
  public int available() throws IOException {
    checkException();
    if (done.get()) {
      return 0;
    }
    final var len = pipedInputStream.available();
    if (len <= 0) {
      return len;
    }
    return StandardProperties.getBufSize();
  }

  @Override
  public void close() throws IOException {
    SystemTools.silentlyCloseNoException(pipedOutputStream);
    SystemTools.silentlyCloseNoException(pipedInputStream);
    SystemTools.silentlyCloseNoException(inputStream);
    done.set(true);
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    checkException();
    if (done.get()) {
      return 0;
    }
    final var read = SystemTools.transferTo(this, out);
    done.set(true);
    return read;
  }

  @Override
  public String toString() {
    return "TIS: " + getClass().getSimpleName() + " hasException? " + (exceptionAtomicReference.get() != null);
  }
}
