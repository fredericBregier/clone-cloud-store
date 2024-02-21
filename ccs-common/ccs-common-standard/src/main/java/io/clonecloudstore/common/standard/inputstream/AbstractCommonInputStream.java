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

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;

import static io.clonecloudstore.common.standard.properties.StandardProperties.DEFAULT_PIPED_BUFFER_SIZE;

/**
 * Base class for InputStream extension
 */
public abstract class AbstractCommonInputStream extends InputStream {
  protected final InputStream inputStream;
  protected final OutputStream outputStream;
  protected final PipedInputStream pipedInputStream;
  protected final PipedOutputStream pipedOutputStream;
  protected final AtomicReference<IOException> ioExceptionAtomicReference = new AtomicReference<>();
  protected long sizeRead = 0;
  protected long sizeOutput = 0;
  protected final AtomicBoolean done = new AtomicBoolean(false);

  protected abstract OutputStream getNewOutputStream(Object extraArgument) throws IOException;

  protected AbstractCommonInputStream(final InputStream inputStream, final Object extraArgument) throws IOException {
    this.inputStream = inputStream;
    pipedInputStream = new PipedInputStream(DEFAULT_PIPED_BUFFER_SIZE);
    pipedOutputStream = new PipedOutputStream(pipedInputStream);
    outputStream = getNewOutputStream(extraArgument);
    if (outputStream != null) {
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          final var len = SystemTools.transferTo(this.inputStream, outputStream);
          sizeRead += len;
          outputStream.flush();
          Thread.yield();
        } catch (final IOException e) {
          ioExceptionAtomicReference.compareAndSet(null, e);
        } finally {
          SystemTools.silentlyCloseNoException(outputStream);
          SystemTools.silentlyCloseNoException(this.inputStream);
        }
      });
      Thread.yield();
    }
  }

  protected void checkException() throws IOException {
    if (ioExceptionAtomicReference.get() != null) {
      throw ioExceptionAtomicReference.get();
    }
  }

  @Override
  public int read() throws IOException {
    checkException();
    if (done.get()) {
      return -1;
    }
    final var read = pipedInputStream.read();
    if (read >= 0) {
      sizeOutput++;
    } else {
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
      throw new CcsInvalidArgumentRuntimeException("buffer cannot be null");
    }
    checkException();
    if (done.get()) {
      return -1;
    }
    final var read = pipedInputStream.read(b, off, len);
    if (read >= 0) {
      sizeOutput += read;
    } else {
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
    SystemTools.silentlyCloseNoException(inputStream);
    SystemTools.silentlyCloseNoException(pipedOutputStream);
    SystemTools.silentlyCloseNoException(pipedInputStream);
    SystemTools.silentlyCloseNoException(outputStream);
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
    return "InputStream: " + getClass().getSimpleName() + " while alreadyWrote: " + sizeOutput + " alreadyRead:  " +
        sizeRead + " hasException? " + (ioExceptionAtomicReference.get() != null);
  }
}
