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

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
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
 * Cipher InputStream: takes an InputStream as entry and give back a crypt/decrypt InputStream
 */
public class CipherInputStream extends InputStream {
  private final InputStream inputStream;
  private final CipherOutputStream cipherOutputStream;
  // Buffer size limit memory usage
  private final PipedInputStream pipedInputStream;
  private final PipedOutputStream pipedOutputStream;
  private final AtomicReference<IOException> exceptionAtomicReference = new AtomicReference<>();
  private long sizeRead = 0;
  private long sizeCipher = 0;
  private final AtomicBoolean done = new AtomicBoolean(false);

  /**
   * Constructor allowing to not flush on all packets
   */
  public CipherInputStream(final InputStream inputStream, final Cipher cipher) throws IOException {
    this.inputStream = inputStream;
    pipedInputStream = new PipedInputStream(DEFAULT_PIPED_BUFFER_SIZE);
    pipedOutputStream = new PipedOutputStream(pipedInputStream);
    cipherOutputStream = new CipherOutputStream(pipedOutputStream, cipher);
    StandardProperties.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        final var len = SystemTools.transferTo(this.inputStream, cipherOutputStream);
        sizeRead += len;
        cipherOutputStream.flush();
        Thread.yield();
      } catch (final IOException e) {
        exceptionAtomicReference.compareAndSet(null, e);
      } finally {
        SystemTools.silentlyCloseNoException(cipherOutputStream);
        SystemTools.silentlyCloseNoException(this.inputStream);
      }
    });
    Thread.yield();
  }

  public long getSizeRead() {
    return sizeRead;
  }

  public long getSizeCipher() {
    return sizeCipher;
  }

  private void checkException() throws IOException {
    if (exceptionAtomicReference.get() != null) {
      throw exceptionAtomicReference.get();
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
      sizeCipher++;
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
      sizeCipher += read;
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
    SystemTools.silentlyCloseNoException(cipherOutputStream);
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
    return "ZCI: " + getClass().getSimpleName() + " while alreadyWrote: " + getSizeCipher() + " alreadyRead:  " +
        getSizeRead() + " hasException? " + (exceptionAtomicReference.get() != null);
  }
}
