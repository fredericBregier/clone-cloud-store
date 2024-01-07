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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdOutputStream;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;

import static io.clonecloudstore.common.standard.properties.StandardProperties.DEFAULT_PIPED_BUFFER_SIZE;
import static io.clonecloudstore.common.standard.properties.StandardProperties.VIRTUAL_EXECUTOR_SERVICE;

/**
 * Zstd InputStream: takes an InputStream as entry and give back a compressed InputStream
 */
public class ZstdCompressInputStream extends InputStream {
  private final InputStream inputStream;
  private final ZstdOutputStream zstdOutputStream;
  // Buffer size limit memory usage
  private final PipedInputStream pipedInputStream;
  private final PipedOutputStream pipedOutputStream;
  private final AtomicReference<IOException> exceptionAtomicReference = new AtomicReference<>();
  private long sizeRead = 0;
  private long sizeCompressed = 0;
  private final AtomicBoolean done = new AtomicBoolean(false);

  /**
   * Default constructor with flush by packet
   */
  public ZstdCompressInputStream(final InputStream inputStream) throws IOException {
    this(inputStream, true);
  }

  /**
   * Constructor allowing to not flush on all packets
   */
  public ZstdCompressInputStream(final InputStream inputStream, final boolean flushOnPacket) throws IOException {
    this(inputStream, flushOnPacket, -1);
  }

  public ZstdCompressInputStream(final InputStream inputStream, final boolean flushOnPacket, final int level)
      throws IOException {
    this.inputStream = inputStream;
    pipedInputStream = new PipedInputStream(DEFAULT_PIPED_BUFFER_SIZE);
    pipedOutputStream = new PipedOutputStream(pipedInputStream);
    zstdOutputStream = new ZstdOutputStream(pipedOutputStream, RecyclingBufferPool.INSTANCE, level);
    zstdOutputStream.setChecksum(false);
    zstdOutputStream.setCloseFrameOnFlush(true);
    StandardProperties.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        final var len = SystemTools.transferTo(this.inputStream, zstdOutputStream, flushOnPacket);
        sizeRead += len;
        zstdOutputStream.flush();
      } catch (final IOException e) {
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
          SystemTools.silentlyCloseNoException(zstdOutputStream);
          SystemTools.silentlyCloseNoException(this.inputStream);
        });
        Thread.yield();
      }
    });
  }

  public long getSizeRead() {
    return sizeRead;
  }

  public long getSizeCompressed() {
    return sizeCompressed;
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
      sizeCompressed++;
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
    if (b == null) {
      throw new CcsInvalidArgumentRuntimeException("buffer cannot be null");
    }
    checkException();
    if (done.get()) {
      return -1;
    }
    final var read = pipedInputStream.read(b, off, len);
    if (read >= 0) {
      sizeCompressed += read;
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
    return "ZCI: " + getClass().getSimpleName() + " while alreadyWrote: " + getSizeCompressed() + " alreadyRead:  " +
        getSizeRead() + " hasException? " + (exceptionAtomicReference.get() != null);
  }
}
