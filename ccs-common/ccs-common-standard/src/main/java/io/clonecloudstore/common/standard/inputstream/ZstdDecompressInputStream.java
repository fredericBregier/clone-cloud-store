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
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStream;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;

/**
 * Zstd InputStream: takes an InputStream as entry and give back a uncompressed InputStream
 */
public class ZstdDecompressInputStream extends InputStream {
  private final InputStream inputStream;
  private final ZstdInputStream zstdInputStream;
  private long sizeDecompressed = 0;
  private final AtomicBoolean done = new AtomicBoolean(false);

  public ZstdDecompressInputStream(final InputStream inputStream) throws IOException {
    this.inputStream = inputStream;
    zstdInputStream = new ZstdInputStream(inputStream, RecyclingBufferPool.INSTANCE);
    zstdInputStream.setContinuous(true);
  }

  public long getSizeDecompressed() {
    return sizeDecompressed;
  }

  @Override
  public int read() throws IOException {
    if (done.get()) {
      return -1;
    }
    final var read = zstdInputStream.read();
    if (read >= 0) {
      sizeDecompressed++;
    } else {
      done.set(true);
      SystemTools.silentlyCloseNoException(inputStream);
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
    if (done.get()) {
      return -1;
    }
    final var read = zstdInputStream.read(b, off, len);
    if (read >= 0) {
      sizeDecompressed += read;
    } else {
      done.set(true);
      SystemTools.silentlyCloseNoException(inputStream);
    }
    return read;
  }

  @Override
  public long skip(final long n) throws IOException {
    if (done.get()) {
      return 0;
    }
    return SystemTools.skip(this, n);
  }

  @Override
  public int available() throws IOException {
    if (done.get()) {
      return 0;
    }
    final var len = Math.max(zstdInputStream.available(), inputStream.available());
    if (len <= 0) {
      return len;
    }
    return StandardProperties.getBufSize();
  }

  @Override
  public void close() throws IOException {
    SystemTools.silentlyCloseNoException(inputStream);
    SystemTools.silentlyCloseNoException(zstdInputStream);
    done.set(true);
  }

  @Override
  public boolean markSupported() {
    return zstdInputStream.markSupported();
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    if (done.get()) {
      return 0;
    }
    final var read = SystemTools.transferTo(this, out);
    done.set(true);
    return read;
  }

  @Override
  public String toString() {
    return "ZCI: " + getClass().getSimpleName() + " while alreadyWrote: " + getSizeDecompressed();
  }
}
