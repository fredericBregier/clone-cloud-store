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
import java.io.OutputStream;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.SystemTools;

/**
 * Transform one InputStream to Multiple InputStream, each one being a chunk of the primary one.
 */
public class ChunkInputStreamNotBuffered extends InputStream implements ChunkInputStreamInterface {
  public static final int MIN_CHUNK_SIZE = 5 * 1024 * 1024;
  public final int maxChunkSize = QuarkusProperties.getDriverMaxChunkSize();
  private final InputStream inputStream;
  private final int chunkSize;
  private final long totalLen;
  private int currentLen;
  private long currentTotalRead = 0;
  private long currentChunkSize;
  private boolean closed = false;

  /**
   * @param inputStream the InputStream to split as multiple InputStream by chunk
   * @param len         the real InputStream size if known
   * @param chunkSize   the chunk size to split on
   */
  public ChunkInputStreamNotBuffered(final InputStream inputStream, final long len, final int chunkSize) {
    this.inputStream = inputStream;
    totalLen = len > 0 ? len : -1;
    var chunkLen = Math.max(Math.min(chunkSize, maxChunkSize), MIN_CHUNK_SIZE);
    if (totalLen > 0) {
      chunkLen = (int) Math.min(totalLen, chunkLen);// NOSONAR
    }
    this.chunkSize = chunkLen;
  }

  /**
   * @return True if the next Chunk of InputStream is ready, possibly empty, else implies closing of native
   * InputStream
   * @throws IOException if an issue occurs
   */
  @Override
  public boolean nextChunk() throws IOException {
    currentLen = 0;
    int maxLength = chunkSize;
    if (totalLen > 0) {
      if (totalLen - currentTotalRead > Integer.MAX_VALUE) {
        maxLength = Integer.MAX_VALUE;
      } else {
        maxLength = (int) (totalLen - currentTotalRead);
      }
    }
    currentChunkSize = Math.min(chunkSize, maxLength);
    if (totalLen > 0 && currentTotalRead >= totalLen || inputStream.available() <= 0) {
      internalClose();
      return false;
    }
    return !closed;
  }

  private void internalClose() throws IOException {
    closed = true;
    inputStream.close();
  }

  /**
   * @return True if all chunks are done
   */
  @Override
  public boolean isChunksDone() {
    return closed;
  }

  /**
   * @return if known, the chunk size currently available, else -1
   */
  @Override
  public long getAvailableChunkSize() {
    return currentChunkSize - currentLen;
  }

  /**
   * @return the current buffer size
   */
  @Override
  public long getChunkSize() {
    return currentChunkSize;
  }

  /**
   * @return the current position in the buffer
   */
  @Override
  public int getCurrentPos() {
    return currentLen;
  }

  /**
   * @return the total number of read bytes
   */
  @Override
  public long getCurrentTotalRead() {
    return currentTotalRead;
  }

  @Override
  public int read() throws IOException {
    if (currentLen >= currentChunkSize) {
      return -1;
    }
    final var val = bufRead();
    if (val >= 0) {
      currentLen++;
      currentTotalRead++;
    }
    return val;
  }

  private int bufRead() throws IOException {
    if (checkBufferEnd()) {
      return -1;
    }
    final var read = inputStream.read();
    if (read < 0) {
      internalClose();
      return read;
    }
    return read & 0xFF;
  }

  private boolean checkBufferEnd() {
    return closed || currentChunkSize - currentLen <= 0;
  }

  private int getMaxLen(final long len) {
    if (currentLen >= currentChunkSize) {
      return -1;
    }
    var realLen = len;
    final var maxLen = currentChunkSize - currentLen;
    if (maxLen < len) {
      realLen = maxLen;
    }
    return (int) realLen;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (b == null) {
      throw new CcsInvalidArgumentRuntimeException("buffer cannot be null");
    }
    final var realLen = getMaxLen(len);
    if (realLen < 0) {
      return -1;
    }
    final var read = bufRead(b, off, realLen);
    if (read >= 0) {
      currentLen += read;
      currentTotalRead += read;
    }
    return read;
  }

  @Override
  public long skip(final long len) throws IOException {
    final var realLen = getMaxLen(len);
    if (realLen < 0) {
      return 0;
    }
    final var read = bufSkip(realLen);
    long lenCurrent = currentLen + read;
    if (lenCurrent > currentChunkSize) {
      currentLen = checkedSize();
    } else {
      currentLen = (int) lenCurrent;
    }
    currentTotalRead += read;
    return read;
  }

  private long bufSkip(final long len) throws IOException {
    if (checkBufferEnd()) {
      return 0;
    }
    var finalLen = Math.min(len, currentChunkSize - currentLen);
    var read = inputStream.skip(finalLen);
    if (read <= 0) {
      internalClose();
    }
    return read;
  }

  private int checkedSize() {
    int checkedSize;
    if (currentChunkSize > Integer.MAX_VALUE) {
      checkedSize = Integer.MAX_VALUE;
    } else {
      checkedSize = (int) currentChunkSize;
    }
    return checkedSize;
  }

  @Override
  public int available() throws IOException {
    if (currentLen >= currentChunkSize) {
      return 0;
    }
    int checkedSize = checkedSize();
    return Math.min(inputStream.available(), checkedSize - currentLen);
  }

  @Override
  public void close() throws IOException {
    currentLen = checkedSize();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    return SystemTools.transferTo(this, out);
  }

  private int bufRead(final byte[] b, final int off, final int len) throws IOException {
    if (checkBufferEnd()) {
      return -1;
    }
    final var newLen = Math.min(len, checkedSize() - currentLen);
    var read = inputStream.read(b, off, newLen);
    if (read < 0) {
      internalClose();
    }
    return read;
  }
}
