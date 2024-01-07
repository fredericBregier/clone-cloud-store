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
import io.clonecloudstore.common.standard.properties.StandardProperties;

/**
 * Transform one InputStream to Multiple InputStream, each one being a chunk of the primary one.
 * Note that if length is not provided, chunk size will be the buffer size used internally, so
 * the buffer size will be reduced to 512 MB for memory capacity handling.
 */
public class ChunkInputStream extends InputStream {
  public static final int MIN_CHUNK_SIZE = 5 * 1024 * 1024;
  public final int maxChunkSize = QuarkusProperties.getDriverMaxChunkSize();
  private final InputStream inputStream;
  private final int chunkSize;
  private final long totalLen;
  private int currentLen;
  private long currentTotalRead = 0;
  private byte[] buffer;
  private int bufferSize = -1;

  /**
   * @param inputStream the InputStream to split as multiple InputStream by chunk
   * @param len         the real InputStream size; if 0, chunk size will be the buffer size used
   * @param chunkSize   the chunk size to split on
   */
  public ChunkInputStream(final InputStream inputStream, final long len, final int chunkSize) {
    this.inputStream = inputStream;
    totalLen = len > 0 ? len : -1;
    var chunkLen = Math.max(Math.min(chunkSize, maxChunkSize), MIN_CHUNK_SIZE);
    if (totalLen > 0) {
      chunkLen = (int) Math.min(totalLen, chunkLen);
    }
    this.chunkSize = chunkLen;
    buffer = new byte[chunkSize];
  }

  /**
   * @return True if the next Chunk of InputStream is ready, else implies closing of native
   * InputStream
   * @throws IOException if an issue occurs
   */
  public boolean nextChunk() throws IOException {
    currentLen = 0;
    if (totalLen > 0 && currentTotalRead >= totalLen || fillBuffer() < 0) {
      internalClose();
      return false;
    }
    return true;
  }

  /**
   * @return True if the chunks are done
   */
  public boolean isChunksDone() {
    return buffer == null;
  }

  private void internalClose() throws IOException {
    buffer = null;
    inputStream.close();
  }

  private long fillBuffer() throws IOException {
    var fillRead = 0;
    var offset = 0;
    while (fillRead < chunkSize) {
      final var read = inputStream.read(buffer, offset, chunkSize - fillRead);
      if (read > 0) {
        fillRead += read;
        offset += read;
      } else {
        break;
      }
      Thread.yield();
    }
    bufferSize = fillRead > 0 ? fillRead : -1;
    return bufferSize;
  }

  /**
   * @return if known, the chunk size currently available, else -1
   */
  public int getChunkSize() {
    return bufferSize - currentLen;
  }

  /**
   * @return the current buffer size
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * @return the current position in the buffer
   */
  public int getCurrentPos() {
    return currentLen;
  }

  /**
   * @return the underlying buffer but not truncated, so getBufferSize is mandatory, and possibly getCurrentPos if
   * read occurred before
   */
  public byte[] getBuffer() {
    return buffer;
  }

  /**
   * @return the total number of read bytes
   */
  public long getCurrentTotalRead() {
    return currentTotalRead;
  }

  @Override
  public int read() throws IOException {
    if (currentLen >= chunkSize) {
      return -1;
    }
    final var val = bufRead();
    if (val >= 0) {
      currentLen++;
      currentTotalRead++;
    }
    return val;
  }

  private int bufRead() {
    if (checkBufferEnd()) {
      return -1;
    }
    return buffer[currentLen] & 0xFF;
  }

  private boolean checkBufferEnd() {
    return bufferSize - currentLen <= 0;
  }

  private int getMaxLen(final long len) {
    if (currentLen >= chunkSize) {
      return -1;
    }
    var realLen = len;
    final var maxLen = chunkSize - currentLen;
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
    if (lenCurrent > bufferSize) {
      currentLen = bufferSize;
    } else {
      currentLen = (int) lenCurrent;
    }
    currentTotalRead += read;
    return read;
  }

  private long bufSkip(final long len) {
    if (checkBufferEnd()) {
      return 0;
    }
    return Math.min(len, (long) bufferSize - currentLen);
  }

  @Override
  public int available() throws IOException {
    if (currentLen >= chunkSize) {
      return 0;
    }
    return Math.max(0, bufferSize - currentLen);
  }

  @Override
  public void close() throws IOException {
    currentLen = chunkSize;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    var transferred = 0L;
    var read = 0;
    for (final var buf = new byte[StandardProperties.getBufSize()]; (read = read(buf, 0, buf.length)) >= 0;
         transferred += read) {
      out.write(buf, 0, read);
    }
    return transferred;
  }

  private int bufRead(final byte[] b, final int off, final int len) {
    if (checkBufferEnd()) {
      return -1;
    }
    final var newLen = Math.min(len, bufferSize - currentLen);
    System.arraycopy(buffer, currentLen, b, off, newLen);
    return newLen;
  }
}
