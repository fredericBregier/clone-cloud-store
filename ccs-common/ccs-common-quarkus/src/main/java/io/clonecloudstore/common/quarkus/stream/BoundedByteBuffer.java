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
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;

/**
 * Buffer with buffering allowing One Writer and Multiple Readers.
 * <p>
 * - Storage is done in a fixed size circular buffer (https://en.wikipedia.org/wiki/Circular_buffer)
 * - Reader &amp; writers are synchronized using multiple Producer-Consumer locks : (https://en.wikipedia
 * .org/wiki/Producer%E2%80%93consumer_problem)
 * - Writer can write till circular buffer is full. Then it blocks until free space is available (ALL readers have
 * read some data)
 * - Reader cannot read till the Writer writes data to the circular buffer.
 */
public class BoundedByteBuffer implements Closeable {
  /**
   * Represents the end-of-file (or stream).
   */
  public static final int EOF = -1;
  private final int bufferSize;
  private final byte[] circularBuffer;

  private final ProducerConsumerLock[] locks;
  private final int readerCount;

  private final AtomicBoolean endOfStream = new AtomicBoolean(false);

  private final Writer writer;
  private final Reader[] readers;

  public BoundedByteBuffer(final int bufferSize, final int readerCount) {
    this.bufferSize = bufferSize;
    this.readerCount = readerCount;

    this.circularBuffer = new byte[bufferSize];

    this.locks = new ProducerConsumerLock[readerCount];
    for (var i = 0; i < readerCount; i++) {
      this.locks[i] = new ProducerConsumerLock(bufferSize);
    }

    writer = new Writer();
    readers = new Reader[readerCount];
    for (var i = 0; i < readerCount; i++) {
      readers[i] = new Reader(i);
    }
  }

  public Writer getWriter() {
    return writer;
  }

  public InputStream getReader(final int index) {
    if (index < 0 || index >= readerCount) {
      throw new CcsInvalidArgumentRuntimeException("Invalid index");
    }
    return readers[index];
  }

  @Override
  public void close() {
    writer.close();
    for (final var reader : readers) {
      reader.close();
    }
  }

  /**
   * Writes data to the {@link BoundedByteBuffer}
   * At the end of data, should write and End Of File (EOF) using the writeEOF() method
   * Closing the Writer without EOF would throw a IOException (Broken stream)
   * <p>
   * Non thread safe. Writer should be used by a single thread.
   */
  public class Writer implements Closeable {

    private int writePos;
    private boolean closed;

    private Writer() {
      writePos = 0;
      closed = false;
    }

    /**
     * Writes data to buffer.
     * Cannot write more than buffer size
     */
    public void write(final byte[] src, final int offset, final int length) throws InterruptedException, IOException {

      if (offset < 0 || length < 0 || offset + length > src.length || length > bufferSize) {
        throw new CcsInvalidArgumentRuntimeException("Invalid offset / length");
      }

      if (closed) {
        throw new IOException("Cannot write to closed buffer");
      }

      // Await for free space
      awaitFreeBufferSpace(length);

      // Write to end (from offset to end)
      final var bytesToWriteAtEnd = Math.min(bufferSize - writePos, length);
      if (bytesToWriteAtEnd > 0) {
        System.arraycopy(src, offset, circularBuffer, writePos, bytesToWriteAtEnd);
      }

      // Write from beginning (from 0)
      final var bytesToWriteAtBeginning = length - bytesToWriteAtEnd;
      if (bytesToWriteAtBeginning > 0) {
        System.arraycopy(src, offset + bytesToWriteAtEnd, circularBuffer, 0, bytesToWriteAtBeginning);
      }

      writePos = (writePos + length) % bufferSize;

      // notify
      notifyConsumers(length);
    }

    private void awaitFreeBufferSpace(final int length) throws InterruptedException, IOException {
      var atLeastOneReaderAlive = false;
      for (final var lock : locks) {
        final var acquired = lock.tryBeginProduce(length);
        if (acquired) {
          atLeastOneReaderAlive = true;
        }
      }
      if (!atLeastOneReaderAlive) {
        throw new IOException("Broken stream. No more active readers");
      }
    }

    private void notifyConsumers(final int length) {
      for (final var lock : locks) {
        lock.endProduce(length);
      }
    }

    /**
     * Signals that stream ended successfully.
     */
    public void writeEOF() {
      endOfStream.set(true);
    }

    /**
     * Closes the writer &amp; all associated resources.
     * If close() is invoked without writeEOF() the reader side will get an IOException (broken stream).
     */
    public void close() {
      this.closed = true;
      for (final var lock : locks) {
        lock.close();
      }
    }
  }


  /**
   * Reader InputStream.
   * Every reader has a read index from the circular buffer.
   * <p>
   * Non thread safe. A Reader should be used by a single thread.
   */
  private class Reader extends InputStream implements Closeable {

    private final ProducerConsumerLock lock;
    private int readPos;
    private boolean closed;

    private Reader(final int index) {
      if (index < 0 || index >= readerCount) {
        throw new CcsInvalidArgumentRuntimeException("Invalid index");
      }
      this.readPos = 0;
      this.lock = locks[index];
      this.closed = false;
    }

    /**
     * Reads next byte
     *
     * @return 0-255 if byte read successfully. -1 if EOF (writer stream is closed AFTER writeEOF method invoked).
     * @throws IOException is reader stream is closed, or writer stream is closed WITHOUT writeEOF method invocation.
     */
    @Override
    public int read() throws IOException {
      final var buffer = new byte[1];
      final var res = read(buffer, 0, 1);
      if (res == EOF) {
        return EOF;
      }
      return buffer[0] & 0xFF;
    }

    /**
     * Reads from stream and fills buffer
     *
     * @return Read data length, if any. -1 if EOF (writer stream is closed AFTER writeEOF method invoked).
     * @throws IOException is reader stream is closed, or writer stream is closed WITHOUT writeEOF method invocation.
     */
    @Override
    public int read(final byte[] buffer) throws IOException {
      if (buffer == null) {
        throw new CcsInvalidArgumentRuntimeException("buffer cannot be null");
      }
      return read(buffer, 0, buffer.length);
    }

    /**
     * Reads from stream and fills buffer
     *
     * @param buffer the buffer into which the data is written.
     * @param offset the start offset at which the data is written.
     * @param length the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, if any. OR -1 if EndOfFile (writer stream is
     * closed AFTER writeEOF method invoked).
     * @throws IOException is reader stream is closed, or writer stream is closed WITHOUT writeEOF method invocation.
     */
    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
      if (buffer == null) {
        throw new CcsInvalidArgumentRuntimeException("buffer cannot be null");
      }
      if (offset < 0 || length < 0 || offset + length > buffer.length) {
        throw new CcsInvalidArgumentRuntimeException("Invalid length");
      }

      if (closed) {
        throw new IOException("Cannot read from closed buffer");
      }

      if (length == 0) {
        return 0;
      }

      // Await available data
      final var availableLength = awaitDataAvailableToRead(length);

      // Check end of file
      if (availableLength == 0) {
        if (endOfStream.get()) {
          return EOF;
        }

        throw new IOException("Broken stream. Buffer closed without EOF");
      }

      // Copy buffer
      // Read from pos to end
      final var bytesToReadFromPos = Math.min(bufferSize - readPos, availableLength);
      System.arraycopy(circularBuffer, readPos, buffer, offset, bytesToReadFromPos);

      // Copy from beginning
      final var bytesToReadFromBeginning = availableLength - bytesToReadFromPos;
      if (bytesToReadFromBeginning > 0) {
        System.arraycopy(circularBuffer, 0, buffer, offset + bytesToReadFromPos, bytesToReadFromBeginning);
      }
      readPos = (readPos + availableLength) % bufferSize;

      // Release writer lock
      notifyWriter(availableLength);

      return availableLength;
    }

    @Override
    public int available() throws IOException {
      return lock.possibleAvailable();
    }

    /**
     * Closes the reader & all associated resources.
     * If all readers are closed, the writer might get an IOException (broken stream) if it tries to write to buffer.
     */
    @Override
    public void close() {
      this.closed = true;
      this.lock.close();
    }

    private int awaitDataAvailableToRead(final int length) throws IOException {
      final int availableLength;
      try {
        availableLength = lock.tryBeginConsume(length);
      } catch (final InterruptedException e) {// SONAR intentional
        throw new IOException("Interrupted thread", e);
      }
      return availableLength;
    }

    private void notifyWriter(final int availableLength) {
      lock.endConsume(availableLength);
    }
  }
}
