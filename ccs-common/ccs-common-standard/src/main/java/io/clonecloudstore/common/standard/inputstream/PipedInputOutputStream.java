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
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;

public class PipedInputOutputStream extends InputStream {
  private static final int DEFAULT_PIPE_SIZE = 4;
  private static final int WAIT_TIME = 50;
  private static final String PIPE_CLOSED = "Pipe closed";
  private final AtomicBoolean closedByWriter = new AtomicBoolean();
  private final AtomicBoolean closedByReader = new AtomicBoolean();
  private byte[] buffer = null;
  private final BlockingQueue<byte[]> buffers;
  private final AtomicReference<Exception> exceptionAtomicReference;
  /**
   * The index of the position in the buffer at which the next
   * byte of data will be read by this piped input stream.
   */
  private int pos;

  /**
   * Data bytes written will then be available as input from this stream.
   */
  public PipedInputOutputStream() {
    this(null, DEFAULT_PIPE_SIZE);
  }

  /**
   * Data bytes written will then be available as input from this stream.
   */
  public PipedInputOutputStream(final AtomicReference<Exception> exceptionAtomicReference) {
    this(exceptionAtomicReference, DEFAULT_PIPE_SIZE);
  }

  /**
   * Data bytes written will then be available as input from this stream.
   *
   * @param pipeSize the size of the pipe's buffer (number of buffer, not bytes).
   */
  public PipedInputOutputStream(final AtomicReference<Exception> exceptionAtomicReference, final int pipeSize) {
    if (pipeSize <= 0) {
      throw new IllegalArgumentException("Pipe Size <= 0");
    }
    buffers = new LinkedBlockingQueue<>(pipeSize);
    pos = 0;
    this.exceptionAtomicReference = exceptionAtomicReference;
  }

  /**
   * Async transfer of the given InputStream to this PipedInputOutputStream
   */
  public void transferFromAsync(final InputStream inputStream) {
    SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        this.transferFrom(inputStream);
      } catch (final IOException e) {
        if (exceptionAtomicReference != null) {
          exceptionAtomicReference.set(e);
        }
      }
    });
    Thread.yield();
  }

  /**
   * Sync transfer of the given InputStream to this PipedInputOutputStream
   */
  public long transferFrom(final InputStream inputStream) throws IOException {
    var transferred = 0L;
    var read = 0;
    final var buf = new byte[StandardProperties.getBufSize()];
    while ((read = inputStream.read(buf, 0, buf.length)) >= 0) {
      write(buf, 0, read);
      transferred += read;
      Thread.yield();
    }
    flush();
    inputStream.close();
    closeOutput();
    return transferred;
  }

  private int checkBuffer() throws IOException {
    if (buffer == null || pos >= buffer.length) {
      /* now empty */
      pos = 0;
      buffer = null;
      return checkAvailable();
    }
    return buffer.length - pos;
  }

  private int checkAvailable() throws IOException {
    while (buffer == null) {
      if (closedByWriter.get() && buffers.isEmpty()) {
        /* closed by writer, return EOF */
        return -1;
      }
      try {
        buffer = buffers.poll(WAIT_TIME, TimeUnit.MILLISECONDS);
        pos = 0;
      } catch (final InterruptedException e) { // NOSONAR intentional
        throw new IOException(e);
      }
    }
    return buffer.length - pos;
  }

  /**
   * Reads the next byte of data from this piped input stream. The
   * value byte is returned as an {@code int} in the range
   * {@code 0} to {@code 255}.
   * This method blocks until input data is available, the end of the
   * stream is detected, or an exception is thrown.
   *
   * @return {@inheritDoc}
   * @throws IOException if the pipe is unconnected,
   *                     <a href="#BROKEN"> {@code broken}</a>, closed,
   *                     or if an I/O error occurs.
   */
  @Override
  public int read() throws IOException {
    if (closedByReader.get()) {
      throw new IOException(PIPE_CLOSED);
    }
    var len = checkBuffer();
    if (len < 0) {
      /* closed by writer, return EOF */
      return -1;
    }
    int ret = buffer[pos++] & 0xFF;
    checkBuffer();
    return ret;
  }

  @Override
  public int read(final byte[] b) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    return read(b, 0, b.length);
  }

  /**
   * Reads up to {@code len} bytes of data from this piped input
   * stream into an array of bytes. Less than {@code len} bytes
   * will be read if the end of the data stream is reached or if
   * {@code len} exceeds the pipe's buffer size.
   * If {@code len } is zero, then no bytes are read and 0 is returned;
   * otherwise, the method blocks until at least 1 byte of input is
   * available, end of the stream has been detected, or an exception is
   * thrown.
   *
   * @param b   {@inheritDoc}
   * @param off {@inheritDoc}
   * @param len {@inheritDoc}
   * @return {@inheritDoc}
   * @throws NullPointerException      {@inheritDoc}
   * @throws IndexOutOfBoundsException {@inheritDoc}
   * @throws IOException               if the pipe is <a href="#BROKEN"> {@code broken}</a>,
   *                                   unconnected,
   *                                   closed, or if an I/O error occurs.
   */
  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) {
      return 0;
    }
    if (closedByReader.get()) {
      throw new IOException(PIPE_CLOSED);
    }

    int read = 0;
    int available = 0;
    var offNew = off;
    var lenNew = len;
    while (lenNew > 0 && (available = checkBuffer()) >= 0) {
      available = Math.min(available, lenNew);
      System.arraycopy(buffer, pos, b, offNew, available);
      pos += available;
      offNew += available;
      lenNew -= available;
      read += available;
    }
    checkBuffer();
    if (read == 0 && available <= 0) {
      return -1;
    }
    return read;
  }

  /**
   * Returns the number of bytes that can be read from this input
   * stream without blocking.
   *
   * @return the number of bytes that can be read from this input stream
   * without blocking, or {@code 0} if this input stream has been
   * closed by invoking its {@link #close()} method, or if the pipe
   * is unconnected, or
   * <a href="#BROKEN"> {@code broken}</a>.
   * @throws IOException {@inheritDoc}
   */
  @Override
  public int available() throws IOException {
    var len = checkBuffer();
    return Math.max(len, 0);
  }

  /**
   * {@inheritDoc}
   *
   * @throws IOException {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    closedByReader.set(true);
  }

  /**
   * Receives data into an array of bytes.  This method will
   * block until some input is available.
   * Using this method leads to copy.
   *
   * @param b   the buffer into which the data is received
   * @param off the start offset of the data
   * @param len the maximum number of bytes received
   * @throws IOException If the pipe is <a href="#BROKEN"> broken</a>,
   *                     closed, or if an I/O error occurs.
   */
  private void receive(final byte[] b, final int off, final int len) throws IOException {
    receive(Arrays.copyOfRange(b, off, len));
  }

  /**
   * Receives data into an array of bytes.  This method will
   * block until some input is available.
   * Using this method leads to no copy.
   *
   * @param b the buffer into which the data is received
   * @throws IOException If the pipe is <a href="#BROKEN"> broken</a>,
   *                     closed, or if an I/O error occurs.
   */
  private void receive(final byte[] b) throws IOException {
    checkStateForReceive();
    try {
      buffers.put(b);
    } catch (final InterruptedException e) { // NOSONAR intentional
      throw new IOException(e);
    }
  }

  private void checkStateForReceive() throws IOException {
    if (closedByWriter.get() || closedByReader.get()) {
      throw new IOException(PIPE_CLOSED);
    }
  }

  /**
   * Writes {@code len} bytes from the specified byte array
   * starting at offset {@code off} to this piped output stream.
   * This method blocks until all the bytes are written to the output
   * stream.
   *
   * @param b buffer with no copy from it
   * @throws IOException if the pipe is <a href=#BROKEN> broken</a>,
   *                     unconnected,
   *                     closed, or if an I/O error occurs.
   */
  public void write(final byte[] b) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (b.length == 0) {
      return;
    }
    receive(b);
  }

  /**
   * Writes {@code len} bytes from the specified byte array
   * starting at offset {@code off} to this piped output stream.
   * This method blocks until all the bytes are written to the output
   * stream.
   *
   * @param b   buffer with copy from it
   * @param off offset
   * @param len length
   * @throws IOException               if the pipe is <a href=#BROKEN> broken</a>,
   *                                   unconnected,
   *                                   closed, or if an I/O error occurs.
   * @throws IndexOutOfBoundsException if issue on bounds
   */
  public void write(final byte[] b, final int off, final int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) {
      return;
    }
    receive(b, off, len);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes
   * to be written out.
   */
  public void flush() {
    // Empty
  }

  /**
   * Closes this piped output stream and releases any system resources
   * associated with this stream. This stream may no longer be used for
   * writing bytes.
   */
  public void closeOutput() {
    closedByWriter.set(true);
  }

  public OutputStream getOutputStream() {
    return new VirtualOutputStream(this);
  }

  private static class VirtualOutputStream extends OutputStream {
    private final PipedInputOutputStream outputStream;
    private final byte[] buffer;
    private int written = 0;

    private VirtualOutputStream(final PipedInputOutputStream outputStream) {
      this.outputStream = outputStream;
      buffer = new byte[StandardProperties.getBufSize()];
    }

    @Override
    public void write(final int b) throws IOException {
      buffer[written] = (byte) b;
      written++;
      if (written >= buffer.length) {
        outputStream.write(buffer.clone());
        written = 0;
      }
    }

    @Override
    public void write(final byte[] b) throws IOException {
      if (written > 0) {
        outputStream.write(b, 0, written);
        written = 0;
      }
      outputStream.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      if (written > 0) {
        outputStream.write(b, 0, written);
        written = 0;
      }
      if (off == 0 && len == b.length) {
        outputStream.write(b);
      } else {
        outputStream.write(b, off, len);
      }
    }

    @Override
    public void flush() {
      outputStream.flush();
    }

    @Override
    public void close() {
      outputStream.closeOutput();
    }
  }
}
