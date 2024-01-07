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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.BaseXx;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;

import static io.clonecloudstore.common.standard.properties.StandardProperties.VIRTUAL_EXECUTOR_SERVICE;

public class MultipleActionsInputStream extends InputStream {
  private final InputStream inputStream;
  private final CountDownLatch countDownLatch = new CountDownLatch(1);
  private final long maxWaitMs;
  private final AtomicReference<IOException> raisedExceptionTimeout = new AtomicReference<>();
  private final AtomicReference<Boolean> exceptionDuringCount = new AtomicReference<>(Boolean.TRUE);
  private final CountingInputStream countingInputStream;
  private InputStream workInputStream;
  private MessageDigest digest = null;
  private boolean isCompress = false;
  private boolean isDecompress = false;
  private byte[] digestValue = null;
  private long size = 0;
  private long lastTime;
  private boolean closed = false;
  private boolean pipedVersion = false;

  public static MultipleActionsInputStream create(final InputStream inputStream) {
    if (inputStream instanceof MultipleActionsInputStream mai) {
      return mai;
    }
    return new MultipleActionsInputStream(inputStream);
  }

  public MultipleActionsInputStream(final InputStream inputStream) {
    this(inputStream, StandardProperties.getMaxWaitMs());
  }

  public MultipleActionsInputStream(final InputStream inputStream, final long maxWaitMs) {
    ParametersChecker.checkParameter("Parameters cannot be null or empty", inputStream);
    this.inputStream = inputStream;
    this.maxWaitMs = maxWaitMs;
    this.countingInputStream = new CountingInputStream(inputStream);
    workInputStream = countingInputStream;
    lastTime = System.currentTimeMillis();
  }

  public void asyncPipedInputStream(final AtomicReference<Exception> callerExceptionAtomicReference) {
    try {
      if (pipedVersion) {
        return;
      }
      final TransferInputStream transfertInputStream =
          new TransferInputStream(inputStream, callerExceptionAtomicReference);
      countingInputStream.changeInputStream(transfertInputStream);
      transfertInputStream.startCopyAsync();
      pipedVersion = true;
    } catch (final IOException e) {
      raisedExceptionTimeout.compareAndSet(null, e);
    }
  }

  public void computeDigest(final DigestAlgo digestAlgo) throws NoSuchAlgorithmException {
    ParametersChecker.checkParameter("Parameters cannot be null or empty", digestAlgo);
    try {
      digest = MessageDigest.getInstance(digestAlgo.algoName);
    } catch (final NoSuchAlgorithmException e) {
      throw new NoSuchAlgorithmException(digestAlgo.algoName + " : algorithm not supported by this JVM", e);
    }
  }

  public void compress() throws IOException {
    compress(-1);
  }

  public void compress(final int level) throws IOException {
    workInputStream = new ZstdCompressInputStream(workInputStream, true, level);
    isCompress = true;
  }

  public void decompress() throws IOException {
    workInputStream = new ZstdDecompressInputStream(workInputStream);
    isDecompress = true;
  }

  public long getSourceRead() {
    if (countingInputStream != null) {
      return countingInputStream.getRead();
    }
    return size;
  }

  /**
   * @return the size if the read is over, or returns -1 if read not finished in the given time
   */
  public long waitForAllRead(long timeWaitMs) {
    try {
      long toWait = timeWaitMs;
      while (toWait > 0 && !closed) {
        if (countDownLatch.await(Math.min(100, toWait), TimeUnit.MILLISECONDS)) {
          return size;
        }
        toWait -= 100;
        if (raisedExceptionTimeout.get() != null) {
          return -1;
        }
      }
      if (countDownLatch.getCount() > 0) {
        if (raisedExceptionTimeout.compareAndSet(null, new IOException("TimeOut during Read"))) {
          VIRTUAL_EXECUTOR_SERVICE.execute(() -> SystemTools.silentlyCloseNoException(workInputStream));
        }
        Thread.yield();
        return -1;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return size;
  }

  private long count(final long read) {
    final var now = System.currentTimeMillis();
    if (now - lastTime > maxWaitMs) {
      if (Boolean.TRUE.equals(exceptionDuringCount.get())) {
        raisedExceptionTimeout.compareAndSet(null, new IOException("TimeOut during Read"));
      }
      VIRTUAL_EXECUTOR_SERVICE.execute(() -> SystemTools.silentlyCloseNoException(workInputStream));
      Thread.yield();
    }
    lastTime = now;
    if (read < 0) {
      countDownLatch.countDown();
    } else {
      size += read;
    }
    return read;
  }

  public void changeExceptionDuringCount(final boolean valid) {
    exceptionDuringCount.set(valid);
  }

  private void check() throws IOException {
    final var ioException = raisedExceptionTimeout.get();
    if (ioException != null) {
      VIRTUAL_EXECUTOR_SERVICE.execute(() -> SystemTools.silentlyCloseNoException(workInputStream));
      Thread.yield();
      throw ioException;
    }
  }

  @Override
  public int read(final byte[] b) throws IOException {
    ParametersChecker.checkParameter("Buffer cannot be null", b);
    return read(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    ParametersChecker.checkParameter("Buffer cannot be null", b);
    check();
    final var read = workInputStream.read(b, off, len);
    if (read > 0 && digest != null) {
      digest.update(b, off, read);
    }
    return (int) count(read);
  }

  @Override
  public int available() throws IOException {
    check();
    lastTime = System.currentTimeMillis();
    return workInputStream.available();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    lastTime = System.currentTimeMillis();
    try {
      workInputStream.close();
      if (workInputStream != countingInputStream) {
        countingInputStream.close();
      }
      inputStream.close();
    } finally {
      if (digest != null) {
        getDigestValue();
        digest.reset();
      }
      closed = true;
      countDownLatch.countDown();
    }
    check();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long skip(final long n) throws IOException {
    check();
    return SystemTools.skip(this, n);
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    check();
    return SystemTools.transferTo(this, out);
  }

  @Override
  public int read() throws IOException {
    check();
    lastTime = System.currentTimeMillis();
    var read = workInputStream.read();
    if (read < 0) {
      count(-1);
    } else {
      count(1);
      if (digest != null) {
        digest.update((byte) read);
      }
    }
    return read;
  }

  /**
   * @return the digest value (to be called once the InputStream is over)
   */
  public byte[] getDigestValue() {
    if (digestValue == null && digest != null) {
      digestValue = digest.digest();
    }
    return digestValue;
  }

  /**
   * @return the digest value (to be called once the InputStream is over)
   */
  public String getDigestBase64() {
    final var bytes = getDigestValue();
    if (bytes == null) {
      return null;
    }
    return BaseXx.getBase64Padding(bytes);
  }

  /**
   * @return the digest value (to be called once the InputStream is over)
   */
  public String getDigestBase32() {
    final var bytes = getDigestValue();
    if (bytes == null) {
      return null;
    }
    return BaseXx.getBase32(bytes);
  }

  /**
   * Default Digest base 32
   */
  public String getDigest() {
    return getDigestBase32();
  }

  public boolean isDigestEnabled() {
    return digest != null;
  }

  @Override
  public String toString() {
    return "AllRead: " + size + (countingInputStream != null ? countingInputStream : "") + " Compress: " + isCompress +
        " Decompress: " + isDecompress + " Digest? " + (digest != null) + " Waiting? " +
        (countDownLatch.getCount() > 0) + " Closed: " + closed;
  }

  private static class CountingInputStream extends InputStream {
    private InputStream inputStream;
    private long size = 0;
    private long maxChunkSize = 0;
    private long minChunkSize = Integer.MAX_VALUE;
    private long nbChunks = 0;
    private boolean closed = false;

    private CountingInputStream(final InputStream inputStream) {
      this.inputStream = inputStream;
    }

    private void changeInputStream(final InputStream inputStream) {
      this.inputStream = inputStream;
    }

    public long getRead() {
      return size;
    }

    private long count(final long read) {
      if (read > 0) {
        size += read;
        maxChunkSize = Math.max(maxChunkSize, read);
        minChunkSize = Math.min(minChunkSize, read);
        nbChunks++;
      }
      return read;
    }

    @Override
    public int read(final byte[] b) throws IOException {
      return (int) count(inputStream.read(b));
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      return (int) count(inputStream.read(b, off, len));
    }

    @Override
    public long skip(final long n) throws IOException {
      return SystemTools.skip(this, n);
    }

    @Override
    public int available() throws IOException {
      return inputStream.available();
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      inputStream.close();
      closed = true;
    }

    @Override
    public long transferTo(final OutputStream out) throws IOException {
      return SystemTools.transferTo(this, out);
    }

    @Override
    public int read() throws IOException {
      var read = inputStream.read();
      if (read >= 0) {
        count(1);
      } else {
        count(read);
      }
      return read;
    }

    @Override
    public String toString() {
      return " SizeRead: " + size + " MaxChunkSize: " + maxChunkSize + " MinChunkSize: " + minChunkSize +
          " NbChunks: " + nbChunks + (nbChunks > 0 ? " AverageChunk: " + size / nbChunks : "") + " Closed: " + closed;
    }
  }
}
