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

package org.jboss.resteasy.reactive.server.vertx;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedChannelException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.impl.ConnectionBase;
import io.vertx.ext.web.RoutingContext;
import org.jboss.resteasy.reactive.ConfigurationVertxInputStream;
import org.jboss.resteasy.reactive.common.core.BlockingNotAllowedException;

public class VertxInputStream extends InputStream {

  public static final String MAX_REQUEST_SIZE_KEY = "io.quarkus.max-request-size";
  public final byte[] oneByte = new byte[1];
  private final VertxBlockingInput exchange;

  private boolean closed;
  private boolean finished;
  private ByteBuf pooled;
  private final long limit;
  private final VertxResteasyReactiveRequestContext vertxResteasyReactiveRequestContext;

  public VertxInputStream(RoutingContext request, long timeout,
                          VertxResteasyReactiveRequestContext vertxResteasyReactiveRequestContext) {
    this.vertxResteasyReactiveRequestContext = vertxResteasyReactiveRequestContext;
    this.exchange = new VertxBlockingInput(request.request(), timeout);
    Long limitObj = request.get(MAX_REQUEST_SIZE_KEY);
    if (limitObj == null) {
      limit = -1;
    } else {
      limit = limitObj;
    }
  }

  public VertxInputStream(RoutingContext request, long timeout, ByteBuf existing,
                          VertxResteasyReactiveRequestContext vertxResteasyReactiveRequestContext) {
    this.vertxResteasyReactiveRequestContext = vertxResteasyReactiveRequestContext;
    this.exchange = new VertxBlockingInput(request.request(), timeout);
    Long limitObj = request.get(MAX_REQUEST_SIZE_KEY);
    if (limitObj == null) {
      limit = -1;
    } else {
      limit = limitObj;
    }
    this.pooled = existing;
  }

  @Override
  public int read() throws IOException {
    int read = read(oneByte);
    if (read == -1) {
      return -1;
    }
    return oneByte[0] & 0xff;
  }

  @Override
  public int read(final byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (b == null || b.length < off + len) {
      throw new IOException("Incompatible Buffer size");
    }
    if (vertxResteasyReactiveRequestContext.continueState ==
        VertxResteasyReactiveRequestContext.ContinueState.REQUIRED) {
      vertxResteasyReactiveRequestContext.continueState = VertxResteasyReactiveRequestContext.ContinueState.SENT;
      vertxResteasyReactiveRequestContext.response.writeContinue();
    }
    readIntoBuffer();
    if (limit > 0 && exchange.request.bytesRead() > limit) {
      HttpServerResponse response = exchange.request.response();
      if (response.headWritten()) {
        //the response has been written, not much we can do
        exchange.request.connection().close();
        throw new IOException("Request too large");
      } else {
        response.setStatusCode(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE.code());
        response.headers().add(HttpHeaderNames.CONNECTION, "close");
        response.endHandler(new Handler<Void>() {
          @Override
          public void handle(Void event) {
            exchange.request.connection().close();
          }
        });
        response.end();
        throw new IOException("Request too large");
      }
    }
    if (finished) {
      return -1;
    }
    if (len == 0) {
      return 0;
    }
    ByteBuf buffer = pooled;
    int copied = Math.min(len, buffer.readableBytes());
    buffer.readBytes(b, off, copied);
    if (!buffer.isReadable()) {
      pooled.release();
      pooled = null;
    }
    return copied;
  }

  private void readIntoBuffer() throws IOException {
    if (pooled == null && !finished) {
      pooled = exchange.readBlocking();
      if (pooled == null) {
        finished = true;
      }
    }
  }

  @Override
  public int available() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
    if (finished) {
      return 0;
    }
    if (pooled != null && pooled.isReadable()) {
      return pooled.readableBytes() + exchange.readBytesAvailable();
    }

    return exchange.readBytesAvailable();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      while (!finished) {
        readIntoBuffer();
        if (pooled != null) {
          pooled.release();
          pooled = null;
        }
      }
    } catch (IOException | RuntimeException e) {
      //our exchange is all broken, just end it
      throw e;
    } finally {
      if (pooled != null) {
        pooled.release();
        pooled = null;
      }
      finished = true;
    }
  }

  public static class VertxBlockingInput implements Handler<Buffer> {
    protected final HttpServerRequest request;
    protected final Deque<Buffer> inputOverflow = new ConcurrentLinkedDeque<>();
    private static final int INTERNAL_READ_WAIT_MS = 50;
    protected boolean endOfWrite = false;
    protected IOException readException;
    private final long timeout;
    private final int headerLen;

    public VertxBlockingInput(HttpServerRequest request, long timeout) {
      this.request = request;
      this.headerLen = getLengthFromHeader();
      this.timeout = timeout;
      final ConnectionBase connection = (ConnectionBase) request.connection();
      if (!connection.channel().isOpen()) {
        readException = new ClosedChannelException();
      } else if (!request.isEnded()) {
        request.pause();
        request.handler(this);
        request.endHandler(new Handler<Void>() {
          @Override
          public void handle(Void event) {
            endOfWrite = true;
            wakeupReader();
          }
        });
        request.exceptionHandler(new Handler<Throwable>() {
          @Override
          public void handle(Throwable event) {
            readException = new IOException(event);
            Buffer d = inputOverflow.poll();
            while (d != null) {
              d.getByteBuf().release();
              d = inputOverflow.poll();
            }
            wakeupReader();
          }
        });
        request.fetch(ConfigurationVertxInputStream.INITIAL_PREFETCH);
      } else {
        endOfWrite = true;
      }
    }

    private void wakeupReader() {
      synchronized (request.connection()) {
        request.connection().notifyAll();
      }
    }

    private Buffer removeHead() throws IOException {
      if (inputOverflow.isEmpty()) {
        synchronized (request.connection()) {
          try {
            request.connection().wait(INTERNAL_READ_WAIT_MS);
          } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
          }
        }
      }
      return inputOverflow.poll();
    }

    protected ByteBuf readBlocking() throws IOException {
      long expire = System.currentTimeMillis() + timeout;
      Buffer ret;
      boolean status;
      if (readException != null) {
        throw readException;
      }
      // Preemptive fetch
      if (inputOverflow.isEmpty()) {
        request.fetch(1);
      }
      // First read before testing endOfWrite
      ret = removeHead();
      status = ret == null && !endOfWrite && readException == null;
      while (status) {
        long rem = expire - System.currentTimeMillis();
        if (rem <= 0) {
          //everything is broken, if read has timed out we can assume that the underling connection
          //is wrecked, so just close it
          request.connection().close();
          readException = new IOException("Read timed out");
          throw readException;
        }

        if (Context.isOnEventLoopThread()) {
          throw new BlockingNotAllowedException("Attempting a blocking read on io thread");
        }
        ret = removeHead();
        status = ret == null && !endOfWrite && readException == null;
        Thread.yield();
      }
      if (readException != null) {
        throw readException;
      }
      if (!endOfWrite && inputOverflow.isEmpty()) {
        request.fetch(1);
      }
      if (ret == null && !inputOverflow.isEmpty()) {
        // Might not be the end yet if queue is not empty
        ret = inputOverflow.poll();
      }
      return ret == null ? null : ret.getByteBuf();
    }

    @Override
    public void handle(Buffer event) {
      if (event.length() == 0 && request.version() == HttpVersion.HTTP_2) {
        // When using HTTP/2 H2, this indicates that we won't receive anymore data.
        endOfWrite = true;
        wakeupReader();
        return;
      }
      if (readException == null) {
        inputOverflow.addLast(event);
      } else {
        event.getByteBuf().release();
      }
      wakeupReader();
    }

    public int readBytesAvailable() {
      Buffer buf = inputOverflow.peek();
      if (buf != null) {
        int len = buf.getByteBuf().readableBytes();
        if (len > 0) {
          return len;
        }
      }
      return headerLen;
    }

    private int getLengthFromHeader() {
      String length = request.getHeader(HttpHeaders.CONTENT_LENGTH);
      if (length == null) {
        return 8192;
      }
      try {
        return Integer.parseInt(length);
      } catch (NumberFormatException e) {
        Long.parseLong(length); // ignore the value as can only return an int anyway
        return Integer.MAX_VALUE;
      }
    }
  }

}
