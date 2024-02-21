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

package io.clonecloudstore.accessor.apache.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.clonecloudstore.common.standard.system.SystemTools;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;

public class InputStreamClosingContext extends InputStream {
  private final InputStream original;
  private final ClassicHttpResponse response;
  private final CloseableHttpClient closeableHttpClient;

  public InputStreamClosingContext(final InputStream original, final ClassicHttpResponse response,
                                   final CloseableHttpClient client) {
    this.original = original;
    this.response = response;
    this.closeableHttpClient = client;
  }

  @Override
  public int read(final byte[] b) throws IOException {
    return original.read(b);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    return original.read(b, off, len);
  }

  @Override
  public long skip(final long n) throws IOException {
    return original.skip(n);
  }

  @Override
  public int available() throws IOException {
    return original.available();
  }

  @Override
  public void close() throws IOException {
    var ioException = SystemTools.silentlyClose(original);
    try {
      if (response != null) {
        response.close();
      }
    } catch (final IOException e) {
      if (ioException == null) {
        ioException = e;
      }
    }
    try {
      if (closeableHttpClient != null) {
        closeableHttpClient.close();
      }
    } catch (final IOException e) {
      if (ioException == null) {
        ioException = e;
      }
    }
    if (ioException != null) {
      throw ioException;
    }
  }

  @Override
  public void mark(final int readlimit) {
    original.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    original.reset();
  }

  @Override
  public boolean markSupported() {
    return original.markSupported();
  }

  @Override
  public long transferTo(final OutputStream out) throws IOException {
    return SystemTools.transferTo(original, out);
  }

  @Override
  public int read() throws IOException {
    return original.read();
  }
}
