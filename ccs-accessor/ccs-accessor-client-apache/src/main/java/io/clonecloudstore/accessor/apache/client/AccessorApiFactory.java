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

package io.clonecloudstore.accessor.apache.client;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

public class AccessorApiFactory implements Closeable {
  private static final int DEFAULT_TIMEOUT = 30;
  private static final int DEFAULT_MAX_CLIENTS = 100;
  public static final int BUF_SIZE = 98304;
  private final String baseUri;
  private final String clientId;
  private final int timeout;
  private final int maxClients;
  private final PoolingHttpClientConnectionManager cm;
  private final RequestConfig requestConfig;

  public AccessorApiFactory(final String baseUri, final String clientId) {
    this(baseUri, clientId, DEFAULT_TIMEOUT);
  }

  public AccessorApiFactory(final String baseUri, final String clientId, final int timeout) {
    this(baseUri, clientId, timeout, DEFAULT_MAX_CLIENTS);
  }

  public AccessorApiFactory(final String baseUri, final String clientId, final int timeout, final int maxClients) {
    this.baseUri = baseUri;
    this.clientId = clientId;
    this.timeout = timeout;
    this.maxClients = maxClients;
    cm = new PoolingHttpClientConnectionManager();
    requestConfig = setupConnectionManager();
  }

  private RequestConfig setupConnectionManager() {
    cm.setMaxTotal(maxClients);
    cm.setDefaultMaxPerRoute(maxClients);
    ConnectionConfig connConfig = ConnectionConfig.custom().setConnectTimeout(getTimeout(), TimeUnit.SECONDS)
        .setSocketTimeout(getTimeout(), TimeUnit.SECONDS).build();
    cm.setDefaultConnectionConfig(connConfig);
    SocketConfig socketConfig =
        SocketConfig.custom().setRcvBufSize(BUF_SIZE).setRcvBufSize(BUF_SIZE).setSoKeepAlive(true)
            .setSoReuseAddress(true).build();
    cm.setDefaultSocketConfig(socketConfig);
    return RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofSeconds(getTimeout()))
        .setContentCompressionEnabled(false).build();
  }

  public String getBaseUri() {
    return baseUri;
  }

  public String getClientId() {
    return clientId;
  }

  public int getTimeout() {
    return timeout;
  }

  CloseableHttpClient getCloseableHttpClient() {
    return HttpClientBuilder.create().setDefaultRequestConfig(getRequestConfig()).setConnectionManager(getCm())
        .setConnectionManagerShared(true).disableContentCompression().evictExpiredConnections()
        .evictIdleConnections(TimeValue.ofSeconds(getTimeout())).build();
  }

  public AccessorClient newClient() {
    return new AccessorClient(this);
  }

  private PoolingHttpClientConnectionManager getCm() {
    return cm;
  }

  private RequestConfig getRequestConfig() {
    return requestConfig;
  }

  @Override
  public void close() {
    cm.close();
  }
}
