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

package io.clonecloudstore.common.quarkus.client;

import java.io.Closeable;
import java.net.URI;

import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * The Abstract implementation for the Client Factory.
 * Note that the implementation can use ApplicationScoped annotation.
 * Use property quarkus.rest-client."org.acme.rest.client.ExtensionsService".url to setup the right URL for Quarkus
 * service.
 *
 * @param <S> the type for the Rest Service as Quarkus definition
 */
public abstract class SimpleClientFactoryAbstract<S extends Closeable> implements Closeable {
  private static final Logger LOGGER = Logger.getLogger(SimpleClientFactoryAbstract.class);
  private static final String PREFIX_URL = "quarkus.rest-client.\"";
  private static final String POSTFIX_URL = "\".url";
  public static final String DEFAULT_VALUE = "http://127.0.0.1:8081";
  private static final String QUARKUS_REST_CLIENT_INTERFACE = "$$QuarkusRestClientInterface";
  Vertx vertx = null;
  private boolean tls;
  private URI uri;

  /**
   * Empty constructor
   */
  protected SimpleClientFactoryAbstract() {
    // Empty
  }

  @Inject
  void setup() {
    vertx = CDI.current().select(Vertx.class).get();
    // Load quarkus.rest-client."org.acme.rest.client.ExtensionsService".url
    try {
      final String className;
      final var serviceClass = getServiceClass();
      if (serviceClass != null) {
        className = serviceClass.getName();
      } else {
        try (final var service = getService(URI.create(DEFAULT_VALUE))) {
          // Remove $$QuarkusRestClientInterface from class name to get the real Interface name
          className = service.getClass().getName().replace(QUARKUS_REST_CLIENT_INTERFACE, "");
        }
      }
      final var uriString =
          QuarkusSystemPropertyUtil.getStringConfig(PREFIX_URL + className + POSTFIX_URL, DEFAULT_VALUE);
      final var uriSimple = URI.create(uriString);
      prepare(uriSimple);
    } catch (final Exception ignore) {
      // Ignore
    }
  }

  /**
   * Shall be overridden
   *
   * @return the class of API Interface
   */
  protected abstract Class<?> getServiceClass();

  /**
   * Allows to change the target after initialization.
   * This shall be called from Properties at runtime for each Factory (1 per destination).
   * Not necessary if the configuration contains and if the factory is for only one target:<br/>
   * quarkus.rest-client."org.acme.rest.client.ExtensionsService".url=https://hostname:port/api
   */
  public SimpleClientFactoryAbstract<S> prepare(final URI uri) {
    final var scheme = uri.getScheme() == null ? "http" : uri.getScheme();
    final var isTls = "https".equals(scheme);
    final var host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
    final var port = uri.getPort() <= 0 ? 8081 : uri.getPort();
    final var path = uri.getPath();
    return prepare(isTls, host, port, path);
  }

  /**
   * Allows to change the target after initialization.
   * This shall be called from Properties at runtime for each Factory (1 per destination).
   * Not necessary if the configuration contains and if the factory is for only one target:<br/>
   * quarkus.rest-client."org.acme.rest.client.ExtensionsService".url=https://hostname:port/api
   */
  public SimpleClientFactoryAbstract<S> prepare(final boolean tls, final String hostname, final int quarkusPort,
                                                final String path) {
    if (vertx == null) {
      setup();
    }
    this.tls = tls;
    final var finalPath = '/' + (ParametersChecker.isNotEmpty(path) ? path : "");
    final var base = finalPath.replaceAll("\\/+", "/");
    uri = URI.create((tls ? "https://" : "http://") + hostname + ':' + quarkusPort + base);
    return this;
  }

  /**
   * @return the Vertx CDI instance
   */
  public Vertx getVertx() {
    return vertx;
  }

  /**
   * @return the URI as specified by tls, hostname and quarkusPort
   */
  public URI getUri() {
    return uri;
  }

  @Override
  @PreDestroy
  public void close() {
    // Empty
  }

  /**
   * @return True if TLS is required
   */
  public boolean isTls() {
    return tls;
  }

  /**
   * In general, implementation is: QuarkusRestClientBuilder.newBuilder().baseUri(uri).build(ServiceRest.class)
   * where ServiceRest is your Java Interface of the Rest Service.
   *
   * @return the Rest Service associated
   */
  protected S getService(final URI uri) {
    final var apiClass = getServiceClass();
    LOGGER.debugf("Uri %s Class %s", uri, apiClass);
    return (S) QuarkusRestClientBuilder.newBuilder().baseUri(uri)
        .property("io.quarkus.rest.client.max-chunk-size", StandardProperties.getBufSize()).build(apiClass);
  }

  /**
   * In general, implementation is simply: return new ClientImpl(this);
   *
   * @return the new Client with this associated factory
   */
  public abstract SimpleClientAbstract<S> newClient();

  /**
   * For Factory with multiple targets, to ensure correctness of URI
   *
   * @return the new Client with this associated factory for this URI
   */
  public synchronized SimpleClientAbstract<S> newClient(final URI uri) {
    return prepare(uri).newClient();
  }
}
