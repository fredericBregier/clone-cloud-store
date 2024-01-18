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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.properties.ApiConstants;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import org.jboss.logging.Logger;
import org.jboss.logmanager.MDC;

/**
 * Client Abstraction
 *
 * @param <S> the type for the Rest Service as Quarkus definition
 */
public abstract class SimpleClientAbstract<S extends Closeable> implements Closeable {
  private static final Logger LOGGER = Logger.getLogger(SimpleClientAbstract.class);
  public static final String MDC_COMPRESSED_CONTENT = "mdc-compressed-content";
  public static final String MDC_COMPRESSED_RESPONSE = "mdc-compressed-response";
  public static final String MDC_QUERY_HEADERS = "mdc-query-headers";
  protected static final ClientResponseExceptionMapper exceptionMapper = new ClientResponseExceptionMapper();
  private static final Map<String, Object> INPUTSTREAM_OBJECT_MAP = new ConcurrentHashMap<>();
  private static final Map<String, Boolean> INPUTSTREAM_COMPRESSED_MAP = new ConcurrentHashMap<>();
  private S service;
  private final SimpleClientFactoryAbstract<S> factory;
  private final URI uri;
  private final AtomicReference<String> opId = new AtomicReference<>();

  /**
   * Constructor used by the Factory
   */
  protected SimpleClientAbstract(final SimpleClientFactoryAbstract<S> factory, final URI uri) {
    this.factory = factory;
    this.uri = uri;
    service = factory.getService(uri);
  }

  /**
   * @return the Factory used by this client
   */
  protected SimpleClientFactoryAbstract<S> getFactory() {
    return factory;
  }

  /**
   * Close and reopens Quarkus Rest client
   */
  public void reopen() {
    try {
      service.close();
      service = factory.getService(uri);
    } catch (final Exception ignore) {
      // Ignore
    }
  }

  @Override
  public void close() {
    resetQueryContext();
    resetMdcOpId();
    try {
      service.close();
    } catch (final Exception ignore) {
      // Ignore
    }
  }

  /**
   * Set the current Operation Id
   */
  public void setOpId(final String opId) {
    this.opId.set(setMdcOpId(opId));
  }

  /**
   * @param compressedBody True, client provides compressed body
   */
  public static void bodyCompressed(final boolean compressedBody) {
    LOGGER.debugf("Compress: %b", compressedBody);
    if (compressedBody) {
      MDC.put(MDC_COMPRESSED_CONTENT, "1");
    } else {
      MDC.remove(MDC_COMPRESSED_CONTENT);
    }
  }

  /**
   * @return True if the client provides compressed body
   */
  public static boolean isBodyCompressed() {
    var encoding = MDC.get(MDC_COMPRESSED_CONTENT);
    LOGGER.debugf("Compress: %s", encoding);
    return ParametersChecker.isNotEmpty(encoding);
  }

  /**
   * @param acceptCompression True, client requires compressed response
   */
  public static void acceptCompression(final boolean acceptCompression) {
    LOGGER.debugf("AcceptCompress: %b", acceptCompression);
    if (acceptCompression) {
      MDC.put(MDC_COMPRESSED_RESPONSE, "1");
    } else {
      MDC.remove(MDC_COMPRESSED_RESPONSE);
    }
  }

  /**
   * @return True if the client requires compressed response
   */
  public static boolean isAcceptCompression() {
    var encoding = MDC.get(MDC_COMPRESSED_RESPONSE);
    LOGGER.debugf("AcceptCompress: %s", encoding);
    return ParametersChecker.isNotEmpty(encoding);
  }

  /**
   * @param headersMap the apiBusinessIn as map to setup as headers
   */
  public static void setHeadersMap(final Map<String, String> headersMap) {
    LOGGER.debugf("HeadersMap: %s", headersMap);
    MDC.putObject(MDC_QUERY_HEADERS, headersMap);
  }

  /**
   * @return True if the client requires compressed response
   */
  public static Map<String, String> getHeadersMap() {
    var queryHeaders = MDC.getObject(MDC_QUERY_HEADERS);
    LOGGER.debugf("HeadersMap: %s", queryHeaders);
    return (Map<String, String>) queryHeaders;
  }

  /**
   * Set object received from headers
   */
  public static void setDtoFromHeaders(final Object result) {
    LOGGER.debugf("Set DTO %s %s", getMdcOpId(), result);
    if (result != null) {
      INPUTSTREAM_OBJECT_MAP.put(getMdcOpId(), result);
    }
  }

  /**
   * Set Compression status received from headers
   */
  public static void setCompressionStatusFromHeaders(final Boolean compressed) {
    LOGGER.debugf("Set Compression %s %b", getMdcOpId(), compressed);
    if (compressed != null) {
      INPUTSTREAM_COMPRESSED_MAP.put(getMdcOpId(), compressed);
    }
  }

  /**
   * @return received Object from Headers
   */
  public static Object getDtoFromHeaders() {
    LOGGER.debugf("Status Contains DTO %s %b", getMdcOpId(), INPUTSTREAM_OBJECT_MAP.containsKey(getMdcOpId()));
    return INPUTSTREAM_OBJECT_MAP.remove(getMdcOpId());
  }

  /**
   * @return received Compression status from Headers
   */
  public static boolean getCompressionStatusFromHeaders() {
    LOGGER.debugf("Status Contains Compression %s %b", getMdcOpId(),
        INPUTSTREAM_COMPRESSED_MAP.containsKey(getMdcOpId()));
    var compressed = INPUTSTREAM_COMPRESSED_MAP.remove(getMdcOpId());
    if (compressed != null) {
      return compressed;
    }
    return false;
  }

  /**
   * Clean all Query context
   */
  public void resetQueryContext() {
    LOGGER.debugf("Clear Query Status and Context (%s)", INPUTSTREAM_COMPRESSED_MAP);
    MDC.remove(MDC_COMPRESSED_CONTENT);
    MDC.remove(MDC_COMPRESSED_RESPONSE);
    MDC.removeObject(MDC_QUERY_HEADERS);
    if (opId.get() != null) {
      INPUTSTREAM_OBJECT_MAP.remove(opId.get());
      INPUTSTREAM_COMPRESSED_MAP.remove(opId.get());
    }
    MDC.remove(ApiConstants.X_OP_ID);
  }

  /**
   * Check opId and put it in MDC
   *
   * @return the value to use
   */
  public static String setMdcOpId(final String opId) {
    final var opIdReal = ParametersChecker.isNotEmpty(opId) ? opId : GuidLike.getGuid();
    LOGGER.debugf("Renew OpId? %b (%s => %s)", !ParametersChecker.isNotEmpty(opId), opId, opIdReal);
    MDC.put(ApiConstants.X_OP_ID, opIdReal);
    QuarkusProperties.refreshModuleMdc();
    return opIdReal;
  }

  /**
   * @return the current OpId or a new one if none
   */
  public static String getMdcOpId() {
    var opId = MDC.get(ApiConstants.X_OP_ID);
    if (ParametersChecker.isEmpty(opId)) {
      opId = setMdcOpId(null);
    } else {
      QuarkusProperties.refreshModuleMdc();
    }
    return opId;
  }

  /**
   * Get the current Operation Id
   */
  public String getOpId() {
    var opIdGet = this.opId.get();
    if (opIdGet == null) {
      opIdGet = getMdcOpId();
      this.setOpId(opIdGet);
    }
    return opIdGet;
  }

  /**
   * Reset OpId to empty
   */
  public void resetMdcOpId() {
    MDC.remove(ApiConstants.X_OP_ID);
    this.opId.set(null);
  }

  /**
   * Get the associated ServiceRest
   */
  protected S getService() {
    return service;
  }

  /**
   * Get the URI defined from construction
   */
  protected URI getUri() {
    return uri;
  }
}
