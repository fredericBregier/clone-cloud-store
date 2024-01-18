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

package io.clonecloudstore.common.quarkus.client.utils;

import java.util.Arrays;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.standard.properties.ApiConstants;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientResponseFilter;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;

@Dependent
public abstract class AbstractResponseClientFilter<O> implements ResteasyReactiveClientResponseFilter {
  private static final Logger LOGGER = Logger.getLogger(AbstractResponseClientFilter.class);
  private static final String[] DEFAULT_METHODS = new String[]{HttpMethod.GET, HttpMethod.PUT};

  protected abstract O getOutFromHeader(final ResteasyReactiveClientRequestContext requestContext,
                                        final ClientResponseContext responseContext,
                                        final MultivaluedMap<String, String> headers);

  /**
   * Default are GET and PUT
   */
  protected String[] validMethods() {
    return DEFAULT_METHODS;
  }

  @Override
  public void filter(final ResteasyReactiveClientRequestContext requestContext,
                     final ClientResponseContext responseContext) {
    SimpleClientAbstract.setMdcOpId((String) requestContext.getHeaders().getFirst(X_OP_ID));
    final var method = requestContext.getMethod();
    LOGGER.debugf("Finalize Response for %s: is inputStream %s : %b", method,
        requestContext.getHeaders().getFirst(HttpHeaders.ACCEPT),
        MediaType.APPLICATION_OCTET_STREAM.equals(requestContext.getHeaders().getFirst(HttpHeaders.ACCEPT)));
    final var validMethod = Arrays.stream(validMethods()).filter(valid -> valid.equalsIgnoreCase(method)).count() > 0;
    if (validMethod &&
        MediaType.APPLICATION_OCTET_STREAM.equals(requestContext.getHeaders().getFirst(HttpHeaders.ACCEPT))) {
      final var headers = responseContext.getHeaders();
      if (headers != null) {
        LOGGER.debugf("Headers %s", headers);
        var ce = headers.getFirst(HttpHeaders.CONTENT_ENCODING);
        LOGGER.debugf("Status Compression  %s", ce);
        if (ce != null && ce.equalsIgnoreCase(ApiConstants.COMPRESSION_ZSTD)) {
          SimpleClientAbstract.setCompressionStatusFromHeaders(Boolean.TRUE);
        }
        final O businessOut = getOutFromHeader(requestContext, responseContext, headers);
        if (businessOut != null) {
          SimpleClientAbstract.setDtoFromHeaders(businessOut);
        }
      }
    }
  }
}
