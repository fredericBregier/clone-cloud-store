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

package io.clonecloudstore.common.quarkus.client.example;

import java.time.Instant;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientResponseFilter;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_LEN;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;

@ApplicationScoped
public class ResponseClientFilter implements ResteasyReactiveClientResponseFilter {
  private static final Logger LOGGER = Logger.getLogger(ResponseClientFilter.class);

  @Override
  public void filter(final ResteasyReactiveClientRequestContext requestContext,
                     final ClientResponseContext responseContext) {
    LOGGER.infof("Finalize Response: is inputStream %s : %b", requestContext.getHeaders().getFirst(HttpHeaders.ACCEPT),
        MediaType.APPLICATION_OCTET_STREAM.equals(requestContext.getHeaders().getFirst(HttpHeaders.ACCEPT)));
    if (MediaType.APPLICATION_OCTET_STREAM.equals(requestContext.getHeaders().getFirst(HttpHeaders.ACCEPT))) {
      SimpleClientAbstract.setMdcOpId((String) requestContext.getHeaders().getFirst(X_OP_ID));
      final var headers = responseContext.getHeaders();
      LOGGER.infof("Headers %s", headers);
      ApiBusinessOut businessOut = new ApiBusinessOut();
      if (headers != null) {
        businessOut.name = headers.getFirst(ApiConstants.X_NAME);
        var instant = headers.getFirst(X_CREATION_DATE);
        if (instant != null) {
          businessOut.creationDate = Instant.parse(headers.getFirst(X_CREATION_DATE));
        }
        var len = headers.getFirst(X_LEN);
        if (len != null) {
          businessOut.len = Long.parseLong(headers.getFirst(X_LEN));
        }
      }
      SimpleClientAbstract.setDtoFromHeaders(businessOut);
    }
  }
}
