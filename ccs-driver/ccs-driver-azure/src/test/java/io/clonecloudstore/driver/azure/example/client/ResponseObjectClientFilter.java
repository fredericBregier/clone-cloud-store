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

package io.clonecloudstore.driver.azure.example.client;

import java.time.Instant;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.model.StorageObject;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.client.ClientResponseContext;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientResponseFilter;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

public class ResponseObjectClientFilter implements ResteasyReactiveClientResponseFilter {
  private static final Logger LOGGER = Logger.getLogger(ResponseObjectClientFilter.class);

  @Override
  public void filter(final ResteasyReactiveClientRequestContext requestContext,
                     final ClientResponseContext responseContext) {
    SimpleClientAbstract.setMdcOpId((String) requestContext.getHeaders().getFirst(X_OP_ID));
    final var method = requestContext.getMethod();
    if (HttpMethod.GET.equalsIgnoreCase(method) || HttpMethod.POST.equalsIgnoreCase(method)) {
      if (responseContext.getHeaders() != null) {
        var headers = responseContext.getHeaders();
        LOGGER.debugf("Headers: %s", headers);
        long len = 0;
        var slen = headers.getFirst(ApiConstants.X_LEN);
        if (ParametersChecker.isNotEmpty(slen)) {
          len = Long.parseLong(slen);
        }
        Instant instant = null;
        var sInstant = headers.getFirst(ApiConstants.X_CREATION_DATE);
        if (ParametersChecker.isNotEmpty(sInstant)) {
          instant = Instant.parse(sInstant);
        }
        var bucket = headers.getFirst(ApiConstants.X_BUCKET);
        var sobject = headers.getFirst(ApiConstants.X_OBJECT);
        final var object = new StorageObject(bucket, sobject, headers.getFirst(ApiConstants.X_HASH), len, instant);
        SimpleClientAbstract.setDtoFromHeaders(object);
      }
    }
  }
}
