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

package io.clonecloudstore.replicator.client.api;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.client.ClientResponseContext;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientResponseFilter;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;

public class ResponseObjectClientFilter implements ResteasyReactiveClientResponseFilter {

  @Override
  public void filter(final ResteasyReactiveClientRequestContext requestContext,
                     final ClientResponseContext responseContext) {
    SimpleClientAbstract.setMdcOpId((String) requestContext.getHeaders().getFirst(X_OP_ID));
    final var method = requestContext.getMethod();
    if (HttpMethod.GET.equalsIgnoreCase(method)) {
      final var object = new AccessorObject();
      if (responseContext.getHeaders() != null) {
        AccessorHeaderDtoConverter.objectFromMap(object, responseContext.getHeaders());
      }
      SimpleClientAbstract.setDtoFromHeaders(object);
    }
  }
}
