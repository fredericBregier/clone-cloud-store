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

import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.client.utils.AbstractResponseClientFilter;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.MultivaluedMap;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_NAME;

@ApplicationScoped
public class ResponseClientFilter extends AbstractResponseClientFilter<ApiBusinessOut> {

  @Override
  protected ApiBusinessOut getOutFromHeader(final ResteasyReactiveClientRequestContext requestContext,
                                            final ClientResponseContext responseContext,
                                            final MultivaluedMap<String, String> headers) {
    if (ParametersChecker.isEmpty(headers.getFirst(X_NAME))) {
      return null;
    }
    final var businessOut = new ApiBusinessOut();
    businessOut.name = headers.getFirst(X_NAME);
    var instant = headers.getFirst(X_CREATION_DATE);
    if (instant != null) {
      businessOut.creationDate = Instant.parse(instant);
    }
    var len = headers.getFirst(X_LEN);
    if (len != null) {
      businessOut.len = Long.parseLong(len);
    }
    return businessOut;
  }
}
