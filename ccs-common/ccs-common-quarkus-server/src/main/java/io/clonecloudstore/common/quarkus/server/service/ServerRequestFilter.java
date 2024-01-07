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

package io.clonecloudstore.common.quarkus.server.service;

import java.io.IOException;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

@Provider
@PreMatching
public class ServerRequestFilter implements ContainerRequestFilter {
  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    if (requestContext.getMethod().equalsIgnoreCase("POST")) {
      String override = requestContext.getHeaders().getFirst("X-HTTP-Method-Override");
      if (ParametersChecker.isNotEmpty(override)) {
        requestContext.setMethod(override);
      }
    }
    var xOpId = requestContext.getHeaders().containsKey(X_OP_ID) ? requestContext.getHeaderString(X_OP_ID) : "";
    if (ParametersChecker.isEmpty(xOpId)) {
      xOpId = GuidLike.getGuid();
      requestContext.getHeaders().putSingle(X_OP_ID, xOpId);
    }
    SimpleClientAbstract.setMdcOpId(xOpId);
  }
}
