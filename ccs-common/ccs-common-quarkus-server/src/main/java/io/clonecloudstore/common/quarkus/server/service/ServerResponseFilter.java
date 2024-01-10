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
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.smallrye.mutiny.subscription.UniEmitter;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.jboss.resteasy.reactive.client.api.WebClientApplicationException;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_ERROR;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_MODULE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;

@Provider
public class ServerResponseFilter implements ContainerResponseFilter {
  @Override
  public void filter(final ContainerRequestContext containerRequestContext,
                     final ContainerResponseContext responseContext) throws IOException {
    if (!responseContext.getHeaders().containsKey(X_OP_ID)) {
      responseContext.getHeaders().putSingle(X_OP_ID, SimpleClientAbstract.getMdcOpId());
    }
    if (!responseContext.getHeaders().containsKey(X_MODULE)) {
      responseContext.getHeaders().putSingle(X_MODULE, QuarkusProperties.getCcsModule().name());
    }
  }

  public static void handleException(final UniEmitter<? super Response> em, final Exception e) {
    switch (e) {
      case final CcsClientGenericException cc:
        em.complete(Response.status(cc.getStatus()).header(X_ERROR, cc.getMessage()).build());
        break;
      case final CcsServerGenericException cs:
        em.complete(Response.status(cs.getStatus()).header(X_ERROR, cs.getMessage()).build());
        break;
      case final WebClientApplicationException wc:
        em.complete(wc.getResponse());
        break;
      default:
        em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).header(X_ERROR, e.getMessage()).build());
    }
  }

  public static void handleExceptionFail(final UniEmitter<?> em, final Exception e) {
    switch (e) {
      case final CcsClientGenericException cc:
        em.fail(cc);
        break;
      case final CcsServerGenericException cs:
        em.fail(cs);
        break;
      case final WebClientApplicationException wc:
        em.fail(wc);
        break;
      default:
        em.fail(new CcsOperationException(e.getMessage()));
    }
  }
}
