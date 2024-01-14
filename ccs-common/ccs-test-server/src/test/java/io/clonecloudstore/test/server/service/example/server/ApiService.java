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

package io.clonecloudstore.test.server.service.example.server;

import java.io.InputStream;
import java.time.Instant;

import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.test.server.service.example.client.ApiConstants;
import io.clonecloudstore.test.server.service.example.model.ApiBusinessIn;
import io.clonecloudstore.test.server.service.example.model.ApiBusinessOut;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.NoCache;
import org.jboss.resteasy.reactive.RestHeader;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.test.server.service.example.client.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.test.server.service.example.client.ApiConstants.X_LEN;
import static io.clonecloudstore.test.server.service.example.client.ApiConstants.X_NAME;

@Path(ApiConstants.API_ROOT)
@NoCache
public class ApiService extends StreamServiceAbstract<ApiBusinessIn, ApiBusinessOut, ServerNativeStreamHandler> {
  public static final long LEN = 50 * 1024 * 1024;
  private static final Logger LOG = Logger.getLogger(ApiService.class);

  // Example of REST API out of any InputStream usage
  @Path(ApiConstants.API_COLLECTIONS + "/{name}")
  @HEAD
  public Uni<Response> checkName(final String name) {
    return Uni.createFrom().emitter(em -> {
      // Business code should come here
      try {
        em.complete(Response.noContent().header(X_NAME, name).header(X_LEN, Long.toString(LEN))
            .header(X_CREATION_DATE, Instant.now().toString()).build());
      } catch (final CcsClientGenericException | CcsServerGenericException e) {
        em.complete(createErrorResponse(e));
      } catch (final Exception e) {
        LOG.error(e.getMessage(), e);
        em.complete(createErrorResponse(e));
      }
    });
  }

  // REST API for receiving InputStream from client
  @Path(ApiConstants.API_COLLECTIONS)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createObject(final HttpServerRequest request, @Context final Closer closer,
                                    final InputStream inputStream,
                                    @DefaultValue("name") @RestHeader(X_NAME) final String name,
                                    @DefaultValue("0") @RestHeader(X_LEN) final long len) {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    return createObject(request, closer, businessIn, businessIn.len, null, inputStream);
  }

  // REST API for sending InputStream back to client
  @Path(ApiConstants.API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> readObject(@RestPath final String business, @DefaultValue("0") @RestHeader(X_LEN) final long len,
                                  final HttpServerRequest request, @Context final Closer closer) {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = business;
    // Fake LEN
    businessIn.len = len > 0 ? len : LEN;
    return readObject(request, closer, businessIn);
  }

  // Example of REST API out of any InputStream usage but same URI (different accept through Produces annotation)
  @Path(ApiConstants.API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> getObjectMetadata(@RestPath final String business) {
    return Uni.createFrom().emitter(em -> {
      // Business code should come here
      try {
        final var businessOut = new ApiBusinessOut();
        businessOut.name = business;
        businessOut.len = LEN;
        businessOut.creationDate = Instant.now();
        em.complete(Response.ok(businessOut).build());
      } catch (final CcsClientGenericException | CcsServerGenericException e) {
        em.complete(createErrorResponse(e));
      } catch (final Exception e) {
        LOG.error(e.getMessage());
        em.complete(createErrorResponse(e));
      }
    });
  }
}
