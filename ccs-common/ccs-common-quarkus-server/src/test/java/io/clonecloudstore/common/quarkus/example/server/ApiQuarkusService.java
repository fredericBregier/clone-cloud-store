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

package io.clonecloudstore.common.quarkus.example.server;

import javax.crypto.Cipher;
import java.io.InputStream;
import java.time.Instant;

import io.clonecloudstore.common.quarkus.example.client.ApiConstants;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.NoCache;
import org.jboss.resteasy.reactive.RestHeader;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.API_COLLECTIONS;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.API_FULLROOT;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_NAME;

@Path(API_FULLROOT)
@NoCache
public class ApiQuarkusService extends StreamServiceAbstract<ApiBusinessIn, ApiBusinessOut, ServerStreamHandler> {
  public static final long LEN = 50 * 1024 * 1024;
  public static final String NOT_FOUND_NAME = "notFoundName";
  public static final String CONFLICT_NAME = "conflictName";
  public static final String NOT_ACCEPTABLE_NAME = "notAcceptableName";
  public static final String THROWABLE_NAME = "throwableName";
  public static final String PROXY_TEST = "PROXY_";
  public static final String PROXY_COMP_TEST = "PROXY_COMP_";
  public static final String ULTRA_COMPRESSION_TEST = "ULTRA_";
  public static final String DELAY_TEST = "DELAY_";
  public static final String CIPHER = "CIPHER";
  private static final Logger LOG = Logger.getLogger(ApiQuarkusService.class);
  public static final String THROUGH = "/through";
  public static Cipher cipherEnc;
  public static Cipher cipherDec;

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
  @Path(API_COLLECTIONS)
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
    var keepCompressed = name.startsWith(PROXY_TEST);
    return createObject(request, closer, businessIn, businessIn.len, null, keepCompressed, inputStream);
  }

  @Path(API_COLLECTIONS)
  @PUT
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createObjectUsingPut(final HttpServerRequest request, @Context final Closer closer,
                                            final InputStream inputStream,
                                            @DefaultValue("name") @RestHeader(X_NAME) final String name,
                                            @DefaultValue("0") @RestHeader(X_LEN) final long len) {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    var keepCompressed = name.startsWith(PROXY_TEST);
    return createObject(request, closer, businessIn, businessIn.len, null, keepCompressed, inputStream);
  }

  // REST API for receiving InputStream from client and sending it to server (proxy)
  @Path(API_COLLECTIONS + THROUGH)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createObjectThrough(final HttpServerRequest request, @Context final Closer closer,
                                           final InputStream inputStream,
                                           @DefaultValue("name") @RestHeader(X_NAME) final String name,
                                           @DefaultValue("0") @RestHeader(X_LEN) final long len) {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    // proxy to next API
    return createObject(request, closer, businessIn, businessIn.len, null, inputStream);
  }

  // REST API for sending InputStream back to client
  @Path(API_COLLECTIONS + "/{business}")
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
    var futureAlreadyCompressed = business.startsWith(PROXY_COMP_TEST);
    return readObject(request, closer, businessIn, false);
  }

  // REST API for sending InputStream back to client
  @Path(API_COLLECTIONS + "/{business}")
  @PUT
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> readObjectPut(@RestPath final String business,
                                     @DefaultValue("0") @RestHeader(X_LEN) final long len,
                                     final HttpServerRequest request, @Context final Closer closer) {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = business;
    businessIn.len = len > 0 ? len : LEN;
    var futureAlreadyCompressed = business.startsWith(PROXY_COMP_TEST);
    return readObject(request, closer, businessIn, false);
  }

  // REST API for sending InputStream back to client
  @Path(API_COLLECTIONS + THROUGH + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> readObjectThrough(@RestPath final String business,
                                         @DefaultValue("0") @RestHeader(X_LEN) final long len,
                                         final HttpServerRequest request, @Context final Closer closer) {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = business;
    businessIn.len = len > 0 ? len : LEN;
    return readObject(request, closer, businessIn, false);
  }

  // Example of REST API out of any InputStream usage but same URI (different accept through Produces annotation)
  @Path(ApiConstants.API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> getObjectMetadata(@RestPath final String business) {
    return Uni.createFrom().emitter(em -> {
      // Business code should come here
      switch (business) {
        case NOT_FOUND_NAME -> em.complete(createErrorResponse(new CcsNotExistException("Test of NotFound")));
        case CONFLICT_NAME -> em.complete(createErrorResponse(new CcsAlreadyExistException("Test of Conflict")));
        case NOT_ACCEPTABLE_NAME ->
            em.complete(createErrorResponse(new CcsNotAcceptableException("Test of NotAcceptable")));
        default -> {
          try {
            final var businessOut = new ApiBusinessOut();
            businessOut.name = business.startsWith(PROXY_TEST) ? business.substring(PROXY_TEST.length()) : business;
            businessOut.len = LEN;
            businessOut.creationDate = Instant.now();
            em.complete(Response.ok(businessOut).build());
          } catch (final CcsClientGenericException | CcsServerGenericException e) {
            em.complete(createErrorResponse(e));
          } catch (final Exception e) {
            LOG.error(e.getMessage());
            em.complete(createErrorResponse(e));
          }
        }
      }
    });
  }
}
