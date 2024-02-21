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

package io.clonecloudstore.common.quarkus.client.example.server;

import java.io.InputStream;
import java.time.Instant;

import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.BadRequestException;
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
import org.jboss.resteasy.reactive.RestHeader;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.API_COLLECTIONS;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.API_ROOT;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.BIG_LONG;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.MB10;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_NAME;

@Path(API_ROOT)
public class ApiService extends StreamServiceAbstract<ApiBusinessIn, ApiBusinessOut> {
  public static final long LEN = MB10;
  public static final String NOT_FOUND_NAME = "notFoundName";
  public static final String CONFLICT_NAME = "conflictName";
  public static final String NOT_ACCEPTABLE_NAME = "notAcceptableName";
  private static final Logger LOG = Logger.getLogger(ApiService.class);

  @Path(API_COLLECTIONS)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createObject(final HttpServerRequest request, @Context final Closer closer,
                                    @DefaultValue("name") @RestHeader(X_NAME) final String name,
                                    @DefaultValue("0") @RestHeader(X_LEN) final long len,
                                    final InputStream inputStream) {
    if ("INTERRUPT".equals(name)) {
      return Uni.createFrom().emitter(em -> {
        em.complete(createErrorResponse(new BadRequestException("Interrupted name")));
      });
    } else if (CONFLICT_NAME.equals(name)) {
      return Uni.createFrom().emitter(em -> {
        em.complete(createErrorResponse(new CcsAlreadyExistException("Conflict error")));
      });
    }
    ApiBusinessIn businessIn = new ApiBusinessIn();
    businessIn.len = len;
    businessIn.name = name;
    return createObject(request, closer, businessIn, len, null, true, inputStream);
  }

  @Path(API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> readObject(final HttpServerRequest request, @Context final Closer closer,
                                  @RestPath final String business,
                                  @DefaultValue("0") @RestHeader(X_LEN) final long len) {
    if ("INTERRUPT".equals(business)) {
      return Uni.createFrom().emitter(em -> {
        em.complete(createErrorResponse(new BadRequestException("Interrupted name")));
      });
    } else if (NOT_FOUND_NAME.equals(business)) {
      return Uni.createFrom().emitter(em -> {
        em.complete(createErrorResponse(new CcsNotExistException("Not exists error")));
      });
    }
    ApiBusinessIn businessIn = new ApiBusinessIn();
    businessIn.len = len;
    businessIn.name = business;
    return readObject(request, closer, businessIn, len, true);
  }

  @Path(API_COLLECTIONS + "/{business}")
  @HEAD
  public Uni<Response> checkName(final String business, @RestQuery("length") final long length,
                                 @RestHeader("x-length") final long xlength,
                                 @RestHeader("x-slength") final String xslength) {
    return Uni.createFrom().emitter(em -> {
      try {
        if (length != BIG_LONG) {
          LOG.error("Long as Query Parameter exceeds capacity: " + length);
        }
        if (xlength != BIG_LONG) {
          LOG.error("Long as Header Parameter exceeds capacity: " + xlength);
        }
        final var fromString = Long.parseLong(xslength);
        if (fromString != BIG_LONG) {
          LOG.error("Long as Header Parameter as String exceeds capacity: " + xslength);
        }
        LOG.info("Recv Long: " + length + " " + xlength + " " + xslength);
        em.complete(Response.noContent().header(X_NAME, business).header(X_LEN, Long.toString(BIG_LONG))
            .header(X_CREATION_DATE, Instant.now().toString()).build());
      } catch (final CcsClientGenericException | CcsServerGenericException e) {
        em.complete(createErrorResponse(e));
      } catch (final Exception e) {
        LOG.error(e.getMessage(), e);
        em.complete(createErrorResponse(e));
      }
    });
  }

  @Path(API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<ApiBusinessOut> getObjectMetadata(@RestPath final String business) {
    return Uni.createFrom().emitter(em -> {
      switch (business) {
        case NOT_FOUND_NAME -> em.fail(new CcsNotExistException("Test of NotFound"));
        case CONFLICT_NAME -> em.fail(new CcsAlreadyExistException("Test of Conflict"));
        case NOT_ACCEPTABLE_NAME -> em.fail(new CcsNotAcceptableException("Test of NotAcceptable"));
        default -> {
          try {
            final var businessOut = new ApiBusinessOut();
            businessOut.name = business;
            businessOut.len = LEN;
            businessOut.creationDate = Instant.now();
            em.complete(businessOut);
          } catch (final CcsClientGenericException | CcsServerGenericException e) {
            throw e;
          } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            throw new CcsOperationException(e);
          }
        }
      }
    });
  }

  @Override
  protected ServerNativeStreamHandler createNativeStreamHandler() {
    return new ServerNativeStreamHandler();
  }
}
