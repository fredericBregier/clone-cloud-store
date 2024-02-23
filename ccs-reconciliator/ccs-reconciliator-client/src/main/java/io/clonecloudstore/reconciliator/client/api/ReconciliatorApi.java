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

package io.clonecloudstore.reconciliator.client.api;

import java.io.Closeable;
import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.quarkus.rest.client.reactive.ComputedParamContext;
import io.quarkus.rest.client.reactive.NotBody;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.NoCache;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_CENTRAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_IMPORT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_LOCAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_PURGE;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_REQUESTS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_SYNC;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.SUB_COLL_LISTING;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_EXPIRED_SECONDS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TARGET_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_METADATA;
import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Path(AccessorConstants.Api.RECONCILIATOR_ROOT)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@NoCache
public interface ReconciliatorApi extends Closeable {
  @POST
  @Path(COLL_CENTRAL + COLL_REQUESTS)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createRequestCentral(final ReconciliationRequest request);

  @GET
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  Uni<ReconciliationRequest> getRequestStatus(@PathParam("idRequest") final String idRequest);

  @POST
  @Path(COLL_LOCAL + COLL_REQUESTS)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createRequestLocal(final ReconciliationRequest request);

  @PUT
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  Uni<Response> endRequestLocal(@PathParam("idRequest") final String idRequest,
                                @PathParam("remoteId") final String remoteId);

  @GET
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT, value = MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT_ENCODING, value = "{computeCompressionModel}", required = false)
  Uni<InputStream> getSitesListing(@NotBody final boolean acceptCompression,
                                   @PathParam("idRequest") final String idRequest);

  @PUT
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  Uni<Response> endRequestCentral(@PathParam("idRequest") final String idRequest);

  @GET
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT, value = MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT_ENCODING, value = "{computeCompressionModel}", required = false)
  Uni<InputStream> getActionsListing(@NotBody final boolean acceptCompression,
                                     @PathParam("idRequest") final String idRequest,
                                     @PathParam("remoteId") final String remoteId);

  @GET
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  Uni<ReconciliationRequest> getLocalRequestStatus(@PathParam("idRequest") final String idRequest);

  @POST
  @Path(COLL_PURGE)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> launchPurge(@HeaderParam(X_CLIENT_ID) final String clientId,
                            @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds);

  @GET
  @Path(COLL_PURGE + "/{idPurge}")
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Object> getPurgeStatus(@PathParam("idPurge") final String idPurge);

  @POST
  @Path(COLL_IMPORT + "/{bucket}")
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> launchImport(@PathParam("bucket") final String bucket, @HeaderParam(X_CLIENT_ID) final String clientId,
                             @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds,
                             @HeaderParam(X_OBJECT_METADATA) final String defaultMetadata);

  @GET
  @Path(COLL_IMPORT + "/{bucket}/{idImport}")
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Object> getImportStatus(@PathParam("bucket") final String bucket, @PathParam("idImport") final String idImport);

  @POST
  @Path(COLL_SYNC + "/{bucket}")
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> launchSync(@PathParam("bucket") final String bucket, @HeaderParam(X_CLIENT_ID) final String clientId,
                           @HeaderParam(X_TARGET_ID) final String targetSite);

  default String computeCompressionModel(ComputedParamContext context) {
    int argPos = 0;
    if ((boolean) context.methodParameters().get(argPos).value()) {
      return COMPRESSION_ZSTD;
    }
    return null;
  }
}
