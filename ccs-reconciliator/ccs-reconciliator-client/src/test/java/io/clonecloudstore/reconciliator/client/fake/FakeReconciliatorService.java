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

package io.clonecloudstore.reconciliator.client.fake;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.reconciliator.model.ReconciliationAction;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.smallrye.common.annotation.Blocking;
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

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_CENTRAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_IMPORT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_LOCAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_PURGE;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_REQUESTS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_SYNC;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.SUB_COLL_LISTING;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_EXPIRED_SECONDS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_REQUEST_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TARGET_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_METADATA;

@Path(AccessorConstants.Api.RECONCILIATOR_ROOT)
public class FakeReconciliatorService
    extends StreamServiceAbstract<ReconciliationRequest, ReconciliationRequest, FakeStreamHandler> {
  public static int errorCode = 0;
  public static long length = 0;

  /**
   * Initial creation of Reconciliation Request<p>
   * Probably returns the object with Id abd Accepted status
   */
  @POST
  @Path(COLL_CENTRAL + COLL_REQUESTS)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createRequestCentral(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.accepted().header(X_REQUEST_ID, id).build());
    });
  }

  /**
   * Once all is done for a Request, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<ReconciliationRequest> getRequestStatus(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      em.complete(new ReconciliationRequest(idRequest, "clientId", "bucket", null, ServiceProperties.getAccessorSite(),
          ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), "site2"), false));
    });
  }

  /**
   * Local creation of Reconciliation Request from existing one in Central.
   * Will run all local steps (1 to 5). <p>
   * If request is with purge, will clean nativeListing
   * <p>
   * Probably returns with Accepted status
   */
  @POST
  @Path(COLL_LOCAL + COLL_REQUESTS)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createRequestLocal(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.accepted().header(X_REQUEST_ID, id).build());
    });
  }

  /**
   * Once Local sites listing is ready, inform through Replicator the Central of the local process end.
   */
  @PUT
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Blocking
  public Uni<Response> endRequestLocal(@PathParam("idRequest") final String idRequest,
                                       @PathParam("remoteId") final String remoteId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.ok().header(X_REQUEST_ID, id).build());
    });
  }

  /**
   * Once Local sites listing is ready, and it has informed through Replicator the Central,
   * then Central will request the listing from remote Local. Once all listing are done, Central will compute Actions.
   * <p>
   * If request is with purge, will clean sitesListing
   */
  @GET
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<InputStream> getSitesListing(@PathParam("idRequest") final String idRequest) {
    // Send back an inputstream
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode);
      }
      final var siteListing = new ReconciliationSitesListing(GuidLike.getGuid(), idRequest, "bucket", "name",
          List.of(new SingleSiteObject("site", ReconciliationAction.UPLOAD_ACTION.getStatus(), Instant.now())));
      try {
        final var json = JsonUtil.getInstance().writeValueAsBytes(siteListing);
        final var byteArrayInputStream = new ByteArrayInputStream(json);
        em.complete(byteArrayInputStream);
      } catch (final JsonProcessingException e) {
        throw new CcsOperationException(e);
      }
    });
  }

  /**
   * Once Central action listing is ready, inform through Replicator the Local remote of the process end.
   */
  @PUT
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Blocking
  public Uni<Response> endRequestCentral(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.ok().header(X_REQUEST_ID, id).build());
    });
  }

  /**
   * Once all listing are done, Central will compute Actions and then inform through Replicator remote sites.
   * Each one will ask for their own local actions. Those will be saved locally as final actions.
   */
  @GET
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<InputStream> getActionsListing(@PathParam("idRequest") final String idRequest,
                                            @PathParam("remoteId") final String remoteId) {
    // Send back an inputstream
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode);
      }
      final var request = new ReconciliationSitesAction(GuidLike.getGuid(), idRequest, "bucket", "nameObject",
          ReconciliationAction.UPLOAD_ACTION.getStatus(), List.of(ServiceProperties.getAccessorSite()),
          List.of(ServiceProperties.getAccessorSite(), remoteId));
      try {
        final var json = JsonUtil.getInstance().writeValueAsBytes(request);
        final var byteArrayInputStream = new ByteArrayInputStream(json);
        em.complete(byteArrayInputStream);
      } catch (final JsonProcessingException e) {
        throw new CcsOperationException(e);
      }
    });
  }

  /**
   * Once all is done for a Local Request, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<ReconciliationRequest> getLocalRequestStatus(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      em.complete(new ReconciliationRequest(idRequest, "clientId", "bucket", null, ServiceProperties.getAccessorSite(),
          ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), "site2"), false));
    });
  }

  /**
   * Perform local purge actions.<p>
   * Probably returns an Id with Accepted status
   */
  @POST
  @Path(COLL_PURGE)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> launchPurge(@HeaderParam(X_CLIENT_ID) final String clientId,
                                   @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.accepted().header(X_REQUEST_ID, id).build());
    });
  }

  /**
   * Once all is done for a Purge, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Path(COLL_PURGE + "/{idPurge}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Object> getPurgeStatus(@PathParam("idPurge") final String idPurge) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      em.complete(new ReconciliationRequest(idPurge, "clientId", "bucket", null, ServiceProperties.getAccessorSite(),
          ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), "site2"), false));
    });
  }

  /**
   * Perform local import actions. <p>
   * Probably returns an Id with Accepted status
   */
  @POST
  @Path(COLL_IMPORT + "/{bucket}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> launchImport(@PathParam("bucket") final String bucket,
                                    @HeaderParam(X_CLIENT_ID) final String clientId,
                                    @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds,
                                    @HeaderParam(X_OBJECT_METADATA) final String defaultMetadata) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.accepted().header(X_REQUEST_ID, id).build());
    });
  }

  /**
   * Once all is done for an Import, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Path(COLL_IMPORT + "/{bucket}/{idImport}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Object> getImportStatus(@PathParam("bucket") final String bucket,
                                     @PathParam("idImport") final String idImport) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      em.complete(new ReconciliationRequest(idImport, "clientId", bucket, null, ServiceProperties.getAccessorSite(),
          ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), "site2"), false));
    });
  }

  /**
   * Perform Sync actions for an "empty" remote Site. <p>
   * Probably returns an Id of Request with Accepted status
   */
  @POST
  @Path(COLL_SYNC + "/{bucket}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> launchSync(@PathParam("bucket") final String bucket,
                                  @HeaderParam(X_CLIENT_ID) final String clientId,
                                  @HeaderParam(X_TARGET_ID) final String targetSite) {
    // FIXME missing Filter headers
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        return;
      }
      final var id = GuidLike.getGuid();
      em.complete(Response.accepted().header(X_REQUEST_ID, id).build());
    });
  }
}
