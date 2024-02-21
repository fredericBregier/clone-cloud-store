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

package io.clonecloudstore.reconciliator.server.resource;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.reconciliator.database.model.CentralReconciliationService;
import io.clonecloudstore.reconciliator.database.model.InitializationService;
import io.clonecloudstore.reconciliator.database.model.LocalReconciliationService;
import io.clonecloudstore.reconciliator.database.model.PurgeService;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;

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

@Path(AccessorConstants.Api.RECONCILIATOR_ROOT)
public class ReconciliatorResource {
  private static final Logger LOGGER = Logger.getLogger(ReconciliatorResource.class);
  private final PurgeService purgeService;
  private final LocalReconciliationService localReconciliationService;
  private final CentralReconciliationService centralReconciliationService;
  private final InitializationService initializationService;

  public ReconciliatorResource(final PurgeService purgeService,
                               final LocalReconciliationService localReconciliationService,
                               final CentralReconciliationService centralReconciliationService,
                               final InitializationService initializationService) {
    this.purgeService = purgeService;
    this.localReconciliationService = localReconciliationService;
    this.centralReconciliationService = centralReconciliationService;
    this.initializationService = initializationService;
  }
  // FIXME Implements, add headers, add support for Downloading InputStream

  /**
   * Initial creation of Reconciliation Request<p>
   * Probably returns the object with Id abd Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_CENTRAL + COLL_REQUESTS)
  @Path(COLL_CENTRAL + COLL_REQUESTS)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> createRequestCentral(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> em.complete(Response.accepted().build()));
  }

  /**
   * Once all is done for a Request, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_CENTRAL + COLL_REQUESTS)
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> getRequestStatus(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> em.complete(Response.ok().build()));
  }

  /**
   * Once all listing are done, Central will compute Actions and then inform through Replicator remote sites.
   * Each one will ask for their own local actions. Those will be saved locally as final actions.
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_CENTRAL + SUB_COLL_LISTING)
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Uni<Response> getActionsListing(@PathParam("idRequest") final String idRequest,
                                         @PathParam("remoteId") final String remoteId) {
    // Send back an inputstream
    return Uni.createFrom().emitter(em -> em.complete(Response.ok().build()));
  }

  /**
   * Local creation of Reconciliation Request from existing one in Central.
   * Will run all local steps (1 to 5). <p>
   * If request is with purge, will clean nativeListing
   * <p>
   * Probably returns with Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + COLL_REQUESTS)
  @Path(COLL_LOCAL + COLL_REQUESTS)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> createRequestLocal(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> em.complete(Response.accepted().build()));
  }

  /**
   * Once all is done for a Local Request, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + COLL_REQUESTS)
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> getLocalRequestStatus(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> em.complete(Response.ok().build()));
  }

  /**
   * Once Local sites listing is ready, inform through Replicator and topic event the Central,
   * then Central will request the listing from remote Local. Once all listing are done, Central will compute Actions.
   * <p>
   * If request is with purge, will clean sitesListing
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + SUB_COLL_LISTING)
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Uni<Response> getSitesListing(@PathParam("idRequest") final String idRequest) {
    // Send back an inputstream
    return Uni.createFrom().emitter(em -> em.complete(Response.ok().build()));
  }

  /**
   * Perform local purge actions.<p>
   * Probably returns an Id with Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_PURGE)
  @Path(COLL_PURGE)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> launchPurge(@HeaderParam(X_CLIENT_ID) final String clientId,
                                   @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds) {
    return Uni.createFrom().emitter(em -> em.complete(Response.accepted().build()));
  }

  /**
   * Once all is done for a Purge, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_PURGE)
  @Path(COLL_PURGE + "/{idPurge}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> getPurgeStatus(@PathParam("idPurge") final String idPurge) {
    return Uni.createFrom().emitter(em -> em.complete(Response.ok().build()));
  }

  /**
   * Perform local import actions. <p>
   * Probably returns an Id with Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_IMPORT)
  @Path(COLL_IMPORT + "/{bucket}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> launchImport(@PathParam("bucket") final String bucket,
                                    @HeaderParam(X_CLIENT_ID) final String clientId,
                                    @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds,
                                    @HeaderParam(X_OBJECT_METADATA) final String defaultMetadata) {
    return Uni.createFrom().emitter(em -> em.complete(Response.accepted().build()));
  }

  /**
   * Once all is done for an Import, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_IMPORT)
  @Path(COLL_IMPORT + "/{bucket}/{idImport}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> getImportStatus(@PathParam("bucket") final String bucket,
                                       @PathParam("idImport") final String idImport) {
    return Uni.createFrom().emitter(em -> em.complete(Response.ok().build()));
  }

  /**
   * Perform Sync actions for an "empty" remote Site. <p>
   * Probably returns an Id of Request with Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_SYNC)
  @Path(COLL_SYNC + "/{bucket}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> launchSync(@PathParam("bucket") final String bucket,
                                  @HeaderParam(X_CLIENT_ID) final String clientId,
                                  @HeaderParam(X_TARGET_ID) final long targetSite) {
    // FIXME missing Filter headers
    return Uni.createFrom().emitter(em -> em.complete(Response.accepted().build()));
  }
}
