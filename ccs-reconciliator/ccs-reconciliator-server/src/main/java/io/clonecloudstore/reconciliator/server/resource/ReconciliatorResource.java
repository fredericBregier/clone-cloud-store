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

import java.time.Instant;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.reconciliator.database.model.CentralReconciliationService;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesAction;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.database.model.InitializationService;
import io.clonecloudstore.reconciliator.database.model.LocalReconciliationService;
import io.clonecloudstore.reconciliator.database.model.PurgeService;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.server.application.EventConsumer;
import io.clonecloudstore.reconciliator.server.topic.ReconciliationBrokerService;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
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
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_REQUEST_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TARGET_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_METADATA;

@Path(AccessorConstants.Api.RECONCILIATOR_ROOT)
public class ReconciliatorResource {
  private static final Logger LOGGER = Logger.getLogger(ReconciliatorResource.class);
  private final ReconciliationBrokerService brokerService;
  private final EventConsumer eventConsumer;
  private final DaoRequestRepository requestRepository;
  private final PurgeService purgeService;
  private final LocalReconciliationService localReconciliationService;
  private final CentralReconciliationService centralReconciliationService;
  private final InitializationService initializationService;

  public ReconciliatorResource(final Instance<DaoRequestRepository> requestRepositoryInstance,
                               final PurgeService purgeService,
                               final LocalReconciliationService localReconciliationService,
                               final CentralReconciliationService centralReconciliationService,
                               final InitializationService initializationService,
                               final ReconciliationBrokerService brokerService, final EventConsumer eventConsumer) {
    requestRepository = requestRepositoryInstance.get();
    this.purgeService = purgeService;
    this.localReconciliationService = localReconciliationService;
    this.centralReconciliationService = centralReconciliationService;
    this.initializationService = initializationService;
    this.brokerService = brokerService;
    this.eventConsumer = eventConsumer;
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
  @Blocking
  public Uni<Response> createRequestCentral(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> {
      try {
        final var id = eventConsumer.createNewRequest(request);
        em.complete(Response.accepted().header(X_REQUEST_ID, id).build());
      } catch (final CcsWithStatusException e) {
        em.fail(e);
      }
    });
  }

  /**
   * Once all is done for a Request, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_CENTRAL + COLL_REQUESTS)
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> getRequestStatus(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      try {
        var dao = requestRepository.findWithPk(idRequest);
        if (dao != null) {
          if (eventConsumer.isActive(idRequest)) {
            LOGGER.infof("Still in progress: %s", idRequest);
            em.complete(Response.status(Response.Status.PARTIAL_CONTENT).entity(dao.getDto()).build());
          } else {
            em.complete(Response.status(Response.Status.OK).entity(dao.getDto()).build());
          }
          return;
        }
        em.fail(new CcsNotExistException(idRequest));
      } catch (final CcsDbException e) {
        em.fail(new CcsOperationException(e));
      }
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
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + COLL_REQUESTS)
  @Path(COLL_LOCAL + COLL_REQUESTS)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createRequestLocal(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> {
      var dao = requestRepository.createEmptyItem().fromDto(request);
      dao.setCurrentSite(ServiceProperties.getAccessorSite()).setStart(Instant.now());
      try {
        requestRepository.insert(dao);
      } catch (CcsDbException e) {
        em.fail(new CcsOperationException(e));
        return;
      }
      brokerService.sendLocalCompute(dao);
      em.complete(Response.accepted().header(X_REQUEST_ID, dao.getId()).build());
    });
  }

  /**
   * Once Local sites listing is ready, inform through Replicator the Central of the local process end.
   */
  @PUT
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_CENTRAL + SUB_COLL_LISTING)
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Blocking
  public Uni<Response> endRequestLocal(@PathParam("idRequest") final String idRequest,
                                       @PathParam("remoteId") final String remoteId) {
    return Uni.createFrom().emitter(em -> {
      try {
        var dao = requestRepository.findWithPk(idRequest);
        if (dao == null) {
          em.fail(new CcsNotExistException(idRequest));
          return;
        }
        brokerService.sendLocalToCentral(idRequest, remoteId);
        em.complete(Response.ok().header(X_REQUEST_ID, idRequest).build());
      } catch (CcsDbException e) {
        em.fail(new CcsOperationException(e));
      }
    });
  }

  /**
   * Once Local sites listing is ready, and it has informed through Replicator the Central,
   * then Central will request the listing from remote Local. Once all listing are done, Central will compute Actions.
   * <p>
   * If request is with purge, will clean sitesListing
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + SUB_COLL_LISTING)
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> getSitesListing(@PathParam("idRequest") final String idRequest, @Context Closer closer) {
    // Send back an inputstream
    // FIXME as for listing, use compression
    return Uni.createFrom().emitter(em -> {
      try {
        var dao = requestRepository.findWithPk(idRequest);
        if (dao == null) {
          em.fail(new CcsNotExistException(idRequest));
          return;
        }
        final var iterator = localReconciliationService.getSiteListing(dao);
        final var inputStream =
            StreamIteratorUtils.getInputStreamFromIterator(iterator, source -> ((DaoSitesListing) source).getDto(),
                ReconciliationSitesListing.class);
        closer.add(inputStream);
        em.complete(Response.ok().entity(inputStream).build());
      } catch (CcsDbException e) {
        em.fail(new CcsOperationException(e));
      }
    });
  }

  /**
   * Once Central action listing is ready, inform through Replicator the Local remote of the process end.
   */
  @PUT
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + SUB_COLL_LISTING)
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Blocking
  public Uni<Response> endRequestCentral(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      try {
        var dao = requestRepository.findWithPk(idRequest);
        if (dao == null) {
          em.fail(new CcsNotExistException(idRequest));
          return;
        }
        brokerService.sendCentralToLocal(dao);
        em.complete(Response.ok().header(X_REQUEST_ID, idRequest).build());
      } catch (CcsDbException e) {
        em.fail(new CcsOperationException(e));
      }
    });
  }

  /**
   * Once all listing are done, Central will compute Actions and then inform through Replicator remote sites.
   * Each one will ask for their own local actions. Those will be saved locally as final actions.
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_CENTRAL + SUB_COLL_LISTING)
  @Path(COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> getActionsListing(@PathParam("idRequest") final String idRequest,
                                         @PathParam("remoteId") final String remoteId, @Context Closer closer) {
    // Send back an inputstream
    // FIXME as for listing, use compression
    return Uni.createFrom().emitter(em -> {
      try {
        var dao = requestRepository.findWithPk(idRequest);
        if (dao == null) {
          em.fail(new CcsNotExistException(idRequest));
          return;
        }
        final var iterator = centralReconciliationService.getSitesActon(dao, remoteId);
        final var inputStream =
            StreamIteratorUtils.getInputStreamFromIterator(iterator, source -> ((DaoSitesAction) source).getDto(),
                ReconciliationSitesAction.class);
        closer.add(inputStream);
        em.complete(Response.ok().entity(inputStream).build());
      } catch (CcsDbException e) {
        em.fail(new CcsOperationException(e));
      }
    });
  }

  /**
   * Once all is done for a Local Request, can have its full status (statistic or whatever).
   * May return not finished (206 Partial Content)
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_LOCAL + COLL_REQUESTS)
  @Path(COLL_LOCAL + COLL_REQUESTS + "/{idRequest}")
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> getLocalRequestStatus(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      try {
        var dao = requestRepository.findWithPk(idRequest);
        if (dao != null) {
          if (eventConsumer.isActive(idRequest)) {
            LOGGER.infof("Still in progress: %s", idRequest);
            em.complete(Response.status(Response.Status.PARTIAL_CONTENT).entity(dao.getDto()).build());
          } else {
            em.complete(Response.status(Response.Status.OK).entity(dao.getDto()).build());
          }
          return;
        }
        em.fail(new CcsNotExistException(idRequest));
      } catch (final CcsDbException e) {
        em.fail(new CcsOperationException(e));
      }
    });
  }

  /**
   * Perform local purge actions.<p>
   * Probably returns an Id with Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_RECONCILIATOR + COLL_PURGE)
  @Path(COLL_PURGE)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> launchPurge(@HeaderParam(X_CLIENT_ID) final String clientId,
                                   @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds) {
    // FIXME TODO
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
  @Blocking
  public Uni<Response> getPurgeStatus(@PathParam("idPurge") final String idPurge) {
    // FIXME TODO
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
  @Blocking
  public Uni<Response> launchImport(@PathParam("bucket") final String bucket,
                                    @HeaderParam(X_CLIENT_ID) final String clientId,
                                    @HeaderParam(X_EXPIRED_SECONDS) final long expiredInSeconds,
                                    @HeaderParam(X_OBJECT_METADATA) final String defaultMetadata) {
    // FIXME TODO
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
  @Blocking
  public Uni<Response> getImportStatus(@PathParam("bucket") final String bucket,
                                       @PathParam("idImport") final String idImport) {
    // FIXME TODO
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
  @Blocking
  public Uni<Response> launchSync(@PathParam("bucket") final String bucket,
                                  @HeaderParam(X_CLIENT_ID) final String clientId,
                                  @HeaderParam(X_TARGET_ID) final long targetSite) {
    // FIXME TODO
    // FIXME missing Filter headers
    return Uni.createFrom().emitter(em -> em.complete(Response.accepted().build()));
  }
}
