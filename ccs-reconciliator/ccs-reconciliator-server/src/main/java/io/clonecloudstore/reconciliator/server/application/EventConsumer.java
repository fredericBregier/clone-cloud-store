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

package io.clonecloudstore.reconciliator.server.application;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.reconciliator.database.model.CentralReconciliationService;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.database.model.LocalReconciliationService;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import io.clonecloudstore.reconciliator.model.ReconciliationStep;
import io.clonecloudstore.reconciliator.server.topic.ReconciliationBrokerService;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClient;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClientFactory;
import io.quarkus.arc.Unremovable;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.jboss.logging.Logger;

@ApplicationScoped
@Unremovable
public class EventConsumer {
  private static final Logger LOGGER = Logger.getLogger(EventConsumer.class);
  private final ReconciliationBrokerService brokerService;
  private final DaoRequestRepository requestRepository;
  private final DaoNativeListingRepository nativeListingRepository;
  private final DaoSitesActionRepository sitesActionRepository;
  private final DaoSitesListingRepository sitesListingRepository;
  private final LocalReplicatorApiClientFactory replicatorApiClientFactory;
  private final LocalReconciliationService localReconciliationService;
  private final CentralReconciliationService centralReconciliationService;
  private final Set<String> activeRequests = new ConcurrentHashSet<>();

  protected EventConsumer(final Instance<DaoRequestRepository> requestRepositoryInstance,
                          final Instance<LocalReconciliationService> localReconciliationServiceInstance,
                          final Instance<CentralReconciliationService> centralReconciliationServiceInstance,
                          final Instance<DaoNativeListingRepository> nativeListingRepositoryInstance,
                          final Instance<DaoSitesActionRepository> sitesActionRepositoryInstance,
                          final Instance<DaoSitesListingRepository> sitesListingRepositoryInstance,
                          final LocalReplicatorApiClientFactory replicatorApiClientFactory,
                          final ReconciliationBrokerService brokerService) {
    requestRepository = requestRepositoryInstance.get();
    localReconciliationService = localReconciliationServiceInstance.get();
    centralReconciliationService = centralReconciliationServiceInstance.get();
    nativeListingRepository = nativeListingRepositoryInstance.get();
    sitesActionRepository = sitesActionRepositoryInstance.get();
    sitesListingRepository = sitesListingRepositoryInstance.get();
    this.replicatorApiClientFactory = replicatorApiClientFactory;
    this.brokerService = brokerService;
  }

  public boolean isActive(final String id) {
    return activeRequests.contains(id);
  }

  public String createNewRequest(final ReconciliationRequest request) throws CcsWithStatusException {
    String id = GuidLike.getGuid();
    try (final var client = replicatorApiClientFactory.newClient()) {
      var dao = requestRepository.createEmptyItem().fromDto(request);
      dao.setId(id).setCurrentSite(ServiceProperties.getAccessorSite()).setFromSite(ServiceProperties.getAccessorSite())
          .setStart(Instant.now()).setActions(0).setContextSitesDone(List.of()).setChecked(0).setCheckedDb(0)
          .setCheckedDriver(0).setCheckedRemote(0).setStop(null);
      activeRequests.add(id);
      // First save Request
      requestRepository.insert(dao);
      // Inform other sites
      LOGGER.infof("Start Request %s", dao);
      CcsWithStatusException exception = null;
      for (var remote : dao.getContextSites()) {
        if (!request.fromSite().equals(remote)) {
          dao.setCurrentSite(remote);
          exception = informRemoteReconciliator(remote, client, dao, exception);
        }
      }
      if (exception != null) {
        dao.setStop(Instant.now()).setCheckedDb(-1).setStep(ReconciliationStep.CREATE_ERROR);
        requestRepository.updateFull(dao);
        throw exception;
      }
      // Launch async Local computation
      dao.setCurrentSite(ServiceProperties.getAccessorSite());
      LOGGER.infof("Queue Local Request %s", dao);
      brokerService.sendLocalCompute(dao);
      return dao.getId();
    } catch (final CcsDbException e) {
      LOGGER.errorf("Error while creating new request in Central Reconciliator: (%s)", e);
      throw new CcsWithStatusException(null, 500, e);
    } finally {
      activeRequests.remove(id);
    }
  }

  private static CcsWithStatusException informRemoteReconciliator(final String remote,
                                                                  final LocalReplicatorApiClient client,
                                                                  final DaoRequest dao,
                                                                  CcsWithStatusException exception) {
    try {
      client.createRequestLocal(dao.getDto(), dao.getClientId(), remote, SimpleClientAbstract.getMdcOpId());
    } catch (CcsWithStatusException e) {
      exception = e;
      LOGGER.errorf("Error for %s while informing remote Reconciliator for Creation: %s (%s)", dao.getId(), remote, e);
    }
    return exception;
  }

  public void localReconciliation(final DaoRequest request) {
    activeRequests.add(request.getId());
    try {
      // Step 1
      LOGGER.infof("Start Step 1 %s", request);
      localReconciliationService.step1CleanUpObjectsNativeListings(request);
      // Step 2
      var daoPreviousIterator = requestRepository.findIterator(new DbQuery(RestQuery.CONJUNCTION.AND,
          new DbQuery(RestQuery.QUERY.EQ, DaoRequestRepository.BUCKET, request.getBucket()),
          new DbQuery(RestQuery.QUERY.EQ, DaoRequestRepository.CLIENTID, request.getClientId()),
          new DbQuery(RestQuery.QUERY.EQ, DaoRequestRepository.FROMSITE, request.getFromSite())));
      DaoRequest previous = null;
      while (daoPreviousIterator.hasNext()) {
        var newOne = daoPreviousIterator.next();
        if (previous == null) {
          previous = newOne;
        } else {
          if (previous.getStart().isBefore(newOne.getStart())) {
            nativeListingRepository.delete(
                new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.REQUESTID, previous.getId()));
            sitesActionRepository.delete(
                new DbQuery(RestQuery.QUERY.EQ, DaoSitesActionRepository.REQUESTID, previous.getId()));
            sitesListingRepository.delete(
                new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.REQUESTID, previous.getId()));
            requestRepository.deleteWithPk(previous.getId());
            previous = newOne;
          } else {
            nativeListingRepository.delete(
                new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.REQUESTID, newOne.getId()));
            sitesActionRepository.delete(
                new DbQuery(RestQuery.QUERY.EQ, DaoSitesActionRepository.REQUESTID, newOne.getId()));
            sitesListingRepository.delete(
                new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.REQUESTID, newOne.getId()));
            requestRepository.deleteWithPk(newOne.getId());
          }
        }
      }
      if (previous != null) {
        LOGGER.infof("Start Step 2 %s from %s", request, previous);
        localReconciliationService.step2ContinueFromPreviousRequest(previous.getId(), request, true);
      }
      // Step 3
      LOGGER.infof("Start Step 3 %s", request);
      localReconciliationService.step3SaveNativeListingDb(request);
      // Step 4
      LOGGER.infof("Start Step 4 %s", request);
      localReconciliationService.step4SaveNativeListingDriver(request);
      // Step 5
      LOGGER.infof("Start Step 5 %s", request);
      localReconciliationService.step5CompareNativeListingDbDriver(request);
      // Now inform back central
      LOGGER.infof("Final local step %s", request);
      var lastState = requestRepository.findWithPk(request.getId());
      lastState.setStep(ReconciliationStep.CENTRALIZE);
      requestRepository.update(DbQuery.idEquals(request.getId()),
          new DbUpdate().set(DaoRequestRepository.STEP, ReconciliationStep.CENTRALIZE));
      if (request.getCurrentSite().equals(request.getFromSite())) {
        tryFinalCentralReconciliation(request);
      } else {
        informCentralOfLocalEnding(lastState);
      }
      // Clean native listing
      localReconciliationService.cleanNativeListing(request);
    } catch (final CcsDbException e) {
      try {
        var lastState = requestRepository.findWithPk(request.getId());
        lastState.setStop(Instant.now()).setCheckedRemote(-1).setStep(ReconciliationStep.LOCAL_ERROR);
        requestRepository.updateFull(lastState);
      } catch (final CcsDbException ignore) {
        // Ignore
      }
      LOGGER.errorf("Error during local reconciliation %s (%s)", request.getId(), e);
    } finally {
      activeRequests.remove(request.getId());
    }
  }

  private void informCentralOfLocalEnding(final DaoRequest lastState) throws CcsDbException {
    try (final var client = replicatorApiClientFactory.newClient()) {
      client.endRequestLocal(lastState.getId(), lastState.getCurrentSite(), lastState.getClientId(),
          lastState.getFromSite(), SimpleClientAbstract.getMdcOpId());
    } catch (CcsWithStatusException e) {
      LOGGER.errorf("Error for %s while informing Central Reconciliator: %s (%s)", lastState.getId(),
          lastState.getFromSite(), e);
      lastState.setStop(Instant.now()).setCheckedRemote(-1).setStep(ReconciliationStep.LOCAL_ERROR);
      requestRepository.updateFull(lastState);
    }
  }

  public void localToCentralReconciliation(final DaoRequest request, final String remote) {
    try (var client = replicatorApiClientFactory.newClient()) {
      LOGGER.infof("Local To Central: %s from %s", request.getId(), remote);
      // Save remote native listing
      final var iterator =
          client.getSitesListing(request.getId(), request.getClientId(), remote, ClientAbstract.getMdcOpId());
      if (iterator == null) {
        LOGGER.errorf("Error for %s while Local to Central Reconciliator from %s (iterator is null)", request.getId(),
            request.getCurrentSite());
        return;
      }
      centralReconciliationService.saveRemoteNativeListing(request, iterator);
      // Try Final Central Reconciliation
      request.setCurrentSite(remote);
      tryFinalCentralReconciliation(request);
    } catch (final CcsDbException | CcsWithStatusException e) {
      LOGGER.errorf("Error for %s while Local to Central Reconciliator from %s (%s)", request.getId(),
          request.getCurrentSite(), e);
      try {
        request.setStep(ReconciliationStep.CENTRALIZE_ERROR);
        requestRepository.update(DbQuery.idEquals(request.getId()),
            new DbUpdate().set(DaoRequestRepository.STOP, Instant.now()).set(DaoRequestRepository.CHECKED_REMOTE, -1)
                .set(DaoRequestRepository.STEP, request.getStep()));
      } catch (final CcsDbException ex) {
        LOGGER.errorf("Cannot update error status for %s (%s)", request.getId(), ex);
      }
    }
  }

  private synchronized void tryFinalCentralReconciliation(final DaoRequest request) {
    try {
      // Check if end of all native Listing
      if (centralReconciliationService.updateRequestFromRemoteListing(request)) {
        // If end, compute Actions and count them
        centralReconciliationService.computeActions(request);
        // If DryRun stops here
        if (request.isDryRun()) {
          var existing = requestRepository.findWithPk(request.getId());
          LOGGER.infof("Reconciliation DryRun stops: %s", existing);
          existing.setStep(ReconciliationStep.DRYRUN);
          requestRepository.update(DbQuery.idEquals(existing.getId()),
              new DbUpdate().set(DaoRequestRepository.STEP, ReconciliationStep.DRYRUN));
          // FIXME push into Topic dry run is finished
        } else {
          // If not dryRun, then inform all remote site
          informAllSites(request);
        }
      }
    } catch (final CcsDbException e) {
      LOGGER.errorf("Error for %s while computing Central Reconciliator Actions (%s)", request.getId(), e);
      try {
        request.setStep(ReconciliationStep.COMPUTE_ERROR);
        requestRepository.update(DbQuery.idEquals(request.getId()),
            new DbUpdate().set(DaoRequestRepository.STOP, Instant.now()).set(DaoRequestRepository.ACTIONS, -1)
                .set(DaoRequestRepository.STEP, request.getStep()));
      } catch (final CcsDbException ex) {
        LOGGER.errorf("Cannot update error status for %s (%s)", request.getId(), ex);
      }
    }
  }

  private void informAllSites(final DaoRequest daoRequest) throws CcsDbException {
    try (final var client = replicatorApiClientFactory.newClient()) {
      for (final var remote : daoRequest.getContextSitesDone()) {
        if (!daoRequest.getFromSite().equals(remote) &&
            sitesActionRepository.count(ReconciliationConstants.subsetForOneSite(daoRequest, remote)) > 0) {
          try {
            client.endRequestCentral(daoRequest.getId(), daoRequest.getClientId(), remote,
                SimpleClientAbstract.getMdcOpId());
          } catch (final CcsWithStatusException e) {
            LOGGER.errorf("Error for %s while informing Local Reconciliator for Actions ready: %s (%s)",
                daoRequest.getId(), daoRequest.getFromSite(), e);
          }
        } else {
          // Local Reconciliation
          brokerService.sendLocalReconciliation(daoRequest);
        }
      }
    }
  }

  public void centralToLocalReconciliation(final DaoRequest request) {
    try (final var client = replicatorApiClientFactory.newClient()) {
      requestRepository.update(DbQuery.idEquals(request.getId()),
          new DbUpdate().set(DaoRequestRepository.STEP, ReconciliationStep.DISPATCH));
      // Clean local site listing ? FIXME add an option
      localReconciliationService.cleanSitesListing(request);
      // Save remote central action listing
      Iterator<ReconciliationSitesAction> iterator =
          client.getActionsListing(request.getId(), ServiceProperties.getAccessorSite(), request.getClientId(),
              request.getFromSite(), ClientAbstract.getMdcOpId());
      if (iterator == null) {
        LOGGER.errorf("Error for %s while Central to Local Reconciliator from %s (iterator is null)", request.getId(),
            request.getCurrentSite());
        return;
      }
      localReconciliationService.importActions(request, iterator);
      // Final Local Reconciliation
      // FIXME reload request
      brokerService.sendLocalReconciliation(request);
    } catch (final CcsDbException | CcsWithStatusException e) {
      LOGGER.errorf("Error for %s while getting Central Reconciliation Actions: %s (%s)", request.getId(),
          request.getFromSite(), e);
      request.setStop(Instant.now()).setCheckedRemote(-1).setStep(ReconciliationStep.DISPATCH_ERROR);
      try {
        requestRepository.updateFull(request);
      } catch (final CcsDbException ignore) {
        // Ignore
      }
    }
  }

  public void finalLocalReconciliation(final DaoRequest request) {
    try {
      // Launch reconciliation actions
      requestRepository.update(DbQuery.idEquals(request.getId()),
          new DbUpdate().set(DaoRequestRepository.STEP, ReconciliationStep.ACTION));
      localReconciliationService.applyActions(request);
      // Once all done, set as Request Ends
      // Count actions and clean
      // FIXME
      request.setStop(Instant.now()).setStep(ReconciliationStep.DONE);
      requestRepository.updateFull(request);
    } catch (final CcsDbException e) {
      LOGGER.errorf("Error for %s while resolving Local Reconciliation Actions: %s (%s)", request.getId(),
          request.getFromSite(), e);
      request.setStop(Instant.now()).setActions(-1).setStep(ReconciliationStep.ACTION_ERROR);
      try {
        requestRepository.updateFull(request);
      } catch (final CcsDbException ignore) {
        // Ignore
      }
    }
  }
}
