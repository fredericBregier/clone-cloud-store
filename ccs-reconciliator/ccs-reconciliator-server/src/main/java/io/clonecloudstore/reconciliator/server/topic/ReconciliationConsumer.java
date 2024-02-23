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

package io.clonecloudstore.reconciliator.server.topic;

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.LocalRequest;
import io.clonecloudstore.reconciliator.model.ReconciliationStep;
import io.clonecloudstore.reconciliator.server.application.EventConsumer;
import io.quarkus.arc.Unremovable;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_LOCAL;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_READY_FOR_CENTRAL;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_READY_FOR_LOCAL;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_RECONCILIATION;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.WAY_IN;

@ApplicationScoped
@Unremovable
public class ReconciliationConsumer {
  private static final Logger LOGGER = Logger.getLogger(ReconciliationConsumer.class);
  private static final String NOT_FOUND = "Not found %d (%s)";
  private final EventConsumer eventConsumer;
  private final DaoRequestRepository requestRepository;

  public ReconciliationConsumer(final EventConsumer eventConsumer,
                                final Instance<DaoRequestRepository> requestRepositoryInstance) {
    this.eventConsumer = eventConsumer;
    this.requestRepository = requestRepositoryInstance.get();
  }

  @Incoming(REQUEST_LOCAL + WAY_IN)
  @Blocking
  public void consumeLocal(final String requestId) {
    QuarkusProperties.refreshModuleMdc();
    try {
      var request = requestRepository.findWithPk(requestId);
      request.setStep(ReconciliationStep.LOCAL);
      requestRepository.update(DbQuery.idEquals(requestId),
          new DbUpdate().set(DaoRequestRepository.STEP, ReconciliationStep.LOCAL));
      LOGGER.infof("Launch Local for %s", request);
      eventConsumer.localReconciliation(request);
    } catch (final CcsDbException e) {
      LOGGER.warnf(NOT_FOUND, requestId, e);
    }
  }

  @Incoming(REQUEST_READY_FOR_CENTRAL + WAY_IN)
  @Blocking
  public void consumeReadyLocal(final LocalRequest localRequest) {
    QuarkusProperties.refreshModuleMdc();
    try {
      var request = requestRepository.findWithPk(localRequest.requestId());
      LOGGER.infof("Launch Local To Central for %s", request);
      eventConsumer.localToCentralReconciliation(request, localRequest.remoteId());
    } catch (final CcsDbException e) {
      LOGGER.warnf(NOT_FOUND, localRequest.requestId(), e);
    }
  }

  @Incoming(REQUEST_READY_FOR_LOCAL + WAY_IN)
  @Blocking
  public void consumeReadyCentral(final String requestId) {
    QuarkusProperties.refreshModuleMdc();
    try {
      var request = requestRepository.findWithPk(requestId);
      LOGGER.infof("Launch Central to Local for %s", request);
      eventConsumer.centralToLocalReconciliation(request);
    } catch (final CcsDbException e) {
      LOGGER.warnf(NOT_FOUND, requestId, e);
    }
  }

  @Incoming(REQUEST_RECONCILIATION + WAY_IN)
  @Blocking
  public void consumeReconciliation(final String requestId) {
    QuarkusProperties.refreshModuleMdc();
    try {
      var request = requestRepository.findWithPk(requestId);
      LOGGER.infof("Launch Central to Local for %s", request);
      eventConsumer.finalLocalReconciliation(request);
    } catch (final CcsDbException e) {
      LOGGER.warnf(NOT_FOUND, requestId, e);
    }
  }
}
