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

import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.LocalRequest;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_LOCAL;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_READY_FOR_CENTRAL;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_READY_FOR_LOCAL;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.REQUEST_RECONCILIATION;
import static io.clonecloudstore.reconciliator.server.application.ReconciliationConstants.WAY_OUT;

@ApplicationScoped
@Unremovable
public class ReconciliationBrokerService {
  private final Emitter<String> localEmitter;
  private final Emitter<LocalRequest> localReadyEmitter;
  private final Emitter<String> centralReadyEmitter;
  private final Emitter<String> localReconciliationEmitter;

  public ReconciliationBrokerService(@Channel(REQUEST_LOCAL + WAY_OUT) final Emitter<String> localEmitter, @Channel(
      REQUEST_READY_FOR_CENTRAL + WAY_OUT) final Emitter<LocalRequest> localReadyEmitter, @Channel(
      REQUEST_READY_FOR_LOCAL + WAY_OUT) final Emitter<String> centralReadyEmitter, @Channel(
      REQUEST_RECONCILIATION + WAY_OUT) final Emitter<String> localReconciliationEmitter) {
    this.localEmitter = localEmitter;
    this.localReadyEmitter = localReadyEmitter;
    this.centralReadyEmitter = centralReadyEmitter;
    this.localReconciliationEmitter = localReconciliationEmitter;
  }

  public void sendLocalCompute(final DaoRequest request) {
    localEmitter.send(request.getId());
  }

  public void sendLocalToCentral(final String requestId, final String remoteId) {
    localReadyEmitter.send(new LocalRequest(requestId, remoteId));
  }

  public void sendCentralToLocal(final DaoRequest request) {
    centralReadyEmitter.send(request.getId());
  }

  public void sendLocalReconciliation(final DaoRequest request) {
    localReconciliationEmitter.send(request.getId());
  }
}
