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

package io.clonecloudstore.replicator.server.remote.resource;

import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class RemoteReplicatorOrderEmitter {
  private final Emitter<ReplicatorOrder> replicatorOrderEmitter;

  public RemoteReplicatorOrderEmitter(
      @Channel(ReplicatorConstants.Topic.REPLICATOR_ACTION_OUT) Emitter<ReplicatorOrder> replicatorOrderEmitter) {
    this.replicatorOrderEmitter = replicatorOrderEmitter;
  }

  public void generate(final ReplicatorOrder replicatorOrder) {
    replicatorOrderEmitter.send(replicatorOrder);
  }
}
