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

package io.clonecloudstore.accessor.replicator.test;

import io.clonecloudstore.replicator.model.ReplicatorOrder;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import static io.clonecloudstore.replicator.config.ReplicatorConstants.Topic.REPLICATOR_ACTION_OUT;

public class FakeReplicatorProducer {
  private final Emitter<ReplicatorOrder> emitter;

  public FakeReplicatorProducer(@Channel(REPLICATOR_ACTION_OUT) Emitter<ReplicatorOrder> emitter) {
    this.emitter = emitter;
  }

  public void send(final ReplicatorOrder order) {
    emitter.send(order);
  }
}
