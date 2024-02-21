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

package io.clonecloudstore.replicator.server.test.fake.accessor;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class FakeAccessorTopicConsumer {
  public static final LinkedBlockingQueue<ReplicatorOrder> replicatorOrders = new LinkedBlockingQueue<>();
  private static final Logger LOGGER = Logger.getLogger(FakeAccessorTopicConsumer.class);

  @Incoming(ReplicatorConstants.Topic.REPLICATOR_ACTION_IN)
  @Blocking(ordered = true)
  public void consume(List<ReplicatorOrder> list) {
    QuarkusProperties.refreshModuleMdc();
    for (final var replicatorOrder : list) {
      SimpleClientAbstract.setMdcOpId(replicatorOrder.opId());
      LOGGER.debugf("Recv %s", replicatorOrder);
      replicatorOrders.add(replicatorOrder);
    }
  }
}
