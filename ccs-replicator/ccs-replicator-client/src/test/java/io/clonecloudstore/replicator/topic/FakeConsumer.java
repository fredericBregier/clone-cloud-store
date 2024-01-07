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

package io.clonecloudstore.replicator.topic;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class FakeConsumer {
  private static final Logger LOGGER = Logger.getLogger(FakeConsumer.class);
  static final CountDownLatch bucketCreate = new CountDownLatch(1);
  static final CountDownLatch bucketDelete = new CountDownLatch(1);
  static final CountDownLatch objectCreate = new CountDownLatch(1);
  static final CountDownLatch objectDelete = new CountDownLatch(1);

  @Incoming(ReplicatorConstants.Topic.REPLICATOR_REQUEST_IN)
  public void consume(final List<ReplicatorOrder> orders) {
    QuarkusProperties.refreshModuleMdc();
    for (final var order : orders) {
      SimpleClientAbstract.setMdcOpId(order.opId());
      LOGGER.infof("Recv %s", order);
      if (order.objectName() != null) {
        switch (order.action()) {
          case CREATE -> objectCreate.countDown();
          case DELETE -> objectDelete.countDown();
        }
      } else {
        switch (order.action()) {
          case CREATE -> bucketCreate.countDown();
          case DELETE -> bucketDelete.countDown();
        }
      }
    }
  }
}
