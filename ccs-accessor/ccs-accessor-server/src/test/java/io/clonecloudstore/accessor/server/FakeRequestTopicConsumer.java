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

package io.clonecloudstore.accessor.server;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class FakeRequestTopicConsumer {
  private static final Logger LOGGER = Logger.getLogger(FakeRequestTopicConsumer.class);
  static final AtomicLong bucketCreate = new AtomicLong(0);
  static final AtomicLong bucketDelete = new AtomicLong(0);
  static final AtomicLong objectCreate = new AtomicLong(0);
  static final AtomicLong objectDelete = new AtomicLong(0);

  @Incoming(ReplicatorConstants.Topic.REPLICATOR_REQUEST_IN)
  @Blocking(ordered = true)
  public void consume(final List<ReplicatorOrder> orders) {
    QuarkusProperties.refreshModuleMdc();
    for (final var order : orders) {
      SimpleClientAbstract.setMdcOpId(order.opId());
      LOGGER.infof("Recv %s", order);
      if (order.objectName() != null) {
        switch (order.action()) {
          case CREATE -> objectCreate.incrementAndGet();
          case DELETE -> objectDelete.incrementAndGet();
        }
      } else {
        switch (order.action()) {
          case CREATE -> bucketCreate.incrementAndGet();
          case DELETE -> bucketDelete.incrementAndGet();
        }
      }
    }
  }

  public static void reset() {
    bucketCreate.set(0);
    bucketDelete.set(0);
    objectCreate.set(0);
    objectDelete.set(0);
  }

  public static long getBucketCreate() {
    return bucketCreate.get();
  }

  public static long getBucketDelete() {
    return bucketDelete.get();
  }

  public static long getObjectCreate() {
    return objectCreate.get();
  }

  public static long getObjectDelete() {
    return objectDelete.get();
  }
}
