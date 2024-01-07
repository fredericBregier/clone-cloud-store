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

package io.clonecloudstore.replicator.server.remote;

import java.net.URI;

import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.server.remote.client.RemoteReplicatorApiClientFactory;
import io.clonecloudstore.replicator.server.test.fake.accessor.FakeAccessorTopicConsumer;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.replicator.server.test.conf.Constants.BUCKET_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OBJECT_PATH;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OP_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.SITE;
import static io.clonecloudstore.replicator.server.test.conf.Constants.SITE_REMOTE;
import static io.clonecloudstore.replicator.server.test.conf.Constants.URI_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(KafkaProfile.class)
class RemoteReplicatorEmitterTest {
  private static final Logger LOGGER = Logger.getLogger(RemoteReplicatorEmitterTest.class);
  @Inject
  RemoteReplicatorApiClientFactory remoteReplicatorApiClientFactory;

  @BeforeEach
  void beforeEach() {
    FakeDriverFactory.cleanUp();
  }

  @Test
  void testRemoteEmitter() throws InterruptedException {
    LOGGER.infof("Check ReplicateOrder");
    final var replicateOrder =
        new ReplicatorOrder(OP_ID, SITE, SITE_REMOTE, CLIENT_ID, BUCKET_ID, OBJECT_PATH, 0, "hash",
            ReplicatorConstants.Action.CREATE);
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      client.createOrder(replicateOrder).await().indefinitely();
      final var replicatorOrder2 = FakeAccessorTopicConsumer.replicatorOrders.take();
      assertEquals(replicateOrder, replicatorOrder2);
    }
    final var replicateOrder2 =
        new ReplicatorOrder(OP_ID, SITE, SITE_REMOTE, CLIENT_ID, BUCKET_ID, OBJECT_PATH, 0, "hash",
            ReplicatorConstants.Action.DELETE);
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      for (int i = 0; i < 10; i++) {
        final var result1 = client.createOrder(replicateOrder);
        final var result2 = client.createOrder(replicateOrder2);
        final var uni1 = result1.await();
        final var uni2 = result2.await();
        uni1.indefinitely();
        uni2.indefinitely();
      }
      for (int i = 0; i < 10; i++) {
        final var res1 = FakeAccessorTopicConsumer.replicatorOrders.take();
        final var res2 = FakeAccessorTopicConsumer.replicatorOrders.take();
        assertEquals(replicateOrder, res1);
        assertEquals(replicateOrder2, res2);
      }
    }
  }
}
