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

package io.clonecloudstore.replicator.server.local;

import java.util.concurrent.TimeUnit;

import io.clonecloudstore.administration.client.TopologyApiClientFactory;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.server.test.fake.accessor.FakeAccessorTopicConsumer;
import io.clonecloudstore.replicator.server.test.fake.topology.FakeTopologyResource;
import io.clonecloudstore.replicator.topic.LocalBrokerService;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.replicator.server.test.conf.Constants.BUCKET_NAME;
import static io.clonecloudstore.replicator.server.test.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OBJECT_PATH;
import static io.clonecloudstore.replicator.server.test.conf.Constants.TOPOLOGY_NAME;
import static io.clonecloudstore.replicator.server.test.conf.Constants.URI_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusTest
@TestProfile(KafkaProfile.class)
class LocalReplicatorConsumerTest {
  private static final Logger LOGGER = Logger.getLogger(LocalReplicatorConsumerTest.class);
  @Inject
  LocalBrokerService localBrokerService;
  @Inject
  TopologyApiClientFactory topologyApiClientFactory;

  @BeforeAll
  static void beforeAll() {
    final var topology = new Topology(TOPOLOGY_NAME, TOPOLOGY_NAME, URI_SERVER, TopologyStatus.UP);
    FakeTopologyResource.topology = topology;
    FakeDriverFactory.cleanUp();
  }

  @Test
  void testRemoteEmitter() throws InterruptedException {
    LOGGER.infof("Check ReplicateOrder");
    localBrokerService.createBucket(BUCKET_NAME, CLIENT_ID);
    localBrokerService.createObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID, 120, "hash");

    var replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertNull(replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(0, replicatorOrder.size());
    assertNull(replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.CREATE, replicatorOrder.action());

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertEquals(OBJECT_PATH, replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(120, replicatorOrder.size());
    assertEquals("hash", replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.CREATE, replicatorOrder.action());

    localBrokerService.deleteObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID);
    localBrokerService.deleteBucket(BUCKET_NAME, CLIENT_ID);

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertEquals(OBJECT_PATH, replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(0, replicatorOrder.size());
    assertNull(replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.DELETE, replicatorOrder.action());

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertNull(replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(0, replicatorOrder.size());
    assertNull(replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.DELETE, replicatorOrder.action());

    localBrokerService.createBucket(BUCKET_NAME, CLIENT_ID);
    localBrokerService.createObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID, 120, "hash");
    localBrokerService.deleteObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID);
    localBrokerService.deleteBucket(BUCKET_NAME, CLIENT_ID);

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertNull(replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(0, replicatorOrder.size());
    assertNull(replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.CREATE, replicatorOrder.action());

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertEquals(OBJECT_PATH, replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(120, replicatorOrder.size());
    assertEquals("hash", replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.CREATE, replicatorOrder.action());

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertEquals(OBJECT_PATH, replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(0, replicatorOrder.size());
    assertNull(replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.DELETE, replicatorOrder.action());

    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.take();
    assertEquals(BUCKET_NAME, replicatorOrder.bucketName());
    assertNull(replicatorOrder.objectName());
    assertEquals(CLIENT_ID, replicatorOrder.clientId());
    assertEquals(0, replicatorOrder.size());
    assertNull(replicatorOrder.hash());
    assertEquals(ReplicatorConstants.Action.DELETE, replicatorOrder.action());

    // Now with error since no Topology available
    topologyApiClientFactory.clearCache();
    FakeTopologyResource.topology = null;
    localBrokerService.createBucket(BUCKET_NAME, CLIENT_ID);
    replicatorOrder = FakeAccessorTopicConsumer.replicatorOrders.poll(100, TimeUnit.MILLISECONDS);
    assertNull(replicatorOrder);
  }
}
