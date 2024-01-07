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

import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.replicator.test.conf.Constants.BUCKET_NAME;
import static io.clonecloudstore.replicator.test.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.replicator.test.conf.Constants.OBJECT_PATH;
import static io.clonecloudstore.replicator.topic.FakeConsumer.bucketCreate;
import static io.clonecloudstore.replicator.topic.FakeConsumer.bucketDelete;
import static io.clonecloudstore.replicator.topic.FakeConsumer.objectCreate;
import static io.clonecloudstore.replicator.topic.FakeConsumer.objectDelete;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@QuarkusTest
@TestProfile(KafkaProfile.class)
class LocalBrokerServiceTest {
  private static final Logger logger = Logger.getLogger(LocalBrokerServiceTest.class);
  @Inject
  LocalBrokerService brokerService;
  @Inject
  FakeConsumer fakeConsumer;

  @Test
  void checkSendBucketToBroadcastTopic() throws InterruptedException {
    logger.debugf("\n\nTesting send bucket event to Broadcast topic");
    assertDoesNotThrow(() -> brokerService.createBucket(BUCKET_NAME, CLIENT_ID));
    assertDoesNotThrow(() -> brokerService.deleteBucket(BUCKET_NAME, CLIENT_ID));
    bucketCreate.await();
    bucketDelete.await();
  }

  @Test
  void checkSendObjectToBroadcastTopic() throws InterruptedException {
    logger.debugf("\n\nTesting send object event to Broadcast topic");
    assertDoesNotThrow(() -> brokerService.createObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID, 0, null));
    assertDoesNotThrow(() -> brokerService.deleteObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID));
    objectCreate.await();
    objectDelete.await();
  }
}
