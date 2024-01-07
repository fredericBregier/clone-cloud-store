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

package io.clonecloudstore.accessor.replicator.topic;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.replicator.test.FakeReplicatorProducer;
import io.clonecloudstore.accessor.replicator.test.fake.FakeNativeStreamHandlerImpl;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@QuarkusTest
@TestProfile(KafkaProfile.class)
class RequestActionConsumerNoDbTest {
  private static final Logger LOG = Logger.getLogger(RequestActionConsumerNoDbTest.class);
  public static final String OP_ID = GuidLike.getGuid();
  public static final String CLIENTID_BUCKET0 = "clientid-bucket0";
  public static final String CLIENTID_BUCKET = "clientid-bucket";
  public static final String CLIENTID = "clientid";
  public static final String FROM = "from";
  public static final String TO = "to";
  public static final String OBJECT_NAME = "/directory/objectname";
  public static final int WAIT_FOR_CONSUME = 100;
  private static final AtomicBoolean initDone = new AtomicBoolean(false);
  @Inject
  DriverApiFactory storageDriverFactory;
  @Inject
  FakeReplicatorProducer emitter;

  // No Metrics since no DB
  @BeforeEach
  void beforeAll() throws DriverException, InterruptedException {
    if (initDone.compareAndSet(false, true)) {
      // Warm up Topic
      final var order =
          new ReplicatorOrder(OP_ID, FROM, TO, CLIENTID, CLIENTID_BUCKET, ReplicatorConstants.Action.CREATE);
      emitter.send(order);
      try (final var driver = storageDriverFactory.getInstance()) {
        Thread.sleep(100);
        assertFalse(driver.bucketExists(CLIENTID_BUCKET0));
      }
    }
    FakeNativeStreamHandlerImpl.fakeInputStream = null;
    FakeNativeStreamHandlerImpl.fakeAnswer = null;
  }

  @Test
  void testReplicatorOrders()
      throws DriverException, InterruptedException, NoSuchAlgorithmException, CcsDbException, IOException {
    LOG.info("Check Creation of Bucket");
    // Check creation of Bucket
    final var orderBucket =
        new ReplicatorOrder(OP_ID, FROM, TO, CLIENTID, CLIENTID_BUCKET, ReplicatorConstants.Action.CREATE);
    try (final var driver = storageDriverFactory.getInstance()) {
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      emitter.send(orderBucket);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      // Try recreate
      emitter.send(orderBucket);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
    }
    LOG.info("Check Creation of Object");
    // Check creation of Object
    final var orderObject = new ReplicatorOrder(OP_ID, FROM, TO, CLIENTID, CLIENTID_BUCKET, OBJECT_NAME, 120, null,
        ReplicatorConstants.Action.CREATE);
    try (final var driver = storageDriverFactory.getInstance()) {
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      emitter.send(orderObject);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      // Virtually Create Object on Remote
      final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(120L, (byte) 'A'));
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      FakeInputStream.consumeAll(digestInputStream);
      final var hash = digestInputStream.getDigestBase32();
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      FakeNativeStreamHandlerImpl.fakeAnswer = new HashMap<>();
      final var accessorObject =
          new AccessorObject().setBucket(CLIENTID_BUCKET).setCreation(Instant.now()).setId(GuidLike.getGuid())
              .setSize(120).setHash(hash).setName(OBJECT_NAME).setSite(FROM).setStatus(AccessorStatus.READY);
      AccessorHeaderDtoConverter.objectToMap(accessorObject, FakeNativeStreamHandlerImpl.fakeAnswer);
    }
    LOG.info("Check Delete of Object");
    // Check delete for Object
    try (final var driver = storageDriverFactory.getInstance()) {
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      emitter.send(orderObject2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
    }
    LOG.info("Check Delete of Bucket");
    // Check delete for Bucket
    try (final var driver = storageDriverFactory.getInstance()) {
      // First recreate Object to check nonempty bucket
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.CREATE);
      emitter.send(orderObject2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      final var orderBucket2 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.DELETE);
      emitter.send(orderBucket2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      // Now delete object then bucket
      final var orderObject3 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      emitter.send(orderObject3);
      emitter.send(orderBucket2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
    }
    LOG.info("Check Ordered events");
    // Finally multiple requests
    try (final var driver = storageDriverFactory.getInstance()) {
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.CREATE);
      final var orderBucket2 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.CREATE);
      final var orderObject3 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      final var orderBucket3 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.DELETE);
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      emitter.send(orderBucket2);
      emitter.send(orderObject2);
      emitter.send(orderObject3);
      emitter.send(orderBucket3);
      Thread.sleep(WAIT_FOR_CONSUME);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
    }
  }
}
