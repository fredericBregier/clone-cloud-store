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
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.test.metrics.MetricsCheck;
import io.clonecloudstore.test.resource.MongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.micrometer.core.instrument.Counter;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
class RequestActionConsumerTest {
  private static final Logger LOG = Logger.getLogger(RequestActionConsumerTest.class);
  public static final String OP_ID = GuidLike.getGuid();
  public static final String CLIENTID_BUCKET0 = "clientid-bucket0";
  public static final String CLIENTID_BUCKET = "clientid-bucket";
  public static final String CLIENTID = "clientid";
  public static final String FROM = "from";
  public static final String TO = "to";
  public static final String OBJECT_NAME = "directory/objectname";
  public static final int WAIT_FOR_CONSUME = 300;
  @Inject
  FakeReplicatorProducer emitter;
  @Inject
  BulkMetrics bulkMetrics;
  private static final AtomicBoolean initDone = new AtomicBoolean(false);
  @Inject
  DriverApiFactory storageDriverFactory;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  @Inject
  Instance<DaoAccessorObjectRepository> objectRepositoryInstance;
  DaoAccessorObjectRepository objectRepository;

  private Counter createBucket;
  private Counter createObject;
  private Counter deleteBucket;
  private Counter deleteObject;

  @BeforeEach
  void beforeEach() throws DriverException, InterruptedException {
    bucketRepository = bucketRepositoryInstance.get();
    assertNotNull(bucketRepository);
    objectRepository = objectRepositoryInstance.get();
    assertNotNull(objectRepository);
    if (initDone.compareAndSet(false, true)) {
      createBucket =
          bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_BUCKET, RequestActionConsumer.TAG_CREATE);
      createObject =
          bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_OBJECT, RequestActionConsumer.TAG_CREATE);
      deleteBucket =
          bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_BUCKET, RequestActionConsumer.TAG_DELETE);
      deleteObject =
          bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_OBJECT, RequestActionConsumer.TAG_DELETE);
      // Warm up Topic
      final var order = new ReplicatorOrder(OP_ID, TO, FROM, CLIENTID, CLIENTID_BUCKET0, null, 0, null,
          ReplicatorConstants.Action.CREATE);
      emitter.send(order);
      try (final var driver = storageDriverFactory.getInstance()) {
        for (var i = 0; i < WAIT_FOR_CONSUME; i++) {
          if (driver.bucketExists(CLIENTID_BUCKET0)) {
            break;
          } else {
            Thread.sleep(100);
          }
        }
        assertTrue(driver.bucketExists(CLIENTID_BUCKET0));
        MetricsCheck.waitForValueTest(createBucket, 1.0, 300);
        assertEquals(1.0, createBucket.count());
      }
    }
    FakeNativeStreamHandlerImpl.fakeInputStream = null;
    FakeNativeStreamHandlerImpl.fakeAnswer = null;
  }

  private DaoAccessorObject getObject(final String bucket, final String name)
      throws InterruptedException, CcsDbException {
    for (int i = 0; i < 100; i++) {
      var dao = objectRepository.getObject(bucket, name);
      if (dao != null) {
        return dao;
      }
      Thread.sleep(10);
    }
    throw new CcsDbException(new CcsNotExistException(bucket + " / " + name));
  }

  private void checkObject(final String bucket, final String name, final AccessorStatus status)
      throws InterruptedException, CcsDbException {
    DaoAccessorObject dao = null;
    for (int i = 0; i < 100; i++) {
      dao = getObject(bucket, name);
      if (status.equals(dao.getStatus())) {
        return;
      }
      Thread.sleep(10);
    }
    fail("Object has not the desired status: " + status + " = " + dao);
  }

  @Test
  void testReplicatorOrders()
      throws DriverException, InterruptedException, NoSuchAlgorithmException, CcsDbException, IOException {
    LOG.info("Check Creation of Bucket");
    // Check creation of Bucket
    final var orderBucket =
        new ReplicatorOrder(OP_ID, TO, FROM, CLIENTID, CLIENTID_BUCKET, ReplicatorConstants.Action.CREATE);
    double delObject = deleteObject.count();
    double delBucket = deleteBucket.count();
    double creObject = createObject.count();
    double creBucket = createBucket.count();
    double temp = 0;
    try (final var driver = storageDriverFactory.getInstance()) {
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      emitter.send(orderBucket);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createBucket, creBucket + 1, 300);
      assertEquals(creBucket + 1, temp);
      creBucket = temp;
      assertTrue(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.READY, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
      // Try recreate
      emitter.send(orderBucket);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createBucket, creBucket, 300);
      assertEquals(creBucket, temp);
      creBucket = temp;
      assertTrue(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.READY, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
    }
    LOG.info("Check Creation of Object");
    // Check creation of Object
    final var orderObject = new ReplicatorOrder(OP_ID, TO, FROM, CLIENTID, CLIENTID_BUCKET, OBJECT_NAME, 120, null,
        ReplicatorConstants.Action.CREATE);
    try (final var driver = storageDriverFactory.getInstance()) {
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      assertNull(objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME));
      emitter.send(orderObject);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createObject, creObject, 300);
      assertEquals(creObject, temp);
      creObject = temp;
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      checkObject(CLIENTID_BUCKET, OBJECT_NAME, AccessorStatus.ERR_UPL);
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
      // Change status of Object
      objectRepository.updateObjectStatus(CLIENTID_BUCKET, OBJECT_NAME, AccessorStatus.DELETED, Instant.now());
      emitter.send(orderObject);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createObject, creObject + 1, 300);
      assertEquals(creObject + 1, temp);
      creObject = temp;
      checkObject(CLIENTID_BUCKET, OBJECT_NAME, AccessorStatus.READY);
      assertEquals(StorageType.OBJECT, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      // Try recreate
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      emitter.send(orderObject);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createObject, creObject, 300);
      assertEquals(creObject, temp);
      creObject = temp;
      Assertions.assertEquals(AccessorStatus.READY,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      assertEquals(StorageType.OBJECT, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
    }
    LOG.info("Check Delete of Object");
    // Check delete for Object
    try (final var driver = storageDriverFactory.getInstance()) {
      Assertions.assertEquals(AccessorStatus.READY,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      assertEquals(StorageType.OBJECT, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      emitter.send(orderObject2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(deleteObject, delObject + 1, 300);
      assertEquals(delObject + 1, temp);
      delObject = temp;
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      Assertions.assertEquals(AccessorStatus.DELETED,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      // Try to re delete
      emitter.send(orderObject2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(deleteObject, delObject, 300);
      assertEquals(delObject, temp);
      delObject = temp;
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      Assertions.assertEquals(AccessorStatus.DELETED,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
    }
    LOG.info("Check Delete of Bucket");
    // Check delete for Bucket
    try (final var driver = storageDriverFactory.getInstance()) {
      // First recreate Object to check nonempty bucket
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.CREATE);
      emitter.send(orderObject2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createObject, creObject + 1, 300);
      assertEquals(creObject + 1, temp);
      creObject = temp;
      Assertions.assertEquals(AccessorStatus.READY,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      assertEquals(StorageType.OBJECT, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      final var orderBucket2 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.DELETE);
      emitter.send(orderBucket2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(deleteBucket, delBucket, 300);
      assertEquals(delBucket, temp);
      delBucket = temp;
      assertTrue(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.READY, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
      // Now delete object then bucket
      final var orderObject3 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      emitter.send(orderObject3);
      emitter.send(orderBucket2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(deleteObject, delObject + 1, 300);
      assertEquals(delObject + 1, temp);
      delObject = temp;
      temp = MetricsCheck.waitForValueTest(deleteBucket, delBucket + 1, 300);
      assertEquals(delBucket + 1, temp);
      delBucket = temp;
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      Assertions.assertEquals(AccessorStatus.DELETED,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.DELETED, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
      // Try to re delete
      emitter.send(orderBucket2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(deleteBucket, delBucket, 300);
      assertEquals(delBucket, temp);
      delBucket = temp;
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      Assertions.assertEquals(AccessorStatus.DELETED,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.DELETED, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
      // Recreate Bucket then Delete it
      final var orderBucket3 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.CREATE);
      emitter.send(orderBucket3);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createBucket, creBucket + 1, 300);
      assertEquals(creBucket + 1, temp);
      creBucket = temp;
      assertTrue(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.READY, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
      emitter.send(orderBucket2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(deleteBucket, delBucket + 1, 300);
      assertEquals(delBucket + 1, temp);
      delBucket = temp;
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.DELETED, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
    }
    LOG.info("Check Ordered events");
    // Finally multiple requests
    try (final var driver = storageDriverFactory.getInstance()) {
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      final var orderBucket2 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.DELETE);
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      emitter.send(orderBucket);
      emitter.send(orderObject);
      emitter.send(orderObject2);
      emitter.send(orderBucket2);
      Thread.yield();
      temp = MetricsCheck.waitForValueTest(createBucket, creBucket + 1, 300);
      assertEquals(creBucket + 1, temp);
      creBucket = temp;
      temp = MetricsCheck.waitForValueTest(createObject, creObject + 1, 300);
      assertEquals(creObject + 1, temp);
      creObject = temp;
      temp = MetricsCheck.waitForValueTest(deleteObject, delObject + 1, 300);
      assertEquals(delObject + 1, temp);
      delObject = temp;
      temp = MetricsCheck.waitForValueTest(deleteBucket, delBucket + 1, 300);
      assertEquals(delBucket + 1, temp);
      delBucket = temp;
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(CLIENTID_BUCKET, OBJECT_NAME));
      Assertions.assertEquals(AccessorStatus.DELETED,
          objectRepository.getObject(CLIENTID_BUCKET, OBJECT_NAME).getStatus());
      assertFalse(driver.bucketExists(CLIENTID_BUCKET));
      Assertions.assertEquals(AccessorStatus.DELETED, bucketRepository.findBucketById(CLIENTID_BUCKET).getStatus());
    }
  }

  @Test
  void testExtremeRequestBucket() throws DriverException, InterruptedException, CcsDbException, DriverNotFoundException,
      DriverNotAcceptableException {
    final var orderBucket =
        new ReplicatorOrder(OP_ID, TO, FROM, "clientid2", "clientid2-bucket", ReplicatorConstants.Action.UNKNOWN);
    LOG.info("Check Unknown Op");
    try (final var driver = storageDriverFactory.getInstance()) {
      // Unknown op
      assertFalse(driver.bucketExists("clientid2-bucket"));
      emitter.send(orderBucket);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists("clientid2-bucket"));
      assertNull(bucketRepository.findBucketById("clientid2-bucket"));
    }
    LOG.info("Check Wrong Name");
    final var orderBucket2 =
        new ReplicatorOrder(OP_ID, TO, FROM, "clientid2", "WrongBucketName", ReplicatorConstants.Action.CREATE);
    try (final var driver = storageDriverFactory.getInstance()) {
      // Wrong name
      assertFalse(driver.bucketExists("WrongBucketName"));
      emitter.send(orderBucket2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists("WrongBucketName"));
      assertNull(bucketRepository.findBucketById("WrongBucketName"));
    }
    final var orderBucket3 =
        new ReplicatorOrder(OP_ID, TO, FROM, "clientid2", "clientid3-bucket", ReplicatorConstants.Action.DELETE);
    try (final var driver = storageDriverFactory.getInstance()) {
      LOG.info("Check Delete non existing");
      // Delete non existing
      emitter.send(orderBucket3);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists("clientid3-bucket"));
      assertNull(bucketRepository.findBucketById("clientid3-bucket"));
      LOG.info("Check Delete non existing S3 but Db");
      // Create but remove Storage then try to delete
      final var orderBucket4 = new ReplicatorOrder(orderBucket3, ReplicatorConstants.Action.CREATE);
      emitter.send(orderBucket4);
      Thread.sleep(WAIT_FOR_CONSUME);
      driver.bucketDelete("clientid3-bucket");
      Assertions.assertEquals(AccessorStatus.READY, bucketRepository.findBucketById("clientid3-bucket").getStatus());
      emitter.send(orderBucket3);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists("clientid3-bucket"));
      Assertions.assertEquals(AccessorStatus.ERR_DEL, bucketRepository.findBucketById("clientid3-bucket").getStatus());
      LOG.info("Check Retry Delete");
      // Retry to delete already in Error Delete
      emitter.send(orderBucket3);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertFalse(driver.bucketExists("clientid3-bucket"));
      Assertions.assertEquals(AccessorStatus.ERR_DEL, bucketRepository.findBucketById("clientid3-bucket").getStatus());
    }
    final var orderBucket4 =
        new ReplicatorOrder(OP_ID, TO, FROM, "clientid2", "clientid3-bucket2", ReplicatorConstants.Action.DELETE);
    try (final var driver = storageDriverFactory.getInstance()) {
      LOG.info("Create a bucket that already exists");
      // Create first
      final var orderBucket5 = new ReplicatorOrder(orderBucket4, ReplicatorConstants.Action.CREATE);
      emitter.send(orderBucket5);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertTrue(driver.bucketExists("clientid3-bucket2"));
      Assertions.assertEquals(AccessorStatus.READY, bucketRepository.findBucketById("clientid3-bucket2").getStatus());
      // Now delete DB entry
      bucketRepository.deleteWithPk("clientid3-bucket2");
      assertTrue(driver.bucketExists("clientid3-bucket2"));
      assertNull(bucketRepository.findBucketById("clientid3-bucket2"));
      // And try to recreate it
      emitter.send(orderBucket5);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertTrue(driver.bucketExists("clientid3-bucket2"));
      Assertions.assertEquals(AccessorStatus.ERR_UPL, bucketRepository.findBucketById("clientid3-bucket2").getStatus());
    }
  }

  @Test
  void testExtremeRequestObject() throws DriverException, InterruptedException, CcsDbException, DriverNotFoundException,
      DriverNotAcceptableException {
    final var orderBucket =
        new ReplicatorOrder(OP_ID, TO, FROM, "clientidok", "clientidok-bucket", ReplicatorConstants.Action.CREATE);
    final var orderObject =
        new ReplicatorOrder(OP_ID, TO, FROM, "clientidok", "clientidok-bucket", OBJECT_NAME, 120, null,
            ReplicatorConstants.Action.CREATE);
    emitter.send(orderBucket);
    try (final var driver = storageDriverFactory.getInstance()) {
      LOG.info("Check Create with non existing bucket");
      final var orderObject0 = new ReplicatorOrder(OP_ID, TO, FROM, "clientidok", "notexist", OBJECT_NAME, 120, null,
          ReplicatorConstants.Action.CREATE);
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      FakeNativeStreamHandlerImpl.fakeAnswer = new HashMap<>();
      final var accessorObject =
          new AccessorObject().setBucket(orderObject0.bucketName()).setCreation(Instant.now()).setId(GuidLike.getGuid())
              .setSize(120).setHash(null).setName(orderObject0.objectName()).setSite(FROM)
              .setStatus(AccessorStatus.READY);
      AccessorHeaderDtoConverter.objectToMap(accessorObject, FakeNativeStreamHandlerImpl.fakeAnswer);
      // Bucket not exists
      assertFalse(driver.bucketExists("notexist"));
      emitter.send(orderObject0);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE,
          driver.directoryOrObjectExistsInBucket(orderObject0.bucketName(), orderObject0.objectName()));
      assertNull(objectRepository.getObject(orderObject0.bucketName(), orderObject0.objectName()));
      LOG.info("Check Create with wrong name");
      // Wrong name
      final var orderObject1 =
          new ReplicatorOrder(OP_ID, TO, FROM, "clientidok", "clientidok-bucket", "Wrong@ObjectName$", 120, null,
              ReplicatorConstants.Action.CREATE);
      emitter.send(orderObject1);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE,
          driver.directoryOrObjectExistsInBucket(orderObject1.bucketName(), orderObject1.objectName()));
      assertNull(objectRepository.getObject(orderObject1.bucketName(), orderObject1.objectName()));
      LOG.info("Check ReCreate with non existing object in Db but in S3");
      // Object created, then delete from Repository before trying to recreate
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      accessorObject.setBucket(orderObject.bucketName()).setCreation(Instant.now()).setId(GuidLike.getGuid())
          .setSize(120).setHash(null).setName(orderObject.objectName()).setSite(FROM).setStatus(AccessorStatus.READY);
      AccessorHeaderDtoConverter.objectToMap(accessorObject, FakeNativeStreamHandlerImpl.fakeAnswer);
      emitter.send(orderObject);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.OBJECT,
          driver.directoryOrObjectExistsInBucket(orderObject.bucketName(), orderObject.objectName()));
      final var dao = objectRepository.getObject(orderObject.bucketName(), orderObject.objectName());
      Assertions.assertEquals(AccessorStatus.READY, dao.getStatus());
      objectRepository.deleteWithPk(dao.getId());
      assertNull(objectRepository.getObject(orderObject.bucketName(), orderObject.objectName()));
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      LOG.info("Final check on recreate");
      emitter.send(orderObject);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.OBJECT,
          driver.directoryOrObjectExistsInBucket(orderObject.bucketName(), orderObject.objectName()));
      final var dao2 = objectRepository.getObject(orderObject.bucketName(), orderObject.objectName());
      Assertions.assertEquals(AccessorStatus.ERR_UPL, dao2.getStatus());
      // Object IN Progress while creating again
      LOG.info("Object IN Progress while creating again");
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      objectRepository.updateObjectStatus(orderObject.bucketName(), orderObject.objectName(), AccessorStatus.UPLOAD,
          Instant.now());
      emitter.send(orderObject);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.OBJECT,
          driver.directoryOrObjectExistsInBucket(orderObject.bucketName(), orderObject.objectName()));
      final var dao3 = objectRepository.getObject(orderObject.bucketName(), orderObject.objectName());
      Assertions.assertEquals(AccessorStatus.UPLOAD, dao3.getStatus());
      // Try to delete while in progress
      final var orderObject2 = new ReplicatorOrder(orderObject, ReplicatorConstants.Action.DELETE);
      final var orderBucket2 = new ReplicatorOrder(orderBucket, ReplicatorConstants.Action.DELETE);
      LOG.info("Try to delete while in progress");
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.OBJECT,
          driver.directoryOrObjectExistsInBucket(orderObject2.bucketName(), orderObject2.objectName()));
      final var dao4 = objectRepository.getObject(orderObject2.bucketName(), orderObject2.objectName());
      Assertions.assertEquals(AccessorStatus.UPLOAD, dao4.getStatus());
      // Try to delete while object does not exist in S3
      LOG.info("Try to delete while object does not exist in S3");
      objectRepository.updateObjectStatus(orderObject2.bucketName(), orderObject2.objectName(), AccessorStatus.READY,
          Instant.now());
      driver.objectDeleteInBucket(orderObject2.bucketName(), orderObject2.objectName());
      emitter.send(orderObject2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE,
          driver.directoryOrObjectExistsInBucket(orderObject2.bucketName(), orderObject2.objectName()));
      final var dao5 = objectRepository.getObject(orderObject2.bucketName(), orderObject2.objectName());
      Assertions.assertEquals(AccessorStatus.DELETED, dao5.getStatus());
      // Recreate then delete
      LOG.info("Recreate then delete");
      FakeNativeStreamHandlerImpl.fakeInputStream = new FakeInputStream(120L, (byte) 'A');
      emitter.send(orderObject);
      emitter.send(orderObject2);
      Thread.sleep(WAIT_FOR_CONSUME);
      assertEquals(StorageType.NONE,
          driver.directoryOrObjectExistsInBucket(orderObject2.bucketName(), orderObject2.objectName()));
      final var dao6 = objectRepository.getObject(orderObject2.bucketName(), orderObject2.objectName());
      Assertions.assertEquals(AccessorStatus.DELETED, dao6.getStatus());
      // Try to delete non existing object
      LOG.info("Try to delete non existing object");
      final var orderObject3 = new ReplicatorOrder(OP_ID, TO, FROM, "clientidok", "notexist", "non-exist", 120, null,
          ReplicatorConstants.Action.CREATE);
      emitter.send(orderObject3);
      Thread.sleep(WAIT_FOR_CONSUME);
    }
  }
}
