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

package io.clonecloudstore.reconciliator.database.mongodb;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ReconciliatorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.CentralReconciliationService;
import io.clonecloudstore.reconciliator.database.model.PurgeService;
import io.clonecloudstore.reconciliator.fake.FakeRequestTopicConsumer;
import io.clonecloudstore.test.driver.fake.FakeDriver;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.MongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class MgPurgeTest {
  private static final Logger LOGGER = Logger.getLogger(MgPurgeTest.class);
  private static final String BUCKET = "mybucket";
  private static final String CLIENT_ID = "client-id";
  private static final String FROM_SITE = "from-site";
  private static final String OBJECT_NAME = "dir/object_";
  private static final String REQUEST_ID = "request-id";
  @Inject
  Instance<DaoAccessorObjectRepository> objectRepositoryInstance;
  DaoAccessorObjectRepository objectRepository;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  @Inject
  Instance<PurgeService> daoPurgeServiceInstance;
  PurgeService purgeService;
  @Inject
  Instance<CentralReconciliationService> daoCentralServiceInstance;
  CentralReconciliationService daoCentralService;
  @Inject
  DriverApiFactory storageDriverFactory;
  AtomicBoolean init = new AtomicBoolean(false);

  @BeforeEach
  void beforeEach() throws CcsDbException {
    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
    purgeService = daoPurgeServiceInstance.get();
    daoCentralService = daoCentralServiceInstance.get();
    if (init.compareAndSet(false, true)) {
      ((MgDaoAccessorBucketRepository) bucketRepository).createIndex();
      ((MgDaoAccessorObjectRepository) objectRepository).createIndex();
    }
    deleteAll();
  }

  private void deleteAll() throws CcsDbException {
    bucketRepository.deleteAllDb();
    objectRepository.deleteAllDb();
    FakeDriverFactory.cleanUp();
    FakeRequestTopicConsumer.reset();
  }

  private void insertBucket(final String name) {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.bucketCreate(new StorageBucket(name, CLIENT_ID, Instant.now().minusSeconds(110)));
    } catch (DriverException e) {
      fail(e);
    }
    try {
      bucketRepository.insertBucket(
          new AccessorBucket().setId(name).setSite(AccessorProperties.getAccessorSite()).setStatus(AccessorStatus.READY)
              .setCreation(Instant.now().minusSeconds(110)).setClientId(CLIENT_ID));
    } catch (CcsDbException e) {
      fail(e);
    }
  }

  private void insertObject(final String name, final AccessorStatus status, final boolean store,
                            final Instant creationTime, final Instant expireTime) {
    final var daoObject = objectRepository.createEmptyItem();
    daoObject.setId(GuidLike.getGuid()).setBucket(BUCKET).setName(name).setCreation(creationTime).setExpires(expireTime)
        .setSite(ServiceProperties.getAccessorSite()).setStatus(status).setHash("hash").setSize(10L);
    assertDoesNotThrow(() -> objectRepository.insert(daoObject));
    if (store) {
      try (final var driver = storageDriverFactory.getInstance(); final var inpustream = new FakeInputStream(10L)) {
        driver.objectPrepareCreateInBucket(
            new StorageObject(BUCKET, name, "hash", 10L, creationTime, expireTime, HashMap.newHashMap(0)), inpustream);
        driver.objectFinalizeCreateInBucket(BUCKET, name, 10L, "hash");
      } catch (DriverException | IOException e) {
        fail(e);
      }
    }
  }

  private void createStorageContent(final Instant creationTime, final Instant expireTime, final int count) {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.bucketCreate(new StorageBucket(BUCKET, CLIENT_ID, Instant.now().minusSeconds(1000)));
      try {
        bucketRepository.insertBucket(new AccessorBucket().setId(BUCKET).setSite(AccessorProperties.getAccessorSite())
            .setStatus(AccessorStatus.READY).setCreation(Instant.now().minusSeconds(1000)).setClientId(CLIENT_ID));
      } catch (CcsDbException e) {
        fail(e);
      }
      List<String> names = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        var status = AccessorStatus.values()[i % (AccessorStatus.values().length - 1) + 1];
        if (status == AccessorStatus.READY || status == AccessorStatus.UPLOAD || status == AccessorStatus.DELETING) {
          final String name = OBJECT_NAME + i;
          names.add(name);
        }
      }
      var name = OBJECT_NAME + "ExtraDoc";
      names.add(name);
      final var object = new StorageObject(BUCKET, OBJECT_NAME, "hash", 10L, creationTime, expireTime, null);
      ((FakeDriver) driver).forTestsOnlyCreateMultipleObjects(object, 10L, "hash", names);
    } catch (DriverException e) {
      fail(e);
    }
  }

  @Test
  void step0PurgeNoArchive() throws CcsDbException, InterruptedException {
    // Create buckets
    final var bucketPurge = "archival";
    insertBucket(bucketPurge);
    insertBucket(BUCKET);
    // Create objects for final purge
    final var creation = Instant.now().minusSeconds(100);
    final var expire = Instant.now().minusSeconds(1);
    final var expireFuture = Instant.now().plusSeconds(100);
    insertObject("DeleteNotPurge", AccessorStatus.DELETED, true, creation, expireFuture);
    insertObject("DeleteNotPurgeNoStore", AccessorStatus.DELETED, false, creation, expireFuture);
    insertObject("DeletePurge", AccessorStatus.DELETED, true, creation, expire);
    insertObject("DeletePurgeNoStore", AccessorStatus.DELETED, false, creation, expire);
    // Create objects for Ready expiry
    insertObject("ReadyNotPurge", AccessorStatus.READY, true, creation, expireFuture);
    insertObject("ReadyNotPurgeNoStore", AccessorStatus.READY, false, creation, expireFuture);
    insertObject("ReadyPurge", AccessorStatus.READY, true, creation, expire);
    insertObject("ReadyPurgeNoStore", AccessorStatus.READY, false, creation, expire);
    // Insert extra bucket and object not owned by ClientID
    final var extraClient = GuidLike.getGuid();
    final var extraBucket = "extrabucket";
    try {
      bucketRepository.insertBucket(
          new AccessorBucket().setId(extraBucket).setSite(AccessorProperties.getAccessorSite())
              .setStatus(AccessorStatus.READY).setCreation(Instant.now().minusSeconds(110)).setClientId(extraClient));
    } catch (CcsDbException e) {
      fail(e);
    }
    final var daoObject = objectRepository.createEmptyItem();
    daoObject.setId(GuidLike.getGuid()).setBucket(extraBucket).setName("fictive").setCreation(Instant.now())
        .setExpires(Instant.now().minusSeconds(10)).setSite(ServiceProperties.getAccessorSite())
        .setStatus(AccessorStatus.READY).setHash("hash").setSize(10L);
    assertDoesNotThrow(() -> objectRepository.insert(daoObject));

    objectRepository.findStream(new DbQuery()).forEach(item -> LOGGER.debugf("Before: %s", item));
    assertEquals(9, objectRepository.countAll());

    // Now check
    purgeService.purgeObjectsOnExpiredDate(CLIENT_ID, null, 200);
    objectRepository.findStream(new DbQuery()).forEach(item -> LOGGER.debugf("After: %s", item));
    assertEquals(7, objectRepository.countAll());
    objectRepository.findStream(new DbQuery()).forEach(item -> {
      switch (item.getName()) {
        case "DeleteNotPurge", "DeleteNotPurgeNoStore" -> assertEquals(AccessorStatus.DELETED, item.getStatus());
        case "ReadyPurge", "ReadyPurgeNoStore" -> {
          assertEquals(AccessorStatus.DELETED, item.getStatus());
          assertTrue(item.getExpires().isAfter(expire));
        }
        case "DeletePurge", "DeletePurgeNoStore" -> fail("Should not exist");
        case "ReadyNotPurge", "ReadyNotPurgeNoStore" -> assertEquals(AccessorStatus.READY, item.getStatus());
      }
    });
    for (int i = 0; i < 100; i++) {
      Thread.sleep(20);
      if (FakeRequestTopicConsumer.getObjectDelete() > 0) {
        break;
      }
    }
    Thread.sleep(20);
    LOGGER.infof("RequestConsume Object Delete %d", FakeRequestTopicConsumer.getObjectDelete());
    assertTrue(FakeRequestTopicConsumer.getObjectDelete() > 0);
  }

  @Test
  void step0PurgeArchive() throws CcsDbException, InterruptedException {
    // Create buckets
    final var bucketPurge = "archival";
    insertBucket(bucketPurge);
    insertBucket(BUCKET);
    // Create objects for final purge
    final var creation = Instant.now().minusSeconds(100);
    final var expire = Instant.now().minusSeconds(1);
    final var expireFuture = Instant.now().plusSeconds(100);
    insertObject("DeleteNotPurge", AccessorStatus.DELETED, true, creation, expireFuture);
    insertObject("DeleteNotPurgeNoStore", AccessorStatus.DELETED, false, creation, expireFuture);
    insertObject("DeletePurge", AccessorStatus.DELETED, true, creation, expire);
    insertObject("DeletePurgeNoStore", AccessorStatus.DELETED, false, creation, expire);
    // Create objects for Ready expiry
    insertObject("ReadyNotPurge", AccessorStatus.READY, true, creation, expireFuture);
    insertObject("ReadyNotPurgeNoStore", AccessorStatus.READY, false, creation, expireFuture);
    insertObject("ReadyPurge", AccessorStatus.READY, true, creation, expire);
    insertObject("ReadyPurgeNoStore", AccessorStatus.READY, false, creation, expire);
    // Insert extra bucket and object not owned by ClientID
    final var extraClient = GuidLike.getGuid();
    final var extraBucket = "extrabucket";
    try {
      bucketRepository.insertBucket(
          new AccessorBucket().setId(extraBucket).setSite(AccessorProperties.getAccessorSite())
              .setStatus(AccessorStatus.READY).setCreation(Instant.now().minusSeconds(110)).setClientId(extraClient));
    } catch (CcsDbException e) {
      fail(e);
    }
    final var daoObject = objectRepository.createEmptyItem();
    daoObject.setId(GuidLike.getGuid()).setBucket(extraBucket).setName("fictive").setCreation(Instant.now())
        .setExpires(Instant.now().minusSeconds(10)).setSite(ServiceProperties.getAccessorSite())
        .setStatus(AccessorStatus.READY).setHash("hash").setSize(10L);
    assertDoesNotThrow(() -> objectRepository.insert(daoObject));

    objectRepository.findStream(new DbQuery()).forEach(item -> LOGGER.debugf("Before: %s", item));
    assertEquals(9, objectRepository.countAll());

    // Now check
    purgeService.purgeObjectsOnExpiredDate(CLIENT_ID, bucketPurge, 200);
    objectRepository.findStream(new DbQuery()).forEach(item -> LOGGER.debugf("After: %s", item));
    assertEquals(8, objectRepository.countAll());
    objectRepository.findStream(new DbQuery()).forEach(item -> {
      switch (item.getName()) {
        case "DeleteNotPurge", "DeleteNotPurgeNoStore" -> assertEquals(AccessorStatus.DELETED, item.getStatus());
        case "ReadyPurge" -> {
          if (item.getBucket().equals(BUCKET)) {
            assertEquals(AccessorStatus.DELETED, item.getStatus());
            assertTrue(item.getExpires().isAfter(expire));
          } else {
            assertEquals(bucketPurge, item.getBucket());
            assertEquals(AccessorStatus.READY, item.getStatus());
            assertTrue(item.getExpires().isAfter(expire));
          }
        }
        case "ReadyPurgeNoStore" -> {
          assertEquals(BUCKET, item.getBucket());
          assertEquals(AccessorStatus.DELETED, item.getStatus());
          assertTrue(item.getExpires().isAfter(expire));
        }
        case "DeletePurge", "DeletePurgeNoStore" -> fail("Should not exist");
        case "ReadyNotPurge", "ReadyNotPurgeNoStore" -> assertEquals(AccessorStatus.READY, item.getStatus());
      }
    });
    for (int i = 0; i < 100; i++) {
      Thread.sleep(20);
      if (FakeRequestTopicConsumer.getObjectDelete() > 0) {
        break;
      }
    }
    Thread.sleep(20);
    LOGGER.infof("RequestConsume Object Delete %d", FakeRequestTopicConsumer.getObjectDelete());
    assertTrue(FakeRequestTopicConsumer.getObjectDelete() > 0);
  }

  private void insertDefaultObjects(final int count, final Instant create, final Instant expire) throws CcsDbException {
    for (int i = 0; i < count; i++) {
      final String name = OBJECT_NAME + i;
      var status = AccessorStatus.values()[i % (AccessorStatus.values().length - 1) + 1];
      final var daoObject = objectRepository.createEmptyItem();
      daoObject.setId(GuidLike.getGuid()).setBucket(BUCKET).setName(name).setCreation(create).setExpires(expire)
          .setSite(ServiceProperties.getAccessorSite()).setStatus(status).setHash("hash");
      objectRepository.addToInsertBulk(daoObject);
    }
    objectRepository.flushAll();
  }

  @Test
  void step0PurgeArchiveWith10000() throws CcsDbException, InterruptedException {
    // 5 000/s
    final var limit = 10000;
    LOGGER.infof("Create Object");
    // Create buckets
    final var bucketPurge = "archival";
    insertBucket(bucketPurge);
    final var creation = Instant.now().minusSeconds(100);
    final var expire = Instant.now().minusSeconds(1);
    insertDefaultObjects(limit, creation, expire);
    // Init storage
    LOGGER.infof("Create Some StorageObject");
    // Create objects for final purge
    createStorageContent(creation, expire, limit);
    Thread.sleep(10);
    LOGGER.infof("Start");
    assertEquals(limit, objectRepository.countAll());
    long countToPurge = 0;
    long countToDelete = 0;
    for (var status : AccessorStatus.values()) {
      var count = objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status));
      LOGGER.debugf("Count %s: %d", status, count);
      if (status.equals(AccessorStatus.DELETED)) {
        countToPurge += count;
      }
      if (status.equals(AccessorStatus.READY)) {
        countToDelete += count;
      }
    }
    // Now check
    var start = System.nanoTime();
    var log = ReconciliatorProperties.isReconciliatorPurgeLog();
    ReconciliatorProperties.setCcsReconciliatorPurgeLog(false);
    try {
      purgeService.purgeObjectsOnExpiredDate(CLIENT_ID, bucketPurge, 200);
    } finally {
      ReconciliatorProperties.setCcsReconciliatorPurgeLog(log);
    }
    var stop = System.nanoTime();
    long duration = (stop - start) / 1000000;
    float speed = limit * 1000 / (float) duration;
    LOGGER.infof("Duration: %d ms, Speed on 1 site: %f item/s", duration, speed);
    for (int i = 0; i < 100; i++) {
      Thread.sleep(20);
      if (FakeRequestTopicConsumer.getObjectDelete() >= countToDelete) {
        break;
      }
    }
    LOGGER.infof("RequestConsume Object Delete %d", FakeRequestTopicConsumer.getObjectDelete());
    for (var status : AccessorStatus.values()) {
      var count = objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status));
      LOGGER.debugf("Post Count %s: %d", status, count);
    }
    assertEquals(limit - countToPurge + countToDelete, objectRepository.countAll());
    assertTrue(FakeRequestTopicConsumer.getObjectDelete() > 0);
  }
}
