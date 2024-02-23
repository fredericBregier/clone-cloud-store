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
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.InitializationService;
import io.clonecloudstore.reconciliator.fake.FakeRequestTopicConsumer;
import io.clonecloudstore.reconciliator.model.ReconciliationAction;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.metrics.MetricsCheck;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class MgInitializationTest {
  private static final Logger LOGGER = Logger.getLogger(MgInitializationTest.class);
  private static final String BUCKET = "mybucket";
  private static final String CLIENT_ID = "client-id";
  private static final String FROM_SITE = "from-site";
  private static final String DIR_NAME = "dir/";
  private static final String OBJECT_NAME = DIR_NAME + "object_";
  private static final String OBJECT_OUT_NAME = "dir2/object_";
  private static final String REQUEST_ID = "request-id";
  @Inject
  Instance<DaoAccessorObjectRepository> objectRepositoryInstance;
  DaoAccessorObjectRepository objectRepository;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  @Inject
  Instance<DaoRequestRepository> requestRepositoryInstance;
  DaoRequestRepository requestRepository;
  @Inject
  Instance<DaoSitesActionRepository> sitesActionRepositoryInstance;
  DaoSitesActionRepository sitesActionRepository;
  @Inject
  DriverApiFactory storageDriverFactory;
  @Inject
  Instance<InitializationService> initializationServiceInstance;
  InitializationService initializationService;
  @Inject
  BulkMetrics bulkMetrics;
  AtomicBoolean init = new AtomicBoolean(false);
  private final AtomicReference<CcsDbException> exceptionAtomicReference = new AtomicReference<>();

  @BeforeEach
  void beforeEach() throws CcsDbException {
    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
    requestRepository = requestRepositoryInstance.get();
    sitesActionRepository = sitesActionRepositoryInstance.get();
    initializationService = initializationServiceInstance.get();
    if (init.compareAndSet(false, true)) {
      ((MgDaoAccessorBucketRepository) bucketRepository).createIndex();
      ((MgDaoAccessorObjectRepository) objectRepository).createIndex();
      ((MgDaoRequestRepository) requestRepository).createIndex();
      ((MgDaoSitesActionRepository) sitesActionRepository).createIndex();
    }
    deleteAll();
  }

  private void deleteAll() throws CcsDbException {
    bucketRepository.deleteAllDb();
    objectRepository.deleteAllDb();
    requestRepository.deleteAllDb();
    sitesActionRepository.deleteAllDb();
    FakeDriverFactory.cleanUp();
    FakeRequestTopicConsumer.reset();
  }

  @Test
  void importExistingObjects() throws CcsDbException, InterruptedException {
    // Create bucket
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.bucketCreate(new StorageBucket(BUCKET, CLIENT_ID, Instant.now().minusSeconds(110)));
    } catch (DriverException e) {
      fail(e);
    }
    // Create some objects
    final var creation = Instant.now().minusSeconds(100);
    final var map = Map.of("key1", "value1");
    try (final var driver = storageDriverFactory.getInstance()) {
      try (var is = new FakeInputStream(100)) {
        var storageObject = new StorageObject(BUCKET, OBJECT_NAME + 1, "hash", 100, creation, null, map);
        driver.objectPrepareCreateInBucket(storageObject, is);
        driver.objectFinalizeCreateInBucket(storageObject, 100, "hash");
      } catch (IOException e) {
        fail(e);
      }
      try (var is = new FakeInputStream(100)) {
        var storageObject = new StorageObject(BUCKET, OBJECT_NAME + 2, "hash", 100, creation, null, null);
        driver.objectPrepareCreateInBucket(storageObject, is);
        driver.objectFinalizeCreateInBucket(storageObject, 100, "hash");
      } catch (IOException e) {
        fail(e);
      }
      try (var is = new FakeInputStream(100)) {
        var storageObject =
            new StorageObject(BUCKET, OBJECT_NAME + 3, "hash", 100, creation.minusSeconds(1000), null, null);
        driver.objectPrepareCreateInBucket(storageObject, is);
        driver.objectFinalizeCreateInBucket(storageObject, 100, "hash");
      } catch (IOException e) {
        fail(e);
      }
      try (var is = new FakeInputStream(100)) {
        var storageObject = new StorageObject(BUCKET, OBJECT_OUT_NAME + 4, "hash", 100, creation, null, map);
        driver.objectPrepareCreateInBucket(storageObject, is);
        driver.objectFinalizeCreateInBucket(storageObject, 100, "hash");
      } catch (IOException e) {
        fail(e);
      }
    } catch (DriverException e) {
      fail(e);
    }

    // Check empty
    assertEquals(0, bucketRepository.countAll());
    assertEquals(0, objectRepository.countAll());
    var counter = bulkMetrics.getCounter(MgInitializationService.INITIALIZATION_SERVICE, BulkMetrics.KEY_OBJECT,
        BulkMetrics.TAG_CREATE);
    var previousValue = counter.count();
    final var defaultMap = Map.of("key2", "value2");


    // Import filter such that no one
    try {
      initializationService.importFromExistingBucket(CLIENT_ID, BUCKET + "notexist", "ZZZ", creation.minusSeconds(10),
          null, 1000, defaultMap);
      fail("Should raised an exception");
    } catch (CcsWithStatusException e) {
      // OK
    }
    assertEquals(0, bucketRepository.countAll());
    assertEquals(0, objectRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue, 500);

    // Import filter such that no one
    try {
      initializationService.importFromExistingBucket(CLIENT_ID, BUCKET, "ZZZ", creation.minusSeconds(10), null, 1000,
          defaultMap);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
    assertEquals(1, bucketRepository.countAll());
    assertEquals(0, objectRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue, 500);

    // Import filter such that no one
    try {
      initializationService.importFromExistingBucket(CLIENT_ID, BUCKET, DIR_NAME, Instant.now(), null, 1000,
          defaultMap);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
    assertEquals(1, bucketRepository.countAll());
    assertEquals(0, objectRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue, 500);

    // Import filter on prefix and date of creation
    try {
      initializationService.importFromExistingBucket(CLIENT_ID, BUCKET, DIR_NAME, creation.minusSeconds(10), null, 1000,
          defaultMap);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
    assertEquals(1, bucketRepository.countAll());
    assertEquals(2, objectRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue + 2, 500);
    objectRepository.findStream(new DbQuery()).forEach(dao -> {
      assertEquals(ServiceProperties.getAccessorSite(), dao.getSite());
      assertEquals(BUCKET, dao.getBucket());
      assertTrue(dao.getName().startsWith(OBJECT_NAME));
      assertEquals(creation.truncatedTo(ChronoUnit.MILLIS), dao.getCreation());
      assertEquals(AccessorStatus.READY, dao.getStatus());
      assertEquals("hash", dao.getHash());
      assertEquals(100, dao.getSize());
      assertTrue(Instant.now().isBefore(dao.getExpires()));
      if (dao.getName().equals(OBJECT_NAME + 2)) {
        assertFalse(dao.getMetadata().containsKey("key1"));
      } else {
        assertTrue(dao.getMetadata().containsKey("key1"));
      }
      assertTrue(dao.getMetadata().containsKey("key2"));
    });

    // Import filter on prefix but no date of creation
    try {
      initializationService.importFromExistingBucket(CLIENT_ID, BUCKET, DIR_NAME, null, null, 0, defaultMap);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
    assertEquals(1, bucketRepository.countAll());
    assertEquals(3, objectRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue + 3, 500);
    objectRepository.findStream(new DbQuery()).forEach(dao -> {
      assertEquals(ServiceProperties.getAccessorSite(), dao.getSite());
      assertEquals(BUCKET, dao.getBucket());
      assertTrue(dao.getName().startsWith(OBJECT_NAME));
      assertEquals(AccessorStatus.READY, dao.getStatus());
      assertEquals("hash", dao.getHash());
      assertEquals(100, dao.getSize());
      assertNotNull(dao.getCreation());
      if (dao.getCreation().isBefore(creation.minusSeconds(10))) {
        assertNull(dao.getExpires());
      } else {
        assertTrue(Instant.now().isBefore(dao.getExpires()));
      }
      if (dao.getName().equals(OBJECT_NAME + 2) || dao.getName().equals(OBJECT_NAME + 3)) {
        assertFalse(dao.getMetadata().containsKey("key1"));
      } else {
        assertTrue(dao.getMetadata().containsKey("key1"));
      }
      assertTrue(dao.getMetadata().containsKey("key2"));
    });


    // Import filter on prefix but no date of creation
    try {
      initializationService.importFromExistingBucket(CLIENT_ID, BUCKET, null, null, null, 0, null);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
    assertEquals(1, bucketRepository.countAll());
    assertEquals(4, objectRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue + 4, 500);
    objectRepository.findStream(new DbQuery()).forEach(dao -> {
      assertEquals(ServiceProperties.getAccessorSite(), dao.getSite());
      assertEquals(BUCKET, dao.getBucket());
      assertEquals(AccessorStatus.READY, dao.getStatus());
      assertEquals("hash", dao.getHash());
      assertEquals(100, dao.getSize());
      assertNotNull(dao.getCreation());
      if (dao.getName().startsWith(OBJECT_NAME)) {
        if (dao.getCreation().isBefore(creation.minusSeconds(10))) {
          assertNull(dao.getExpires());
        } else {
          assertTrue(Instant.now().isBefore(dao.getExpires()));
        }
        if (dao.getName().equals(OBJECT_NAME + 2) || dao.getName().equals(OBJECT_NAME + 3)) {
          assertFalse(dao.getMetadata().containsKey("key1"));
        } else {
          assertTrue(dao.getMetadata().containsKey("key1"));
        }
        assertTrue(dao.getMetadata().containsKey("key2"));
      } else {
        assertNull(dao.getExpires());
        assertTrue(dao.getMetadata().containsKey("key1"));
        assertFalse(dao.getMetadata().containsKey("key2"));
      }
    });
  }

  @Test
  void syncFromExistingSite() throws CcsDbException, InterruptedException {
    var daoBucket =
        bucketRepository.createEmptyItem().setSite(ServiceProperties.getAccessorSite()).setCreation(Instant.now())
            .setStatus(AccessorStatus.READY).setId(BUCKET).setClientId(CLIENT_ID);
    bucketRepository.insert(daoBucket);
    var daoObject =
        objectRepository.createEmptyItem().setSite(ServiceProperties.getAccessorSite()).setCreation(Instant.now())
            .setBucket(BUCKET).setName(OBJECT_NAME + 1).setHash("hash").setSize(100).setStatus(AccessorStatus.READY)
            .setId(GuidLike.getGuid()).setMetadata(Map.of("key1", "value1"));
    objectRepository.insert(daoObject);
    daoObject.setName(OBJECT_NAME + 2).setStatus(AccessorStatus.READY).setId(GuidLike.getGuid());
    objectRepository.insert(daoObject);
    daoObject.setName(OBJECT_NAME + 3).setCreation(Instant.now().minusSeconds(100)).setStatus(AccessorStatus.READY)
        .setId(GuidLike.getGuid());
    objectRepository.insert(daoObject);
    daoObject.setName(OBJECT_NAME + 4).setCreation(Instant.now()).setStatus(AccessorStatus.DELETED)
        .setId(GuidLike.getGuid());
    objectRepository.insert(daoObject);
    daoObject.setName(OBJECT_OUT_NAME + 5).setStatus(AccessorStatus.READY).setId(GuidLike.getGuid());
    objectRepository.insert(daoObject);

    assertEquals(0, sitesActionRepository.countAll());
    var counter = bulkMetrics.getCounter(MgCentralReconciliationService.CENTRAL_RECONCILIATION, BulkMetrics.KEY_OBJECT,
        BulkMetrics.TAG_TO_ACTIONS);
    var previousValue = counter.count();

    // First check bucket not existing
    assertNull(initializationService.syncFromExistingSite(CLIENT_ID, BUCKET + "notexists", "remote", null));

    // Using a filter
    AccessorFilter filter =
        new AccessorFilter().setNamePrefix(DIR_NAME).setCreationAfter(Instant.now().minusSeconds(10));
    var request = initializationService.syncFromExistingSite(CLIENT_ID, BUCKET, "remote", filter);

    assertEquals(2, sitesActionRepository.countAll());
    MetricsCheck.waitForValueTest(counter, previousValue + 2, 500);

    sitesActionRepository.findStream(new DbQuery()).forEach(daoSitesAction -> {
      assertEquals(request.getId(), daoSitesAction.getRequestId());
      assertEquals(BUCKET, daoSitesAction.getBucket());
      assertTrue(daoSitesAction.getName().startsWith(OBJECT_NAME));
      assertEquals(ReconciliationAction.UPLOAD_ACTION.getStatus(), daoSitesAction.getNeedAction());
      assertEquals(ServiceProperties.getAccessorSite(), daoSitesAction.getNeedActionFrom().getFirst());
      assertEquals("remote", daoSitesAction.getSites().getFirst());
    });
  }
}
