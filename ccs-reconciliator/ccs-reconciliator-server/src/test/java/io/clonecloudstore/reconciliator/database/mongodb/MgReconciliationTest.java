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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.CentralReconciliationService;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesAction;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.database.model.LocalReconciliationService;
import io.clonecloudstore.reconciliator.fake.FakeRequestTopicConsumer;
import io.clonecloudstore.reconciliator.model.ReconciliationAction;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
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

import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.REQUESTID;
import static io.clonecloudstore.reconciliator.database.model.LocalReconciliationService.DELETED_RANK;
import static io.clonecloudstore.reconciliator.database.model.LocalReconciliationService.DELETING_RANK;
import static io.clonecloudstore.reconciliator.database.model.LocalReconciliationService.READY_RANK;
import static io.clonecloudstore.reconciliator.database.model.LocalReconciliationService.STATUS_NAME_ORDERED;
import static io.clonecloudstore.reconciliator.database.model.LocalReconciliationService.TO_UPDATE_RANK;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.DELETED_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.DELETE_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.ERROR_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.READY_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UPDATE_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UPLOAD_ACTION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class MgReconciliationTest {
  private static final Logger LOGGER = Logger.getLogger(MgReconciliationTest.class);
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
  Instance<DaoRequestRepository> requestRepositoryInstance;
  DaoRequestRepository requestRepository;
  @Inject
  Instance<DaoNativeListingRepository> nativeListingRepositoryInstance;
  DaoNativeListingRepository nativeListingRepository;
  @Inject
  Instance<DaoSitesListingRepository> sitesListingRepositoryInstance;
  DaoSitesListingRepository sitesListingRepository;
  @Inject
  Instance<DaoSitesActionRepository> sitesActionRepositoryInstance;
  DaoSitesActionRepository sitesActionRepository;
  @Inject
  Instance<LocalReconciliationService> daoServiceInstance;
  LocalReconciliationService daoService;
  @Inject
  Instance<CentralReconciliationService> daoCentralServiceInstance;
  CentralReconciliationService daoCentralService;
  @Inject
  DriverApiFactory storageDriverFactory;
  AtomicBoolean init = new AtomicBoolean(false);
  private final AtomicReference<CcsDbException> exceptionAtomicReference = new AtomicReference<>();

  @BeforeEach
  void beforeEach() throws CcsDbException {
    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
    requestRepository = requestRepositoryInstance.get();
    nativeListingRepository = nativeListingRepositoryInstance.get();
    sitesListingRepository = sitesListingRepositoryInstance.get();
    sitesActionRepository = sitesActionRepositoryInstance.get();
    daoService = daoServiceInstance.get();
    daoCentralService = daoCentralServiceInstance.get();
    if (init.compareAndSet(false, true)) {
      ((MgDaoAccessorBucketRepository) bucketRepository).createIndex();
      ((MgDaoAccessorObjectRepository) objectRepository).createIndex();
      ((MgDaoRequestRepository) requestRepository).createIndex();
      ((MgDaoNativeListingRepository) nativeListingRepository).createIndex();
      ((MgDaoSitesListingRepository) sitesListingRepository).createIndex();
      ((MgDaoSitesActionRepository) sitesActionRepository).createIndex();
    }
    deleteAll();
  }

  private void deleteAll() throws CcsDbException {
    bucketRepository.deleteAllDb();
    objectRepository.deleteAllDb();
    requestRepository.deleteAllDb();
    nativeListingRepository.deleteAllDb();
    sitesListingRepository.deleteAllDb();
    sitesActionRepository.deleteAllDb();
    FakeDriverFactory.cleanUp();
    FakeRequestTopicConsumer.reset();
  }

  private DaoAccessorObject insertNewObject(final String name, final AccessorStatus status) {
    final var daoObject = objectRepository.createEmptyItem();
    daoObject.setId(GuidLike.getGuid()).setBucket(BUCKET).setName(name).setCreation(Instant.now().minusSeconds(100))
        .setSite(ServiceProperties.getAccessorSite()).setStatus(status).setHash("hash");
    assertDoesNotThrow(() -> objectRepository.insert(daoObject));
    return daoObject;
  }

  private void insertNewNativeDb(final String requestId, final String name, final AccessorStatus status) {
    final var daoNative = nativeListingRepository.createEmptyItem();
    daoNative.setId(GuidLike.getGuid()).setBucket(BUCKET).setName(name).setRequestId(requestId);
    daoNative.setDb(new SingleSiteObject(ServiceProperties.getAccessorSite(), (short) status.ordinal(),
        Instant.now().minusSeconds(100)));
    assertDoesNotThrow(() -> nativeListingRepository.insert(daoNative));
  }

  private void insertDefaultObjects(final String requestId, final boolean fillNative) throws CcsDbException {
    for (final var status : AccessorStatus.values()) {
      final String name = OBJECT_NAME + status.name();
      final var daoObject = insertNewObject(name, status);
      if (fillNative) {
        insertNewNativeDb(requestId, daoObject.getName(), daoObject.getStatus());
      }
    }
    for (final var status : AccessorStatus.values()) {
      assertEquals(1,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      if (fillNative) {
        assertEquals(1, nativeListingRepository.count(
            new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
                status.ordinal())));
      }
    }
  }

  private void createStorageContent(final DaoAccessorObject daoAccessorObject) {
    try (final var driver = storageDriverFactory.getInstance(); final var inpustream = new FakeInputStream(10L)) {
      if (!driver.bucketExists(daoAccessorObject.getBucket())) {
        driver.bucketCreate(
            new StorageBucket(daoAccessorObject.getBucket(), CLIENT_ID, Instant.now().minusSeconds(110)));
        try {
          bucketRepository.insertBucket(
              new AccessorBucket().setId(daoAccessorObject.getBucket()).setSite(AccessorProperties.getAccessorSite())
                  .setStatus(AccessorStatus.READY).setCreation(Instant.now().minusSeconds(110)).setClientId(CLIENT_ID));
        } catch (CcsDbException e) {
          fail(e);
        }
      }
      driver.objectPrepareCreateInBucket(
          new StorageObject(daoAccessorObject.getBucket(), daoAccessorObject.getName(), "hash", 10L,
              daoAccessorObject.getCreation()), inpustream);
      driver.objectFinalizeCreateInBucket(daoAccessorObject.getBucket(), daoAccessorObject.getName(), 10L, "hash");
    } catch (DriverException | IOException e) {
      fail(e);
    }
  }

  private void createStorageContent(final Instant creationTime, final Instant creationTime2) {
    try (final var driver = storageDriverFactory.getInstance(); final var inpustream = new FakeInputStream(10L);
         final var inpustreamDel = new FakeInputStream(10L); final var inpustream50 = new FakeInputStream(50L);
         final var inpustream100 = new FakeInputStream(100L)) {
      driver.bucketCreate(new StorageBucket(BUCKET, CLIENT_ID, Instant.now().minusSeconds(110)));
      try {
        bucketRepository.insertBucket(new AccessorBucket().setId(BUCKET).setSite(AccessorProperties.getAccessorSite())
            .setStatus(AccessorStatus.READY).setCreation(Instant.now().minusSeconds(110)).setClientId(CLIENT_ID));
      } catch (CcsDbException e) {
        fail(e);
      }
      var name = OBJECT_NAME + AccessorStatus.READY.name();
      driver.objectPrepareCreateInBucket(new StorageObject(BUCKET, name, "hash", 10L, creationTime), inpustream);
      driver.objectFinalizeCreateInBucket(BUCKET, name, 10L, "hash");
      name = OBJECT_NAME + AccessorStatus.UPLOAD.name();
      driver.objectPrepareCreateInBucket(new StorageObject(BUCKET, name, "hash2", 50L, creationTime), inpustream50);
      driver.objectFinalizeCreateInBucket(BUCKET, name, 50L, "hash2");
      name = OBJECT_NAME + AccessorStatus.DELETING.name();
      driver.objectPrepareCreateInBucket(new StorageObject(BUCKET, name, "hash", 10L, creationTime), inpustreamDel);
      driver.objectFinalizeCreateInBucket(BUCKET, name, 10L, "hash");
      name = OBJECT_NAME + "ExtraDoc";
      driver.objectPrepareCreateInBucket(new StorageObject(BUCKET, name, "hash3", 100L, creationTime2), inpustream100);
      driver.objectFinalizeCreateInBucket(BUCKET, name, 100L, "hash3");
    } catch (DriverException | IOException e) {
      fail(e);
    }
  }

  @Test
  void step1CleanUpNativeListingNoDateNoId() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }

    // Test
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Unknown Source: %s", dao);
    }
    final var countObjectCleanUp1 = objectRepository.countAll();
    final var countNativeCleanUp1 = nativeListingRepository.countAll();
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
    }

    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Upload/Delete Source: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, ERR_DEL, ERR_UPL -> 0;
        case READY, DELETED -> 1;
        case DELETING -> 2;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
      assertEquals(expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())), status.name());
    }
    assertEquals(2, objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS,
        LocalReconciliationService.TO_UPDATE_RANK)));

    ((MgLocalReconciliationService) daoService).step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(
        daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(2, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, UPLOAD, ERR_UPL, DELETED, ERR_DEL -> 0;
        case READY, DELETING -> 1;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
      assertEquals(expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
    }
  }

  @Test
  void step1CleanUpNativeListingNoDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }

    // Test
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Unknown Source: %s", dao);
    }
    final var countObjectCleanUp1 = objectRepository.countAll();
    final var countNativeCleanUp1 = nativeListingRepository.countAll();
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
    }

    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Upload/Delete Source: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, ERR_DEL, ERR_UPL -> 0;
        case READY, DELETED -> 1;
        case DELETING -> 2;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
      assertEquals(expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())), status.name());
    }
    assertEquals(2, objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS,
        LocalReconciliationService.TO_UPDATE_RANK)));

    ((MgLocalReconciliationService) daoService).step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(
        daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(2, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, UPLOAD, ERR_UPL, DELETED, ERR_DEL -> 0;
        case READY, DELETING -> 1;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
      assertEquals(expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())), status.name());
    }
  }

  @Test
  void step1CleanUpNativeListingNoDateWithObject() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
      if (dao.getStatus() != AccessorStatus.UNKNOWN) {
        createStorageContent(dao);
      }
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }

    // Test
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Unknown Source: %s", dao);
    }
    final var countObjectCleanUp1 = objectRepository.countAll();
    final var countNativeCleanUp1 = nativeListingRepository.countAll();
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
    }

    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Upload/Delete Source: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, ERR_DEL, ERR_UPL -> 0;
        case READY, DELETED -> 1;
        case DELETING -> 2;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
      assertEquals(expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
    }

    ((MgLocalReconciliationService) daoService).step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(
        daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(2, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, UPLOAD, ERR_UPL, DELETED, ERR_DEL -> 0;
        case READY, DELETING -> 1;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
      assertEquals(expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
    }
  }

  @Test
  void step1CleanUpNativeListingWithPastDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }

    // Test
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Unknown Source: %s", dao);
    }
    final var countObjectCleanUp1 = objectRepository.countAll();
    final var countNativeCleanUp1 = nativeListingRepository.countAll();
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
    }

    daoRequest.setStart(Instant.now().minusSeconds(1000));
    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Upload/Delete Source: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN -> 0;
        case UPLOAD, DELETING, ERR_DEL, READY, ERR_UPL, DELETED -> 1;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
      assertEquals(expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
    }
    assertEquals(0, objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS,
        LocalReconciliationService.TO_UPDATE_RANK)));

    ((MgLocalReconciliationService) daoService).step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(
        daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(2, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, UPLOAD, ERR_UPL, DELETED, ERR_DEL -> 0;
        case READY, DELETING -> 1;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
      assertEquals(expectedNative, nativeListingRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              status.ordinal())));
    }
  }

  @Test
  void step1CleanUpNativeListingWithFutureDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }
    // Test with date Now
    daoRequest.setStart(Instant.now());
    requestRepository.updateFull(daoRequest);
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    final var countObjectCleanUp1 = objectRepository.countAll();
    final var countNativeCleanUp1 = nativeListingRepository.countAll();
    assertEquals(6, countObjectCleanUp1);
    assertEquals(6, countNativeCleanUp1);
    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Now Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, ERR_DEL, ERR_UPL -> 0;
        case READY, DELETED -> 1;
        case DELETING -> 2;
      };
      LOGGER.debugf("%s Expected %d Found %d", status, expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      assertEquals(expected,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
      LOGGER.debugf("%s Expected %d Found %d", status, expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())));
      assertEquals(expectedRstatus, objectRepository.count(
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.RSTATUS, status.ordinal())), status.name());
    }
  }

  @Test
  void step2ContinueFromPreviousRequest() throws CcsDbException {
    step1CleanUpNativeListingWithFutureDate();
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 1).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    var countNative = nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.EQ, MgDaoNativeListingRepository.REQUESTID, REQUEST_ID + 0));
    var countAll = nativeListingRepository.countAll();
    assertEquals(countNative, countAll);

    // Test
    daoService.step2ContinueFromPreviousRequest(REQUEST_ID + 0, daoRequest, false);

    countAll = nativeListingRepository.countAll();
    assertEquals(countNative * 2, countAll);
    var countNativeCreated = nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.EQ, MgDaoNativeListingRepository.REQUESTID, REQUEST_ID + 1));
    assertEquals(countNative, countNativeCreated);

    // Check in place
    nativeListingRepository.delete(
        new DbQuery(RestQuery.QUERY.EQ, MgDaoNativeListingRepository.REQUESTID, REQUEST_ID + 1));
    countAll = nativeListingRepository.countAll();
    assertEquals(countNative, countAll);

    // Test
    daoService.step2ContinueFromPreviousRequest(REQUEST_ID + 0, daoRequest, true);

    countAll = nativeListingRepository.countAll();
    assertEquals(countNative, countAll);
    countNativeCreated = nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.EQ, MgDaoNativeListingRepository.REQUESTID, REQUEST_ID + 1));
    assertEquals(countNative, countNativeCreated);
  }

  @Test
  void step3SaveNativeListingDbNoDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), false);

    // Prepare
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Source: %s", dao);
    }
    assertEquals(0, nativeListingRepository.countAll());

    // Test
    daoService.step3SaveNativeListingDb(daoRequest);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    LOGGER.debugf("DaoRequest: %s \n\tvs old %s", daoRequestUpdated, daoRequest);
    assertEquals(AccessorStatus.values().length - 1, daoRequestUpdated.getCheckedDb());
    assertEquals(AccessorStatus.values().length - 1, nativeListingRepository.count(new DbQuery()));
    var listObjects = objectRepository.findStream(new DbQuery()).toList();
    for (final var object : listObjects) {
      final var nstatus = object.getRstatus();
      final var nativeListing = nativeListingRepository.findStream(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              nstatus)).filter(dao -> object.getName().equals(dao.getName())).findFirst().get();
      LOGGER.debugf("Try to find STATUS %s with rank %d: \n\t%s", object.getStatus(), nstatus, nativeListing);
      assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getName(), nativeListing.getName());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      assertNull(nativeListing.getDriver());
    }
  }

  @Test
  void step3SaveNativeListingDbWithDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), false);
    // Date After so nothing
    daoRequest.setFilter(new AccessorFilter().setCreationAfter(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Prepare
    ((MgLocalReconciliationService) daoService).step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    ((MgLocalReconciliationService) daoService).step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(
        daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before Native: %s", dao);
    }

    // Test
    daoService.step3SaveNativeListingDb(daoRequest);

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    LOGGER.debugf("DaoRequest: %s \n\tvs old %s", daoRequestUpdated, daoRequest);
    assertEquals(0, daoRequestUpdated.getCheckedDb());
    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    // Date Before so all
    daoRequest.setFilter(new AccessorFilter().setCreationBefore(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Test
    daoService.step3SaveNativeListingDb(daoRequest);

    daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    LOGGER.debugf("DaoRequest: %s \n\tvs old %s", daoRequestUpdated, daoRequest);
    assertEquals(AccessorStatus.values().length - 1, daoRequestUpdated.getCheckedDb());
    assertEquals(AccessorStatus.values().length - 1, nativeListingRepository.count(new DbQuery()));
    var listObjects = objectRepository.findStream(new DbQuery()).toList();
    for (final var object : listObjects) {
      final var nstatus = object.getRstatus();
      final var nativeListing = nativeListingRepository.findStream(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              nstatus)).filter(dao -> object.getName().equals(dao.getName())).findFirst().get();
      LOGGER.debugf("Try to find STATUS %s with rank %d", object.getStatus(), nstatus);
      assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getName(), nativeListing.getName());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      assertNull(nativeListing.getDriver());
    }
  }

  void step3() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), false);
    daoService.step1CleanUpObjectsNativeListings(daoRequest);
    daoService.step3SaveNativeListingDb(daoRequest);
  }

  @Test
  void step4SaveNativeListingDriverNoDate() throws CcsDbException {
    step3();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    // Init storage
    var creationTime = Instant.now().minusSeconds(99).truncatedTo(ChronoUnit.MILLIS);
    var creationTime2 = Instant.now().minusSeconds(97).truncatedTo(ChronoUnit.MILLIS);
    createStorageContent(creationTime, creationTime2);

    // Test
    daoService.step4SaveNativeListingDriver(daoRequest);

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    assertEquals(AccessorStatus.values().length - 1, daoRequestUpdated.getCheckedDb());
    assertEquals(4, daoRequestUpdated.getCheckedDriver());
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));
    assertEquals(4, nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.NEQ, DaoNativeListingRepository.DRIVER, (Object) null)));
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Step4 Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Step4 Native: %s", dao);
    }
    var listObjects = objectRepository.findStream(new DbQuery()).toList();
    for (final var object : listObjects) {
      final var nstatus = object.getRstatus();
      final var nativeListing = nativeListingRepository.findStream(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              nstatus)).filter(dao -> object.getName().equals(dao.getName())).findFirst().get();
      LOGGER.debugf("Try to find STATUS %s with rank %d: \n\t%s", object.getStatus(), nstatus, nativeListing);
      assertEquals(object.getName(), nativeListing.getName());
      assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      boolean emptyDriver = switch (object.getStatus()) {
        case UNKNOWN, UPLOAD, READY, DELETING -> false;
        case ERR_UPL, DELETED, ERR_DEL -> true;
      };
      if (emptyDriver) {
        assertNull(nativeListing.getDriver());
      } else {
        assertNotNull(nativeListing.getDriver());
        assertEquals(LocalReconciliationService.READY_RANK, nativeListing.getDriver().nstatus());
        assertEquals(creationTime, nativeListing.getDriver().event());
      }
    }
    var listNative = nativeListingRepository.findStream(new DbQuery()).toList();
    for (final var nativeListing : listNative) {
      LOGGER.debugf("Find %s", nativeListing);
      if (nativeListing.getDriver() != null) {
        if (nativeListing.getDb() != null) {
          assertEquals(LocalReconciliationService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime, nativeListing.getDriver().event());
        } else {
          assertEquals(LocalReconciliationService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime2, nativeListing.getDriver().event());
        }
      }
    }
  }

  void step4() throws CcsDbException {
    step3();
    var daoRequest = requestRepository.findOne(new DbQuery());
    // Init storage
    var creationTime = Instant.now().minusSeconds(99).truncatedTo(ChronoUnit.MILLIS);
    var creationTime2 = Instant.now().minusSeconds(97).truncatedTo(ChronoUnit.MILLIS);
    createStorageContent(creationTime, creationTime2);
    daoService.step4SaveNativeListingDriver(daoRequest);
  }

  @Test
  void step4SaveNativeListingDriverWithDate() throws CcsDbException {
    step3();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    // Init storage
    var creationTime = Instant.now().minusSeconds(99).truncatedTo(ChronoUnit.MILLIS);
    var creationTime2 = Instant.now().minusSeconds(97).truncatedTo(ChronoUnit.MILLIS);
    createStorageContent(creationTime, creationTime2);

    // Creation After so none
    daoRequest.setFilter(new AccessorFilter().setCreationAfter(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Test
    daoService.step4SaveNativeListingDriver(daoRequest);

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    assertEquals(0, daoRequestUpdated.getCheckedDriver());
    assertEquals(AccessorStatus.values().length - 1, nativeListingRepository.count(new DbQuery()));
    assertEquals(0, nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.NEQ, DaoNativeListingRepository.DRIVER, (Object) null)));

    // Creation Before so all
    daoRequest.setFilter(new AccessorFilter().setCreationBefore(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Test
    daoService.step4SaveNativeListingDriver(daoRequest);

    daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    assertEquals(AccessorStatus.values().length - 1, daoRequestUpdated.getCheckedDb());
    assertEquals(4, daoRequestUpdated.getCheckedDriver());
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));
    assertEquals(4, nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.NEQ, DaoNativeListingRepository.DRIVER, (Object) null)));
    var listObjects = objectRepository.findStream(new DbQuery()).toList();
    for (final var object : listObjects) {
      final var nstatus = object.getRstatus();
      final var nativeListing = nativeListingRepository.findStream(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              nstatus)).filter(dao -> object.getName().equals(dao.getName())).findFirst().get();
      LOGGER.debugf("Try to find STATUS %s with rank %d: \n\t%s", object.getStatus(), nstatus, nativeListing);
      assertEquals(object.getName(), nativeListing.getName());
      assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      boolean emptyDriver = switch (object.getStatus()) {
        case UNKNOWN, UPLOAD, READY, DELETING -> false;
        case ERR_UPL, DELETED, ERR_DEL -> true;
      };
      if (emptyDriver) {
        assertNull(nativeListing.getDriver());
      } else {
        assertNotNull(nativeListing.getDriver());
        assertEquals(LocalReconciliationService.READY_RANK, nativeListing.getDriver().nstatus());
        assertEquals(creationTime, nativeListing.getDriver().event());
      }
    }
    var listNative = nativeListingRepository.findStream(new DbQuery()).toList();
    for (final var nativeListing : listNative) {
      LOGGER.debugf("Find %s", nativeListing);
      if (nativeListing.getDriver() != null) {
        if (nativeListing.getDb() != null) {
          assertEquals(LocalReconciliationService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime, nativeListing.getDriver().event());
        } else {
          assertEquals(LocalReconciliationService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime2, nativeListing.getDriver().event());
        }
      }
    }
  }

  @Test
  void step5CompareNativeListingDbDriverSubStep1And2() throws CcsDbException {
    step4();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    final var countOriginal = objectRepository.count(new DbQuery());

    var nameExtra = OBJECT_NAME + AccessorStatus.READY.name();
    final var objectReady =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    nameExtra = OBJECT_NAME + AccessorStatus.UPLOAD.name();
    final var objectUpload =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    nameExtra = OBJECT_NAME + AccessorStatus.DELETING.name();
    final var objectDeleting =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    // Test
    ((MgLocalReconciliationService) daoService).step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());

    // Check the 2 unchanged
    // READY
    nameExtra = OBJECT_NAME + AccessorStatus.READY.name();
    var newObject =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertEquals(objectReady, newObject);
    // UPLOAD
    nameExtra = OBJECT_NAME + AccessorStatus.UPLOAD.name();
    newObject = objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertEquals(objectUpload, newObject);
    // DELETING
    nameExtra = OBJECT_NAME + AccessorStatus.DELETING.name();
    newObject = objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertEquals(objectDeleting, newObject);

    // Redo should not create a new one
    nameExtra = OBJECT_NAME + "ExtraDoc";
    objectRepository.deleteWithPk(newObject.getId());
    newObject.setId(GuidLike.getGuid());
    objectRepository.insert(newObject);

    // Check extra
    assertEquals(countOriginal + 1, objectRepository.count(new DbQuery()));
    assertEquals(1,
        objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra)));
    nameExtra = OBJECT_NAME + "ExtraDoc";
    newObject = objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertNotNull(newObject);
    LOGGER.debugf("Found %s", newObject);
    assertEquals(nameExtra, newObject.getName());
    assertNull(newObject.getHash());
    assertNotNull(newObject.getCreation());
    assertEquals(AccessorStatus.READY, newObject.getStatus());

    // Redo test
    ((MgLocalReconciliationService) daoService).step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());

    var newObjectNotChanged =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertNotNull(newObjectNotChanged);
    LOGGER.debugf("Found %s", newObjectNotChanged);
    assertEquals(nameExtra, newObjectNotChanged.getName());
    assertNull(newObjectNotChanged.getHash());
    assertEquals(newObject.getId(), newObjectNotChanged.getId());

    // Now Step 2 should generate 1 entry in SiteListing with special status
    ((MgLocalReconciliationService) daoService).step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest,
        exceptionAtomicReference, null);
    assertNull(exceptionAtomicReference.get());

    assertEquals(1, sitesListingRepository.count(new DbQuery()));
    final var sitesListing = sitesListingRepository.findOne(new DbQuery());
    assertNotNull(sitesListing);
    LOGGER.debugf("Found %s", sitesListing);
    assertEquals(nameExtra, sitesListing.getName());
    assertEquals(1, sitesListing.getLocal().size());
    assertEquals(ReconciliationAction.UPDATE_ACTION.getStatus(), sitesListing.getLocal().getFirst().nstatus());
    assertEquals(newObjectNotChanged.getSite(), sitesListing.getLocal().getFirst().site());
    assertEquals(newObjectNotChanged.getCreation(), sitesListing.getLocal().getFirst().event());
  }

  @Test
  void step5CompareNativeListingDbDriverSubStep3And4() throws CcsDbException {
    step4();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    ((MgLocalReconciliationService) daoService).step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    ((MgLocalReconciliationService) daoService).step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest,
        exceptionAtomicReference, null);
    assertNull(exceptionAtomicReference.get());
    // Test
    Map<String, DaoAccessorObject> map = new HashMap<>();
    Map<String, DaoSitesListing> mapSite = new HashMap<>();
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Step5A Object: %s", dao);
      map.put(dao.getName(), dao);
      final var nativeListing1 = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, dao.getName()));
      LOGGER.debugf("Step5A Native: %s", nativeListing1);
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, dao.getName()));
      LOGGER.debugf("Step5A Site: %s", sitesListing);
      if (sitesListing != null) {
        mapSite.put(sitesListing.getName(), sitesListing);
      }
    }
    ((MgLocalReconciliationService) daoService).step53UpdateWhereNoDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    ((MgLocalReconciliationService) daoService).step54UpsertWhereNoDriverIntoSiteListing(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    assertEquals(4, sitesListingRepository.count(new DbQuery()));
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Step5B Object: %s", dao);
      final var nativeListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, dao.getName()));
      LOGGER.debugf("Step5B Site: %s", nativeListing);
      switch (dao.getName()) {
        case "dir/object_ERR_UPL" -> {
          assertNotEquals(map.get(dao.getName()), dao);
          assertEquals(AccessorStatus.UPLOAD, dao.getStatus());
          assertEquals(TO_UPDATE_RANK, dao.getRstatus());
          assertNotNull(nativeListing);
          assertNotNull(nativeListing.getLocal());
          assertEquals(UPLOAD_ACTION.getStatus(), nativeListing.getLocal().getFirst().nstatus());
          assertEquals(dao.getCreation(), nativeListing.getLocal().getFirst().event());
        }
        case "dir/object_ERR_DEL" -> {
          assertNotEquals(map.get(dao.getName()), dao);
          assertEquals(AccessorStatus.DELETED, dao.getStatus());
          assertEquals(DELETED_RANK, dao.getRstatus());
          assertNotNull(nativeListing);
          assertNotNull(nativeListing.getLocal());
          assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), nativeListing.getLocal().getFirst().nstatus());
          assertEquals(dao.getCreation(), nativeListing.getLocal().getFirst().event());
        }
        case "dir/object_DELETED" -> {
          assertEquals(map.get(dao.getName()), dao);
          assertEquals(AccessorStatus.DELETED, dao.getStatus());
          assertEquals(DELETED_RANK, dao.getRstatus());
          assertNotNull(nativeListing);
          assertNotNull(nativeListing.getLocal());
          assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), nativeListing.getLocal().getFirst().nstatus());
          assertEquals(dao.getCreation(), nativeListing.getLocal().getFirst().event());
        }
        default -> {
          assertEquals(map.get(dao.getName()), dao);
          if (nativeListing != null) {
            assertEquals(mapSite.get(dao.getName()), nativeListing);
          } else {
            LOGGER.infof("No NativeListing for %s", dao.getName());
          }
        }
      }
    }
  }

  @Test
  void step5CompareNativeListingDbDriverSubStep5And6() throws CcsDbException {
    step4();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    ((MgLocalReconciliationService) daoService).step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    ((MgLocalReconciliationService) daoService).step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest,
        exceptionAtomicReference, null);
    assertNull(exceptionAtomicReference.get());
    ((MgLocalReconciliationService) daoService).step53UpdateWhereNoDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    ((MgLocalReconciliationService) daoService).step54UpsertWhereNoDriverIntoSiteListing(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    Map<String, DaoAccessorObject> map = new HashMap<>();
    Map<String, DaoSitesListing> mapSite = new HashMap<>();
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Step5A Object: %s", dao);
      map.put(dao.getName(), dao);
      final var nativeListing1 = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, dao.getName()));
      LOGGER.debugf("Step5A Native: %s", nativeListing1);
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, dao.getName()));
      LOGGER.debugf("Step5A Site: %s", sitesListing);
      if (sitesListing != null) {
        mapSite.put(sitesListing.getName(), sitesListing);
      }
    }

    // Test
    ((MgLocalReconciliationService) daoService).step55UpdateBothDbDriverIntoObjects(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    ((MgLocalReconciliationService) daoService).step56UpdateBothDbDriverIntoSiteListing(daoRequest,
        exceptionAtomicReference, null);
    assertNull(exceptionAtomicReference.get());

    assertEquals(7, sitesListingRepository.count(new DbQuery()));
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Step5B Object: %s", dao);
      final var nativeListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, dao.getName()));
      LOGGER.debugf("Step5B Site: %s", nativeListing);
      switch (dao.getName()) {
        case "dir/object_UPLOAD" -> {
          assertNotEquals(map.get(dao.getName()), dao);
          assertEquals(AccessorStatus.READY, dao.getStatus());
          assertEquals(TO_UPDATE_RANK, dao.getRstatus());
          assertNotNull(nativeListing);
          assertNotNull(nativeListing.getLocal());
          assertEquals(ReconciliationAction.UPDATE_ACTION.getStatus(), nativeListing.getLocal().getFirst().nstatus());
          assertTrue(dao.getCreation().isBefore(nativeListing.getLocal().getFirst().event()));
        }
        case "dir/object_DELETING" -> {
          assertEquals(map.get(dao.getName()), dao);
          assertEquals(AccessorStatus.DELETING, dao.getStatus());
          assertEquals(DELETING_RANK, dao.getRstatus());
          assertNotNull(nativeListing);
          assertNotNull(nativeListing.getLocal());
          assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), nativeListing.getLocal().getFirst().nstatus());
          assertTrue(dao.getCreation().isBefore(nativeListing.getLocal().getFirst().event()));
        }
        case "dir/object_READY" -> {
          assertEquals(map.get(dao.getName()), dao);
          assertEquals(AccessorStatus.READY, dao.getStatus());
          assertEquals(READY_RANK, dao.getRstatus());
          assertNotNull(nativeListing);
          assertNotNull(nativeListing.getLocal());
          assertEquals(ReconciliationAction.READY_ACTION.getStatus(), nativeListing.getLocal().getFirst().nstatus());
          assertTrue(dao.getCreation().isBefore(nativeListing.getLocal().getFirst().event()));
        }
        default -> {
          assertEquals(map.get(dao.getName()), dao);
          assertNotNull(nativeListing);
          assertEquals(mapSite.get(dao.getName()), nativeListing);
        }
      }
    }
  }

  @Test
  void step5CompareNativeListingDbDriverSubSteps() throws CcsDbException {
    step4();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    // Test
    daoService.step5CompareNativeListingDbDriver(daoRequest);

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length, daoRequest.getChecked());
    for (var nameExt : STATUS_NAME_ORDERED) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt);
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt));
      if (nameExt.equals(AccessorStatus.UNKNOWN.name())) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(1, sitesListing.getLocal().size());
      final var nativeListing = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, sitesListing.getName()));
      LOGGER.debugf("Compare [%s] (%s)", sitesListing, nativeListing);
    }
    final var nameExtra = OBJECT_NAME + "ExtraDoc";
    final var sitesListingExtra =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(1, sitesListingExtra.getLocal().size());
    final var nativeListingExtra = nativeListingRepository.findOne(
        new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, sitesListingExtra.getName()));
    LOGGER.debugf("Compare [%s] (%s)", sitesListingExtra, nativeListingExtra);

    /*
        - Unknown => none
        - UPLOAD: 11/UPDATE - db 7 dr 2
        - READY: 10/READY - db 2 dr 2
        - ERR_UPL: 2°/UPLOAD - db 7 dr ()
        - DELETING: 1/DELETE - db 4 dr 2
        - DELETED: 1/DELETE - db 5 dr ()
        - ERR_DEL: 1/DELETE - db 5 dr ()
        - Extra: 11/UPDATE - db () dr 2
     */
    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(1, sitesListing.getLocal().size());
      var local = sitesListing.getLocal().getFirst();
      final var nativeListing = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, sitesListing.getName()));
      LOGGER.debugf("Compare [%s] (%s)", sitesListing, nativeListing);
      switch (nameExt) {
        case UPLOAD -> {
          assertEquals(READY_ACTION.getStatus(), local.nstatus());
          assertNotNull(nativeListing.getDb());
          assertNotNull(nativeListing.getDriver());
          assertEquals(READY_RANK, nativeListing.getDb().nstatus());
          assertEquals(READY_RANK, nativeListing.getDriver().nstatus());
        }
        case READY -> {
          assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
          assertNotNull(nativeListing.getDb());
          assertNotNull(nativeListing.getDriver());
          assertEquals(READY_RANK, nativeListing.getDb().nstatus());
          assertEquals(READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(nativeListing.getDriver().event(), local.event());
        }
        case ERR_UPL -> {
          assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
          assertNotNull(nativeListing.getDb());
          assertNull(nativeListing.getDriver());
          assertEquals(TO_UPDATE_RANK, nativeListing.getDb().nstatus());
          assertEquals(nativeListing.getDb().event(), local.event());
        }
        case DELETING -> {
          assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
          assertNotNull(nativeListing.getDb());
          assertNotNull(nativeListing.getDriver());
          assertEquals(DELETING_RANK, nativeListing.getDb().nstatus());
          assertEquals(READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(nativeListing.getDriver().event(), local.event());
        }
        case DELETED -> {
          assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
          assertNotNull(nativeListing.getDb());
          assertNull(nativeListing.getDriver());
          assertEquals(DELETED_RANK, nativeListing.getDb().nstatus());
          assertEquals(nativeListing.getDb().event(), local.event());
        }
        case ERR_DEL -> {
          assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
          assertNotNull(nativeListing.getDb());
          assertNull(nativeListing.getDriver());
          assertEquals(DELETING_RANK, nativeListing.getDb().nstatus());
          assertEquals(nativeListing.getDb().event(), local.event());
        }
      }
    }
    assertEquals(1, sitesListingExtra.getLocal().size());
    var local = sitesListingExtra.getLocal().getFirst();
    assertEquals(READY_ACTION.getStatus(), local.nstatus());
    assertNotNull(nativeListingExtra.getDb());
    assertEquals(READY_RANK, nativeListingExtra.getDb().nstatus());
    assertEquals(nativeListingExtra.getDb().event(), local.event());
    assertNotNull(nativeListingExtra.getDriver());
    assertEquals(READY_RANK, nativeListingExtra.getDriver().nstatus());
    assertEquals(nativeListingExtra.getDriver().event(), local.event());
  }

  void step5() throws CcsDbException {
    step4();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    daoService.step5CompareNativeListingDbDriver(daoRequest);
  }

  private void insertDefaultObjects(final int count) throws CcsDbException {
    for (int i = 0; i < count; i++) {
      final String name = OBJECT_NAME + i;
      var status = AccessorStatus.values()[i % (AccessorStatus.values().length - 1) + 1];
      final var daoObject = objectRepository.createEmptyItem();
      daoObject.setId(GuidLike.getGuid()).setBucket(BUCKET).setName(name).setCreation(Instant.now().minusSeconds(100))
          .setSite(ServiceProperties.getAccessorSite()).setStatus(status).setHash("hash");
      objectRepository.addToInsertBulk(daoObject);
    }
    objectRepository.flushAll();
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

  void allRemoteSteps() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    LOGGER.infof("Step1");
    daoService.step1CleanUpObjectsNativeListings(daoRequest);
    LOGGER.infof("Step3");
    daoService.step3SaveNativeListingDb(daoRequest);
    LOGGER.infof("Step4");
    daoService.step4SaveNativeListingDriver(daoRequest);
    LOGGER.infof("Step5");
    daoService.step5CompareNativeListingDbDriver(daoRequest);
    LOGGER.infof("End");
  }

  @Test
  void step5RemoteAllStepsWith10000() throws CcsDbException, InterruptedException {
    // 3 200/s
    final var limit = 10000;
    LOGGER.infof("Create Object");
    insertDefaultObjects(limit);
    // Init storage
    LOGGER.infof("Create Some StorageObject");
    var creationTime = Instant.now().minusSeconds(99).truncatedTo(ChronoUnit.MILLIS);
    createStorageContent(creationTime, null, limit);
    Thread.sleep(10);
    LOGGER.infof("Start");
    var start = System.nanoTime();
    allRemoteSteps();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    daoService.cleanNativeListing(daoRequest);
    final var iterator = daoService.getSiteListing(daoRequest);
    final var count = SystemTools.consumeAll(iterator);
    var stop = System.nanoTime();
    assertEquals(limit + 1, count);
    long duration = (stop - start) / 1000000;
    float speed = limit * 1000 / (float) duration;
    LOGGER.infof("Duration: %d ms, Speed on 1 site: %f item/s", duration, speed);
  }

  @Test
  void step6GetSiteListingStep7SaveRemoteNativeListingNotSameDate() throws CcsDbException {
    step5();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));

    // Test
    daoService.cleanNativeListing(daoRequest);

    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    final var iterator = daoService.getSiteListing(daoRequest);
    final var list = StreamIteratorUtils.getListFromIterator(iterator).stream().map(DaoSitesListing::getDto).toList();
    assertEquals(AccessorStatus.values().length, list.size());
    list.forEach(item -> LOGGER.debugf("Found0 %s", item));

    // Test
    daoService.cleanSitesListing(daoRequest);

    assertEquals(0, sitesListingRepository.count(new DbQuery()));

    // Extra test: list with no Local
    final var listNoLocal = new ArrayList<ReconciliationSitesListing>(list.size());
    for (var item : list) {
      var newItem = new ReconciliationSitesListing(item.id(), item.requestId(), item.bucket(), item.name(), List.of());
      listNoLocal.add(newItem);
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, listNoLocal.iterator());
    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(0, sitesListing.getLocal().size());
    }
    final var nameExtra = OBJECT_NAME + "ExtraDoc";
    final var sitesListingExtra0 =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(0, sitesListingExtra0.getLocal().size());

    // Test
    daoCentralService.saveRemoteNativeListing(daoRequest, list.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 2, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(1, sitesListing.getLocal().size());
      var local = sitesListing.getLocal().getFirst();
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
    }
    final var sitesListingExtra =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(1, sitesListingExtra.getLocal().size());
    var localExtra = sitesListingExtra.getLocal().getFirst();
    assertEquals(READY_ACTION.getStatus(), localExtra.nstatus());


    final var NONPRIO = "nonprio";
    var list2 = new ArrayList<ReconciliationSitesListing>();
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.local().getFirst();
      first = new SingleSiteObject(NONPRIO, first.nstatus(), first.event().minusSeconds(100));
      listNew.add(first);
      list2.add(new MgDaoSitesListing().fromDto(item).setLocal(listNew).getDto());
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list2.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 3L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(2, sitesListing.getLocal().size());
      LOGGER.debugf("Step 2: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(1);
      assertEquals(NONPRIO, local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
    }
    final var sitesListingExtra2 =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(2, sitesListingExtra2.getLocal().size());
    for (var local : sitesListingExtra2.getLocal()) {
      assertEquals(READY_ACTION.getStatus(), local.nstatus());
    }

    final var PRIO = "prioritysite";
    var list3 = new ArrayList<ReconciliationSitesListing>();
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.local().getFirst();
      first = new SingleSiteObject(PRIO, first.nstatus(), first.event().plusSeconds(200));
      listNew.add(first);
      list3.add(new MgDaoSitesListing().fromDto(item).setLocal(listNew).getDto());
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list3.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 4L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(3, sitesListing.getLocal().size());
      LOGGER.debugf("Step 3: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(PRIO, local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(1);
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(2);
      assertEquals(NONPRIO, local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
    }
    final var sitesListingExtra3 =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(3, sitesListingExtra3.getLocal().size());
    for (var local : sitesListingExtra3.getLocal()) {
      assertEquals(READY_ACTION.getStatus(), local.nstatus());
    }

    // Redo last step to check consistency
    daoCentralService.saveRemoteNativeListing(daoRequest, list3.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 5L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(3, sitesListing.getLocal().size());
      LOGGER.debugf("Step 3: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(PRIO, local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(1);
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(2);
      assertEquals(NONPRIO, local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
    }
    final var sitesListingExtra4 =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(3, sitesListingExtra4.getLocal().size());
    for (var local : sitesListingExtra4.getLocal()) {
      assertEquals(READY_ACTION.getStatus(), local.nstatus());
    }
  }

  @Test
  void step6GetSiteListingStep7SaveRemoteNativeListingNotSameAction() throws CcsDbException {
    step5();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));

    // Test
    daoService.cleanNativeListing(daoRequest);

    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    final var iterator = daoService.getSiteListing(daoRequest);
    final var list = StreamIteratorUtils.getListFromIterator(iterator).stream().map(DaoSitesListing::getDto).toList();
    assertEquals(AccessorStatus.values().length, list.size());
    list.forEach(item -> LOGGER.debugf("Found0 %s", item));

    // Test
    daoService.cleanSitesListing(daoRequest);

    assertEquals(0, sitesListingRepository.count(new DbQuery()));

    // Test
    daoCentralService.saveRemoteNativeListing(daoRequest, list.iterator());
    sitesListingRepository.findStream(new DbQuery())
        .forEach(daoSitesListing -> LOGGER.debugf("Step 1: %s", daoSitesListing));

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(1, sitesListing.getLocal().size());
      var local = sitesListing.getLocal().getFirst();
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
    }
    final var nameExtra = OBJECT_NAME + "ExtraDoc";
    final var sitesListingExtra =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(1, sitesListingExtra.getLocal().size());
    var localExtra = sitesListingExtra.getLocal().getFirst();
    assertEquals(READY_ACTION.getStatus(), localExtra.nstatus());


    final var NONPRIO = "nonprio";
    var list2 = new ArrayList<ReconciliationSitesListing>();
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.local().getFirst();
      first = new SingleSiteObject(NONPRIO, (short) 21, first.event());
      listNew.add(first);
      list2.add(new MgDaoSitesListing().fromDto(item).setLocal(listNew).getDto());
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list2.iterator());
    sitesListingRepository.findStream(new DbQuery())
        .forEach(daoSitesListing -> LOGGER.debugf("Step 2: %s", daoSitesListing));

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 2L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(2, sitesListing.getLocal().size());
      LOGGER.debugf("Step 2: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(1);
      assertEquals(NONPRIO, local.site());
      assertEquals(21, local.nstatus());
    }

    final var PRIO = "prioritysite";
    var list3 = new ArrayList<ReconciliationSitesListing>();
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.local().getFirst();
      first = new SingleSiteObject(PRIO, (short) 0, first.event());
      listNew.add(first);
      list3.add(new MgDaoSitesListing().fromDto(item).setLocal(listNew).getDto());
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list3.iterator());
    sitesListingRepository.findStream(new DbQuery())
        .forEach(daoSitesListing -> LOGGER.debugf("Step 3: %s", daoSitesListing));

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 3L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(3, sitesListing.getLocal().size());
      LOGGER.debugf("Step 3: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(PRIO, local.site());
      assertEquals(0, local.nstatus());
      local = sitesListing.getLocal().get(1);
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(2);
      assertEquals(NONPRIO, local.site());
      assertEquals(21, local.nstatus());
    }
  }

  @Test
  void step6GetSiteListingStep7SaveRemoteNativeListingNotSameDateAction() throws CcsDbException {
    step5();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));

    // Test
    daoService.cleanNativeListing(daoRequest);

    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    final var iterator = daoService.getSiteListing(daoRequest);
    final var list = StreamIteratorUtils.getListFromIterator(iterator).stream().map(DaoSitesListing::getDto).toList();
    assertEquals(AccessorStatus.values().length, list.size());
    list.forEach(item -> LOGGER.debugf("Found0 %s", item));

    // Test
    daoService.cleanSitesListing(daoRequest);

    assertEquals(0, sitesListingRepository.count(new DbQuery()));

    // Test
    daoCentralService.saveRemoteNativeListing(daoRequest, list.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(1, sitesListing.getLocal().size());
      var local = sitesListing.getLocal().getFirst();
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
    }
    final var nameExtra = OBJECT_NAME + "ExtraDoc";
    final var sitesListingExtra =
        sitesListingRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, nameExtra));
    assertEquals(1, sitesListingExtra.getLocal().size());
    var localExtra = sitesListingExtra.getLocal().getFirst();
    assertEquals(READY_ACTION.getStatus(), localExtra.nstatus());


    final var NONPRIO = "nonprio";
    var list2 = new ArrayList<ReconciliationSitesListing>();
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.local().getFirst();
      first = new SingleSiteObject(NONPRIO, (short) 0, first.event().minusSeconds(100));
      listNew.add(first);
      list2.add(new MgDaoSitesListing().fromDto(item).setLocal(listNew).getDto());
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list2.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 2L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(2, sitesListing.getLocal().size());
      LOGGER.debugf("Step 2: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(1);
      assertEquals(NONPRIO, local.site());
      assertEquals(0, local.nstatus());
    }

    final var PRIO = "prioritysite";
    var list3 = new ArrayList<ReconciliationSitesListing>();
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.local().getFirst();
      first = new SingleSiteObject(PRIO, (short) 21, first.event().plusSeconds(200));
      listNew.add(first);
      list3.add(new MgDaoSitesListing().fromDto(item).setLocal(listNew).getDto());
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list3.iterator());

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length * 3L, daoRequest.getCheckedRemote());

    for (var nameExt : AccessorStatus.values()) {
      LOGGER.debugf("%s", OBJECT_NAME + nameExt.name());
      final var sitesListing = sitesListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoSitesListingRepository.NAME, OBJECT_NAME + nameExt.name()));
      if (nameExt.equals(AccessorStatus.UNKNOWN)) {
        assertNull(sitesListing);
        continue;
      }
      assertEquals(3, sitesListing.getLocal().size());
      LOGGER.debugf("Step 3: %s", sitesListing);
      var local = sitesListing.getLocal().getFirst();
      assertEquals(PRIO, local.site());
      assertEquals(21, local.nstatus());
      local = sitesListing.getLocal().get(1);
      assertEquals(AccessorProperties.getAccessorSite(), local.site());
      switch (nameExt) {
        case UPLOAD -> assertEquals(READY_ACTION.getStatus(), local.nstatus());
        case READY -> assertEquals(ReconciliationAction.READY_ACTION.getStatus(), local.nstatus());
        case ERR_UPL -> assertEquals(UPLOAD_ACTION.getStatus(), local.nstatus());
        case DELETING -> assertEquals(ReconciliationAction.DELETE_ACTION.getStatus(), local.nstatus());
        case DELETED, ERR_DEL -> assertEquals(ReconciliationAction.DELETED_ACTION.getStatus(), local.nstatus());
      }
      local = sitesListing.getLocal().get(2);
      assertEquals(NONPRIO, local.site());
      assertEquals(0, local.nstatus());
    }
  }

  @Test
  void step6GetSiteListingStep7SaveRemoteNativeListingWith10000Items()
      throws CcsDbException, InterruptedException, IOException {
    // 12 800/s
    final var limit = 10000;
    step6GetSiteListingStep7SaveRemoteNativeListingWithXItems(limit);
  }

  void step6GetSiteListingStep7SaveRemoteNativeListingWithXItems(final int limit)
      throws CcsDbException, InterruptedException, IOException {
    step5();
    // First and only one
    final var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    assertEquals(AccessorStatus.values().length, sitesListingRepository.count(new DbQuery()));
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));

    // Test
    daoService.cleanNativeListing(daoRequest);

    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    daoService.cleanSitesListing(daoRequest);
    final var list = new ArrayList<DaoSitesListing>(limit);
    for (int i = 0; i < limit; i++) {
      DaoSitesListing sitesListing = new MgDaoSitesListing();
      var local =
          new SingleSiteObject(AccessorProperties.getAccessorSite(), ReconciliationAction.READY_ACTION.getStatus(),
              Instant.now());
      sitesListing.setId(GuidLike.getGuid()).setBucket(daoRequest.getBucket()).setRequestId(daoRequest.getId())
          .setName("test" + i).setLocal(List.of(local));
      list.add(sitesListing);
    }
    final var list2 = new ArrayList<DaoSitesListing>(limit);
    final var NONPRIO = "nonprio";
    for (var item : list) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.getLocal().getFirst();
      first = new SingleSiteObject(NONPRIO, ReconciliationAction.UNKNOWN_ACTION.getStatus(),
          first.event().minusSeconds(100));
      listNew.add(first);
      DaoSitesListing sitesListing = new MgDaoSitesListing();
      sitesListing.setId(GuidLike.getGuid()).setBucket(daoRequest.getBucket()).setRequestId(daoRequest.getId())
          .setName(item.getName()).setLocal(listNew);
      list2.add(sitesListing);
    }
    final var list3 = new ArrayList<DaoSitesListing>(limit);
    final var PRIO = "prioritysite";
    for (var item : list2) {
      var listNew = new ArrayList<SingleSiteObject>();
      var first = item.getLocal().getFirst();
      first =
          new SingleSiteObject(PRIO, ReconciliationAction.UPGRADE_ACTION.getStatus(), first.event().plusSeconds(200));
      listNew.add(first);
      DaoSitesListing sitesListing = new MgDaoSitesListing();
      sitesListing.setId(GuidLike.getGuid()).setBucket(daoRequest.getBucket()).setRequestId(daoRequest.getId())
          .setName(item.getName()).setLocal(listNew);
      list3.add(sitesListing);
    }

    LOGGER.infof("Start micro bench");
    // Test
    var start = System.nanoTime();
    var iterator = StreamIteratorUtils.getIteratorFromInputStream(
        StreamIteratorUtils.getInputStreamFromIterator(list.iterator(), MgDaoSitesListing.class),
        ReconciliationSitesListing.class);
    daoCentralService.saveRemoteNativeListing(daoRequest, iterator);

    assertEquals(limit, sitesListingRepository.count(new DbQuery()));
    var daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(limit, daoRequest2.getCheckedRemote());

    iterator = StreamIteratorUtils.getIteratorFromInputStream(
        StreamIteratorUtils.getInputStreamFromIterator(list2.iterator(), MgDaoSitesListing.class),
        ReconciliationSitesListing.class);
    daoCentralService.saveRemoteNativeListing(daoRequest, iterator);

    assertEquals(limit, sitesListingRepository.count(new DbQuery()));
    daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(limit * 2L, daoRequest2.getCheckedRemote());

    iterator = StreamIteratorUtils.getIteratorFromInputStream(
        StreamIteratorUtils.getInputStreamFromIterator(list3.iterator(), MgDaoSitesListing.class),
        ReconciliationSitesListing.class);
    daoCentralService.saveRemoteNativeListing(daoRequest, iterator);
    assertEquals(limit, sitesListingRepository.count(new DbQuery()));
    daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(limit * 3L, daoRequest2.getCheckedRemote());
    var stop = System.nanoTime();
    long duration = (stop - start) / 1000000;
    float speed = limit * 1000 / (float) duration;
    LOGGER.infof("Duration: %d ms, Speed on 3 sites: %f item/s", duration, speed);
  }

  private ReconciliationSitesListing createOneSiteAction(final String site, final ReconciliationAction action,
                                                         final String requestId, final String bucket, final String name,
                                                         final ReconciliationAction action2,
                                                         final ReconciliationAction action3,
                                                         final ReconciliationAction action4,
                                                         final ReconciliationAction action5,
                                                         final ReconciliationAction action6) {
    DaoSitesListing sitesListing = new MgDaoSitesListing();
    var local = new SingleSiteObject(site, action.getStatus(), Instant.now());
    var local2 = new SingleSiteObject(site + 2, action2.getStatus(), Instant.now().minusSeconds(10));
    var local3 = new SingleSiteObject(site + 3, action3.getStatus(), Instant.now().minusSeconds(20));
    var local4 = new SingleSiteObject(site + 4, action4.getStatus(), Instant.now().minusSeconds(30));
    var local5 = new SingleSiteObject(site + 5, action5.getStatus(), Instant.now().minusSeconds(40));
    var local6 = new SingleSiteObject(site + 6, action6.getStatus(), Instant.now().minusSeconds(50));
    sitesListing.setId(GuidLike.getGuid()).setBucket(bucket).setRequestId(requestId).setName(name)
        .setLocal(List.of(local, local2, local3, local4, local5, local6));
    return sitesListing.getDto();
  }

  private boolean checkActionDelete(final DaoSitesAction sitesAction) {
    if (sitesAction.getName().endsWith(DELETED_ACTION.name())) {
      assertEquals(DELETE_ACTION.getStatus(), sitesAction.getNeedAction());
      assertNull(sitesAction.getNeedActionFrom());
      for (int i = 3; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertFalse(sitesAction.getSites().contains("site"));
      assertFalse(sitesAction.getSites().contains("site" + 2));
      return true;
    } else if (sitesAction.getName().endsWith(DELETE_ACTION.name())) {
      assertEquals(DELETE_ACTION.getStatus(), sitesAction.getNeedAction());
      assertNull(sitesAction.getNeedActionFrom());
      for (int i = 3; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      assertFalse(sitesAction.getSites().contains("site" + 2));
      return true;
    }
    return false;
  }

  private boolean checkActionDReady(final DaoSitesAction sitesAction) {
    if (sitesAction.getName().endsWith(READY_ACTION.name())) {
      assertEquals(UPLOAD_ACTION.getStatus(), sitesAction.getNeedAction());
      assertEquals(2, sitesAction.getNeedActionFrom().size());
      assertTrue(sitesAction.getNeedActionFrom().contains("site"));
      assertTrue(sitesAction.getNeedActionFrom().contains("site4"));
      for (int i = 2; i <= 3; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      for (int i = 5; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertFalse(sitesAction.getSites().contains("site"));
      assertFalse(sitesAction.getSites().contains("site" + 4));
      return true;
    } else if (sitesAction.getName().endsWith(UPDATE_ACTION.name())) {
      assertEquals(UPLOAD_ACTION.getStatus(), sitesAction.getNeedAction());
      assertEquals(1, sitesAction.getNeedActionFrom().size());
      assertTrue(sitesAction.getNeedActionFrom().contains("site4"));
      for (int i = 2; i <= 3; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      for (int i = 5; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      assertFalse(sitesAction.getSites().contains("site" + 4));
      return true;
    } else if (sitesAction.getName().endsWith("UPDATE_FROM_UPDATE")) {
      assertEquals(UPLOAD_ACTION.getStatus(), sitesAction.getNeedAction());
      assertEquals(2, sitesAction.getNeedActionFrom().size());
      assertTrue(sitesAction.getNeedActionFrom().contains("site"));
      assertTrue(sitesAction.getNeedActionFrom().contains("site4"));
      for (int i = 2; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      return true;
    } else if (sitesAction.getName().endsWith("UPDATE_NO_UPLOAD_DELETE")) {
      assertEquals(UPDATE_ACTION.getStatus(), sitesAction.getNeedAction());
      assertEquals(2, sitesAction.getNeedActionFrom().size());
      assertTrue(sitesAction.getNeedActionFrom().contains("site5"));
      assertTrue(sitesAction.getNeedActionFrom().contains("site6"));
      for (int i = 2; i <= 4; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      return true;
    }
    return false;
  }

  private boolean checkActionUpload(final DaoSitesAction sitesAction) {
    if (sitesAction.getName().endsWith(UPLOAD_ACTION.name())) {
      assertEquals(UPLOAD_ACTION.getStatus(), sitesAction.getNeedAction());
      assertEquals(1, sitesAction.getNeedActionFrom().size());
      assertTrue(sitesAction.getNeedActionFrom().contains("site4"));
      for (int i = 2; i <= 3; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      for (int i = 5; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      assertFalse(sitesAction.getSites().contains("site" + 4));
      return true;
    } else if (sitesAction.getName().endsWith("UPLOAD_FROM_UPDATE")) {
      assertEquals(UPLOAD_ACTION.getStatus(), sitesAction.getNeedAction());
      assertEquals(1, sitesAction.getNeedActionFrom().size());
      assertTrue(sitesAction.getNeedActionFrom().contains("site4"));
      for (int i = 2; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      return true;
    }
    return false;
  }

  private boolean checkActionInvalid(final DaoSitesAction sitesAction) {
    if (sitesAction.getName().endsWith("INVALID")) {
      assertEquals(ERROR_ACTION.getStatus(), sitesAction.getNeedAction());
      assertNull(sitesAction.getNeedActionFrom());
      for (int i = 3; i <= 6; i++) {
        assertTrue(sitesAction.getSites().contains("site" + i));
      }
      assertTrue(sitesAction.getSites().contains("site"));
      assertFalse(sitesAction.getSites().contains("site" + 2));
      return true;
    }
    return false;
  }

  @Test
  void stepComputeActions() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID)
        .setCurrentSite("site").setContextSites(List.of("site", "site2", "site3", "site4", "site5", "site6"));
    requestRepository.insert(daoRequest);
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    final var list = new ArrayList<ReconciliationSitesListing>(11);
    for (var action : ReconciliationAction.values()) {
      switch (action) {
        case DELETED_ACTION, UPLOAD_ACTION, UPDATE_ACTION, READY_ACTION, DELETE_ACTION -> list.add(
            createOneSiteAction("site", action, daoRequest.getId(), daoRequest.getBucket(), "name_" + action,
                ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
                ReconciliationAction.READY_ACTION, ReconciliationAction.UPDATE_ACTION, UPLOAD_ACTION));
      }
    }
    // Update with no UPLOAD nor DELETE
    list.add(createOneSiteAction("site", UPDATE_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "UPDATE_NO_UPLOAD_DELETE", UPDATE_ACTION, UPDATE_ACTION, UPDATE_ACTION, READY_ACTION, READY_ACTION));
    // Update with no READY but UPDATE
    list.add(createOneSiteAction("site", UPDATE_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "UPDATE_FROM_UPDATE", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
        UPDATE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
    // Upload with no READY but UPDATE
    list.add(createOneSiteAction("site", UPLOAD_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "UPLOAD_FROM_UPDATE", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
        UPDATE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
    // Invalid case: Upload with no READY neither UPDATE
    list.add(createOneSiteAction("site", UPLOAD_ACTION, daoRequest.getId(), daoRequest.getBucket(), "name_" + "INVALID",
        ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION,
        UPLOAD_ACTION));
    // READY for all
    list.add(
        createOneSiteAction("site", READY_ACTION, daoRequest.getId(), daoRequest.getBucket(), "name_" + "READY_FOR_ALL",
            READY_ACTION, READY_ACTION, READY_ACTION, READY_ACTION, READY_ACTION));
    // DELETED for all
    list.add(createOneSiteAction("site", DELETED_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "DELETED_FOR_ALL", DELETED_ACTION, DELETED_ACTION, DELETED_ACTION, DELETED_ACTION, DELETED_ACTION));

    // Empty Test
    List<ReconciliationSitesListing> list2 = List.of();
    daoCentralService.saveRemoteNativeListing(daoRequest, list2.iterator());
    assertEquals(0, sitesActionRepository.countAll());
    // Test
    daoCentralService.saveRemoteNativeListing(daoRequest, list.iterator());
    sitesListingRepository.findStream(new DbQuery())
        .forEach(daoSitesListing -> LOGGER.debugf("Before %s", daoSitesListing));
    ((MgCentralReconciliationService) daoCentralService).computeActionsStepDelete(daoRequest, exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    var requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertTrue(requestTmp.getContextSitesDone() == null || requestTmp.getContextSitesDone().isEmpty());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Delete %s", sitesAction));
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertEquals(1, requestTmp.getContextSitesDone().size());
    daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite()).forEachRemaining(sitesAction -> {
      if (!checkActionDelete(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });

    // Site has one status as to delete action so only one (other is already deleted)
    var iterator = daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.next().getName().endsWith(DELETE_ACTION.name()));
    assertFalse(iterator.hasNext());
    iterator.close();
    daoRequest.setCurrentSite("site2");
    // Site 2 is deleted so no action
    iterator = daoCentralService.getSitesActon(daoRequest, "site2");
    assertFalse(iterator.hasNext());
    iterator.close();

    ((MgCentralReconciliationService) daoCentralService).computeActionsReadyLike(daoRequest, exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Ready %s", sitesAction));
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertEquals(2, requestTmp.getContextSitesDone().size());
    daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite()).forEachRemaining(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    daoRequest.setCurrentSite("site3");
    ((MgCentralReconciliationService) daoCentralService).computeActionsUpload(daoRequest, exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Upload %s", sitesAction));
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertEquals(3, requestTmp.getContextSitesDone().size());
    daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite()).forEachRemaining(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction) && !checkActionUpload(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    daoRequest.setCurrentSite("site4");
    ((MgCentralReconciliationService) daoCentralService).computeActionsInvalidUpload(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Invalid %s", sitesAction));
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertEquals(4, requestTmp.getContextSitesDone().size());
    daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite()).forEachRemaining(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction) && !checkActionUpload(sitesAction) &&
          !checkActionInvalid(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    // Redo last should not change anything
    ((MgCentralReconciliationService) daoCentralService).computeActionsInvalidUpload(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Invalid %s", sitesAction));
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertEquals(4, requestTmp.getContextSitesDone().size());
    daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite()).forEachRemaining(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction) && !checkActionUpload(sitesAction) &&
          !checkActionInvalid(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    daoRequest.setCurrentSite("site3");
    AtomicLong count = new AtomicLong();
    daoCentralService.getSitesActon(daoRequest, "site3").forEachRemaining(item -> count.incrementAndGet());
    assertEquals(9, count.get());
    daoRequest.setCurrentSite("site");
    count.set(0);
    daoCentralService.getSitesActon(daoRequest, "site").forEachRemaining(item -> count.incrementAndGet());
    assertEquals(7, count.get());

    var daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(0, daoRequest2.getActions());
    daoCentralService.countFinalActions(daoRequest2);
    daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(list.size() - 2, daoRequest2.getActions());
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    requestTmp = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    assertEquals(4, requestTmp.getContextSitesDone().size());

    daoRequest.setCurrentSite("site3");
    LOGGER.infof("Current %s", daoRequest.getCurrentSite());
    final var finalListToImport = new ArrayList<ReconciliationSitesAction>();
    daoCentralService.getSitesActon(daoRequest, daoRequest.getCurrentSite())
        .forEachRemaining(daoSitesAction -> finalListToImport.add(daoSitesAction.getDto()));
    finalListToImport.forEach(item -> LOGGER.debugf("Contain %s", item));
    sitesActionRepository.findStream(new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest2.getId()))
        .forEach(item -> LOGGER.debugf("Was %s", item));
    assertEquals(daoRequest2.getActions(), finalListToImport.size());

    sitesActionRepository.deleteAllDb();
    daoService.importActions(daoRequest, finalListToImport.iterator());
    assertEquals(daoRequest.getActions(), finalListToImport.size());
  }

  @Test
  void stepComputeActionsPartialSites() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID)
        .setContextSites(List.of("site", "site2", "site3", "site4", "site5", "site6", "site7"));
    requestRepository.insert(daoRequest);
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    final var list = new ArrayList<ReconciliationSitesListing>(11);
    for (var action : ReconciliationAction.values()) {
      switch (action) {
        case DELETED_ACTION, UPLOAD_ACTION, UPDATE_ACTION, READY_ACTION, DELETE_ACTION -> list.add(
            createOneSiteAction("site", action, daoRequest.getId(), daoRequest.getBucket(), "name_" + action,
                ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
                ReconciliationAction.READY_ACTION, ReconciliationAction.UPDATE_ACTION, UPLOAD_ACTION));
      }
    }
    // Update with no UPLOAD nor DELETE
    list.add(createOneSiteAction("site", UPDATE_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "UPDATE_NO_UPLOAD_DELETE", UPDATE_ACTION, UPDATE_ACTION, UPDATE_ACTION, READY_ACTION, READY_ACTION));
    // Update with no READY but UPDATE
    list.add(createOneSiteAction("site", UPDATE_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "UPDATE_FROM_UPDATE", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
        UPDATE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
    // Upload with no READY but UPDATE
    list.add(createOneSiteAction("site", UPLOAD_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "UPLOAD_FROM_UPDATE", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
        UPDATE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
    // Invalid case: Upload with no READY neither UPDATE
    list.add(createOneSiteAction("site", UPLOAD_ACTION, daoRequest.getId(), daoRequest.getBucket(), "name_" + "INVALID",
        ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION,
        UPLOAD_ACTION));
    // READY for all
    list.add(
        createOneSiteAction("site", READY_ACTION, daoRequest.getId(), daoRequest.getBucket(), "name_" + "READY_FOR_ALL",
            READY_ACTION, READY_ACTION, READY_ACTION, READY_ACTION, READY_ACTION));
    // DELETED for all
    list.add(createOneSiteAction("site", DELETED_ACTION, daoRequest.getId(), daoRequest.getBucket(),
        "name_" + "DELETED_FOR_ALL", DELETED_ACTION, DELETED_ACTION, DELETED_ACTION, DELETED_ACTION, DELETED_ACTION));
    daoCentralService.saveRemoteNativeListing(daoRequest, list.iterator());
    sitesListingRepository.findStream(new DbQuery())
        .forEach(daoSitesListing -> LOGGER.debugf("Before %s", daoSitesListing));
    // Test
    ((MgCentralReconciliationService) daoCentralService).computeActionsStepDelete(daoRequest, exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Delete %s", sitesAction));
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> {
      if (!checkActionDelete(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    ((MgCentralReconciliationService) daoCentralService).computeActionsReadyLike(daoRequest, exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Ready %s", sitesAction));
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    ((MgCentralReconciliationService) daoCentralService).computeActionsUpload(daoRequest, exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Upload %s", sitesAction));
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction) && !checkActionUpload(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    ((MgCentralReconciliationService) daoCentralService).computeActionsInvalidUpload(daoRequest,
        exceptionAtomicReference);
    assertNull(exceptionAtomicReference.get());
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> LOGGER.debugf("Invalid %s", sitesAction));
    sitesActionRepository.findStream(new DbQuery()).forEach(sitesAction -> {
      if (!checkActionDelete(sitesAction) && !checkActionDReady(sitesAction) && !checkActionUpload(sitesAction) &&
          !checkActionInvalid(sitesAction)) {
        fail("Should not contain " + sitesAction);
      }
    });
    var daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(0, daoRequest2.getActions());
    daoCentralService.countFinalActions(daoRequest2);
    daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(list.size() - 2, daoRequest2.getActions());
  }

  @Test
  void stepComputeActionsWith1000() throws CcsDbException, InterruptedException {
    // 58 800/s for Compute, 130 000/s for Export, 70 000/s for Import
    final int limit = 10000;
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID)
        .setContextSites(List.of("site", "site2", "site3", "site4", "site5", "site6"));
    requestRepository.insert(daoRequest);
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    final var list = new ArrayList<ReconciliationSitesListing>(limit);
    for (int i = 0; i < limit / 10; i++) {
      for (var action : ReconciliationAction.values()) {
        switch (action) {
          case DELETED_ACTION, UPLOAD_ACTION, UPDATE_ACTION, READY_ACTION, DELETE_ACTION -> list.add(
              createOneSiteAction("site", action, daoRequest.getId(), daoRequest.getBucket(), "name_" + i + action,
                  ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
                  ReconciliationAction.READY_ACTION, ReconciliationAction.UPDATE_ACTION, UPLOAD_ACTION));
        }
      }
      // Update with no UPLOAD nor DELETE
      list.add(createOneSiteAction("site", UPDATE_ACTION, daoRequest.getId(), daoRequest.getBucket(),
          "name_" + i + "UPDATE_NO_UPLOAD_DELETE", UPDATE_ACTION, UPDATE_ACTION, UPDATE_ACTION, READY_ACTION,
          READY_ACTION));
      // Update with no READY but UPDATE
      list.add(createOneSiteAction("site", UPDATE_ACTION, daoRequest.getId(), daoRequest.getBucket(),
          "name_" + i + "UPDATE_FROM_UPDATE", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
          UPDATE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
      // Upload with no READY but UPDATE
      list.add(createOneSiteAction("site", UPLOAD_ACTION, daoRequest.getId(), daoRequest.getBucket(),
          "name_" + i + "UPLOAD_FROM_UPDATE", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
          UPDATE_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
      // Invalid case: Upload with no READY neither UPDATE
      list.add(createOneSiteAction("site", UPLOAD_ACTION, daoRequest.getId(), daoRequest.getBucket(),
          "name_" + i + "INVALID", ReconciliationAction.DELETED_ACTION, ReconciliationAction.DELETE_ACTION,
          UPLOAD_ACTION, UPLOAD_ACTION, UPLOAD_ACTION));
      // READY for all
      list.add(createOneSiteAction("site", READY_ACTION, daoRequest.getId(), daoRequest.getBucket(),
          "name_" + i + "READY_FOR_ALL", READY_ACTION, READY_ACTION, READY_ACTION, READY_ACTION, READY_ACTION));
    }
    daoCentralService.saveRemoteNativeListing(daoRequest, list.iterator());
    Thread.sleep(10);
    // Test
    var start = System.nanoTime();
    daoCentralService.updateRequestFromRemoteListing(daoRequest);
    daoCentralService.computeActions(daoRequest);
    var stop = System.nanoTime();
    var daoRequest2 = requestRepository.findOne(new DbQuery());
    assertEquals(list.size() - limit / 10, daoRequest2.getActions());
    assertEquals(daoRequest2.getActions(), sitesActionRepository.countAll());
    assertEquals(1, daoRequest2.getContextSitesDone().size());
    long duration = (stop - start) / 1000000;
    float speed = limit * 1000 / (float) duration;
    LOGGER.infof("Duration: %d ms, Speed: %f item/s", duration, speed);

    List<ReconciliationSitesAction> reconciliationSitesActionList = new ArrayList<>();
    daoRequest2.setCurrentSite("site3");
    start = System.nanoTime();
    daoCentralService.getSitesActon(daoRequest2, "site3")
        .forEachRemaining(item -> reconciliationSitesActionList.add(item.getDto()));
    stop = System.nanoTime();
    LOGGER.infof("Count for %s = %d", daoRequest2.getCurrentSite(), reconciliationSitesActionList.size());
    duration = (stop - start) / 1000000;
    speed = limit * 1000 / (float) duration;
    LOGGER.infof("Duration Export: %d ms, Speed: %f item/s", duration, speed);
    daoCentralService.cleanSitesAction(daoRequest2);
    assertEquals(0, sitesActionRepository.countAll());

    start = System.nanoTime();
    daoService.importActions(daoRequest2, reconciliationSitesActionList.iterator());
    stop = System.nanoTime();
    assertEquals(reconciliationSitesActionList.size(), sitesActionRepository.countAll());
    duration = (stop - start) / 1000000;
    speed = limit * 1000 / (float) duration;
    LOGGER.infof("Duration Import: %d ms, Speed: %f item/s", duration, speed);
  }
}
