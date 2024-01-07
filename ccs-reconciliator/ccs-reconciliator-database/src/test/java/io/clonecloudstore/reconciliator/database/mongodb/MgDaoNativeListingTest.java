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
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListing;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoService;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class MgDaoNativeListingTest {
  private static final Logger LOGGER = Logger.getLogger(MgDaoNativeListingTest.class);
  private static final String BUCKET = "mybucket";
  private static final String CLIENT_ID = "client-id";
  private static final String CLIENT_BUCKET = CLIENT_ID + "-" + BUCKET;
  private static final String FROM_SITE = "from-site";
  private static final String OBJECT_NAME = "dir/object_";
  private static final String REQUEST_ID = "request-id";
  @Inject
  Instance<DaoAccessorObjectRepository> objectRepositoryInstance;
  DaoAccessorObjectRepository objectRepository;
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
  Instance<DaoService> daoServiceInstance;
  DaoService daoService;
  @Inject
  DriverApiFactory storageDriverFactory;
  AtomicBoolean init = new AtomicBoolean(false);

  @BeforeEach
  void beforeEach() throws CcsDbException {
    objectRepository = objectRepositoryInstance.get();
    requestRepository = requestRepositoryInstance.get();
    nativeListingRepository = nativeListingRepositoryInstance.get();
    sitesListingRepository = sitesListingRepositoryInstance.get();
    daoService = daoServiceInstance.get();
    if (init.compareAndSet(false, true)) {
      ((MgDaoAccessorObjectRepository) objectRepository).createIndex();
      ((MgDaoRequestRepository) requestRepository).createIndex();
      ((MgDaoNativeListingRepository) nativeListingRepository).createIndex();
      ((MgDaoSitesListingRepository) sitesListingRepository).createIndex();
    }
    deleteAll();
  }

  private void deleteAll() throws CcsDbException {
    objectRepository.deleteAllDb();
    requestRepository.deleteAllDb();
    nativeListingRepository.deleteAllDb();
    sitesListingRepository.deleteAllDb();
    FakeDriverFactory.cleanUp();
  }

  private DaoAccessorObject insertNewObject(final String name, final AccessorStatus status) {
    final var daoObject = objectRepository.createEmptyItem();
    daoObject.setId(GuidLike.getGuid()).setBucket(CLIENT_BUCKET).setName(name)
        .setCreation(Instant.now().minusSeconds(100)).setSite(ServiceProperties.getAccessorSite()).setStatus(status)
        .setHash("hash");
    assertDoesNotThrow(() -> objectRepository.insert(daoObject));
    return daoObject;
  }

  private DaoNativeListing insertNewNativeDb(final String requestId, final String name, final AccessorStatus status) {
    final var daoNative = nativeListingRepository.createEmptyItem();
    daoNative.setId(GuidLike.getGuid()).setBucket(CLIENT_BUCKET).setName(name).setRequestId(requestId);
    daoNative.setDb(new SingleSiteObject(ServiceProperties.getAccessorSite(), (short) status.ordinal(),
        Instant.now().minusSeconds(100)));
    assertDoesNotThrow(() -> nativeListingRepository.insert(daoNative));
    return daoNative;
  }

  private DaoNativeListing insertNewNativeDriver(final String requestId, final String name,
                                                 final AccessorStatus status) {
    final var daoNative = nativeListingRepository.createEmptyItem();
    daoNative.setId(GuidLike.getGuid()).setBucket(CLIENT_BUCKET).setName(name).setRequestId(requestId);
    daoNative.setDriver(new SingleSiteObject(ServiceProperties.getAccessorSite(), (short) status.ordinal(),
        Instant.now().minusSeconds(100)));
    assertDoesNotThrow(() -> nativeListingRepository.insert(daoNative));
    return daoNative;
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
    try (final var driver = storageDriverFactory.getInstance(); final var inpustream = new FakeInputStream(10L);
         final var inpustream50 = new FakeInputStream(50L); final var inpustream100 = new FakeInputStream(100L)) {
      if (!driver.bucketExists(daoAccessorObject.getBucket())) {
        driver.bucketCreate(new StorageBucket(daoAccessorObject.getBucket(), Instant.now().minusSeconds(110)));
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
         final var inpustream50 = new FakeInputStream(50L); final var inpustream100 = new FakeInputStream(100L)) {
      driver.bucketCreate(new StorageBucket(CLIENT_BUCKET, Instant.now().minusSeconds(110)));
      var name = OBJECT_NAME + AccessorStatus.READY.name();
      driver.objectPrepareCreateInBucket(new StorageObject(CLIENT_BUCKET, name, "hash", 10L, creationTime), inpustream);
      driver.objectFinalizeCreateInBucket(CLIENT_BUCKET, name, 10L, "hash");
      name = OBJECT_NAME + AccessorStatus.UPLOAD.name();
      driver.objectPrepareCreateInBucket(new StorageObject(CLIENT_BUCKET, name, "hash2", 50L, creationTime),
          inpustream50);
      driver.objectFinalizeCreateInBucket(CLIENT_BUCKET, name, 50L, "hash2");
      name = OBJECT_NAME + "ExtraDoc";
      driver.objectPrepareCreateInBucket(new StorageObject(CLIENT_BUCKET, name, "hash3", 100L, creationTime2),
          inpustream100);
      driver.objectFinalizeCreateInBucket(CLIENT_BUCKET, name, 100L, "hash3");
    } catch (DriverException | IOException e) {
      fail(e);
    }
  }

  @Test
  void step1CleanUpNativeListingNoDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }

    // Test
    daoService.step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
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

    //daoRequest.setStart(Instant.now().minusSeconds(1000));
    daoService.step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Upload/Delete Source: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, DELETING, ERR_DEL -> 0;
        case READY -> 1;
        case ERR_UPL -> 2;
        case DELETED -> 3;
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

    daoService.step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1 - 3, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, ERR_UPL, DELETED, ERR_DEL -> 0;
        case UPLOAD, READY, DELETING -> 1;
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
  void step1CleanUpNativeListingNoDateWithObject() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
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
    daoService.step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
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

    daoService.step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Upload/Delete Source: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, ERR_UPL, ERR_DEL -> 0;
        case READY -> 3;
        case DELETING -> 2;
        case DELETED -> 1;
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

    daoService.step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1 - 3, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, ERR_UPL, DELETED, ERR_DEL -> 0;
        case UPLOAD, READY, DELETING -> 1;
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
    daoRequest.setId(REQUEST_ID + 0).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }

    // Test
    daoService.step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
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
    daoService.step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoRequest);
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

    daoService.step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(daoRequest);

    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    assertEquals(countObjectCleanUp1, objectRepository.countAll());
    assertEquals(countNativeCleanUp1 - 3, nativeListingRepository.countAll());
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedNative = switch (status) {
        case UNKNOWN, ERR_UPL, DELETED, ERR_DEL -> 0;
        case UPLOAD, READY, DELETING -> 1;
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
    daoRequest.setId(REQUEST_ID + 0).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), true);
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before: %s", dao);
    }
    // Test with date Now
    daoRequest.setStart(Instant.now());
    requestRepository.updateFull(daoRequest);
    daoService.step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    final var countObjectCleanUp1 = objectRepository.countAll();
    final var countNativeCleanUp1 = nativeListingRepository.countAll();
    assertEquals(6, countObjectCleanUp1);
    assertEquals(6, countNativeCleanUp1);
    daoService.step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Error Now Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Native: %s", dao);
    }
    for (final var status : AccessorStatus.values()) {
      int expected = status.equals(AccessorStatus.UNKNOWN) ? 0 : 1;
      int expectedRstatus = switch (status) {
        case UNKNOWN, UPLOAD, DELETING, ERR_DEL -> 0;
        case READY -> 1;
        case ERR_UPL -> 2;
        case DELETED -> 3;
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
  }

  @Test
  void step2ContinueFromPreviousRequest() throws CcsDbException {
    step1CleanUpNativeListingWithFutureDate();
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 1).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
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
    daoRequest.setId(REQUEST_ID + 0).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), false);

    // Prepare
    daoService.step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    daoService.step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Source: %s", dao);
    }
    assertEquals(0, nativeListingRepository.countAll());

    // Test
    nativeListingRepository.saveNativeListingDb(daoRequest);
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
      Assertions.assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getName(), nativeListing.getName());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      assertNull(nativeListing.getDriver());
    }
  }

  @Test
  void step3SaveNativeListingDbWithDate() throws CcsDbException {
    var daoRequest = requestRepository.createEmptyItem();
    daoRequest.setId(REQUEST_ID + 0).setBucket(CLIENT_BUCKET).setFromSite(FROM_SITE).setClientId(CLIENT_ID);
    requestRepository.insert(daoRequest);
    insertDefaultObjects(daoRequest.getId(), false);
    // Date After so nothing
    daoRequest.setFilter(new AccessorFilter().setCreationAfter(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Prepare
    daoService.step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoRequest);
    daoService.step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoRequest);
    for (final var dao : objectRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Clean Source: %s", dao);
    }
    for (final var dao : nativeListingRepository.findStream(new DbQuery()).toList()) {
      LOGGER.debugf("Before Native: %s", dao);
    }

    // Test
    nativeListingRepository.saveNativeListingDb(daoRequest);

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    LOGGER.debugf("DaoRequest: %s \n\tvs old %s", daoRequestUpdated, daoRequest);
    assertEquals(0, daoRequestUpdated.getCheckedDb());
    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    // Date Before so all
    daoRequest.setFilter(new AccessorFilter().setCreationBefore(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Test
    nativeListingRepository.saveNativeListingDb(daoRequest);

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
      Assertions.assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getName(), nativeListing.getName());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      assertNull(nativeListing.getDriver());
    }
  }

  @Test
  void step4SaveNativeListingDriverNoDate() throws CcsDbException {
    step3SaveNativeListingDbNoDate();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    // Init storage
    var creationTime = Instant.now().minusSeconds(99).truncatedTo(ChronoUnit.MILLIS);
    var creationTime2 = Instant.now().minusSeconds(97).truncatedTo(ChronoUnit.MILLIS);
    createStorageContent(creationTime, creationTime2);

    // Test
    nativeListingRepository.saveNativeListingDriver(daoRequest);

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    assertEquals(AccessorStatus.values().length - 1, daoRequestUpdated.getCheckedDb());
    assertEquals(3, daoRequestUpdated.getCheckedDriver());
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));
    assertEquals(3, nativeListingRepository.count(
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
      Assertions.assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      if (nstatus == DaoService.READY_RANK) {
        assertNotNull(nativeListing.getDriver());
        Assertions.assertEquals(DaoService.READY_RANK, nativeListing.getDriver().nstatus());
        assertEquals(creationTime, nativeListing.getDriver().event());
      } else if (object.getRstatus() != DaoService.ERR_UPL_RANK) {
        assertNull(nativeListing.getDriver());
      }
    }
    var listNative = nativeListingRepository.findStream(new DbQuery()).toList();
    for (final var nativeListing : listNative) {
      LOGGER.debugf("Find %s", nativeListing);
      if (nativeListing.getDriver() != null) {
        if (nativeListing.getDb() != null) {
          Assertions.assertEquals(DaoService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime, nativeListing.getDriver().event());
        } else {
          Assertions.assertEquals(DaoService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime2, nativeListing.getDriver().event());
        }
      }
    }
  }

  @Test
  void step4SaveNativeListingDriverWithDate() throws CcsDbException {
    step3SaveNativeListingDbWithDate();
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
    nativeListingRepository.saveNativeListingDriver(daoRequest);

    var daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    assertEquals(0, daoRequestUpdated.getCheckedDriver());
    assertEquals(AccessorStatus.values().length - 1, nativeListingRepository.count(new DbQuery()));
    assertEquals(0, nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.NEQ, DaoNativeListingRepository.DRIVER, (Object) null)));

    // Creation Before so all
    daoRequest.setFilter(new AccessorFilter().setCreationBefore(Instant.now()));
    requestRepository.updateFull(daoRequest);

    // Test
    nativeListingRepository.saveNativeListingDriver(daoRequest);

    daoRequestUpdated = requestRepository.findWithPk(daoRequest.getId());
    assertEquals(AccessorStatus.values().length - 1, daoRequestUpdated.getCheckedDb());
    assertEquals(3, daoRequestUpdated.getCheckedDriver());
    assertEquals(AccessorStatus.values().length, nativeListingRepository.count(new DbQuery()));
    assertEquals(3, nativeListingRepository.count(
        new DbQuery(RestQuery.QUERY.NEQ, DaoNativeListingRepository.DRIVER, (Object) null)));
    var listObjects = objectRepository.findStream(new DbQuery()).toList();
    for (final var object : listObjects) {
      final var nstatus = object.getRstatus();
      final var nativeListing = nativeListingRepository.findStream(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.DB + "." + DaoNativeListingRepository.NSTATUS,
              nstatus)).filter(dao -> object.getName().equals(dao.getName())).findFirst().get();
      LOGGER.debugf("Try to find STATUS %s with rank %d: \n\t%s", object.getStatus(), nstatus, nativeListing);
      assertEquals(object.getName(), nativeListing.getName());
      Assertions.assertEquals(nstatus, nativeListing.getDb().nstatus());
      assertEquals(object.getCreation(), nativeListing.getDb().event());
      if (nstatus == DaoService.READY_RANK) {
        assertNotNull(nativeListing.getDriver());
        Assertions.assertEquals(DaoService.READY_RANK, nativeListing.getDriver().nstatus());
        assertEquals(creationTime, nativeListing.getDriver().event());
      } else if (object.getRstatus() != DaoService.ERR_UPL_RANK) {
        assertNull(nativeListing.getDriver());
      }
    }
    var listNative = nativeListingRepository.findStream(new DbQuery()).toList();
    for (final var nativeListing : listNative) {
      LOGGER.debugf("Find %s", nativeListing);
      if (nativeListing.getDriver() != null) {
        if (nativeListing.getDb() != null) {
          Assertions.assertEquals(DaoService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime, nativeListing.getDriver().event());
        } else {
          Assertions.assertEquals(DaoService.READY_RANK, nativeListing.getDriver().nstatus());
          assertEquals(creationTime2, nativeListing.getDriver().event());
        }
      }
    }
  }

  @Test
  void step5CompareNativeListingDbDriverSubStep1And2() throws CcsDbException {
    step4SaveNativeListingDriverNoDate();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    final var countOriginal = objectRepository.count(new DbQuery());

    // Test
    daoService.step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest);

    assertEquals(countOriginal + 1, objectRepository.count(new DbQuery()));
    final var nameExtra = OBJECT_NAME + "ExtraDoc";
    var newObject =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertNotNull(newObject);
    LOGGER.infof("Found %s", newObject);
    assertEquals(nameExtra, newObject.getName());
    assertNull(newObject.getHash());
    assertNotNull(newObject.getCreation());
    assertEquals(AccessorStatus.READY, newObject.getStatus());

    // Redo should not create a new one
    objectRepository.deleteWithPk(newObject.getId());
    newObject.setId(GuidLike.getGuid());
    objectRepository.insert(newObject);
    assertEquals(1,
        objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra)));

    // Redo test
    daoService.step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest);

    var newObjectNotChanged =
        objectRepository.findOne(new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, nameExtra));
    assertNotNull(newObjectNotChanged);
    LOGGER.infof("Found %s", newObjectNotChanged);
    assertEquals(nameExtra, newObjectNotChanged.getName());
    assertNull(newObjectNotChanged.getHash());
    assertEquals(newObject.getId(), newObjectNotChanged.getId());

    // Now Step 2 should generate 1 entry in SiteListing with special status
    daoService.step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest);

    assertEquals(1, sitesListingRepository.count(new DbQuery()));
    final var sitesListing = sitesListingRepository.findOne(new DbQuery());
    assertNotNull(sitesListing);
    LOGGER.infof("Found %s", sitesListing);
    assertEquals(nameExtra, sitesListing.getName());
    assertEquals(1, sitesListing.getLocal().size());
    Assertions.assertEquals(DaoService.TO_UPDATE_RANK, sitesListing.getLocal().get(0).nstatus());
    assertEquals(newObjectNotChanged.getSite(), sitesListing.getLocal().get(0).site());
    assertEquals(newObjectNotChanged.getCreation(), sitesListing.getLocal().get(0).event());
  }

  @Test
  @Disabled
  void step5CompareNativeListingDbDriverSubStep3And4() throws CcsDbException {
    step4SaveNativeListingDriverNoDate();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    daoService.step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest);
    daoService.step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest);
    // Test
    daoService.step53UpdateWhereNoDriverIntoObjects(daoRequest);
    daoService.step54UpsertWhereNoDriverIntoSiteListing(daoRequest);
    assertEquals(AccessorStatus.values().length - 2, sitesListingRepository.count(new DbQuery()));
    final var sitesListings = sitesListingRepository.findStream(new DbQuery()).toList();
    for (final var sitesListing : sitesListings) {
      assertEquals(1, sitesListing.getLocal().size());
      final var nativeListing = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, sitesListing.getName()));
      assertNotNull(nativeListing.getDb());
      assertNull(nativeListing.getDriver());
      final var originalStatus = nativeListing.getDb().nstatus();
      final var nstatus = originalStatus == 0 ? 0 :
          (originalStatus == 1 || originalStatus == 2 || originalStatus == 3 || originalStatus == 7 ? 1 : 5);
      LOGGER.debugf("S %d %s %s", nstatus, sitesListing, nativeListing);
      assertEquals(nstatus, sitesListing.getLocal().get(0).nstatus());
      assertEquals(nativeListing.getDb().site(), sitesListing.getLocal().get(0).site());
      assertEquals(nativeListing.getDb().event(), sitesListing.getLocal().get(0).event());
    }
  }

  @Test
  @Disabled
  void step5CompareNativeListingDbDriverSubStep5And6() throws CcsDbException {
    step4SaveNativeListingDriverNoDate();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);
    daoService.step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest);
    daoService.step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest);
    daoService.step53UpdateWhereNoDriverIntoObjects(daoRequest);
    daoService.step54UpsertWhereNoDriverIntoSiteListing(daoRequest);

    // Test
    daoService.step55UpdateBothDbDriverIntoObjects(daoRequest);
    daoService.step56UpdateBothDbDriverIntoSiteListing(daoRequest);

    assertEquals(2, sitesListingRepository.count(new DbQuery()));
    final var sitesListings = sitesListingRepository.findStream(new DbQuery()).toList();
    for (final var sitesListing : sitesListings) {
      assertEquals(1, sitesListing.getLocal().size());
      final var nativeListing = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, sitesListing.getName()));
      assertNotNull(nativeListing.getDriver());
      assertNotNull(nativeListing.getDb());
      final var dbStatus = nativeListing.getDb().nstatus();
      final var driverStatus = nativeListing.getDriver().nstatus();
      final var dbEvent = nativeListing.getDb().event();
      final var driverEvent = nativeListing.getDriver().event();
      final var nstatus = dbEvent.compareTo(driverEvent) > 0 ? dbStatus : driverStatus;
      final var event = dbEvent.compareTo(driverEvent) > 0 ? dbEvent : driverEvent;
      assertEquals(nstatus, sitesListing.getLocal().get(0).nstatus());
      assertEquals(nativeListing.getDb().site(), sitesListing.getLocal().get(0).site());
      assertEquals(event, sitesListing.getLocal().get(0).event());
    }
  }

  @Test
  @Disabled
  void step5CompareNativeListingDbDriverSubSteps() throws CcsDbException {
    step4SaveNativeListingDriverNoDate();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    // Test
    nativeListingRepository.compareNativeListingDbDriver(daoRequest);

    assertEquals(AccessorStatus.values().length + 1, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length + 1, daoRequest.getChecked());
    final var sitesListings = sitesListingRepository.findStream(new DbQuery()).toList();
    for (final var sitesListing : sitesListings) {
      assertEquals(1, sitesListing.getLocal().size());
      final var nativeListing = nativeListingRepository.findOne(
          new DbQuery(RestQuery.QUERY.EQ, DaoNativeListingRepository.NAME, sitesListing.getName()));
      if (nativeListing.getDriver() == null) {
        assertNotNull(nativeListing.getDb());
        final var originalStatus = nativeListing.getDb().nstatus();
        final var nstatus =
            originalStatus == 0 || originalStatus == 3 ? 0 : (originalStatus == 1 || originalStatus == 2 ? 1 : 5);
        assertEquals(nstatus, sitesListing.getLocal().get(0).nstatus());
        assertEquals(nativeListing.getDb().site(), sitesListing.getLocal().get(0).site());
        assertEquals(nativeListing.getDb().event(), sitesListing.getLocal().get(0).event());
      } else {
        if (nativeListing.getDb() == null) {
          final var nameExtra = OBJECT_NAME + "ExtraDoc";
          assertEquals(nameExtra, sitesListing.getName());
          assertEquals(1, sitesListing.getLocal().size());
          Assertions.assertEquals(DaoService.TO_UPDATE_RANK, sitesListing.getLocal().get(0).nstatus());
          assertEquals(nativeListing.getDriver().site(), sitesListing.getLocal().get(0).site());
          assertEquals(nativeListing.getDriver().event(), sitesListing.getLocal().get(0).event());
        } else {
          final var dbStatus = nativeListing.getDb().nstatus();
          final var driverStatus = nativeListing.getDriver().nstatus();
          final var dbEvent = nativeListing.getDb().event();
          final var driverEvent = nativeListing.getDriver().event();
          final var nstatus = dbEvent.compareTo(driverEvent) > 0 ? dbStatus : driverStatus;
          final var event = dbEvent.compareTo(driverEvent) > 0 ? dbEvent : driverEvent;
          assertEquals(nstatus, sitesListing.getLocal().get(0).nstatus());
          assertEquals(nativeListing.getDb().site(), sitesListing.getLocal().get(0).site());
          assertEquals(event, sitesListing.getLocal().get(0).event());
        }
      }
    }
  }

  @Test
  @Disabled
  void step6GetSiteListingStep7SaveRemoteNativeListing() throws CcsDbException {
    step4SaveNativeListingDriverNoDate();
    // First and only one
    var daoRequest = requestRepository.findOne(new DbQuery());
    LOGGER.debugf("DaoRequest: %s", daoRequest);

    // Test
    nativeListingRepository.compareNativeListingDbDriver(daoRequest);

    assertEquals(AccessorStatus.values().length + 1, sitesListingRepository.count(new DbQuery()));
    assertEquals(AccessorStatus.values().length + 1, nativeListingRepository.count(new DbQuery()));

    // Test
    nativeListingRepository.cleanNativeListing(daoRequest);

    assertEquals(0, nativeListingRepository.count(new DbQuery()));

    final var iterator = sitesListingRepository.getSiteListing(daoRequest);
    final var list = StreamIteratorUtils.getListFromIterator(iterator);
    assertEquals(AccessorStatus.values().length + 1, list.size());

    // Test
    sitesListingRepository.cleanSitesListing(daoRequest);

    assertEquals(0, sitesListingRepository.count(new DbQuery()));

    // Test
    sitesListingRepository.saveRemoteNativeListing(daoRequest, list.iterator());

    assertEquals(AccessorStatus.values().length + 1, sitesListingRepository.count(new DbQuery()));
    daoRequest = requestRepository.findOne(new DbQuery());
    assertEquals(AccessorStatus.values().length + 1, daoRequest.getCheckedRemote());

    final var sitesListings = sitesListingRepository.findStream(new DbQuery()).toList();
    for (final var sitesListing : sitesListings) {
      assertEquals(1, sitesListing.getLocal().size());
      final var local = sitesListing.getLocal().get(0);
      if (local.nstatus() == DaoService.TO_UPDATE_RANK) {
        final var nameExtra = OBJECT_NAME + "ExtraDoc";
        assertEquals(nameExtra, sitesListing.getName());
      }
    }
  }
}
