/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.reconciliator.server;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.accessor.model.AccessorBucket;
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
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.client.ReconciliatorApiFactory;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoRequestRepository;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoSitesListingRepository;
import io.clonecloudstore.reconciliator.fake.FakeRequestTopicConsumer;
import io.clonecloudstore.reconciliator.fake.MockLocalReplicatorApiClient;
import io.clonecloudstore.reconciliator.model.ReconciliationAction;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClient;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClientFactory;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.MongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusMock;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class ReconciliationResourceTest {
  private static final Logger LOGGER = Logger.getLogger(ReconciliationResourceTest.class);
  @Inject
  ReconciliatorApiFactory factory;
  private static final String BUCKET = "mybucket";
  private static final String CLIENT_ID = "client-id";
  private static final String OBJECT_NAME = "dir/object_";
  private static final String SITE2 = "site2";
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
  DriverApiFactory storageDriverFactory;
  static AtomicBoolean init = new AtomicBoolean(false);
  static String requestId = null;

  static MockLocalReplicatorApiClient mock;
  static LocalReplicatorApiClientFactory customMock;

  @BeforeEach
  void beforeEach() throws CcsDbException {
    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
    requestRepository = requestRepositoryInstance.get();
    nativeListingRepository = nativeListingRepositoryInstance.get();
    sitesListingRepository = sitesListingRepositoryInstance.get();
    sitesActionRepository = sitesActionRepositoryInstance.get();
    if (init.compareAndSet(false, true)) {
      ((MgDaoAccessorBucketRepository) bucketRepository).createIndex();
      ((MgDaoAccessorObjectRepository) objectRepository).createIndex();
      ((MgDaoRequestRepository) requestRepository).createIndex();
      ((MgDaoNativeListingRepository) nativeListingRepository).createIndex();
      ((MgDaoSitesListingRepository) sitesListingRepository).createIndex();
      ((MgDaoSitesActionRepository) sitesActionRepository).createIndex();
      deleteAll();
      mock = new MockLocalReplicatorApiClient();
      customMock = new LocalReplicatorApiClientFactory() {
        @Override
        public LocalReplicatorApiClient newClient() {
          return mock;
        }
      };
    }

    QuarkusMock.installMockForType(customMock, LocalReplicatorApiClientFactory.class);
  }

  private void deleteAll() throws CcsDbException {
    LOGGER.infof("Clean up");
    bucketRepository.deleteAllDb();
    objectRepository.deleteAllDb();
    requestRepository.deleteAllDb();
    nativeListingRepository.deleteAllDb();
    sitesListingRepository.deleteAllDb();
    sitesActionRepository.deleteAllDb();
    FakeDriverFactory.cleanUp();
    FakeRequestTopicConsumer.reset();
  }

  @Test
  void step1CreateNewRequest() throws CcsDbException {
    MockLocalReplicatorApiClient.runClient = false;
    final var request = new ReconciliationRequest(null, CLIENT_ID, BUCKET, null, ServiceProperties.getAccessorSite(),
        ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), SITE2), true);
    insertDefaultObjects();
    try (var client = factory.newClient()) {
      LOGGER.infof("Start Create Request Central for %s", request);
      requestId = client.createRequestCentral(request);
      assertNotNull(requestId);
      LOGGER.infof("Created Request Central: %s", requestId);
      var dto = client.getRequestStatus(requestId);
      var dao = requestRepository.findWithPk(requestId);
      assertEquals(dao.getDto(), dto);
      while (dao.getChecked() == 0 || dao.getCheckedDb() == 0 || dao.getCheckedDriver() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      LOGGER.infof("Created Request Central: %s", dao);
      while (nativeListingRepository.countAll() != 0) {
        Thread.sleep(100);
      }
      assertNotEquals(0, sitesListingRepository.countAll());
      sitesListingRepository.findStream(new DbQuery()).forEach(daoSitesListing -> LOGGER.debugf("Site: [%s] %s",
          ReconciliationAction.fromStatusCode(daoSitesListing.getLocal().getFirst().nstatus()), daoSitesListing));
      dto = client.getRequestStatus(requestId);
      assertEquals(dao.getDto(), dto);
    } catch (Exception e) {
      LOGGER.error(e, e);
      fail(e);
    }
  }

  @Test
  void step2CreateLocalRequest() throws CcsDbException {
    MockLocalReplicatorApiClient.runClient = false;
    LOGGER.infof("Start with %s", requestId);
    var dao = requestRepository.findWithPk(requestId);
    var dto = dao.setCurrentSite(SITE2).setChecked(0).setCheckedDb(0).setCheckedDriver(0).getDto();
    requestRepository.deleteWithPk(requestId);
    try (var client = factory.newClient()) {
      client.createRequestLocal(dto);
      LOGGER.infof("Created Request Local: %s", requestId);
      dto = client.getRequestStatus(requestId);
      dao = requestRepository.findWithPk(requestId);
      assertEquals(dao.getDto(), dto);
      while (dao.getChecked() == 0 || dao.getCheckedDb() == 0 || dao.getCheckedDriver() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      LOGGER.infof("Created Request Local: %s", dao);
      while (nativeListingRepository.countAll() != 0) {
        Thread.sleep(100);
      }
      assertNotEquals(0, sitesListingRepository.countAll());
      sitesListingRepository.findStream(new DbQuery()).forEach(daoSitesListing -> LOGGER.debugf("Site: [%s] %s",
          ReconciliationAction.fromStatusCode(daoSitesListing.getLocal().getFirst().nstatus()), daoSitesListing));
      dto = client.getRequestStatus(requestId);
      assertEquals(dao.getDto(), dto);
    } catch (Exception e) {
      LOGGER.warn(e, e);
      fail(e);
    }
  }

  @Test
  void step3ImportLocalSites() throws CcsDbException {
    MockLocalReplicatorApiClient.runClient = false;
    MockLocalReplicatorApiClient.semaphore.drainPermits();
    deleteAll();
    final var request = new ReconciliationRequest(null, CLIENT_ID, BUCKET, null, ServiceProperties.getAccessorSite(),
        ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), SITE2), true);
    insertDefaultObjects();
    try (var client = factory.newClient()) {
      requestId = client.createRequestCentral(request);
      assertNotNull(requestId);
      MockLocalReplicatorApiClient.semaphore.acquire();
      var dao = requestRepository.findWithPk(requestId);
      while (dao.getChecked() == 0 || dao.getCheckedDb() == 0 || dao.getCheckedDriver() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      LOGGER.infof("Simulate End Request Local %s from %s", requestId, SITE2);
      MockLocalReplicatorApiClient.runClient = true;
      client.endRequestLocal(requestId, SITE2);
      MockLocalReplicatorApiClient.semaphore.acquire();
      dao = requestRepository.findWithPk(requestId);
      while (dao.getCheckedRemote() == 0 || dao.getActions() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      LOGGER.infof("End Dry Run Request %s", dao);
      assertEquals(7, dao.getCheckedRemote());
      assertEquals(2, dao.getActions());
    } catch (Exception e) {
      LOGGER.warn(e, e);
      fail(e);
    }
  }

  @Test
  void step4InformRemoteSites() throws CcsDbException {
    MockLocalReplicatorApiClient.runClient = false;
    MockLocalReplicatorApiClient.semaphore.drainPermits();
    deleteAll();
    final var request = new ReconciliationRequest(null, CLIENT_ID, BUCKET, null, ServiceProperties.getAccessorSite(),
        ServiceProperties.getAccessorSite(), List.of(ServiceProperties.getAccessorSite(), SITE2), true);
    insertDefaultObjects();
    try (var client = factory.newClient()) {
      requestId = client.createRequestCentral(request);
      assertNotNull(requestId);
      MockLocalReplicatorApiClient.semaphore.acquire();
      var dao = requestRepository.findWithPk(requestId);
      while (dao.getChecked() == 0 || dao.getCheckedDb() == 0 || dao.getCheckedDriver() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      MockLocalReplicatorApiClient.runClient = true;
      client.endRequestLocal(requestId, SITE2);
      MockLocalReplicatorApiClient.semaphore.acquire();
      dao = requestRepository.findWithPk(requestId);
      while (dao.getCheckedRemote() == 0 || dao.getActions() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      LOGGER.infof("Send End Central %s", requestId);
      dao.setActions(0);
      requestRepository.updateFull(dao);
      client.endRequestCentral(requestId);
      client.getLocalRequestStatus(requestId);
      MockLocalReplicatorApiClient.semaphore.acquire();
      dao = requestRepository.findWithPk(requestId);
      while (dao.getActions() == 0) {
        Thread.sleep(100);
        dao = requestRepository.findWithPk(requestId);
      }
      LOGGER.infof("Simulated actions for %s", dao);
    } catch (Exception e) {
      LOGGER.warn(e, e);
      fail(e);
    }
  }

  private DaoAccessorObject insertNewObject(final String name, final AccessorStatus status) {
    final var daoObject = objectRepository.createEmptyItem();
    daoObject.setId(GuidLike.getGuid()).setBucket(BUCKET).setName(name).setCreation(Instant.now().minusSeconds(100))
        .setSite(ServiceProperties.getAccessorSite()).setStatus(status).setHash("hash");
    assertDoesNotThrow(() -> objectRepository.insert(daoObject));
    return daoObject;
  }

  private void insertDefaultObjects() throws CcsDbException {
    for (final var status : AccessorStatus.values()) {
      final String name = OBJECT_NAME + status.name();
      final var daoObject = insertNewObject(name, status);
    }
    // Init storage
    var creationTime = Instant.now().minusSeconds(99).truncatedTo(ChronoUnit.MILLIS);
    var creationTime2 = Instant.now().minusSeconds(97).truncatedTo(ChronoUnit.MILLIS);
    createStorageContent(creationTime, creationTime2);
    for (final var status : AccessorStatus.values()) {
      assertEquals(1,
          objectRepository.count(new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, status)));
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

}
