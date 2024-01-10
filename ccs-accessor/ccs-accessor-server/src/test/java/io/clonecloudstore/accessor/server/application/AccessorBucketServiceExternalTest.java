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

package io.clonecloudstore.accessor.server.application;

import java.util.UUID;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.FakeActionTopicConsumer;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.resource.MinioMongoKafkaProfile;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MinioMongoKafkaProfile.class)
class AccessorBucketServiceExternalTest {

  @Inject
  AccessorBucketService service;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  @Inject
  DriverApiFactory storageDriverFactory;

  String clientId = null;

  @BeforeAll
  static void setup() {
    // Setup Minio S3 Driver
    // Bug fix on "localhost"
    var url = MinIoResource.getUrlString();
    if (url.contains("localhost")) {
      url = url.replace("localhost", "127.0.0.1");
    }
    DriverS3Properties.setDynamicS3Parameters(url, MinIoResource.getAccessKey(), MinIoResource.getSecretKey(),
        MinIoResource.getRegion());
  }

  @BeforeEach
  public void cleanBeforeTest() throws CcsDbException {    //Generate fake client id
    clientId = UUID.randomUUID().toString();
    bucketRepository = bucketRepositoryInstance.get();
    //Clean database
    bucketRepository.deleteAllDb();
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
    FakeActionTopicConsumer.reset();
  }

  @Test
  void createBucket() {

    try {
      final var bucketName = "bucketcreatetest";
      final var bucketTechnicalName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);

      //Create bucket
      final var bucket = service.createBucket(clientId, bucketTechnicalName, true);
      Assertions.assertEquals(bucketName, bucket.getName());
      Assertions.assertEquals(bucketTechnicalName, bucket.getId());
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());

      //Check can't create 2 bucket for one client.
      assertThrows(CcsAlreadyExistException.class, () -> service.createBucket(clientId, bucketTechnicalName, true));

      //Check can't create bucket with Maj
      final var bucketNameMaj = "bucketCreateTest";
      assertThrows(CcsOperationException.class, () -> service.createBucket(clientId, bucketNameMaj, true));

      //Check can't create bucket with special char
      final var bucketNameSpecialChar = "bucket*";
      assertThrows(CcsOperationException.class, () -> service.createBucket(clientId, bucketNameSpecialChar, true));


      //Check min limit (min allowed 3)
      final var bucketNameMin = "b";
      assertThrows(CcsOperationException.class, () -> service.createBucket(clientId, bucketNameMin, true));

      //Check max limit (max allowed 31)
      final var bucketNameMax = "uguoktjpshccqaqgyeiekwocfpupbhblvgkykuaosnjhrsylpqawndmjxw";
      final var technicalId = DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucketNameMax);
      final Exception exceptionMax =
          assertThrows(CcsOperationException.class, () -> service.createBucket(clientId, technicalId, true));
      assertNotNull(exceptionMax);


    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private long getBucketCreateFromTopic(final long desired) throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      final var value = FakeActionTopicConsumer.getBucketCreate();
      if (value >= desired) {
        Log.infof("Found %d", value);
        return value;
      }
      Thread.sleep(10);
    }
    return FakeActionTopicConsumer.getBucketCreate();
  }

  @Test
  void getBucket() throws CcsDbException {

    final var unknownBucketTechnicalName =
        DaoAccessorBucketRepository.getBucketTechnicalName(clientId, "unknownbucket");
    //Get Bucket not exist
    final Exception exceptionMin = assertThrows(CcsNotExistException.class,
        () -> service.getBucket(unknownBucketTechnicalName, clientId, GuidLike.getGuid(), true));
    assertNotNull(exceptionMin);
    assertFalse(service.checkBucket(unknownBucketTechnicalName, false, clientId, GuidLike.getGuid(), true));
    // Check remote
    assertTrue(AccessorProperties.isRemoteRead());
    assertTrue(AccessorProperties.isFixOnAbsent());
    try {
      FakeCommonBucketResourceHelper.errorCode = 204;
      assertTrue(service.checkBucket(unknownBucketTechnicalName, false, clientId, GuidLike.getGuid(), true));
      assertEquals(0, FakeActionTopicConsumer.getBucketCreate());
      FakeCommonBucketResourceHelper.errorCode = 200;
      final var bucket = service.getBucket(unknownBucketTechnicalName, clientId, GuidLike.getGuid(), true);
      assertEquals(unknownBucketTechnicalName, bucket.getId());
      assertEquals(1, getBucketCreateFromTopic(1));
      // Only getBucket try to fix from the remote, so increment value
      service.getBucket(unknownBucketTechnicalName, clientId, GuidLike.getGuid(), true);
      assertEquals(2, getBucketCreateFromTopic(2));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      FakeCommonBucketResourceHelper.errorCode = 0;
      FakeCommonObjectResourceHelper.errorCode = 0;
    }
    // Same but fix if absent is off
    assertTrue(AccessorProperties.isRemoteRead());
    AccessorProperties.setFixOnAbsent(false);
    assertFalse(AccessorProperties.isFixOnAbsent());
    try {
      FakeCommonBucketResourceHelper.errorCode = 204;
      assertTrue(service.checkBucket(unknownBucketTechnicalName, false, clientId, GuidLike.getGuid(), true));
      assertEquals(2, FakeActionTopicConsumer.getBucketCreate());
      FakeCommonBucketResourceHelper.errorCode = 200;
      assertThrows(CcsNotExistException.class,
          () -> service.getBucket(unknownBucketTechnicalName, clientId, GuidLike.getGuid(), true));
      Thread.sleep(100);
      assertEquals(2, getBucketCreateFromTopic(2));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      FakeCommonBucketResourceHelper.errorCode = 0;
      FakeCommonObjectResourceHelper.errorCode = 0;
      AccessorProperties.setFixOnAbsent(true);
    }
    // Same but remote is off
    AccessorProperties.setRemoteRead(false);
    assertFalse(AccessorProperties.isRemoteRead());
    assertTrue(AccessorProperties.isFixOnAbsent());
    try {
      FakeCommonBucketResourceHelper.errorCode = 204;
      assertFalse(service.checkBucket(unknownBucketTechnicalName, false, clientId, GuidLike.getGuid(), true));
      assertEquals(2, FakeActionTopicConsumer.getBucketCreate());
      FakeCommonBucketResourceHelper.errorCode = 200;
      assertThrows(CcsNotExistException.class,
          () -> service.getBucket(unknownBucketTechnicalName, clientId, GuidLike.getGuid(), true));
      Thread.sleep(100);
      assertEquals(2, getBucketCreateFromTopic(2));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      FakeCommonBucketResourceHelper.errorCode = 0;
      FakeCommonObjectResourceHelper.errorCode = 0;
      AccessorProperties.setRemoteRead(true);
    }

    //Create a bucket and get Bucket
    final var bucketName = "testgetbucket";
    final var opId = GuidLike.getGuid();
    final var bucketTechnicalName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);
    final AccessorBucket bucket;
    service.createBucket(clientId, bucketTechnicalName, true);
    bucket = service.getBucket(bucketTechnicalName, clientId, opId, true);
    assertEquals(bucketName, bucket.getName());
    assertEquals(AccessorStatus.READY, bucket.getStatus());
    assertTrue(service.checkBucket(bucketTechnicalName, false, clientId, GuidLike.getGuid(), true));

    //Check read bucket deleted
    service.deleteBucket(clientId, bucketTechnicalName, true);

    assertThrows(CcsDeletedException.class, () -> service.getBucket(bucketTechnicalName, clientId, opId, true));
    assertFalse(service.checkBucket(bucketTechnicalName, false, clientId, GuidLike.getGuid(), true));

    final var bucketNameStatusNotA = "bucketnotavailable";
    final var technicalBucketNameNotA = DaoAccessorBucketRepository.getPrefix(clientId) + bucketNameStatusNotA;
    final AccessorBucket bucketStatusNotAvailable;
    bucketStatusNotAvailable = service.createBucket(clientId, technicalBucketNameNotA, true);
    assertEquals(bucketNameStatusNotA, bucketStatusNotAvailable.getName());
    assertEquals(AccessorStatus.READY, bucketStatusNotAvailable.getStatus());
    assertTrue(service.checkBucket(technicalBucketNameNotA, false, clientId, GuidLike.getGuid(), true));

    //Change status to simulate an operation
    final var bucketTmp = bucketRepository.findBucketById(technicalBucketNameNotA);
    bucketRepository.updateBucketStatus(bucketTmp, AccessorStatus.DELETING, null);

    assertThrows(CcsOperationException.class, () -> service.getBucket(technicalBucketNameNotA, clientId, opId, true));
    assertFalse(service.checkBucket(technicalBucketNameNotA, false, clientId, GuidLike.getGuid(), true));
  }

  @Test
  void getBuckets() throws CcsOperationException {
    //Create buckets
    final var bucketName = "testgetbuckets";

    final var prefixBucketTechnicalName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);
    service.createBucket(clientId, prefixBucketTechnicalName + "1", true);
    service.createBucket(clientId, prefixBucketTechnicalName + "2", true);
    service.createBucket(clientId, prefixBucketTechnicalName + "3", true);

    final var buckets = service.getBuckets(clientId);
    assertEquals(3, buckets.size());
    for (final var bucket : buckets) {
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());
    }
  }

  @Test
  void deleteBucket() throws CcsDbException {
    final var bucketName = "bucketdeletedtest";
    final AccessorBucket bucket;
    {
      final var bucketTechnicalName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);
      bucket = service.createBucket(clientId, bucketTechnicalName, true);
      assertEquals(bucketName, bucket.getName());
      assertEquals(bucketTechnicalName, bucket.getId());
      assertEquals(AccessorStatus.READY, bucket.getStatus());
    }
    //Delete bucket already
    {
      final var bucketTechnicalName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);
      final var bucketDeleted = service.deleteBucket(clientId, bucketTechnicalName, true);
      Assertions.assertEquals(AccessorStatus.DELETED, bucketDeleted.getStatus());
      final Exception exceptionDeleted =
          assertThrows(CcsDeletedException.class, () -> service.deleteBucket(clientId, bucketTechnicalName, true));
      assertNotNull(exceptionDeleted);
      //Delete bucket already deleted
      try (final var storageDriver = storageDriverFactory.getInstance()) {
        // retrieving metadata from storage, would data be duplicated in database
        final Exception exceptionDeletedStorage =
            assertThrows(DriverNotFoundException.class, () -> storageDriver.bucketDelete(bucketDeleted.getId()));
        assertNotNull(exceptionDeletedStorage);
      }
    }
    //Delete unknown Bucket
    final var unknownBucketTechnicalName =
        DaoAccessorBucketRepository.getBucketTechnicalName(clientId, "unknownBucket");
    final Exception exceptionMin = assertThrows(CcsNotExistException.class,
        () -> service.deleteBucket(clientId, unknownBucketTechnicalName, true));
    assertNotNull(exceptionMin);

    //Delete bucket not AVAILABLE or DELETED status
    final var bucketNameStatusNotAD = "bucketnotavailablerdeleted";
    final var bucketTechnicalNameNotAD =
        DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucketNameStatusNotAD);

    final AccessorBucket bucketStatusNotAD;
    bucketStatusNotAD = service.createBucket(clientId, bucketTechnicalNameNotAD, true);
    assertEquals(bucketNameStatusNotAD, bucketStatusNotAD.getName());
    assertEquals(AccessorStatus.READY, bucketStatusNotAD.getStatus());

    final var technicalBucketName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);
    //Change status to simulate an operation
    final var bucketTmp = bucketRepository.findBucketById(technicalBucketName);
    bucketRepository.updateBucketStatus(bucketTmp, AccessorStatus.DELETING, null);
    final var bucketTechnicalName = DaoAccessorBucketRepository.getFinalBucketName(clientId, bucketName, true);
    final Exception exceptionStatusAD =
        assertThrows(CcsOperationException.class, () -> service.deleteBucket(clientId, bucketTechnicalName, true));
    assertNotNull(exceptionStatusAD);
  }
}
