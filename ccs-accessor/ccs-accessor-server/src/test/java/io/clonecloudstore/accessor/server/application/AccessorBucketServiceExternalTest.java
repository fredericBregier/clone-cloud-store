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
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.resource.AzureMongoKafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(AzureMongoKafkaProfile.class)
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
  }

  @BeforeEach
  public void cleanBeforeTest() {
    //Generate fake client id
    clientId = UUID.randomUUID().toString();
    bucketRepository = bucketRepositoryInstance.get();
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
    FakeActionTopicConsumer.reset();
    // Clean all
    CleanupTestUtil.cleanUp();
  }

  @Test
  void createBucket() {

    try {
      final var bucketName = "bucketcreatetest";
      //Create bucket
      final var bucket = service.createBucket(bucketName, clientId, true);
      Assertions.assertEquals(bucketName, bucket.getId());
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());

      //Check can't recreate bucket
      assertThrows(CcsAlreadyExistException.class, () -> service.createBucket(bucketName, clientId, true));

      //Check can't create bucket with Maj
      final var bucketNameMaj = "bucketCreateTest";
      assertThrows(CcsOperationException.class, () -> service.createBucket(bucketNameMaj, clientId, true));

      //Check can't create bucket with special char
      final var bucketNameSpecialChar = "bucket*";
      assertThrows(CcsOperationException.class, () -> service.createBucket(bucketNameSpecialChar, clientId, true));


      //Check min limit (min allowed 3)
      final var bucketNameMin = "b";
      assertThrows(CcsOperationException.class, () -> service.createBucket(bucketNameMin, clientId, true));

      //Check max limit (max allowed 63)
      final var bucketNameMax =
          "uguoktjpshccqaqgyeiekwocfpupbhblvgkykuaosnjhrsylpqawndmjxwuguoktjpshccqaqgyeiekwocfpupbhblvgkykuaosnjhrsylpqawndmjxwzzz";
      final Exception exceptionMax =
          assertThrows(CcsOperationException.class, () -> service.createBucket(bucketNameMax, clientId, true));
      assertNotNull(exceptionMax);
      assertNotNull(service.deleteBucket(bucketName, clientId, true));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void getBucket() throws CcsDbException {
    var unknownBucketName = "unknownbucket";
    //Get Bucket not exist
    final Exception exceptionMin = assertThrows(CcsNotExistException.class,
        () -> service.getBucket(unknownBucketName, clientId, GuidLike.getGuid(), true));
    assertNotNull(exceptionMin);
    assertFalse(service.checkBucket(unknownBucketName, false, clientId, GuidLike.getGuid(), true));
    // Check remote
    assertTrue(AccessorProperties.isRemoteRead());
    assertTrue(AccessorProperties.isFixOnAbsent());
    try {
      FakeCommonBucketResourceHelper.errorCode = 204;
      assertTrue(service.checkBucket(unknownBucketName, false, clientId, GuidLike.getGuid(), true));
      assertEquals(0, FakeActionTopicConsumer.getBucketCreate());
      FakeCommonBucketResourceHelper.errorCode = 200;
      final var bucket = service.getBucket(unknownBucketName, clientId, GuidLike.getGuid(), true);
      assertEquals(unknownBucketName, bucket.getId());
      assertEquals(2, FakeActionTopicConsumer.getBucketCreateFromTopic(2));
      // Only getBucket try to fix from the remote, so increment value
      service.getBucket(unknownBucketName, clientId, GuidLike.getGuid(), true);
      assertEquals(3, FakeActionTopicConsumer.getBucketCreateFromTopic(3));
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
      assertTrue(service.checkBucket(unknownBucketName, false, clientId, GuidLike.getGuid(), true));
      assertEquals(3, FakeActionTopicConsumer.getBucketCreate());
      FakeCommonBucketResourceHelper.errorCode = 200;
      assertDoesNotThrow(() -> service.getBucket(unknownBucketName, clientId, GuidLike.getGuid(), true));
      Thread.sleep(100);
      assertEquals(3, FakeActionTopicConsumer.getBucketCreateFromTopic(3));
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
      assertFalse(service.checkBucket(unknownBucketName, false, clientId, GuidLike.getGuid(), true));
      assertEquals(3, FakeActionTopicConsumer.getBucketCreate());
      FakeCommonBucketResourceHelper.errorCode = 200;
      assertThrows(CcsNotExistException.class,
          () -> service.getBucket(unknownBucketName, clientId, GuidLike.getGuid(), true));
      Thread.sleep(100);
      assertEquals(3, FakeActionTopicConsumer.getBucketCreateFromTopic(3));
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
    final AccessorBucket bucket;
    service.createBucket(bucketName, clientId, true);
    bucket = service.getBucket(bucketName, clientId, opId, true);
    assertEquals(bucketName, bucket.getId());
    assertEquals(AccessorStatus.READY, bucket.getStatus());
    assertTrue(service.checkBucket(bucketName, false, clientId, GuidLike.getGuid(), true));

    //Check read bucket deleted
    service.deleteBucket(bucketName, clientId, true);

    assertThrows(CcsDeletedException.class, () -> service.getBucket(bucketName, clientId, opId, true));
    assertFalse(service.checkBucket(bucketName, false, clientId, GuidLike.getGuid(), true));

    final var bucketNameStatusNotA = "bucketnotavailable";
    final AccessorBucket bucketStatusNotAvailable;
    bucketStatusNotAvailable = service.createBucket(bucketNameStatusNotA, clientId, true);
    assertEquals(bucketNameStatusNotA, bucketStatusNotAvailable.getId());
    assertEquals(AccessorStatus.READY, bucketStatusNotAvailable.getStatus());
    assertTrue(service.checkBucket(bucketNameStatusNotA, false, clientId, GuidLike.getGuid(), true));

    //Change status to simulate an operation
    final var bucketTmp = bucketRepository.findBucketById(bucketNameStatusNotA);
    bucketRepository.updateBucketStatus(bucketTmp, AccessorStatus.DELETING, null);

    assertThrows(CcsOperationException.class, () -> service.getBucket(bucketNameStatusNotA, clientId, opId, true));
    assertFalse(service.checkBucket(bucketNameStatusNotA, false, clientId, GuidLike.getGuid(), true));
    bucketRepository.updateBucketStatus(bucketTmp, AccessorStatus.READY, null);
    service.deleteBucket(bucketNameStatusNotA, clientId, true);
  }

  @Test
  void getBuckets() throws CcsOperationException {
    //Create buckets
    final var bucketsBeforeTest = service.getBuckets(clientId);
    final var numberBucketBeforeInsert = bucketsBeforeTest.size();
    final var bucketName = "testgetbuckets";
    service.createBucket(bucketName + "1", clientId, true);
    service.createBucket(bucketName + "2", clientId, true);
    service.createBucket(bucketName + "3", clientId, true);

    final var buckets = service.getBuckets(clientId);
    assertEquals(numberBucketBeforeInsert + 3, buckets.size());
    for (final var bucket : buckets) {
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());
      service.deleteBucket(bucket.getId(), clientId, true);
    }
  }

  @Test
  void deleteBucket() throws CcsDbException {
    final var bucketName = "bucketdeletedtest";
    final AccessorBucket bucket;
    {
      bucket = service.createBucket(bucketName, clientId, true);
      assertEquals(bucketName, bucket.getId());
      assertEquals(AccessorStatus.READY, bucket.getStatus());
    }
    //Delete bucket already
    {
      final var bucketDeleted = service.deleteBucket(bucketName, clientId, true);
      Assertions.assertEquals(AccessorStatus.DELETED, bucketDeleted.getStatus());
      final Exception exceptionDeleted =
          assertThrows(CcsDeletedException.class, () -> service.deleteBucket(bucketName, clientId, true));
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
    final Exception exceptionMin =
        assertThrows(CcsNotExistException.class, () -> service.deleteBucket("unknownBucket", clientId, true));
    assertNotNull(exceptionMin);

    //Delete bucket not AVAILABLE or DELETED status
    final var bucketNameStatusNotAD = "bucketnotavailablerdeleted";
    final AccessorBucket bucketStatusNotAD;
    bucketStatusNotAD = service.createBucket(bucketNameStatusNotAD, clientId, true);
    assertEquals(bucketNameStatusNotAD, bucketStatusNotAD.getId());
    assertEquals(AccessorStatus.READY, bucketStatusNotAD.getStatus());

    //Change status to simulate an operation
    final var bucketTmp = bucketRepository.findBucketById(bucketNameStatusNotAD);
    bucketRepository.updateBucketStatus(bucketTmp, AccessorStatus.DELETING, null);
    final Exception exceptionStatusAD =
        assertThrows(CcsOperationException.class, () -> service.deleteBucket(bucketNameStatusNotAD, clientId, true));
    assertNotNull(exceptionStatusAD);
    bucketRepository.updateBucketStatus(bucketTmp, AccessorStatus.READY, null);
    service.deleteBucket(bucketNameStatusNotAD, clientId, true);
  }
}
