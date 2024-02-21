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

package io.clonecloudstore.accessor.server.simple.application;

import java.util.UUID;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.test.resource.azure.AzureProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
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
@TestProfile(AzureProfile.class)
class AccessorBucketServiceExternalTest {
  @Inject
  AccessorBucketService service;
  @Inject
  DriverApiFactory storageDriverFactory;
  String clientId = null;

  @BeforeAll
  static void setup() {
  }

  @BeforeEach
  public void cleanBeforeTest() {
    //Generate fake client id
    clientId = UUID.randomUUID().toString();
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

      //Check max limit (max allowed 31)
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
  void getBucket() {
    //Get Bucket not exist
    final Exception exceptionMin = assertThrows(CcsNotExistException.class,
        () -> service.getBucket("unknownbucket", clientId, GuidLike.getGuid(), true));
    assertNotNull(exceptionMin);
    assertFalse(service.checkBucket("unknownbucket", true, clientId, GuidLike.getGuid(), true));
    assertFalse(AccessorProperties.isRemoteRead());
    assertFalse(AccessorProperties.isFixOnAbsent());

    //Create a bucket and get Bucket
    final var bucketName = "testgetbucket";
    final AccessorBucket bucket;
    service.createBucket(bucketName, clientId, true);
    bucket = service.getBucket(bucketName, clientId, GuidLike.getGuid(), true);
    assertEquals(bucketName, bucket.getId());
    assertEquals(AccessorStatus.READY, bucket.getStatus());
    assertTrue(service.checkBucket(bucketName, true, clientId, GuidLike.getGuid(), true));

    //Check read bucket deleted
    service.deleteBucket(bucketName, clientId, true);

    assertThrows(CcsNotExistException.class, () -> service.getBucket(bucketName, clientId, GuidLike.getGuid(), true));
    assertFalse(service.checkBucket(bucketName, true, clientId, GuidLike.getGuid(), true));
  }

  @Test
  void getBuckets() throws CcsOperationException {
    //Create buckets
    final var bucketName = "testgetbuckets";
    service.createBucket(bucketName + "1", clientId, true);
    service.createBucket(bucketName + "2", clientId, true);
    service.createBucket(bucketName + "3", clientId, true);

    final var buckets = service.getBuckets(clientId);
    assertEquals(3, buckets.size());
    for (final var bucket : buckets) {
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());
      service.deleteBucket(bucket.getId(), clientId, true);
    }
  }

  @Test
  void deleteBucket() {
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
        final Exception exceptionDeletedStorage =
            assertThrows(DriverNotFoundException.class, () -> storageDriver.bucketDelete(bucketDeleted.getId()));
        assertNotNull(exceptionDeletedStorage);
      }
      //Delete unknown Bucket
      final Exception exceptionDeleted2 =
          assertThrows(CcsDeletedException.class, () -> service.deleteBucket("unknownbucket", clientId, true));
      assertNotNull(exceptionDeleted2);
    }
  }
}
