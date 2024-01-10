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
import io.clonecloudstore.accessor.server.commons.AbstractPublicBucketHelper;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.clonecloudstore.test.resource.s3.MinioProfile;
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
@TestProfile(MinioProfile.class)
class AccessorBucketServiceExternalTest {
  @Inject
  AccessorBucketService service;
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
  public void cleanBeforeTest() {
    //Generate fake client id
    clientId = UUID.randomUUID().toString();
  }

  @Test
  void createBucket() {
    try {
      final var bucketName = "bucketcreatetest";
      final var bucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);

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
      final var technicalId = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketNameMax, true);
      final Exception exceptionMax =
          assertThrows(CcsOperationException.class, () -> service.createBucket(clientId, technicalId, true));
      assertNotNull(exceptionMax);

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void getBucket() {
    final var unknownBucketTechnicalName =
        AbstractPublicBucketHelper.getTechnicalBucketName(clientId, "unknownbucket", true);
    //Get Bucket not exist
    final Exception exceptionMin = assertThrows(CcsNotExistException.class,
        () -> service.getBucket(unknownBucketTechnicalName, clientId, GuidLike.getGuid(), true));
    assertNotNull(exceptionMin);
    assertFalse(service.checkBucket(unknownBucketTechnicalName, true, clientId, GuidLike.getGuid(), true));
    assertFalse(AccessorProperties.isRemoteRead());
    assertFalse(AccessorProperties.isFixOnAbsent());

    //Create a bucket and get Bucket
    final var bucketName = "testgetbucket";
    final var bucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    final AccessorBucket bucket;
    service.createBucket(clientId, bucketTechnicalName, true);
    bucket = service.getBucket(bucketTechnicalName, clientId, GuidLike.getGuid(), true);
    assertEquals(bucketName, bucket.getName());
    assertEquals(AccessorStatus.READY, bucket.getStatus());
    assertTrue(service.checkBucket(bucketTechnicalName, true, clientId, GuidLike.getGuid(), true));

    //Check read bucket deleted
    service.deleteBucket(clientId, bucketTechnicalName, true);

    assertThrows(CcsNotExistException.class,
        () -> service.getBucket(bucketTechnicalName, clientId, GuidLike.getGuid(), true));
    assertFalse(service.checkBucket(bucketTechnicalName, true, clientId, GuidLike.getGuid(), true));
  }

  @Test
  void getBuckets() throws CcsOperationException {
    //Create buckets
    final var bucketName = "testgetbuckets";

    final var prefixBucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
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
  void deleteBucket() {
    final var bucketName = "bucketdeletedtest";
    final AccessorBucket bucket;
    {
      final var bucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
      bucket = service.createBucket(clientId, bucketTechnicalName, true);
      assertEquals(bucketName, bucket.getName());
      assertEquals(bucketTechnicalName, bucket.getId());
      assertEquals(AccessorStatus.READY, bucket.getStatus());
    }
    //Delete bucket already
    {
      final var bucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
      final var bucketDeleted = service.deleteBucket(clientId, bucketTechnicalName, true);
      Assertions.assertEquals(AccessorStatus.DELETED, bucketDeleted.getStatus());
      final Exception exceptionDeleted =
          assertThrows(CcsDeletedException.class, () -> service.deleteBucket(clientId, bucketTechnicalName, true));
      assertNotNull(exceptionDeleted);
      //Delete bucket already deleted
      try (final var storageDriver = storageDriverFactory.getInstance()) {
        final Exception exceptionDeletedStorage =
            assertThrows(DriverNotFoundException.class, () -> storageDriver.bucketDelete(bucketDeleted.getId()));
        assertNotNull(exceptionDeletedStorage);
      }
      //Delete unknown Bucket
      final var unknownBucketTechnicalName =
          AbstractPublicBucketHelper.getTechnicalBucketName(clientId, "unknownBucket", true);
      final Exception exceptionDeleted2 = assertThrows(CcsDeletedException.class,
          () -> service.deleteBucket(clientId, unknownBucketTechnicalName, true));
      assertNotNull(exceptionDeleted2);
    }
  }
}
