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

import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.AbstractPublicBucketHelper;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.clonecloudstore.test.resource.s3.MinioProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.driver.s3.DriverS3Properties.DEFAULT_MAX_SIZE_NOT_PART;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MinioProfile.class)
class AccessorObjectServiceExternalTest {
  @Inject
  AccessorBucketService service;
  @Inject
  AccessorObjectService serviceObject;
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

  @AfterAll
  static void endTests() {
    DriverS3Properties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
  }

  @BeforeEach
  public void cleanBeforeTest() {
    //Generate fake client id
    clientId = UUID.randomUUID().toString();
  }

  @Test
  void checkCreationToDeletionObject() {
    final var bucketName = "bucket-name";
    final var objectName = "dir/objectName";
    final var prefix = "dir/";
    final AccessorBucket bucket;
    final DriverApi driverApi = DriverApiRegistry.getDriverApiFactory().getInstance();
    final var bucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    // First create bucket
    try {
      bucket = service.createBucket(clientId, bucketTechnicalName, true);
      assertEquals(bucketName, bucket.getName());
      assertEquals(bucketTechnicalName, bucket.getId());
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());
    } catch (final CcsAlreadyExistException | CcsServerGenericException e) {
      fail(e);
    }
    final var create = new AccessorObject().setBucket(bucketTechnicalName).setName(objectName);
    AccessorObject object;
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName, null, driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setNamePrefix(prefix), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }

      // Pseudo Create object
      object = serviceObject.createObject(create, "hash", 100);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(object, "hash", 100, clientId, true);
      // Assert object is existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));

      // Check listing
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName, null, driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setNamePrefix(prefix), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setNamePrefix("badprefix"),
                driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      // Full filter test
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setNamePrefix(prefix).setSizeGreaterThan(object.getSize() - 10)
                .setSizeLessThan(object.getSize() + 10), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setStatuses(new AccessorStatus[]{object.getStatus()}), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setNamePrefix(prefix).setCreationAfter(object.getCreation().minusSeconds(10))
                .setCreationBefore(object.getCreation().plusSeconds(10)), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setNamePrefix(prefix).setCreationAfter(object.getCreation().minusSeconds(10))
                .setCreationBefore(object.getCreation().plusSeconds(10)).setSizeGreaterThan(object.getSize() - 10)
                .setSizeLessThan(object.getSize() + 10).setStatuses(new AccessorStatus[]{object.getStatus()}),
            driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setMetadataFilter(new HashMap<>()),
                driverApi);
        try {
          final var iterator = StreamIteratorUtils.getIteratorFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, SystemTools.consumeAll(iterator));
        } catch (final RuntimeException e) {
          fail(e);
        }
      }
      // Filter to empty list
      {
        final var map = new HashMap<String, String>();
        map.put("key", "value");
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setMetadataFilter(map), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setExpiresAfter(Instant.ofEpochMilli(Long.MIN_VALUE))
                .setExpiresBefore(Instant.ofEpochMilli(Long.MAX_VALUE)), driverApi);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }

      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir2/", true));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException e) {
      fail(e);
    }
    // Delete object
    assertThrows(CcsNotExistException.class,
        () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
    // Retry Delete object
    assertThrows(CcsNotExistException.class,
        () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
    assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));

    // Delete Bucket
    try {
      final var bucketDeleted = service.deleteBucket(clientId, bucketTechnicalName, true);
      Assertions.assertEquals(AccessorStatus.DELETED, bucketDeleted.getStatus());
      assertThrows(CcsDeletedException.class, () -> service.deleteBucket(clientId, bucketTechnicalName, true));
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException |
                   CcsNotAcceptableException e) {
      throw new RuntimeException(e);
    }
    driverApi.close();
  }

  @Test
  void checkWithInvalidBucket() {
    final var bucketName = "bucket-not-created";
    final var objectName = "dir/objectName";
    final var bucketTechnicalName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    final var create = new AccessorObject().setBucket(bucketTechnicalName).setName(objectName);
    AccessorObject object;
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      // Create object
      assertThrows(CcsNotExistException.class, () -> serviceObject.createObject(create, "hash", 100));
      // Assert object is existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", true));
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir2/", true));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
    } catch (final CcsServerGenericException e) {
      fail(e);
    }
    // Delete object
    assertThrows(CcsNotExistException.class,
        () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
    assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
  }
}
