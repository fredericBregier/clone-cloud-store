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

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.FakeActionTopicConsumer;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.resource.fakelocalreplicator.FakeLocalReplicatorBucketServiceImpl;
import io.clonecloudstore.accessor.server.resource.fakelocalreplicator.FakeLocalReplicatorObjectServiceImpl;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.MinioMongoKafkaProfile;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.driver.s3.DriverS3Properties.DEFAULT_MAX_SIZE_NOT_PART;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MinioMongoKafkaProfile.class)
class AccessorObjectServiceExternalTest {

  @Inject
  AccessorBucketService service;
  @Inject
  AccessorObjectService serviceObject;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  @Inject
  Instance<DaoAccessorObjectRepository> objectRepositoryInstance;
  DaoAccessorObjectRepository objectRepository;
  String clientId = null;
  String opId = null;

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
    FakeActionTopicConsumer.reset();
  }

  @AfterAll
  static void endTests() {
    DriverS3Properties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
  }

  @BeforeEach
  public void cleanBeforeTest() throws CcsDbException {
    //Generate fake client id
    clientId = UUID.randomUUID().toString();
    opId = GuidLike.getGuid();

    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
    //Clean database
    bucketRepository.deleteAllDb();
    objectRepository.deleteAllDb();
    FakeLocalReplicatorBucketServiceImpl.errorCode = 0;
    FakeLocalReplicatorObjectServiceImpl.errorCode = 0;
  }

  private long getObjectCreateFromTopic(final long desired) throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      final var value = FakeActionTopicConsumer.getObjectCreate();
      if (value >= desired) {
        Log.infof("Found %d", value);
        return value;
      }
      Thread.sleep(10);
    }
    return FakeActionTopicConsumer.getObjectCreate();
  }

  @Test
  void checkCreationToDeletionObject() {
    final var bucketName = "bucket-name";
    final var objectName = "dir/objectName";
    final var prefix = "dir/";
    final AccessorBucket bucket;
    final var bucketTechnicalName = DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucketName);
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
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true, clientId, opId, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.getRemotePullInputStream(bucketTechnicalName, objectName, clientId, "", opId));
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName, null);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setNamePrefix(prefix));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      // Simulate Object exists
      // Check remote
      assertTrue(AccessorProperties.isRemoteRead());
      assertTrue(AccessorProperties.isFixOnAbsent());
      try {
        assertEquals(0, FakeActionTopicConsumer.getObjectCreate());
        FakeLocalReplicatorObjectServiceImpl.errorCode = 204;
        assertEquals(StorageType.OBJECT,
            serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true, clientId, opId, true));
        assertEquals(1, getObjectCreateFromTopic(1));
        FakeLocalReplicatorObjectServiceImpl.errorCode = 200;
        final var objectGet = serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId);
        assertEquals(objectName, objectGet.response().getName());
        Thread.sleep(100);
        assertEquals(1, getObjectCreateFromTopic(1));
        final var streamResponse =
            serviceObject.getRemotePullInputStream(bucketTechnicalName, objectName, clientId, "", opId);
        assertEquals(objectName, streamResponse.dtoOut().getName());
        assertDoesNotThrow(() -> FakeInputStream.consumeAll(streamResponse.inputStream()));
        Thread.sleep(100);
        // Increment is only when calling AccessorObjectResource (External)
        assertEquals(1, getObjectCreateFromTopic(1));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        FakeLocalReplicatorBucketServiceImpl.errorCode = 0;
        FakeLocalReplicatorObjectServiceImpl.errorCode = 0;
      }
      // Same but fix if absent is off
      assertTrue(AccessorProperties.isRemoteRead());
      AccessorProperties.setFixOnAbsent(false);
      assertFalse(AccessorProperties.isFixOnAbsent());
      try {
        assertEquals(1, FakeActionTopicConsumer.getObjectCreate());
        FakeLocalReplicatorObjectServiceImpl.errorCode = 204;
        assertEquals(StorageType.OBJECT,
            serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true, clientId, opId, true));
        Thread.sleep(100);
        assertEquals(1, getObjectCreateFromTopic(1));
        FakeLocalReplicatorObjectServiceImpl.errorCode = 200;
        final var objectGet = serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId);
        assertEquals(objectName, objectGet.response().getName());
        Thread.sleep(100);
        assertEquals(1, getObjectCreateFromTopic(1));
        final var streamResponse =
            serviceObject.getRemotePullInputStream(bucketTechnicalName, objectName, clientId, "", opId);
        assertEquals(objectName, streamResponse.dtoOut().getName());
        assertDoesNotThrow(() -> FakeInputStream.consumeAll(streamResponse.inputStream()));
        Thread.sleep(100);
        // Increment is only when calling AccessorObjectResource (External)
        assertEquals(1, getObjectCreateFromTopic(1));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        FakeLocalReplicatorBucketServiceImpl.errorCode = 0;
        FakeLocalReplicatorObjectServiceImpl.errorCode = 0;
        AccessorProperties.setFixOnAbsent(true);
      }
      // Same but remote is off
      AccessorProperties.setRemoteRead(false);
      assertFalse(AccessorProperties.isRemoteRead());
      assertTrue(AccessorProperties.isFixOnAbsent());
      try {
        assertEquals(1, FakeActionTopicConsumer.getObjectCreate());
        FakeLocalReplicatorObjectServiceImpl.errorCode = 204;
        assertEquals(StorageType.NONE,
            serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true, clientId, opId, true));
        Thread.sleep(100);
        assertEquals(1, getObjectCreateFromTopic(1));
        FakeLocalReplicatorObjectServiceImpl.errorCode = 200;
        assertThrows(CcsNotExistException.class,
            () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
        Thread.sleep(100);
        assertEquals(1, getObjectCreateFromTopic(1));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        FakeLocalReplicatorBucketServiceImpl.errorCode = 0;
        FakeLocalReplicatorObjectServiceImpl.errorCode = 0;
        AccessorProperties.setRemoteRead(true);
      }

      // Create object
      object = serviceObject.createObject(create, "hash", 100).getDto();
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(bucketTechnicalName, objectName, "hash", 100, clientId, true);
      // Assert object is existing
      object = serviceObject.getObjectInfo(bucketTechnicalName, objectName);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, false, clientId, opId, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.getRemotePullInputStream(bucketTechnicalName, objectName, clientId, "", opId));

      // Check listing
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName, null);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setNamePrefix(prefix));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setNamePrefix("badprefix"));
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
                .setSizeLessThan(object.getSize() + 10));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setStatuses(new AccessorStatus[]{object.getStatus()}));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setNamePrefix(prefix).setCreationAfter(object.getCreation().minusSeconds(10))
                .setCreationBefore(object.getCreation().plusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setNamePrefix(prefix).setCreationAfter(object.getCreation().minusSeconds(10))
                .setCreationBefore(object.getCreation().plusSeconds(10)).setSizeGreaterThan(object.getSize() - 10)
                .setSizeLessThan(object.getSize() + 10).setStatuses(new AccessorStatus[]{object.getStatus()}));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketTechnicalName,
            new AccessorFilter().setMetadataFilter(new HashMap<String, String>()));
        try {
          final var iterator = StreamIteratorUtils.getIteratorFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, SystemTools.consumeAll(iterator));
        } catch (final RuntimeException e) {
          fail(e);
        }
      }
      // Filter to empty list
      {
        final var map = new HashMap<String, String>();
        map.put("key", "value");
        final var inputStream =
            serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter().setMetadataFilter(map));
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
                .setExpiresBefore(Instant.ofEpochMilli(Long.MAX_VALUE)));
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }

      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir2/", false, clientId, opId, true));
      // Assert object is readable
      final var dao = serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId);
      Assertions.assertEquals(bucketTechnicalName, dao.response().getBucket());
      Assertions.assertEquals(objectName, dao.response().getName());
      Assertions.assertEquals(AccessorStatus.READY, dao.response().getStatus());
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      // Try recreate but cannot
      assertThrows(CcsAlreadyExistException.class, () -> serviceObject.createObject(create, "hash", 100));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException e) {
      fail(e);
    }
    // Delete object
    try {
      serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true);
    } catch (final CcsServerGenericException | CcsNotExistException | CcsDeletedException e) {
      fail(e);
    }
    // Retry Delete object
    assertThrows(CcsDeletedException.class,
        () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
    assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));

    // Now retry to create and should be ok since in status deleted
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      assertDoesNotThrow(() -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
      // Create object
      object = serviceObject.createObject(create, "hash", 100).getDto();
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(bucketTechnicalName, objectName, "hash", 100, clientId, true);
      // Assert object is existing
      object = serviceObject.getObjectInfo(bucketTechnicalName, objectName);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, false, clientId, opId, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir2/", false, clientId, opId, true));
      // Assert object is readable
      final var dao = serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId);
      Assertions.assertEquals(bucketTechnicalName, dao.response().getBucket());
      Assertions.assertEquals(objectName, dao.response().getName());
      Assertions.assertEquals(AccessorStatus.READY, dao.response().getStatus());
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      // Try recreate but cannot
      assertThrows(CcsAlreadyExistException.class, () -> serviceObject.createObject(create, "hash", 100));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException e) {
      fail(e);
    }
    // Delete object
    try {
      serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true);
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException e) {
      fail(e);
    }
    // Now retry to create and should be ok since in status deleted, without hash and size
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      assertDoesNotThrow(() -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
      // Create object
      object = serviceObject.createObject(create, null, 0).getDto();
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(bucketTechnicalName, objectName, "hash", 100, clientId, true);
      // Assert object is existing
      object = serviceObject.getObjectInfo(bucketTechnicalName, objectName);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, false, clientId, opId, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir2/", false, clientId, opId, true));
      // Assert object is readable
      final var dao = serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId);
      Assertions.assertEquals(bucketTechnicalName, dao.response().getBucket());
      Assertions.assertEquals(objectName, dao.response().getName());
      Assertions.assertEquals(AccessorStatus.READY, dao.response().getStatus());
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      // Try recreate but cannot
      assertThrows(CcsAlreadyExistException.class, () -> serviceObject.createObject(create, "hash", 100));
      final var inputStream = serviceObject.filterObjects(bucketTechnicalName, new AccessorFilter());
      final var len = FakeInputStream.consumeAll(inputStream);
      Log.infof("Len of Filter: %d", len);
      assertNotEquals(0, len);
      // Change object status to error and recheck access
      objectRepository.updateObjectStatus(bucketTechnicalName, objectName, AccessorStatus.ERR_UPL, null);
      object = serviceObject.getObjectInfo(bucketTechnicalName, objectName);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.ERR_UPL, object.getStatus());
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, false, clientId, opId, true));
      // Assert object is not readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
      // Change object status to IN_Progress and recheck creation
      objectRepository.updateObjectStatus(bucketTechnicalName, objectName, AccessorStatus.UPLOAD, null);
      // Create object but cannot
      assertThrows(CcsNotAcceptableException.class, () -> serviceObject.createObject(create, null, 0).getDto());
      object = serviceObject.getObjectInfo(bucketTechnicalName, objectName);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      // FIXME is this correct as status while object in IN_PROGRESS and new creation occurs?
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      // Change object status to Available and recheck access
      objectRepository.updateObjectStatus(bucketTechnicalName, objectName, AccessorStatus.READY, null);
      object = serviceObject.getObjectInfo(bucketTechnicalName, objectName);
      assertEquals(bucketTechnicalName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, false, clientId, opId, true));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException | IOException | CcsDbException e) {
      fail(e);
    }
    // Delete Object
    try {
      serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true);
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException e) {
      fail(e);
    }

    // Delete Bucket
    try {
      final var bucketDeleted = service.deleteBucket(clientId, bucketTechnicalName, true);
      Assertions.assertEquals(AccessorStatus.DELETED, bucketDeleted.getStatus());
      assertThrows(CcsDeletedException.class, () -> service.deleteBucket(clientId, bucketTechnicalName, true));
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException |
                   CcsNotAcceptableException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void checkWithInvalidBucket() {
    final var bucketName = "bucket-not-created";
    final var objectName = "dir/objectName";
    final var bucketTechnicalName = DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucketName);
    final var create = new AccessorObject().setBucket(bucketTechnicalName).setName(objectName);
    AccessorObject object;
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
      // Create object
      assertThrows(CcsNotExistException.class, () -> serviceObject.createObject(create, "hash", 100));
      assertThrows(CcsOperationException.class,
          () -> serviceObject.createObjectFinalize(bucketTechnicalName, objectName, "hash", 100, clientId, true));
      // Assert object is existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketTechnicalName, objectName));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, false, clientId, opId, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketTechnicalName, objectName, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketTechnicalName, "dir2/", false, clientId, opId, true));
      // Assert object is readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketTechnicalName, objectName, true, clientId, opId));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
    } catch (final CcsServerGenericException e) {
      fail(e);
    }
    // Delete object
    assertThrows(CcsNotExistException.class,
        () -> serviceObject.deleteObject(bucketTechnicalName, objectName, clientId, true));
    assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketTechnicalName, objectName));
  }
}
