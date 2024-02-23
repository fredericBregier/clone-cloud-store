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
import java.util.concurrent.atomic.AtomicInteger;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.FakeActionTopicConsumer;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
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
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.azure.DriverAzureProperties;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.resource.AzureMongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.driver.azure.DriverAzureProperties.DEFAULT_MAX_SIZE_NOT_PART;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(AzureMongoKafkaProfile.class)
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

  @AfterAll
  static void endTests() {
    DriverAzureProperties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
  }

  @BeforeEach
  public void cleanBeforeTest() {
    //Generate fake client id
    clientId = UUID.randomUUID().toString();
    opId = GuidLike.getGuid();

    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
    // Clean all
    CleanupTestUtil.cleanUp();
    FakeActionTopicConsumer.reset();
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
    final var bucketName = "bucketname";
    final var objectName = "dir/objectName";
    final var prefix = "dir/";
    final AccessorBucket bucket;
    // First create bucket
    try {
      bucket = service.createBucket(bucketName, clientId, true);
      assertEquals(bucketName, bucket.getId());
      Assertions.assertEquals(AccessorStatus.READY, bucket.getStatus());
    } catch (final CcsAlreadyExistException | CcsServerGenericException e) {
      fail(e);
    }
    final var create = new AccessorObject().setBucket(bucketName).setName(objectName);
    AccessorObject object;
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketName, objectName, clientId));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId, opId, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.deleteObject(bucketName, objectName, clientId, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.getRemotePullInputStream(bucketName, objectName, clientId, "", opId));
      {
        final var inputStream = serviceObject.filterObjects(bucketName, null, clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketName, new AccessorFilter().setNamePrefix(prefix), clientId, true);
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
        FakeCommonObjectResourceHelper.errorCode = 204;
        assertEquals(StorageType.OBJECT,
            serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId, opId, true));
        assertEquals(1, getObjectCreateFromTopic(1));
        FakeCommonObjectResourceHelper.errorCode = 200;
        final var objectGet = serviceObject.checkPullable(bucketName, objectName, true, clientId, opId);
        assertEquals(objectName, objectGet.response().getName());
        assertEquals(1, getObjectCreateFromTopic(1));
        final var streamResponse = serviceObject.getRemotePullInputStream(bucketName, objectName, clientId, "", opId);
        assertEquals(objectName, streamResponse.dtoOut().getName());
        assertDoesNotThrow(() -> FakeInputStream.consumeAll(streamResponse.inputStream()));
        // Increment is only when calling AccessorObjectResource (External)
        assertEquals(1, getObjectCreateFromTopic(1));
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
        assertEquals(1, FakeActionTopicConsumer.getObjectCreate());
        FakeCommonObjectResourceHelper.errorCode = 204;
        assertEquals(StorageType.OBJECT,
            serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId, opId, true));
        assertEquals(1, getObjectCreateFromTopic(1));
        FakeCommonObjectResourceHelper.errorCode = 200;
        final var objectGet = serviceObject.checkPullable(bucketName, objectName, true, clientId, opId);
        assertEquals(objectName, objectGet.response().getName());
        assertEquals(1, getObjectCreateFromTopic(1));
        final var streamResponse = serviceObject.getRemotePullInputStream(bucketName, objectName, clientId, "", opId);
        assertEquals(objectName, streamResponse.dtoOut().getName());
        assertDoesNotThrow(() -> FakeInputStream.consumeAll(streamResponse.inputStream()));
        // Increment is only when calling AccessorObjectResource (External)
        assertEquals(1, getObjectCreateFromTopic(1));
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
        assertEquals(1, FakeActionTopicConsumer.getObjectCreate());
        FakeCommonObjectResourceHelper.errorCode = 204;
        assertEquals(StorageType.NONE,
            serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId, opId, true));
        assertEquals(1, getObjectCreateFromTopic(1));
        FakeCommonObjectResourceHelper.errorCode = 200;
        assertThrows(CcsNotExistException.class,
            () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
        assertEquals(1, getObjectCreateFromTopic(1));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        FakeCommonBucketResourceHelper.errorCode = 0;
        FakeCommonObjectResourceHelper.errorCode = 0;
        AccessorProperties.setRemoteRead(true);
      }

      // Create object
      object = serviceObject.createObject(create, "hash", 100, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(object, "hash", 100, clientId, true);
      // Assert object is existing
      object = serviceObject.getObjectInfo(bucketName, objectName, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, false, clientId, opId, true));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.getRemotePullInputStream(bucketName, objectName, clientId, "", opId));

      // Check listing
      {
        final var inputStream = serviceObject.filterObjects(bucketName, null, clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          final AtomicInteger count = new AtomicInteger();
          stream.forEach(accessorObject -> {
            assertEquals(bucketName, accessorObject.getBucket());
            count.incrementAndGet();
          });
          assertEquals(1, count.get());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketName, new AccessorFilter().setNamePrefix(prefix), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketName, new AccessorFilter().setNamePrefix("badprefix"), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      // Full filter test
      {
        final var inputStream = serviceObject.filterObjects(bucketName,
            new AccessorFilter().setNamePrefix(prefix).setSizeGreaterThan(object.getSize() - 10)
                .setSizeLessThan(object.getSize() + 10), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketName,
            new AccessorFilter().setStatuses(new AccessorStatus[]{object.getStatus()}), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketName,
            new AccessorFilter().setNamePrefix(prefix).setCreationAfter(object.getCreation().minusSeconds(10))
                .setCreationBefore(object.getCreation().plusSeconds(10)), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketName,
            new AccessorFilter().setNamePrefix(prefix).setCreationAfter(object.getCreation().minusSeconds(10))
                .setCreationBefore(object.getCreation().plusSeconds(10)).setSizeGreaterThan(object.getSize() - 10)
                .setSizeLessThan(object.getSize() + 10).setStatuses(new AccessorStatus[]{object.getStatus()}), clientId,
            true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(1, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            serviceObject.filterObjects(bucketName, new AccessorFilter().setMetadataFilter(new HashMap<>()), clientId,
                true);
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
            serviceObject.filterObjects(bucketName, new AccessorFilter().setMetadataFilter(map), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }
      {
        final var inputStream = serviceObject.filterObjects(bucketName,
            new AccessorFilter().setExpiresAfter(Instant.ofEpochMilli(Long.MIN_VALUE))
                .setExpiresBefore(Instant.ofEpochMilli(Long.MAX_VALUE)), clientId, true);
        try {
          final var stream = StreamIteratorUtils.getStreamFromInputStream(inputStream, AccessorObject.class);
          assertEquals(0, stream.count());
        } catch (final CcsNotExistException | CcsWithStatusException e) {
          fail(e);
        }
      }

      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, "dir2/", false, clientId, opId, true));
      // Assert object is not readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      // Try recreate but cannot
      assertThrows(CcsAlreadyExistException.class, () -> serviceObject.createObject(create, "hash", 100, clientId));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException e) {
      fail(e);
    }
    // Delete object
    try {
      serviceObject.deleteObject(bucketName, objectName, clientId, true);
    } catch (final CcsServerGenericException | CcsNotExistException | CcsDeletedException e) {
      fail(e);
    }
    // Retry Delete object
    assertThrows(CcsDeletedException.class, () -> serviceObject.deleteObject(bucketName, objectName, clientId, true));
    assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));

    // Now retry to create and should be ok since in status deleted
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      assertDoesNotThrow(() -> serviceObject.getObjectInfo(bucketName, objectName, clientId));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Create object
      object = serviceObject.createObject(create, "hash", 100, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(object, "hash", 100, clientId, true);
      // Assert object is existing
      object = serviceObject.getObjectInfo(bucketName, objectName, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, false, clientId, opId, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, "dir2/", false, clientId, opId, true));
      // Assert object is not readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      // Try recreate but cannot
      assertThrows(CcsAlreadyExistException.class, () -> serviceObject.createObject(create, "hash", 100, clientId));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException e) {
      fail(e);
    }
    // Delete object
    try {
      serviceObject.deleteObject(bucketName, objectName, clientId, true);
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException e) {
      fail(e);
    }
    // Now retry to create and should be ok since in status deleted, without hash and size
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      assertDoesNotThrow(() -> serviceObject.getObjectInfo(bucketName, objectName, clientId));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Create object
      object = serviceObject.createObject(create, null, 0, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      serviceObject.createObjectFinalize(object, "hash", 100, clientId, true);
      // Assert object is existing
      object = serviceObject.getObjectInfo(bucketName, objectName, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, false, clientId, opId, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, "dir2/", false, clientId, opId, true));
      // Assert object is not readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      // Try recreate but cannot
      assertThrows(CcsAlreadyExistException.class, () -> serviceObject.createObject(create, "hash", 100, clientId));
      final var inputStream = serviceObject.filterObjects(bucketName, new AccessorFilter(), clientId, true);
      final var len = FakeInputStream.consumeAll(inputStream);
      Log.infof("Len of Filter: %d", len);
      assertNotEquals(0, len);
      // Change object status to error and recheck access
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.ERR_UPL, null);
      object = serviceObject.getObjectInfo(bucketName, objectName, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.ERR_UPL, object.getStatus());
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, false, clientId, opId, true));
      // Assert object is not readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Change object status to UPLOAD and recheck creation
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.UPLOAD, null);
      // Create object but cannot
      assertThrows(CcsNotAcceptableException.class, () -> serviceObject.createObject(create, null, 0, clientId));
      object = serviceObject.getObjectInfo(bucketName, objectName, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      // FIXME is this correct as status while object in UPLOAD and new creation occurs?
      assertEquals(AccessorStatus.UPLOAD, object.getStatus());
      // Change object status to Ready and recheck access
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.READY, null);
      object = serviceObject.getObjectInfo(bucketName, objectName, clientId);
      assertEquals(bucketName, object.getBucket());
      assertEquals(objectName, object.getName());
      assertEquals(AccessorStatus.READY, object.getStatus());
      assertEquals(StorageType.DIRECTORY,
          serviceObject.objectOrDirectoryExists(bucketName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.OBJECT,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, false, clientId, opId, true));
    } catch (final CcsAlreadyExistException | CcsServerGenericException | CcsNotExistException |
                   CcsNotAcceptableException | IOException | CcsDbException e) {
      fail(e);
    }
    // Delete Object
    try {
      serviceObject.deleteObject(bucketName, objectName, clientId, true);
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException e) {
      fail(e);
    }

    // Delete Bucket
    try {
      final var bucketDeleted = service.deleteBucket(bucketName, clientId, true);
      Assertions.assertEquals(AccessorStatus.DELETED, bucketDeleted.getStatus());
      assertThrows(CcsDeletedException.class, () -> service.deleteBucket(bucketName, clientId, true));
    } catch (final CcsNotExistException | CcsDeletedException | CcsServerGenericException |
                   CcsNotAcceptableException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void checkWithInvalidBucket() {
    final var bucketName = "bucketnotcreated";
    final var objectName = "dir/objectName";
    final var create = new AccessorObject().setBucket(bucketName).setName(objectName);
    AccessorObject object;
    try {
      // Assert object is not existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketName, objectName, clientId));
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Create object
      assertThrows(CcsNotExistException.class, () -> serviceObject.createObject(create, "hash", 100, clientId));
      assertThrows(CcsOperationException.class,
          () -> serviceObject.createObjectFinalize(create, "hash", 100, clientId, true));
      // Assert object is existing
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectInfo(bucketName, objectName, clientId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, "dir/", false, clientId, opId, true));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, objectName, false, clientId, opId, true));
      // Object not in S3 so NONE
      assertEquals(StorageType.NONE, serviceObject.objectOrDirectoryExists(bucketName, objectName, true, clientId));
      assertEquals(StorageType.NONE,
          serviceObject.objectOrDirectoryExists(bucketName, "dir2/", false, clientId, opId, true));
      // Assert object is readable
      assertThrows(CcsNotExistException.class,
          () -> serviceObject.checkPullable(bucketName, objectName, true, clientId, opId));
      // Throw since Object not in S3
      assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
    } catch (final CcsServerGenericException e) {
      fail(e);
    }
    // Delete object
    assertThrows(CcsNotExistException.class, () -> serviceObject.deleteObject(bucketName, objectName, clientId, true));
    assertThrows(CcsNotExistException.class, () -> serviceObject.getObjectMetadata(bucketName, objectName));
  }
}
