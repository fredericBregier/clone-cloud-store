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

package io.clonecloudstore.accessor.server.commons;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.example.AccessorObjectService;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.test.resource.NoResourceProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(NoResourceProfile.class)
class AccessorObjectResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorObjectResourceTest.class);
  public static final String BUCKET_NAME = "testbucket";
  public static final String BUCKET_MULTI_NAME = "testbucketmulti";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @Inject
  AccessorBucketApiFactory factoryBucket;
  @Inject
  AccessorObjectApiFactory factory;
  @Inject
  DriverApiFactory driverApiFactory;
  @Inject
  AccessorObjectService serviceObject;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException {
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, BUCKET_NAME, true);
    createBucketAndObject(BUCKET_NAME, finalBucketName, OBJECT);
    createBucketAndObject(BUCKET_NAME, finalBucketName, '/' + OBJECT);
  }

  void createBucketAndObject(final String bucketName, final String finalBucketName, final String objectName)
      throws CcsWithStatusException {
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(objectName).setSize(100);
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(finalBucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedName(objectName), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    // Check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, objectName, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, objectName, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      Assertions.assertEquals(original, inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // List objects
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      assertEquals(1, cpt.get());
    }
    // Try delete Bucket not empty
    try (final var client = factoryBucket.newClient()) {
      assertEquals(406,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, clientId)).getStatus());
    }
    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    // Retry and since GONE 404
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, objectName, clientId)).getStatus());
    }
    // Delete non-existing Object
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, objectName + "NotFound", clientId)).getStatus());
    }
    // Test existence on non-existing Object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Get MD Object with DELETED status
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(bucketName, objectName, clientId)).getStatus());
    }
    // Try getting MD and content on DELETED object
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObject(bucketName, objectName, clientId)).getStatus());
    }
    // Get MD Object with non-existing object
    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(bucketName, objectName + "NotFound", clientId));
    }
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void checkTryCreateWhileAlreadyInCreation() throws CcsWithStatusException {
    final var bucketName = "retry-create";
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
    }
    // Create Object with fake Hash
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject =
          new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100).setHash("fakeHash");
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(finalBucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedName(OBJECT), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertEquals(accessorObject.getHash(), original.getHash());
    }
    // Try recreate object
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(409, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, clientId, new FakeInputStream(100))).getStatus());
    }
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, OBJECT, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndObjectRemote() throws CcsWithStatusException {
    final var bucketName = "change-remote";
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var map = new HashMap<String, String>();
      map.put("key", "value");
      final var accessorObject =
          new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100).setMetadata(map)
              .setExpires(Instant.now().plusSeconds(10000));
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(finalBucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedName(OBJECT), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    // Check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, OBJECT, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, OBJECT, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // List objects
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      assertEquals(1, cpt.get());
    }
    // Check listing
    try (final var client = factory.newClient()) {
      {
        final var inputStream = client.listObjects(bucketName, clientId, null);
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix("badprefix"));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      // Full filter test
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setSizeGreaterThan(original.getSize() - 10)
                .setSizeLessThan(original.getSize() + 10));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setSizeGreaterThan(original.getSize() + 10));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setSizeLessThan(original.getSize() - 10));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setStatuses(new AccessorStatus[]{original.getStatus()}));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setStatuses(new AccessorStatus[]{AccessorStatus.DELETED}));
        try {
          // Status not checked
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setCreationAfter(original.getCreation().minusSeconds(10))
                .setCreationBefore(original.getCreation().plusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setCreationAfter(original.getCreation().plusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setCreationBefore(original.getCreation().minusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setCreationAfter(original.getCreation().minusSeconds(10))
                .setCreationBefore(original.getCreation().plusSeconds(10)).setSizeGreaterThan(original.getSize() - 10)
                .setSizeLessThan(original.getSize() + 10).setStatuses(new AccessorStatus[]{original.getStatus()}));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream =
            client.listObjects(bucketName, clientId, new AccessorFilter().setMetadataFilter(new HashMap<>()));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final RuntimeException e) {
          fail(e);
        }
      }
      {
        final var map = new HashMap<String, String>();
        map.put("key", "value");
        final var inputStream = client.listObjects(bucketName, clientId, new AccessorFilter().setMetadataFilter(map));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      // Filter to empty list
      {
        final var map = new HashMap<String, String>();
        map.put("key", "value2");
        final var inputStream = client.listObjects(bucketName, clientId, new AccessorFilter().setMetadataFilter(map));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var map = new HashMap<String, String>();
        map.put("keyNotExist", "value2");
        final var inputStream = client.listObjects(bucketName, clientId, new AccessorFilter().setMetadataFilter(map));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setExpiresAfter(Instant.ofEpochMilli(Long.MIN_VALUE))
                .setExpiresBefore(Instant.ofEpochMilli(Long.MAX_VALUE)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setExpiresAfter(original.getExpires().minusSeconds(10))
                .setExpiresBefore(original.getExpires().plusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(1, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setExpiresAfter(original.getExpires().plusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      {
        final var inputStream = client.listObjects(bucketName, clientId,
            new AccessorFilter().setNamePrefix(DIR_NAME).setExpiresBefore(original.getExpires().minusSeconds(10)));
        try {
          final var stream = StreamIteratorUtils.getListFromIterator(inputStream);
          assertEquals(0, stream.size());
        } catch (final CcsNotExistException e) {
          fail(e);
        }
      }
      final var object = client.getObjectInfo(bucketName, OBJECT, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
      assertTrue(client.deleteObject(bucketName, OBJECT, clientId));
    }
    // Try check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Try re Delete Object
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndMultipleObject() throws CcsWithStatusException {
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, BUCKET_MULTI_NAME, true);
    createBucketAndMultipleObject(BUCKET_MULTI_NAME, finalBucketName, true);
  }

  @Test
  void createBucketAndMultipleObjectNoSize() throws CcsWithStatusException {
    final var finalBucketName =
        AbstractPublicBucketHelper.getTechnicalBucketName(clientId, BUCKET_MULTI_NAME + "2", true);
    createBucketAndMultipleObject(BUCKET_MULTI_NAME + "2", finalBucketName, false);
  }

  void createBucketAndMultipleObject(final String bucketName, final String finalBucketName, final boolean useLen)
      throws CcsWithStatusException {
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucketName, bucket.getName());
    }
    AccessorObject original;
    // Will create, check, read 10 objects
    for (var i = 0; i < 10; i++) {
      try (final var client = factory.newClient()) {
        final var accessorObject =
            new AccessorObject().setBucket(bucketName).setName(OBJECT + i).setSize(useLen ? 100 + i : 0);
        original = client.createObject(accessorObject, clientId, new FakeInputStream(100 + i));
        LOG.infof("Object: %s", original);
        assertEquals(finalBucketName, original.getBucket());
        assertEquals(ParametersChecker.getSanitizedName(OBJECT + i), original.getName());
        assertEquals(100 + i, original.getSize());
        assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
        assertNotNull(original.getHash());
      }
      try (final var client = factory.newClient()) {
        final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT + i, clientId);
        LOG.infof("ObjectType: %s", objectType);
        assertEquals(StorageType.OBJECT, objectType);
      }
      try (final var client = factory.newClient()) {
        final var inputStreamObject = client.getObject(bucketName, OBJECT + i, clientId);
        LOG.infof("Object: %s", inputStreamObject.dtoOut());
        final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
        assertEquals(100 + i, len);
      } catch (final IOException e) {
        LOG.error(e, e);
        fail(e);
      }
    }
    // List Objects
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      assertEquals(10, cpt.get());
    }
    // Delete 10 Objects
    for (var i = 0; i < 10; i++) {
      try (final var client = factory.newClient()) {
        final var deleted = client.deleteObject(bucketName, OBJECT + i, clientId);
        LOG.infof("Object: %b", deleted);
        assertTrue(deleted);
      }
    }
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }
}
