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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorObjectInternalApiFactory;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
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
class AccessorObjectInternalResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorObjectInternalResourceTest.class);
  public static final String BUCKET_NAME = "testbucket";
  public static final String BUCKET_MULTI_NAME = "testbucketmulti";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @Inject
  AccessorBucketInternalApiFactory factoryBucket;
  @Inject
  AccessorBucketApiFactory factoryBucketExternal;
  @Inject
  AccessorObjectInternalApiFactory factory;
  @Inject
  AccessorObjectApiFactory factoryExternal;
  @Inject
  DriverApiFactory driverApiFactory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();
  }

  @Test
  void createBucketAndObjectReplicator() throws CcsWithStatusException {
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, BUCKET_NAME + "2", true);
    createBucketAndObject(finalBucketName, BUCKET_NAME + "2", OBJECT);
    createBucketAndObject(finalBucketName, BUCKET_NAME + "2", '/' + OBJECT);
  }

  void createBucketAndObject(final String finalBucketName, final String bucketName, final String objectName)
      throws CcsWithStatusException {
    // Create Bucket
    try (final var client = factoryBucketExternal.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factoryExternal.newClient()) {
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
      final var objectType = client.checkObjectOrDirectory(finalBucketName, objectName, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, DIR_NAME, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, objectName, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, DIR_NAME, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(finalBucketName, objectName, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(finalBucketName, objectName, clientId);
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
      final var iterator = client.listObjects(finalBucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      assertEquals(1, cpt.get());
    }
    // Try delete Bucket not empty
    try (final var client = factoryBucketExternal.newClient()) {
      assertEquals(406,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, clientId)).getStatus());
    }
    // Delete Object
    try (final var client = factoryExternal.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    // Retry and 404 since no DB
    try (final var client = factoryExternal.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, objectName, clientId)).getStatus());
    }
    // Delete non-existing Object
    try (final var client = factoryExternal.newClient()) {
      assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, objectName + "NotFound", clientId));
    }
    // Test existence on non-existing Object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, objectName, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, DIR_NAME, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, objectName, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, DIR_NAME, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Get MD Object with DELETED status
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(finalBucketName, objectName, clientId)).getStatus());
    }
    // Try getting MD and content on DELETED object
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObject(finalBucketName, objectName, clientId)).getStatus());
    }
    // Get MD Object with non-existing object
    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(finalBucketName, objectName + "NotFound", clientId));
    }
    // Finally delete bucket
    try (final var client = factoryBucketExternal.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void checkTryCreateWhileAlreadyInCreation() throws CcsWithStatusException {
    final var bucketName = "retry-create";
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    // Create Bucket
    try (final var client = factoryBucketExternal.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
      Assertions.assertEquals(bucketName, bucket.getName());
    }
    // Create Object with fake Hash
    AccessorObject original;
    try (final var client = factoryExternal.newClient()) {
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
    try (final var client = factoryExternal.newClient()) {
      final var deleted = client.deleteObject(bucketName, OBJECT, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    try (final var client = factoryBucketExternal.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndObjectRemote() throws CcsWithStatusException {
    final var bucketName = "change-remote";
    final var finalBucketName = AbstractPublicBucketHelper.getTechnicalBucketName(clientId, bucketName, true);
    // Create Bucket
    try (final var client = factoryBucketExternal.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factoryExternal.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
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
      final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, DIR_NAME, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, DIR_NAME, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(finalBucketName, OBJECT, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(finalBucketName, OBJECT, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // List objects
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(finalBucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      assertEquals(1, cpt.get());
    }

    // Now delete locally the Object
    try (final var driver = driverApiFactory.getInstance()) {
      driver.objectDeleteInBucket(finalBucketName, OBJECT);
    } catch (final DriverException e) {
      fail(e);
    }
    // Try check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT, clientId, false);
      LOG.infof("ObjectType: %s", objectType);
      // No DB
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(finalBucketName, OBJECT, clientId)).getStatus());
    }
    // Try getting MD and content on DELETED object
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObject(finalBucketName, OBJECT, clientId)).getStatus());
    }
    // Delete Object
    try (final var client = factoryExternal.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Finally delete bucket
    try (final var client = factoryBucketExternal.newClient()) {
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
    try (final var client = factoryBucketExternal.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucketName, bucket.getName());
    }
    AccessorObject original;
    // Will create, check, read 10 objects
    for (var i = 0; i < 10; i++) {
      try (final var client = factoryExternal.newClient()) {
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
        final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT + i, clientId, false);
        LOG.infof("ObjectType: %s", objectType);
        assertEquals(StorageType.OBJECT, objectType);
      }
      try (final var client = factory.newClient()) {
        final var inputStreamObject = client.getObject(finalBucketName, OBJECT + i, clientId);
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
      final var iterator = client.listObjects(finalBucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
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
      try (final var client = factoryExternal.newClient()) {
        final var deleted = client.deleteObject(bucketName, OBJECT + i, clientId);
        LOG.infof("Object: %b", deleted);
        assertTrue(deleted);
      }
    }
    // Finally delete bucket
    try (final var client = factoryBucketExternal.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }
}
