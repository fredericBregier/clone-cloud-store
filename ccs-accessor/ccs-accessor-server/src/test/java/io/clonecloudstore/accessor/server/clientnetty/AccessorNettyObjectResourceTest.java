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

package io.clonecloudstore.accessor.server.clientnetty;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.MinioMongoKafkaProfile;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MinioMongoKafkaProfile.class)
@Disabled("Wait for Netty client implementation")
class AccessorNettyObjectResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorNettyObjectResourceTest.class);
  public static final String BUCKET_NAME = "testbucket";
  public static final String BUCKET_MULTI_NAME = "testbucketmulti";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @Inject
  AccessorBucketApiFactory factoryBucket;
  @Inject
  AccessorObjectApiFactory factory;
  @Inject
  Instance<DaoAccessorObjectRepository> repositoryInstance;
  DaoAccessorObjectRepository repository;
  @Inject
  DriverApiFactory driverApiFactory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();

    // Bug fix on "localhost"
    var url = MinIoResource.getUrlString();
    if (url.contains("localhost")) {
      url = url.replace("localhost", "127.0.0.1");
    }
    DriverS3Properties.setDynamicS3Parameters(url, MinIoResource.getAccessKey(), MinIoResource.getSecretKey(),
        MinIoResource.getRegion());
  }

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException {
    final var finalBucketName = DaoAccessorBucketRepository.getPrefix(clientId) + BUCKET_NAME;
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
      ;
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
    // Retry and since GONE OK
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      // Already deleted so True
      assertTrue(deleted);
    }
    // Delete non-existing Object
    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, objectName + "NotFound", clientId));
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
      final var object = client.getObjectInfo(bucketName, objectName, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertNotEquals(original, object);
      Assertions.assertEquals(AccessorStatus.DELETED, object.getStatus());
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
  void checkTryCreateWhileAlreadyInCreation() throws CcsWithStatusException, CcsDbException {
    final var bucketName = "retry-create";
    final var finalBucketName = DaoAccessorBucketRepository.getPrefix(clientId) + bucketName;
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
    // Now change status to IN_PROGRESS to check concurrency
    repository.updateObjectStatus(finalBucketName, ParametersChecker.getSanitizedName(OBJECT), AccessorStatus.UPLOAD,
        null);
    // Check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    // Try getting MD and content
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    // Try delete
    try (final var client = factory.newClient()) {
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    // Try recreate object
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(406, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, clientId, new FakeInputStream(100))).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    // Check object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    // Try Get
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Try Delete
    try (final var client = factory.newClient()) {
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }

    // Now change status to UNKNOWN
    repository.updateObjectStatus(finalBucketName, ParametersChecker.getSanitizedName(OBJECT), AccessorStatus.UNKNOWN,
        null);
    // Check Object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Various access in error
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UNKNOWN, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UNKNOWN, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UNKNOWN, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    // Create object while Unknown should change the status to ERROR
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(409, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, clientId, new FakeInputStream(100))).getStatus());
    }
    // Shall change the status
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.ERR_UPL, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.ERR_UPL, client.getObjectInfo(bucketName, OBJECT, clientId).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }

    // Reset status to AVAILABLE
    repository.updateObjectStatus(finalBucketName, ParametersChecker.getSanitizedName(OBJECT), AccessorStatus.READY,
        null);
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
    final var finalBucketName = DaoAccessorBucketRepository.getPrefix(clientId) + bucketName;
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(finalBucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
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
      ;
      assertEquals(1, cpt.get());
    }

    // Now delete locally the Object
    try (final var driver = driverApiFactory.getInstance()) {
      driver.objectDeleteInBucket(finalBucketName, OBJECT);
    } catch (final DriverException e) {
      fail(e);
    }
    // Try delete Bucket not empty since no check of real existence and DB say it's present
    try (final var client = factoryBucket.newClient()) {
      assertEquals(406,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, clientId)).getStatus());
    }
    // Try check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, OBJECT, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Try getting MD and content on DELETED object
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, OBJECT, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndMultipleObject() throws CcsWithStatusException {
    final var finalBucketName = DaoAccessorBucketRepository.getPrefix(clientId) + BUCKET_MULTI_NAME;
    createBucketAndMultipleObject(BUCKET_MULTI_NAME, finalBucketName, true);
  }

  @Test
  void createBucketAndMultipleObjectNoSize() throws CcsWithStatusException {
    final var finalBucketName = DaoAccessorBucketRepository.getBucketTechnicalName(clientId, BUCKET_MULTI_NAME + "2");
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
      ;
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

  @Test
  void createObjectUsingNoChunkMode() {
    final var len = 5 * 1024 * 1024; // Max 10 MB by default
    final var bucket = "chunked";
    final var objectChunked = "dir/chunkedObject";
    final var objectNotChunked = "dir/notChunkedObject";
    final var uriChunked = AccessorConstants.Api.API_ROOT + "/" + bucket + "/" + objectChunked;
    final var uriNotChunked = AccessorConstants.Api.API_ROOT + "/" + bucket + "/" + objectNotChunked;
    final Map<String, String> map = new HashMap<>();
    map.put(AccessorConstants.Api.X_CLIENT_ID, clientId);
    map.put(AccessorConstants.HeaderObject.X_OBJECT_BUCKET, bucket);
    try (final var client = factoryBucket.newClient()) {
      final var accessorBucket = client.createBucket(bucket, clientId);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucket, accessorBucket.getName());
      Assertions.assertEquals(DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucket),
          accessorBucket.getId());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }

    // Content-Length set to empty or -1, Chunked mode
    {
      final var start = System.nanoTime();
      final var accessorObject =
          given().headers(map).header(AccessorConstants.HeaderObject.X_OBJECT_NAME, objectChunked)
              .contentType(MediaType.APPLICATION_OCTET_STREAM).header(X_OP_ID, "1").body(new FakeInputStream(len))
              .when().post("http://127.0.0.1:8081" + uriChunked).then().statusCode(201).extract()
              .as(AccessorObject.class);
      assertEquals(ParametersChecker.getSanitizedName(objectChunked), accessorObject.getName());
      assertEquals(len, accessorObject.getSize());
      assertNotNull(accessorObject.getCreation());
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ((float) len) / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0));
    }
    // Content-Length set array size, not Chunked mode
    {
      final var start = System.nanoTime();
      final var content = new byte[len];
      Arrays.fill(content, 0, len, (byte) 'A');
      final var accessorObject =
          given().headers(map).header(AccessorConstants.HeaderObject.X_OBJECT_NAME, objectNotChunked)
              .contentType(MediaType.APPLICATION_OCTET_STREAM).header(X_OP_ID, "2").body(content).when()
              .post("http://127.0.0.1:8081" + uriNotChunked).then().statusCode(201).extract().as(AccessorObject.class);
      assertEquals(ParametersChecker.getSanitizedName(objectNotChunked), accessorObject.getName());
      assertEquals(len, accessorObject.getSize());
      assertNotNull(accessorObject.getCreation());
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ((float) len) / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0));
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucket, objectChunked, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
      final var objectType2 = client.checkObjectOrDirectory(bucket, objectNotChunked, clientId);
      LOG.infof("ObjectType: %s", objectType2);
      assertEquals(StorageType.OBJECT, objectType2);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucket, objectChunked, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
      final var deleted2 = client.deleteObject(bucket, objectNotChunked, clientId);
      LOG.infof("Object: %b", deleted2);
      assertTrue(deleted2);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucket, clientId));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }
}
