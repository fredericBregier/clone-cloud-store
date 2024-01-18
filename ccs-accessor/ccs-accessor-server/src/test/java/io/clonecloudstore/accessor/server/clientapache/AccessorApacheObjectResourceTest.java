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

package io.clonecloudstore.accessor.server.clientapache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.accessor.apache.client.AccessorApiFactory;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.FakeRequestTopicConsumer;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.resource.AzureMongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(AzureMongoKafkaProfile.class)
class AccessorApacheObjectResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorApacheObjectResourceTest.class);
  public static final String BUCKET_NAME = "testbucket";
  public static final String BUCKET_MULTI_NAME = "testbucketmulti";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";
  AccessorApiFactory factory;
  @Inject
  Instance<DaoAccessorObjectRepository> repositoryInstance;
  DaoAccessorObjectRepository repository;
  @Inject
  DriverApiFactory driverApiFactory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();

    FakeCommonBucketResourceHelper.errorCode = 404;
    FakeCommonObjectResourceHelper.errorCode = 404;
    FakeRequestTopicConsumer.reset();
  }

  @AfterAll
  static void endWithCheckingKafka() {
    LOG.infof("Kafka BC: %d BD: %d OC: %d OD: %d", FakeRequestTopicConsumer.getBucketCreate(),
        FakeRequestTopicConsumer.getBucketDelete(), FakeRequestTopicConsumer.getObjectCreate(),
        FakeRequestTopicConsumer.getObjectDelete());
    assertTrue(FakeRequestTopicConsumer.getBucketCreate() > 0);
    assertTrue(FakeRequestTopicConsumer.getObjectCreate() > 0);
    assertTrue(FakeRequestTopicConsumer.getBucketDelete() > 0);
    assertTrue(FakeRequestTopicConsumer.getObjectDelete() > 0);
  }

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
    factory = new AccessorApiFactory("http://127.0.0.1:8081", clientId);
    // Clean all
    CleanupTestUtil.cleanUp();
  }

  @AfterEach
  void afterEach() {
    factory.close();
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException {
    createBucketAndObject(BUCKET_NAME, OBJECT);
    createBucketAndObject(BUCKET_NAME, '/' + OBJECT);
  }

  void createBucketAndObject(final String bucketName, final String objectName) throws CcsWithStatusException {
    // Create Bucket
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(objectName).setSize(100);
      original = client.createObject(accessorObject, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(bucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedObjectName(objectName), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    // Check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, objectName);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, objectName);
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
      final var iterator = client.listObjects(bucketName, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      assertEquals(1, cpt.get());
    }
    // Try delete Bucket not empty
    try (final var client = factory.newClient()) {
      assertEquals(406, assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName)).getStatus());
    }
    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    // Retry and since GONE OK
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName);
      LOG.infof("Object: %b", deleted);
      // Already deleted so True
      assertTrue(deleted);
    }
    // Delete non-existing Object
    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class, () -> client.deleteObject(bucketName, objectName + "NotFound"));
    }
    // Test existence on non-existing Object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Get MD Object with DELETED status
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, objectName);
      LOG.infof("Object: %s", object);
      Assertions.assertNotEquals(original, object);
      Assertions.assertEquals(AccessorStatus.DELETED, object.getStatus());
    }
    // Try getting MD and content on DELETED object
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, objectName)).getStatus());
    }
    // Get MD Object with non-existing object
    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class, () -> client.getObjectInfo(bucketName, objectName + "NotFound"));
    }
    // Finally delete bucket
    try (final var client = factory.newClient()) {
      assertTrue(client.deleteBucket(bucketName));
    }
  }

  @Test
  void checkTryCreateWhileAlreadyInCreation() throws CcsWithStatusException, CcsDbException {
    final var bucketName = "retrycreate";
    // Create Bucket
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucketName, bucket.getId());
    }
    // Create Object with fake Hash
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject =
          new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100).setHash("fakeHash");
      original = client.createObject(accessorObject, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(bucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedObjectName(OBJECT), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertEquals(accessorObject.getHash(), original.getHash());
    }
    // Now change status to UPLOAD to check concurrency
    repository.updateObjectStatus(bucketName, ParametersChecker.getSanitizedObjectName(OBJECT), AccessorStatus.UPLOAD,
        null);
    // Check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    // Try getting MD and content
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT)).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    // Try delete
    try (final var client = factory.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteObject(bucketName, OBJECT)).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    // Try recreate object
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(406, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, new FakeInputStream(100))).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    // Check object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    // Try Get
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT)).getStatus());
    }
    // Try Delete
    try (final var client = factory.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteObject(bucketName, OBJECT)).getStatus());
    }
    // Check status unchanged
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UPLOAD, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }

    // Now change status to UNKNOWN
    repository.updateObjectStatus(bucketName, ParametersChecker.getSanitizedObjectName(OBJECT), AccessorStatus.UNKNOWN,
        null);
    // Check Object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // Various access in error
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UNKNOWN, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    try (final var client = factory.newClient()) {
      LOG.infof("Get Object: %s", OBJECT);
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT)).getStatus());
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UNKNOWN, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteObject(bucketName, OBJECT)).getStatus());
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.UNKNOWN, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    // Create object while Unknown should change the status to ERROR
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(409, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, new FakeInputStream(100))).getStatus());
    }
    // Shall change the status
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.ERR_UPL, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      Assertions.assertEquals(AccessorStatus.ERR_UPL, client.getObjectInfo(bucketName, OBJECT).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteObject(bucketName, OBJECT)).getStatus());
    }

    // Reset status to AVAILABLE
    repository.updateObjectStatus(bucketName, ParametersChecker.getSanitizedObjectName(OBJECT), AccessorStatus.READY,
        null);
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, OBJECT);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    try (final var client = factory.newClient()) {
      assertTrue(client.deleteBucket(bucketName));
    }
  }

  @Test
  void createBucketAndObjectRemote() throws CcsWithStatusException {
    final var bucketName = "changeremote";
    // Create Bucket
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      original = client.createObject(accessorObject, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(bucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedObjectName(OBJECT), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    // Check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, OBJECT);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, OBJECT);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // List objects
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(bucketName, new AccessorFilter().setNamePrefix(DIR_NAME));
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
      driver.objectDeleteInBucket(bucketName, OBJECT);
    } catch (final DriverException e) {
      fail(e);
    }
    // Try delete Bucket not empty since no check of real existence and DB say it's present
    try (final var client = factory.newClient()) {
      assertEquals(406, assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName)).getStatus());
    }
    // Try check existence
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, OBJECT);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(original, object);
    }
    LOG.infof("Remote Read %b", AccessorProperties.isRemoteRead());
    // Try getting content on physically DELETED object, so try remote
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT)).getStatus());
    }
    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, OBJECT);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    // Finally delete bucket
    try (final var client = factory.newClient()) {
      assertTrue(client.deleteBucket(bucketName));
    }
  }

  @Test
  void createBucketAndMultipleObject() throws CcsWithStatusException {
    createBucketAndMultipleObject(BUCKET_MULTI_NAME, true);
  }

  @Test
  void createBucketAndMultipleObjectNoSize() throws CcsWithStatusException {
    createBucketAndMultipleObject(BUCKET_MULTI_NAME + "2", false);
  }

  void createBucketAndMultipleObject(final String bucketName, final boolean useLen) throws CcsWithStatusException {
    // Create Bucket
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucketName, bucket.getId());
    }
    AccessorObject original;
    // Will create, check, read 10 objects
    for (var i = 0; i < 10; i++) {
      try (final var client = factory.newClient()) {
        final var accessorObject =
            new AccessorObject().setBucket(bucketName).setName(OBJECT + i).setSize(useLen ? 100 + i : 0);
        original = client.createObject(accessorObject, new FakeInputStream(100 + i));
        LOG.infof("Object: %s", original);
        assertEquals(bucketName, original.getBucket());
        assertEquals(ParametersChecker.getSanitizedObjectName(OBJECT + i), original.getName());
        assertEquals(100 + i, original.getSize());
        assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
        assertNotNull(original.getHash());
      }
      try (final var client = factory.newClient()) {
        final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT + i);
        LOG.infof("ObjectType: %s", objectType);
        assertEquals(StorageType.OBJECT, objectType);
      }
      try (final var client = factory.newClient()) {
        final var inputStreamObject = client.getObject(bucketName, OBJECT + i);
        LOG.infof("Object: %s", inputStreamObject.dtoOut());
        final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
        assertEquals(100 + i, len);
      } catch (final IOException e) {
        LOG.error(e, e);
        fail(e);
      }
      Thread.yield();
    }
    // List Objects
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(bucketName, new AccessorFilter().setNamePrefix(DIR_NAME));
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
        final var deleted = client.deleteObject(bucketName, OBJECT + i);
        LOG.infof("Object: %b", deleted);
        assertTrue(deleted);
      }
    }
    // Finally delete bucket
    try (final var client = factory.newClient()) {
      assertTrue(client.deleteBucket(bucketName));
    }
  }

  @Test
  void createObjectUsingNoChunkMode() {
    final var len = 5 * 1024 * 1024; // Max 10 MB by default
    final var bucket = "chunked";
    final var objectChunked = "dir/chunkedObject";
    final var uriChunked = AccessorConstants.Api.API_ROOT + "/" + bucket + "/" + objectChunked;
    final Map<String, String> map = new HashMap<>();
    map.put(AccessorConstants.Api.X_CLIENT_ID, clientId);
    map.put(AccessorConstants.HeaderObject.X_OBJECT_BUCKET, bucket);
    try (final var client = factory.newClient()) {
      final var accessorBucket = client.createBucket(bucket);
      LOG.infof("Bucket: %s", bucket);
      Assertions.assertEquals(bucket, accessorBucket.getId());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    // Content-Length set to empty or -1, Chunked mode
    {
      final var start = System.nanoTime();
      byte[] body = new byte[len];
      final var accessorObject =
          given().headers(map).header(AccessorConstants.HeaderObject.X_OBJECT_NAME, objectChunked)
              .contentType(MediaType.APPLICATION_OCTET_STREAM).header(X_OP_ID, "1").body(body).when()
              .post("http://127.0.0.1:8081" + uriChunked).then().statusCode(201).extract().as(AccessorObject.class);
      assertEquals(ParametersChecker.getSanitizedObjectName(objectChunked), accessorObject.getName());
      assertEquals(len, accessorObject.getSize());
      assertNotNull(accessorObject.getCreation());
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ((float) len) / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0));
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucket, objectChunked);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucket, objectChunked);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertTrue(client.deleteBucket(bucket));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }
}
