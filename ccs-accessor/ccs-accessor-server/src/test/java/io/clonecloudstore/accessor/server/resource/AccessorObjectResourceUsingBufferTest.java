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

package io.clonecloudstore.accessor.server.resource;

import java.io.IOException;
import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.FakeRequestTopicConsumer;
import io.clonecloudstore.accessor.server.application.buffer.CcsBufferAccessorService;
import io.clonecloudstore.accessor.server.commons.buffer.CcsBufferService;
import io.clonecloudstore.accessor.server.commons.buffer.FilesystemHandler;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.AccessorPropertiesChangeValues;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.ExtendDriverApiRegistry;
import io.clonecloudstore.driver.api.FakeDriver;
import io.clonecloudstore.driver.api.MongoKafkaNoDriverForBufferProfile;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverRuntimeException;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.metrics.MetricsCheck;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaNoDriverForBufferProfile.class)
class AccessorObjectResourceUsingBufferTest {
  private static final Logger LOG = Logger.getLogger(AccessorObjectResourceUsingBufferTest.class);
  public static final String BUCKET_NAME = "testbucket";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @Inject
  AccessorBucketApiFactory factoryBucket;
  @Inject
  AccessorObjectApiFactory factory;
  static DriverApiFactory driverApiFactory;
  static DriverApiFactory originalDriverApiFactory;
  private static String clientId = null;
  @Inject
  FilesystemHandler filesystemHandler;
  @Inject
  CcsBufferAccessorService bufferAccessorService;
  @Inject
  BulkMetrics bulkMetrics;


  @BeforeAll
  static void setup() {
    // Mock Driver
    originalDriverApiFactory = DriverApiRegistry.getDriverApiFactory();
    driverApiFactory = new MockDriverApiFactory();
    ExtendDriverApiRegistry.forceSetDriverApiFactory(driverApiFactory);
    AccessorProperties.setStoreActive(true);
    CleanupTestUtil.cleanUp();

    clientId = UUID.randomUUID().toString();
    FakeCommonBucketResourceHelper.errorCode = 404;
    FakeCommonObjectResourceHelper.errorCode = 404;
    FakeRequestTopicConsumer.reset();
  }

  @AfterAll
  static void endWithCheckingKafka() {
    ExtendDriverApiRegistry.forceSetDriverApiFactory(originalDriverApiFactory);
    AccessorProperties.setStoreActive(false);
    FakeDriver.shallRaiseAnException = false;
    LOG.infof("Kafka BC: %d BD: %d OC: %d OD: %d", FakeRequestTopicConsumer.getBucketCreate(),
        FakeRequestTopicConsumer.getBucketDelete(), FakeRequestTopicConsumer.getObjectCreate(),
        FakeRequestTopicConsumer.getObjectDelete());
    assertTrue(FakeRequestTopicConsumer.getBucketCreate() > 0);
    assertTrue(FakeRequestTopicConsumer.getObjectCreate() > 0);
    assertTrue(FakeRequestTopicConsumer.getBucketDelete() > 0);
    assertTrue(FakeRequestTopicConsumer.getObjectDelete() > 0);
    BulkMetrics metrics = CDI.current().select(BulkMetrics.class).get();
    assertTrue(
        metrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE).count() >
            0);
    assertTrue(metrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_UNREGISTER)
        .count() > 0);
  }

  @BeforeEach
  void beforeEach() {
    ExtendDriverApiRegistry.forceSetDriverApiFactory(driverApiFactory);
    CleanupTestUtil.cleanUp();
    ((FakeDriver) driverApiFactory.getInstance()).cleanUp();
    FakeDriver.shallRaiseAnException = false;
  }

  @AfterEach
  void afterEach() throws IOException {
    FakeDriver.shallRaiseAnException = false;
    assertEquals(0, filesystemHandler.count());
    ExtendDriverApiRegistry.forceSetDriverApiFactory(originalDriverApiFactory);
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException, InterruptedException {
    createBucketAndObject(BUCKET_NAME, OBJECT);
    createBucketAndObject(BUCKET_NAME, '/' + OBJECT);
  }

  @Test
  void createBucketAndObjectWithActiveCompression() throws CcsWithStatusException, InterruptedException {
    final var compression = AccessorProperties.isInternalCompression();
    try {
      AccessorPropertiesChangeValues.changeInternalCompression(true);
      createBucketAndObject(BUCKET_NAME, OBJECT);
      createBucketAndObject(BUCKET_NAME, '/' + OBJECT);
    } finally {
      AccessorPropertiesChangeValues.changeInternalCompression(compression);
    }
  }

  void createBucketAndObject(final String bucketName, final String objectName)
      throws CcsWithStatusException, InterruptedException {
    var counterCreate =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    var counterRegister =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_REGISTER);
    var counterUnregister =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_UNREGISTER);
    var countCreate = counterCreate.count();
    var countRegister = counterRegister.count();
    var countUnregister = counterUnregister.count();
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      assertEquals(bucketName, bucket.getId());
    }
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(objectName).setSize(100);
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(bucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedObjectName(objectName), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister, MetricsCheck.waitForValueTest(counterRegister, countRegister, 200));
    assertEquals(countUnregister, MetricsCheck.waitForValueTest(counterUnregister, countUnregister, 200));
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
      final var object = client.getObjectInfo(bucketName, objectName, clientId);
      LOG.infof("Object: %s", object);
      assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, objectName, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      assertEquals(original, inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister, MetricsCheck.waitForValueTest(counterRegister, countRegister, 200));
    assertEquals(countUnregister, MetricsCheck.waitForValueTest(counterUnregister, countUnregister, 200));
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndObjectNoDriverButNoJob() throws CcsWithStatusException, InterruptedException {
    final String bucketName = BUCKET_NAME;
    final String objectName = OBJECT;
    var counterCreate =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    var counterRegister =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_REGISTER);
    var counterUnregister =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_UNREGISTER);
    var countCreate = counterCreate.count();
    var countRegister = counterRegister.count();
    var countUnregister = counterUnregister.count();
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      assertEquals(bucketName, bucket.getId());
    }
    // No Driver first
    FakeDriver.shallRaiseAnException = true;

    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(objectName).setSize(100);
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(bucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedObjectName(objectName), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister + 1, MetricsCheck.waitForValueTest(counterRegister, countRegister + 1, 200));
    assertEquals(countUnregister, MetricsCheck.waitForValueTest(counterUnregister, countUnregister, 200));
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
      final var object = client.getObjectInfo(bucketName, objectName, clientId);
      LOG.infof("Object: %s", object);
      assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, objectName, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      assertEquals(original, inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister + 1, MetricsCheck.waitForValueTest(counterRegister, countRegister + 1, 200));
    assertEquals(countUnregister + 1, MetricsCheck.waitForValueTest(counterUnregister, countUnregister + 1, 200));
    // Reset Driver
    FakeDriver.shallRaiseAnException = false;
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndObjectNoDriverButJob() throws CcsWithStatusException, InterruptedException {
    final String bucketName = BUCKET_NAME;
    final String objectName = OBJECT;
    var counterCreate =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    var counterRegister =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_REGISTER);
    var counterUnregister =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_UNREGISTER);
    var countCreate = counterCreate.count();
    var countRegister = counterRegister.count();
    var countUnregister = counterUnregister.count();
    // Create Bucket
    try (final var client = factoryBucket.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      assertEquals(bucketName, bucket.getId());
    }
    // No Driver first
    FakeDriver.shallRaiseAnException = true;
    // Create Object
    AccessorObject original;
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(objectName).setSize(100);
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(bucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedObjectName(objectName), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(AccessorProperties.getAccessorSite(), original.getSite());
      assertNotNull(original.getHash());
    }
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister + 1, MetricsCheck.waitForValueTest(counterRegister, countRegister + 1, 200));
    assertEquals(countUnregister, MetricsCheck.waitForValueTest(counterUnregister, countUnregister, 200));
    FakeDriver.shallRaiseAnException = false;
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
      final var object = client.getObjectInfo(bucketName, objectName, clientId);
      LOG.infof("Object: %s", object);
      assertEquals(original, object);
    }
    // Get both Object and content
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(bucketName, objectName, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      assertEquals(original, inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
    // Now run Job
    bufferAccessorService.asyncJobRetryImport();
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister + 1, MetricsCheck.waitForValueTest(counterRegister, countRegister + 1, 200));
    assertEquals(countUnregister + 1, MetricsCheck.waitForValueTest(counterUnregister, countUnregister + 1, 200));

    // Delete Object
    try (final var client = factory.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    assertEquals(countCreate + 1, MetricsCheck.waitForValueTest(counterCreate, countCreate + 1, 200));
    assertEquals(countRegister + 1, MetricsCheck.waitForValueTest(counterRegister, countRegister + 1, 200));
    assertEquals(countUnregister + 1, MetricsCheck.waitForValueTest(counterUnregister, countUnregister + 1, 200));
    // Reset Driver
    FakeDriver.shallRaiseAnException = false;
    // Finally delete bucket
    try (final var client = factoryBucket.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  static class MockDriverApiFactory implements DriverApiFactory {
    private final DriverApi driverApi = new FakeDriver();

    @Override
    public DriverApi getInstance() throws DriverRuntimeException {
      return driverApi;
    }
  }
}
