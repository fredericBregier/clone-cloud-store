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

package io.clonecloudstore.test.accessor.serverprivate;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorObjectInternalApiFactory;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
public class AccessorObjectPrivateResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorObjectPrivateResourceTest.class);
  @Inject
  AccessorBucketInternalApiFactory factoryBucket;
  @Inject
  AccessorObjectInternalApiFactory factory;
  @Inject
  AccessorBucketApiFactory factoryBucketExternal;
  @Inject
  AccessorObjectApiFactory factoryExternal;
  @Inject
  DriverApiFactory driverApiFactory;
  private static final String clientId = UUID.randomUUID().toString();
  public static final String BUCKET_NAME = "testbucket";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @BeforeEach
  void beforeEach() {
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
  }

  @Test
  void invalidApi() {
    FakeCommonObjectResourceHelper.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkObjectOrDirectory("bucket", "objectname", clientId, true));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkObjectOrDirectory("bucket", "objectname", clientId, true));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getObjectInfo("bucket", "objectName", clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getObject("bucket", "objectName", clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var filter = new AccessorFilter().setNamePrefix("object");
      client.listObjects("bucket", clientId, filter);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException {
    final var finalBucketName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, BUCKET_NAME, true);
    createBucketAndObject(BUCKET_NAME, finalBucketName, OBJECT, true);
    createBucketAndObject(BUCKET_NAME, finalBucketName, '/' + OBJECT, true);
  }

  void createBucketAndObject(final String bucketName, final String finalBucketName, final String objectName,
                             final boolean isPublic) throws CcsWithStatusException {
    try (final var client = factoryBucket.newClient(); final var clientExternal = factoryBucketExternal.newClient()) {
      final var bucket = clientExternal.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      assertEquals(finalBucketName, bucket.getId());
    }
    AccessorObject original = null;
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(objectName).setSize(100);
      original = clientExternal.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(finalBucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedName(objectName), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(FakeCommonBucketResourceHelper.site, original.getSite());
      assertNotNull(original.getHash());
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
      // No DB so no Id
      object.setId(original.getId());
      assertEquals(original, object);
    }
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(finalBucketName, objectName, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      // No DB so no Id
      inputStreamObject.dtoOut().setId(original.getId());
      assertEquals(original, inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
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
    try (final var client = factoryExternal.newClient()) {
      final var deleted = client.deleteObject(bucketName, objectName, clientId);
      LOG.infof("Object: %b", deleted);
      assertTrue(deleted);
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
    try (final var client = factory.newClient()) {
      // No DB so no GONE
      assertThrows(CcsWithStatusException.class, () -> client.getObjectInfo(finalBucketName, objectName, clientId));
    }
    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(finalBucketName, objectName + "NotFound", clientId));
    }
    try (final var client = factoryBucketExternal.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void createBucketAndObjectRemote() throws CcsWithStatusException {
    final var bucketName = "change-remote";
    final var finalBucketName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, true);
    try (final var client = factoryBucketExternal.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      assertEquals(finalBucketName, bucket.getId());
    }
    AccessorObject original = null;
    try (final var client = factoryExternal.newClient()) {
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      original = client.createObject(accessorObject, clientId, new FakeInputStream(100));
      LOG.infof("Object: %s", original);
      assertEquals(finalBucketName, original.getBucket());
      assertEquals(ParametersChecker.getSanitizedName(OBJECT), original.getName());
      assertEquals(100, original.getSize());
      assertEquals(FakeCommonBucketResourceHelper.site, original.getSite());
      assertNotNull(original.getHash());
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
      // No DB so no Id
      object.setId(original.getId());
      assertEquals(original, object);
    }
    try (final var client = factory.newClient()) {
      final var inputStreamObject = client.getObject(finalBucketName, OBJECT, clientId);
      LOG.infof("Object: %s", inputStreamObject.dtoOut());
      final var len = FakeInputStream.consumeAll(inputStreamObject.inputStream());
      assertEquals(100, len);
    } catch (final IOException e) {
      LOG.error(e, e);
      fail(e);
    }
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
    // Try check existence but no DB so DELETED
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(finalBucketName, OBJECT, clientId, true);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      // No DB so no GONE
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(finalBucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.getObject(finalBucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factoryBucketExternal.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }

  @Test
  void hugeListCheck() throws CcsWithStatusException {
    final var bucketName = "huge";
    final var finalBucketName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, true);
    final var objectName = "plenty";
    try (final var client = factoryBucketExternal.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      LOG.infof("Bucket: %s", bucket);
      assertEquals(finalBucketName, bucket.getId());
    }
    try (final var client = factory.newClient()) {
      FakeCommonObjectResourceHelper.nbList = 100000;
      final var start = System.nanoTime();
      final var filter = new AccessorFilter().setNamePrefix(objectName);
      final var iterator = client.listObjects(bucketName, clientId, filter);
      Assertions.assertEquals(FakeCommonObjectResourceHelper.nbList, SystemTools.consumeAll(iterator));
      final var stop = System.nanoTime();
      LOG.infof("MicroBenchmark on List: %f ms so %f objects/s", ((stop - start) / 1000000.0),
          FakeCommonObjectResourceHelper.nbList * 1000.0 / ((stop - start) / 1000000.0));
    } finally {
      FakeCommonObjectResourceHelper.nbList = 0;
    }
    try (final var client = factoryBucketExternal.newClient()) {
      assertTrue(client.deleteBucket(bucketName, clientId));
    }
  }
}
