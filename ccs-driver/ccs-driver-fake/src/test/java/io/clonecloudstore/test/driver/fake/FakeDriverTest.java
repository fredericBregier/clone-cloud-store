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

package io.clonecloudstore.test.driver.fake;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class FakeDriverTest {
  private static final Logger LOG = Logger.getLogger(FakeDriverTest.class);
  @Inject
  DriverApiFactory factory;

  @BeforeEach
  void beforeAll() {
    if (factory == null) {
      factory = DriverApiRegistry.getDriverApiFactory();
    }
  }

  @AfterEach
  void afterEach() {
    try (final var driver = factory.getInstance()) {
      final var bucketStorageResponse = driver.bucketsIterator();
      while (bucketStorageResponse.hasNext()) {
        final var bucket = bucketStorageResponse.next();
        Iterator<StorageObject> objectStorageResponse = null;
        try {
          objectStorageResponse = driver.objectsIteratorInBucket(bucket.bucket());
        } catch (final Exception e) {
          LOG.error(e.getMessage());
        }
        while (objectStorageResponse.hasNext()) {
          final var object = objectStorageResponse.next();
          try {
            driver.objectDeleteInBucket(object.bucket(), object.name());
          } catch (final Exception e) {
            LOG.error(e.getMessage());
          }
        }
        try {
          driver.bucketDelete(bucket.bucket());
        } catch (final Exception e) {
          LOG.error(e.getMessage());
        }
      }
      FakeDriverFactory.cleanUp();
    } catch (final Exception e) {
      LOG.error(e.getMessage());
    }
  }

  @Test
  void test1NoBuckets() {
    final var bucket = "test1";
    final var object1 = "object1";
    final var storageBucket = new StorageBucket(bucket, Instant.now());
    try (final var driver = factory.getInstance()) {
      assertEquals(0, driver.bucketsCount());
      assertEquals(0, driver.bucketsStream().count());
      assertFalse(driver.bucketExists(bucket));
      try {
        assertEquals(0, driver.objectsStreamInBucket(bucket).count());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, null).count());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsCountInBucket(bucket, null, null, null));
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsCountInBucket(bucket));
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        driver.bucketDelete(bucket);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, object1));
      } catch (final DriverException e) {
        LOG.error(e.getMessage());
        fail(e);
      }
    } catch (final Exception e) {
      LOG.error("Exception", e);
      fail(e);
    }
  }

  @Test
  void test2CreateAndDeleteBucket() {
    final var bucket = "test1";
    final var object1 = "object1";
    final var storageBucket = new StorageBucket(bucket, Instant.now());
    try (final var driver = factory.getInstance()) {
      assertEquals(0, driver.bucketsCount());
      assertEquals(0, driver.bucketsStream().count());
      assertFalse(driver.bucketsIterator().hasNext());
      assertFalse(driver.bucketExists(bucket));
      try {
        assertEquals(0, driver.objectsStreamInBucket(bucket).count());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsCountInBucket(bucket, null, null, null));
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }

      final var bucket1 = driver.bucketCreate(storageBucket);
      assertEquals(storageBucket, bucket1);
      assertEquals(1, driver.bucketsCount());
      final var optionalStorageBucket = driver.bucketsStream().findFirst();
      optionalStorageBucket.ifPresent(StorageBucket -> assertEquals(bucket, StorageBucket.bucket()));
      assertTrue(driver.bucketExists(bucket));
      var iterator = driver.bucketsIterator();
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());

      assertEquals(0, driver.objectsStreamInBucket(bucket).count());
      assertFalse(driver.objectsIteratorInBucket(bucket).hasNext());
      assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, null).count());
      assertFalse(driver.objectsIteratorInBucket(bucket, null, null, null).hasNext());
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, null));

      // Recreate Bucket but same
      assertThrowsExactly(DriverAlreadyExistException.class, () -> driver.bucketCreate(storageBucket));
      assertEquals(1, driver.bucketsCount());

      driver.bucketDelete(bucket);
      assertEquals(0, driver.bucketsCount());
      assertEquals(0, driver.bucketsStream().count());
      assertFalse(driver.bucketExists(bucket));
      try {
        assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, null).count());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, null).count());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsIteratorInBucket(bucket, null, null, null).hasNext());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        assertEquals(0, driver.objectsIteratorInBucket(bucket, null, null, null).hasNext());
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
    } catch (final Exception e) {
      LOG.error("Exception", e);
      fail(e);
    }
  }

  @Test
  void test3CreateAndDeleteObjectsInBucket() {
    final var bucket = "test1";
    final var directory1 = "dir1/";
    final var object1 = directory1 + "object1";
    final var directory2 = "dir2/";
    final var object2 = directory2 + "object2";
    final long len1 = 1000;
    final long len2 = 2000;
    final var storageBucket = new StorageBucket(bucket, Instant.now());
    final var object = new StorageObject(bucket, object1, null, len1, Instant.now().plusSeconds(1));
    try (final var driver = factory.getInstance()) {
      assertFalse(driver.bucketExists(bucket));
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory1));
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory2));

      // Create Bucket
      assertEquals(storageBucket.bucket(), driver.bucketCreate(storageBucket).bucket());
      assertTrue(driver.bucketExists(bucket));
      assertEquals(1, driver.bucketsCount());
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, null));
      assertEquals(0, driver.objectsStreamInBucket(bucket).count());
      assertFalse(driver.objectsIteratorInBucket(bucket).hasNext());
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory1));
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory2));

      // Try reading nonexistent object from bucket
      try {
        driver.objectGetMetadataInBucket(bucket, object1);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try (final var inputStream = driver.objectGetInputStreamInBucket(bucket, object1)) {
        inputStream.read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      }
      final var before = object.creationDate().minusSeconds(1);
      final var after0 = object.creationDate().plusSeconds(1);
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, null));
      assertEquals(0, driver.objectsCountInBucket(bucket, "dir", null, null));
      assertEquals(0, driver.objectsCountInBucket(bucket, null, before, null));
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, before));
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, after0));
      assertEquals(0, driver.objectsCountInBucket(bucket, "dir", before, after0));

      assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, null).count());
      assertEquals(0, driver.objectsStreamInBucket(bucket, "dir", null, null).count());
      assertEquals(0, driver.objectsStreamInBucket(bucket, null, before, null).count());
      assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, after0).count());
      assertEquals(0, driver.objectsStreamInBucket(bucket, "dir", before, after0).count());
      assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, before).count());

      assertFalse(driver.objectsIteratorInBucket(bucket, null, null, null).hasNext());
      assertFalse(driver.objectsIteratorInBucket(bucket, "dir", null, null).hasNext());
      assertFalse(driver.objectsIteratorInBucket(bucket, null, before, null).hasNext());
      assertFalse(driver.objectsIteratorInBucket(bucket, null, null, after0).hasNext());
      assertFalse(driver.objectsIteratorInBucket(bucket, "dir", before, after0).hasNext());
      assertFalse(driver.objectsIteratorInBucket(bucket, null, null, before).hasNext());

      // Create Object driver
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, object1));
      driver.objectPrepareCreateInBucket(object, new FakeInputStream(len1, (byte) 'A'));
      final var object3 = driver.objectFinalizeCreateInBucket(bucket, object1, len1, null);
      assertEquals(object, object3);
      final var after1 = object.creationDate().plusSeconds(1);
      assertEquals(len1, object.size());
      assertEquals(object1, object.name());
      assertEquals(bucket, object.bucket());
      assertEquals(1, driver.objectsCountInBucket(bucket, null, null, null));
      assertEquals(1, driver.objectsCountInBucket(bucket, "dir", null, null));
      assertEquals(1, driver.objectsCountInBucket(bucket, null, before, null));
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, before));
      assertEquals(1, driver.objectsCountInBucket(bucket, null, null, after1));
      assertEquals(1, driver.objectsCountInBucket(bucket, "dir", before, after1));

      assertEquals(1, driver.objectsStreamInBucket(bucket, null, null, null).count());
      driver.objectsStreamInBucket(bucket, null, null, null).findFirst()
          .ifPresent(so -> assertEquals(object1, so.name()));
      assertEquals(1, driver.objectsStreamInBucket(bucket, "dir", null, null).count());
      assertEquals(1, driver.objectsStreamInBucket(bucket, null, before, null).count());
      assertEquals(1, driver.objectsStreamInBucket(bucket, null, null, after1).count());
      assertEquals(1, driver.objectsStreamInBucket(bucket, "dir", before, after1).count());
      assertEquals(0, driver.objectsStreamInBucket(bucket, null, null, before).count());

      var iterator = driver.objectsIteratorInBucket(bucket, null, null, null);
      assertTrue(iterator.hasNext());
      assertEquals(object1, iterator.next().name());
      assertFalse(iterator.hasNext());
      iterator = driver.objectsIteratorInBucket(bucket, "dir", null, null);
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());
      iterator = driver.objectsIteratorInBucket(bucket, null, before, null);
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());
      iterator = driver.objectsIteratorInBucket(bucket, null, null, after1);
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());
      iterator = driver.objectsIteratorInBucket(bucket, "dir", before, after1);
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());
      iterator = driver.objectsIteratorInBucket(bucket, null, null, before);
      assertFalse(iterator.hasNext());

      assertEquals(StorageType.OBJECT, driver.directoryOrObjectExistsInBucket(bucket, object1));
      assertEquals(StorageType.DIRECTORY, driver.directoryOrObjectExistsInBucket(bucket, directory1));
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory2));
      try {
        final var storageObject = driver.objectGetMetadataInBucket(bucket, object1);
        assertEquals(len1, storageObject.size());
        assertEquals(object1, storageObject.name());
        assertEquals(bucket, storageObject.bucket());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var inputStream = driver.objectGetInputStreamInBucket(bucket, object1);
        final var len = FakeInputStream.consumeAll(inputStream);
        inputStream.close();
        assertEquals(len1, len);
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      // Try wrong Delete Bucket/Object
      try {
        driver.bucketDelete(bucket);
        fail("Should produces an exception");
      } catch (final DriverNotAcceptableException e) {
        LOG.info(e.getMessage());
      }
      try {
        driver.objectDeleteInBucket(bucket, object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final DriverException e) {
        fail(e);
      }

      // Try reading nonexistent object from bucket
      try {
        driver.objectGetMetadataInBucket(bucket, object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try (final var inputStream = driver.objectGetInputStreamInBucket(bucket, object2)) {
        inputStream.read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      }

      // Add second Object (wait 2s to keep time correct)
      final var second = after1.plusSeconds(2);
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, object2));
      var storageObject = new StorageObject(bucket, object2, null, len2, second);
      driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len2, (byte) 'A'));
      driver.objectFinalizeCreateInBucket(bucket, object2, len2, null);
      storageObject = driver.objectGetMetadataInBucket(bucket, object2);
      assertEquals(len2, storageObject.size());
      assertEquals(object2, storageObject.name());
      assertEquals(bucket, storageObject.bucket());
      assertEquals(2, driver.objectsCountInBucket(bucket));
      assertEquals(2, driver.objectsCountInBucket(bucket, null, null, null));
      assertEquals(2, driver.objectsCountInBucket(bucket, "dir", null, null));
      assertEquals(2, driver.objectsCountInBucket(bucket, null, before, null));
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, before));
      assertEquals(1, driver.objectsCountInBucket(bucket, null, null, after1));
      final var after2 = second.plusSeconds(1);
      assertEquals(2, driver.objectsCountInBucket(bucket, null, null, after2));
      assertEquals(2, driver.objectsCountInBucket(bucket, "dir", before, after2));
      assertEquals(1, driver.objectsCountInBucket(bucket, "dir", after1, after2));
      assertEquals(2, driver.objectsStreamInBucket(bucket, null, null, null).count());
      assertEquals(2, driver.objectsStreamInBucket(bucket).count());
      assertEquals(StorageType.OBJECT, driver.directoryOrObjectExistsInBucket(bucket, object2));
      assertEquals(StorageType.DIRECTORY, driver.directoryOrObjectExistsInBucket(bucket, directory1));
      assertEquals(StorageType.DIRECTORY, driver.directoryOrObjectExistsInBucket(bucket, directory2));
      try {
        storageObject = driver.objectGetMetadataInBucket(bucket, object2);
        assertEquals(len2, storageObject.size());
        assertEquals(object2, storageObject.name());
        assertEquals(bucket, storageObject.bucket());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var inputStream = driver.objectGetInputStreamInBucket(bucket, object2);
        final var len = FakeInputStream.consumeAll(inputStream);
        inputStream.close();
        assertEquals(len2, len);
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }

      // Retry creating Object2
      try {
        driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len2, (byte) 'A'));
        fail("Should produces an exception");
      } catch (final DriverAlreadyExistException e) {
        LOG.info(e.getMessage());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
      }

      // Try creating Object2 in nonexistent bucket
      try {
        driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len2, (byte) 'A'));
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
      }

      // Try reading object from nonexistent bucket
      try {
        driver.objectGetMetadataInBucket(bucket + "no", object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        final var inputStream = driver.objectGetInputStreamInBucket(bucket + "no", object2);
        inputStream.read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      }
      assertThrowsExactly(DriverNotFoundException.class, () -> driver.bucketDelete(bucket + "no"));
      assertThrowsExactly(DriverNotFoundException.class, () -> driver.objectDeleteInBucket(bucket + "no", object2));

      // Check All
      assertEquals(1, driver.bucketsCount());
      assertEquals(1, driver.bucketsStream().count());
      assertEquals(bucket, driver.bucketsStream().findFirst().get().bucket());
      assertTrue(driver.bucketExists(bucket));
      assertEquals(2, driver.objectsCountInBucket(bucket, null, null, null));
      assertEquals(2, driver.objectsStreamInBucket(bucket, null, null, null).count());
      assertEquals(bucket, driver.objectsStreamInBucket(bucket, null, null, null).findFirst().get().bucket());
      try {
        driver.bucketDelete(bucket);
        fail("Should produces an exception");
      } catch (final DriverNotAcceptableException e) {
        LOG.info(e.getMessage());
      }
      driver.objectDeleteInBucket(bucket, object2);
      assertEquals(1, driver.objectsCountInBucket(bucket, null, null, null));
      assertThrowsExactly(DriverNotFoundException.class, () -> driver.objectDeleteInBucket(bucket, object2));
      driver.objectDeleteInBucket(bucket, object1);
      assertEquals(0, driver.objectsCountInBucket(bucket, null, null, null));
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory1));
      assertEquals(StorageType.NONE, driver.directoryOrObjectExistsInBucket(bucket, directory2));

      try {
        driver.objectDeleteInBucket(bucket, object1);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      driver.bucketDelete(bucket);
      assertEquals(0, driver.bucketsCount());
      assertEquals(0, driver.bucketsStream().count());
      assertFalse(driver.bucketExists(bucket));
    } catch (final Exception e) {
      LOG.error("Exception", e);
      fail(e);
    }
  }

  @Test
  void testCleanUp() {
    final var bucket = "test1";
    final var directory1 = "dir1/";
    final var object1 = directory1 + "object1";
    final var directory2 = "dir2/";
    final var object2 = directory2 + "object2";
    final long len1 = 1000;
    final long len2 = 2000;
    final var storageBucket = new StorageBucket(bucket, Instant.now());
    final var object = new StorageObject(bucket, object1, null, len1, Instant.now().plusSeconds(1));
    try (final var driver = factory.getInstance()) {
      FakeDriverFactory.cleanUp();
      assertEquals(0, driver.bucketsCount());

      assertEquals(storageBucket.bucket(), driver.bucketCreate(storageBucket).bucket());
      assertTrue(driver.bucketExists(bucket));
      assertEquals(1, driver.bucketsCount());
      assertEquals(0, driver.objectsStreamInBucket(storageBucket.bucket()).count());
      driver.objectPrepareCreateInBucket(object, new FakeInputStream(len1, (byte) 'A'));
      final var object3 = driver.objectFinalizeCreateInBucket(bucket, object1, len1, null);
      assertEquals(1, driver.objectsCountInBucket(storageBucket.bucket()));

      FakeDriverFactory.cleanUp();
      assertEquals(0, driver.bucketsCount());
    } catch (final Exception e) {
      LOG.error("Exception", e);
      fail(e);
    }
  }

}
