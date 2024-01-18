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

package io.clonecloudstore.driver.azure;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.driver.azure.example.client.ApiClientFactory;
import io.clonecloudstore.test.stream.FakeInputStream;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

abstract class DriverAzureBase {
  private static final Logger LOG = Logger.getLogger(DriverAzureBase.class);
  // Ensure not exactly a chunk multiplier
  private static final int len1 = 11 * 1024;
  private static final int lenBig = 20 * 1024 * 1024 + 1024;
  // Minimal Chunk Size
  private static final int chunk = 5 * 1024 * 1024;
  protected static ApiClientFactory factory;
  protected static boolean oldSha;
  @Inject
  DriverApiFactory driverApiFactory;
  @Inject
  DriverAzureHelper driverHelper;

  @Test
  public void testDriverEmpty() throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    final var storageObject = new StorageObject(bucket, object1, null, len1, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));

      assertThrows(DriverNotFoundException.class, () -> driverApi.bucketGet(bucket));
      assertThrows(DriverNotFoundException.class, () -> driverApi.bucketDelete(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len1)));
      assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(bucket, object1, len1, null));
      assertThrows(DriverException.class, () -> driverApi.objectGetInputStreamInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverHelper.getObjectInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverHelper.getObjectInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.objectDeleteInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.objectsStreamInBucket(bucket));
      assertThrows(DriverException.class, () -> driverApi.objectsStreamInBucket(bucket, prefix, null, null));
      assertThrows(DriverException.class,
          () -> driverApi.objectsStreamInBucket(bucket, prefix, Instant.now(), Instant.now()));
      assertThrows(DriverException.class, () -> driverApi.objectGetMetadataInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.objectsCountInBucket(bucket));
      assertThrows(DriverException.class, () -> driverApi.objectsCountInBucket(bucket, prefix, null, null));
      assertThrows(DriverException.class,
          () -> driverApi.objectsCountInBucket(bucket, prefix, Instant.now(), Instant.now()));
      assertThrows(DriverException.class, () -> driverApi.objectCopy(null, null));
      assertThrows(DriverException.class, () -> driverApi.objectCopy(bucket, object1, bucket, object1, null, null));
      assertThrows(DriverException.class, () -> driverApi.objectCopy(bucket, object1, null, null, null, null));
      assertThrows(DriverException.class,
          () -> driverApi.objectCopy(bucket, object1, bucket + 1, object1 + 1, null, null));
    }
  }

  @Test
  void nullParameters() {
    assertThrows(DriverException.class, () -> driverHelper.createBucket(null));
    assertThrows(DriverException.class, () -> driverHelper.importBucket(null));
    assertThrows(DriverException.class, () -> driverHelper.deleteBucket(null));
    assertThrows(DriverException.class, () -> driverHelper.getBucket(null));
    assertThrows(DriverException.class, () -> driverHelper.existBucket(null));
    assertThrows(DriverException.class, () -> driverHelper.countObjectsInBucket(null));
    assertThrows(DriverException.class, () -> driverHelper.getObjectsIteratorFilteredInBucket(null, null, null, null));
    assertThrows(DriverException.class, () -> driverHelper.getObjectsStreamFilteredInBucket(null, null, null, null));
    assertThrows(DriverException.class, () -> driverHelper.existObjectInBucket(null, null));
    assertThrows(DriverException.class, () -> driverHelper.existDirectoryOrObjectInBucket(null, null));
    assertThrows(DriverException.class, () -> driverHelper.objectPrepareCreateInBucket(null, null));
    assertThrows(DriverException.class, () -> driverHelper.finalizeObject(null, null, null, 1));
    assertThrows(DriverException.class, () -> driverHelper.getObjectBodyInBucket(null, null));
    assertThrows(DriverException.class, () -> driverHelper.getObjectInBucket(null, null));
    assertThrows(DriverException.class, () -> driverHelper.objectCopyToAnother(null, null));
    assertThrows(DriverException.class, () -> driverHelper.deleteObjectInBucket(null, null));
  }

  @Test
  public void testDriverWithBucket() throws DriverException {
    final var bucket = "test1";
    final var clientId = "client";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    var storageBucket = new StorageBucket(bucket, clientId, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));
      assertFalse(driverApi.bucketsIterator().hasNext());

      try {
        storageBucket = driverApi.bucketCreate(storageBucket);
        assertEquals(bucket, storageBucket.bucket());
        assertEquals(clientId, storageBucket.clientId());
      } catch (final DriverNotAcceptableException | DriverAlreadyExistException e) {
        fail(e);
      }
      final StorageBucket finalStorageBucket = storageBucket;
      assertThrows(DriverAlreadyExistException.class, () -> driverApi.bucketCreate(finalStorageBucket));
      assertEquals(1, driverApi.bucketsStream().count());
      assertEquals(1, driverApi.bucketsCount());
      assertTrue(driverApi.bucketExists(bucket));
      assertEquals(finalStorageBucket, driverApi.bucketGet(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket);
        assertEquals(0, objectStream.count());
        assertFalse(driverApi.objectsIteratorInBucket(bucket).hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, null, null);
        assertEquals(0, objectStream.count());
        assertFalse(driverApi.objectsIteratorInBucket(bucket, prefix, null, null).hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.now(), Instant.now());
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      assertThrows(DriverException.class, () -> driverApi.objectGetInputStreamInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class, () -> driverHelper.getObjectBodyInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectDeleteInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectGetMetadataInBucket(bucket, object1));
      try {
        assertEquals(0, driverApi.objectsCountInBucket(bucket));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        assertEquals(0, driverApi.objectsCountInBucket(bucket, prefix, null, null));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        assertEquals(0, driverApi.objectsCountInBucket(bucket, prefix, Instant.now(), Instant.now()));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }

      try {
        driverApi.bucketDelete(bucket);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
      // Now try to import existing one
      try {
        ((DriverAzure) driverApi).getDriverAzureHelper().getBlobServiceClient().createBlobContainer(bucket);
        final var storageBucketImported = driverApi.bucketImport(finalStorageBucket);
        assertEquals(finalStorageBucket, storageBucketImported);
        driverApi.bucketDelete(bucket);
      } catch (final RuntimeException | DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
    }
  }

  @Test
  public void testDriverWithBucketAndObjects() throws DriverException {
    testDriverWithBucketAndObjectsSha(null, lenBig);
  }

  void testDriverWithBucketAndObjectsSha(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, "client", null);
    final var storageObject = new StorageObject(bucket, object1, sha, length, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));

      try {
        storageBucket = driverApi.bucketCreate(storageBucket);
      } catch (final DriverNotAcceptableException | DriverAlreadyExistException e) {
        fail(e);
      }
      final StorageBucket finalStorageBucket = storageBucket;
      assertThrows(DriverAlreadyExistException.class, () -> driverApi.bucketCreate(finalStorageBucket));
      assertEquals(1, driverApi.bucketsStream().count());
      assertEquals(1, driverApi.bucketsCount());
      assertTrue(driverApi.bucketExists(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket);
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, null, null);
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.now(), Instant.now());
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      // Test with sha given in prepare through StorageObject
      var start = System.nanoTime();
      try {
        driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length));
      } catch (final DriverNotFoundException | DriverAlreadyExistException e) {
        fail(e);
      }
      try {
        final var storageObject1 = driverApi.objectFinalizeCreateInBucket(bucket, object1, length, null);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
      } catch (final DriverException e) {
        fail(e);
      }
      var stop = System.nanoTime();
      LOG.infof("Write Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      start = System.nanoTime();
      try {
        final var inputStream = driverApi.objectGetInputStreamInBucket(bucket, object1);
        assertEquals(length, FakeInputStream.consumeAll(inputStream));
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      LOG.infof("Read Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      try {
        final var storageObject1 = driverApi.objectGetMetadataInBucket(bucket, object1);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      // Try recreate object
      assertThrows(DriverAlreadyExistException.class,
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length)));

      try {
        driverApi.objectDeleteInBucket(bucket, object1);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
      // Test using finalize to give Hash
      final var storageObject2 = new StorageObject(bucket, object1, null, length, null);
      start = System.nanoTime();
      try {
        driverApi.objectPrepareCreateInBucket(storageObject2, new FakeInputStream(length));
      } catch (final DriverNotFoundException | DriverAlreadyExistException e) {
        fail(e);
      }
      try {
        final var storageObject1 = driverApi.objectFinalizeCreateInBucket(bucket, object1, length, sha);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
      } catch (final DriverException e) {
        fail(e);
      }
      stop = System.nanoTime();
      LOG.infof("Write2 Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      start = System.nanoTime();
      try {
        final var inputStream = driverApi.objectGetInputStreamInBucket(bucket, object1);
        assertEquals(length, FakeInputStream.consumeAll(inputStream));
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      LOG.infof("Read2 Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      try {
        final var storageObject1 = driverApi.objectGetMetadataInBucket(bucket, object1);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        driverApi.objectDeleteInBucket(bucket, object1);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
      // Test using no length but hash
      final var storageObject3 = new StorageObject(bucket, object1, sha, 0, null);
      start = System.nanoTime();
      try {
        final var storageObject4 = new StorageObject(bucket, object1, null, length, null);
        driverApi.objectPrepareCreateInBucket(storageObject4, new FakeInputStream(length));
      } catch (final DriverNotFoundException | DriverAlreadyExistException e) {
        fail(e);
      }
      try {
        final var storageObject1 = driverApi.objectFinalizeCreateInBucket(bucket, object1, length, sha);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
      } catch (final DriverException e) {
        fail(e);
      }
      stop = System.nanoTime();
      LOG.infof("Write3 Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      start = System.nanoTime();
      try {
        final var inputStream = driverApi.objectGetInputStreamInBucket(bucket, object1);
        assertEquals(length, FakeInputStream.consumeAll(inputStream));
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      LOG.infof("Read3 Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      try {
        final var storageObject1 = driverApi.objectGetMetadataInBucket(bucket, object1);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }

      try {
        assertEquals(1, driverApi.objectsCountInBucket(bucket));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        assertEquals(1, driverApi.objectsCountInBucket(bucket, prefix, null, null));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        assertEquals(1, driverApi.objectsCountInBucket(bucket, prefix, Instant.MIN, Instant.MAX));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        assertEquals(0, driverApi.objectsCountInBucket(bucket, prefix, Instant.MIN, Instant.MIN));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        assertEquals(0, driverApi.objectsCountInBucket(bucket, prefix, Instant.MAX, Instant.MAX));
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      assertEquals(StorageType.OBJECT, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      assertEquals(StorageType.DIRECTORY, driverApi.directoryOrObjectExistsInBucket(bucket, prefix));
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket);
        assertEquals(1, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, null, null);
        assertEquals(1, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.MIN, Instant.MAX);
        assertEquals(1, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.MIN, Instant.MIN);
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.MAX, null);
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, null, Instant.MIN);
        assertEquals(0, objectStream.count());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      start = System.nanoTime();
      try {
        final var storageObject1 = driverApi.objectGetMetadataInBucket(bucket, object1);
        final var storageArchiveBucket = new StorageBucket("archive", "client", null);
        final var storageArchiveBucket2 = driverApi.bucketCreate(storageArchiveBucket);
        final var map = new HashMap<String, String>();
        if (storageObject1.metadata() != null && !storageObject1.metadata().isEmpty()) {
          map.putAll(storageObject1.metadata());
        }
        map.put("testbucket", storageObject1.bucket());
        map.put("testname", storageObject1.name());
        final var storageObjectArchive =
            new StorageObject(storageArchiveBucket2.bucket(), storageObject1.name() + "arch", storageObject1.hash(),
                storageObject1.size(), storageObject1.creationDate(), Instant.now().plusSeconds(100), map);
        final var storageObjectArchiveResult = driverApi.objectCopy(storageObject1, storageObjectArchive);
        assertEquals(storageArchiveBucket2.bucket(), storageObjectArchiveResult.bucket());
        assertEquals(storageObjectArchive.name(), storageObjectArchiveResult.name());
        assertNotNull(storageObjectArchiveResult.creationDate());
        assertEquals(storageObject1.size(), storageObjectArchiveResult.size());
        assertEquals(storageObject1.hash(), storageObjectArchiveResult.hash());
        assertEquals(storageObjectArchive.expiresDate(), storageObjectArchiveResult.expiresDate());
        assertEquals(storageObject1.bucket(), storageObjectArchiveResult.metadata().get("testbucket"));
        assertEquals(storageObject1.name(), storageObjectArchiveResult.metadata().get("testname"));
        assertEquals(StorageType.OBJECT, driverApi.directoryOrObjectExistsInBucket(storageObjectArchiveResult.bucket(),
            storageObjectArchiveResult.name()));
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      } finally {
        try {
          driverApi.objectDeleteInBucket("archive", object1 + "arch");
        } catch (final Exception ignore) {
          // Ignore
        }
        driverApi.bucketDelete("archive");
      }
      stop = System.nanoTime();
      LOG.infof("Copy Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      assertThrows(DriverNotAcceptableException.class, () -> driverApi.bucketDelete(bucket));
      try {
        driverApi.objectDeleteInBucket(bucket, object1);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }

      try {
        driverApi.bucketDelete(bucket);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
    }
  }

  @Test
  public void testDriverWithMetadata() throws DriverException {
    testDriverWithMetadata(null, len1);
  }

  void testDriverWithMetadata(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/objectmetadata";
    final var prefix = "dir/";
    final var map = new HashMap<String, String>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    var instant = Instant.now().plusSeconds(1000);
    var storageBucket = new StorageBucket(bucket, "client", null);
    final var storageObject = new StorageObject(bucket, object1, sha, length, null, instant, map);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));

      try {
        storageBucket = driverApi.bucketCreate(storageBucket);
      } catch (final DriverNotAcceptableException | DriverAlreadyExistException e) {
        fail(e);
      }
      final StorageBucket finalStorageBucket = storageBucket;
      assertThrows(DriverAlreadyExistException.class, () -> driverApi.bucketCreate(finalStorageBucket));
      assertEquals(1, driverApi.bucketsStream().count());
      assertEquals(1, driverApi.bucketsCount());
      assertTrue(driverApi.bucketExists(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      // Test with sha given in prepare through StorageObject
      var start = System.nanoTime();
      try {
        driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length));
      } catch (final DriverNotFoundException | DriverAlreadyExistException e) {
        fail(e);
      }
      try {
        final var storageObject1 = driverApi.objectFinalizeCreateInBucket(bucket, storageObject.name(), length, null);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
        assertEquals(2, storageObject1.metadata().size());
        assertEquals(map, storageObject1.metadata());
        assertEquals(storageObject, storageObject1);
      } catch (final DriverException e) {
        fail(e);
      }
      var stop = System.nanoTime();
      LOG.infof("Write Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      start = System.nanoTime();
      try {
        final var inputStream = driverApi.objectGetInputStreamInBucket(bucket, object1);
        assertEquals(length, FakeInputStream.consumeAll(inputStream));
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      LOG.infof("Read Len: %d Duration: %d Speed: %f", length, stop - start, length / ((stop - start) / 1000.0));
      try {
        final var storageObject1 = driverApi.objectGetMetadataInBucket(bucket, object1);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
        assertEquals(2, storageObject1.metadata().size());
        assertEquals(map, storageObject1.metadata());
        assertEquals(storageObject, storageObject1);
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        driverApi.objectDeleteInBucket(bucket, object1);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }

      try {
        driverApi.bucketDelete(bucket);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
    }
  }

  @Test
  void testDriverWithBucketAndObjectsMultiple() throws DriverException {
    final var bucket = "test1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, "client", null);
    try (final var driverApi = driverApiFactory.getInstance()) {
      try {
        storageBucket = driverApi.bucketCreate(storageBucket);
      } catch (final DriverNotAcceptableException | DriverAlreadyExistException e) {
        fail(e);
      }
      long start = System.nanoTime();
      for (int i = 0; i < 100; i++) {
        createFile(i);
      }
      long stop = System.nanoTime();
      LOG.infof("Creation Duration: %d Speed: %f", stop - start, 100 / ((stop - start) / 1000.0));
      long start1 = System.nanoTime();
      var stream = driverApi.objectsStreamInBucket(bucket, prefix, null, null);
      long nb = stream.mapToInt(s -> 1).sum();
      assertEquals(100, nb);
      long stop1 = System.nanoTime();
      LOG.infof("Stream Duration: %d Speed: %f", stop1 - start1, 100 / ((stop1 - start1) / 1000.0));
      long start2 = System.nanoTime();
      var iterator = driverApi.objectsIteratorInBucket(bucket, prefix, null, null);
      nb = SystemTools.consumeAll(iterator);
      assertEquals(100, nb);
      long stop2 = System.nanoTime();
      LOG.infof("Iterator Duration: %d Speed: %f", stop2 - start2, 100 / ((stop2 - start2) / 1000.0));
      long start3 = System.nanoTime();
      iterator = driverApi.objectsIteratorInBucket(bucket, null, null, null);
      while (iterator.hasNext()) {
        var item = iterator.next();
        driverApi.objectDeleteInBucket(bucket, item.name());
      }
      long stop3 = System.nanoTime();
      LOG.infof("Iterator Deletion Duration: %d Speed: %f", stop3 - start3, 100 / ((stop3 - start3) / 1000.0));
      driverApi.bucketDelete(bucket);
      double streamTime = (stop1 - start1);
      double iteratorTime = (stop2 - start2);
      LOG.infof("Speed Stream %s Iterator (factor %f)", (streamTime < iteratorTime) ? ">" : "<",
          (streamTime < iteratorTime) ? iteratorTime / streamTime : streamTime / iteratorTime);
    }
  }

  void createFile(final int item) throws DriverException {
    final var length = 10L;
    final var bucket = "test1";
    final var object1 = "dir/object" + item;

    final var storageObject = new StorageObject(bucket, object1, "sha", length, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      // Test with sha given in prepare through StorageObject
      try {
        driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length));
        final var storageObject1 = driverApi.objectFinalizeCreateInBucket(bucket, object1, length, null);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
      } catch (final DriverException e) {
        fail(e);
      }
    }
  }

  @Test
  public void testDriverWithBucketAndObjectsSha() throws DriverException, NoSuchAlgorithmException, IOException {
    final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(lenBig), DigestAlgo.SHA256);
    FakeInputStream.consumeAll(digestInputStream);
    final var sha = digestInputStream.getDigestBase32();
    testDriverWithBucketAndObjectsSha(sha, lenBig);
  }

  @Test
  public void testDriverWithBucketAndObjectsMultiParts() throws DriverException {
    final var old = DriverAzureProperties.getMaxPartSize();
    final var oldUnknown = DriverAzureProperties.getMaxPartSizeForUnknownLength();
    try {
      DriverAzureProperties.setDynamicPartSize(chunk);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(chunk);
      testDriverWithBucketAndObjectsSha(null, lenBig);
    } finally {
      DriverAzureProperties.setDynamicPartSize(old);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }
  }

  @Test
  public void testDriverWithBucketAndObjectsMultiPartsShaOnTheFly() throws DriverException {
    final var old = DriverAzureProperties.getMaxPartSize();
    final var oldUnknown = DriverAzureProperties.getMaxPartSizeForUnknownLength();
    try {
      DriverAzureProperties.setDynamicPartSize(chunk);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(chunk);
      QuarkusProperties.setServerComputeSha256(true);
      testDriverWithBucketAndObjectsSha(null, lenBig);
    } finally {
      DriverAzureProperties.setDynamicPartSize(old);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(oldUnknown);
      QuarkusProperties.setServerComputeSha256(oldSha);
    }
  }

  @Test
  public void testDriverWithBucketAndObjectsMultiPartsSha()
      throws DriverException, NoSuchAlgorithmException, IOException {
    final var old = DriverAzureProperties.getMaxPartSize();
    final var oldUnknown = DriverAzureProperties.getMaxPartSizeForUnknownLength();
    try {
      DriverAzureProperties.setDynamicPartSize(chunk);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(chunk);
      final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(lenBig), DigestAlgo.SHA256);
      FakeInputStream.consumeAll(digestInputStream);
      final var sha = digestInputStream.getDigestBase32();
      testDriverWithBucketAndObjectsSha(sha, lenBig);
    } finally {
      DriverAzureProperties.setDynamicPartSize(old);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }
  }

  private Exception uploadMultipart(final String bucket, final StorageObject object, final InputStream inputStream,
                                    final long len) {
    final var partSize = Math.min(len > 0 ? len : DriverAzureProperties.getMaxPartSizeForUnknownLength(),
        DriverAzureProperties.getMaxPartSizeForUnknownLength());
    try (final var driverApi = driverApiFactory.getInstance()) {
      driverApi.objectPrepareCreateInBucket(object, inputStream);
      driverApi.objectFinalizeCreateInBucket(bucket, object.name(), len, object.hash() != null ? object.hash() : "aaa");
      return null;
    } catch (final DriverException e) {
      return e;
    } catch (final RuntimeException e) {
      return new DriverException("Error on upload", e);
    } finally {
      try {
        inputStream.close();
      } catch (final IOException e) {
        LOG.debug(e);
      }
    }
  }

  void testMultipart(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, "client", null);
    final var storageObject = new StorageObject(bucket, object1, sha, length, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertFalse(driverApi.bucketExists(bucket));

      try {
        storageBucket = driverApi.bucketCreate(storageBucket);
      } catch (final DriverNotAcceptableException | DriverAlreadyExistException e) {
        fail(e);
      }
      assertTrue(driverApi.bucketExists(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      // Test with sha given in prepare through StorageObject
      var start = System.nanoTime();
      // When valid
      try (final var inputStream = new FakeInputStream(length)) {
        Exception exception = uploadMultipart(bucket, storageObject, inputStream, length);
        assertNull(exception);
        assertEquals(StorageType.OBJECT, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      } catch (RuntimeException | IOException e) {
        fail(e);
      }
      var stop = System.nanoTime();
      LOG.infof("Commit Write Len: %d Duration: %d Speed: %f", length, stop - start,
          length / ((stop - start) / 1000.0));
      try {
        driverApi.objectDeleteInBucket(bucket, object1);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
      try {
        driverApi.bucketDelete(bucket);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
    }
  }

  @Test
  public void testMultiPartsValidAndCancel() throws DriverException {
    final var old = DriverAzureProperties.getMaxPartSize();
    final var oldUnknown = DriverAzureProperties.getMaxPartSizeForUnknownLength();
    try {
      DriverAzureProperties.setDynamicPartSize(chunk);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(chunk);
      testMultipart(null, lenBig);
    } finally {
      DriverAzureProperties.setDynamicPartSize(old);
      DriverAzureProperties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }
  }
}
