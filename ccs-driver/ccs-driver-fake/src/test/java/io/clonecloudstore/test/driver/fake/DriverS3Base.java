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
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;

import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.test.stream.FakeInputStream;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

abstract class DriverS3Base {
  private static final Logger LOG = Logger.getLogger(DriverS3Base.class);
  // Ensure not exactly a chunk multiplier
  private static final int len1 = 11 * 1024;
  private static final int lenBig = 20 * 1024 * 1024 + 1024;
  // Minimal Chunk Size
  private static final int chunk = 5 * 1024 * 1024;
  protected static boolean oldSha;
  @Inject
  DriverApiFactory driverApiFactory;

  @Test
  public void testDriverS3Empty() throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    final var storageBucket = new StorageBucket(bucket, "client", null);
    final var storageObject = new StorageObject(bucket, object1, null, len1, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));
      assertFalse(driverApi.bucketExists(storageBucket));

      assertThrows(DriverNotFoundException.class, () -> driverApi.bucketGet(bucket));
      assertThrows(DriverNotFoundException.class, () -> driverApi.bucketDelete(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len1, (byte) 'a')));
      assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(bucket, object1, len1, null));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectGetInputStreamInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectDeleteInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectsStreamInBucket(bucket));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectsStreamInBucket(bucket, prefix, null, null));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectsStreamInBucket(bucket, prefix, Instant.now(), Instant.now()));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectGetMetadataInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectsCountInBucket(bucket));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectsCountInBucket(bucket, prefix, null, null));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectsCountInBucket(bucket, prefix, Instant.now(), Instant.now()));

      assertThrows(DriverNotFoundException.class, () -> driverApi.bucketDelete(storageBucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(storageBucket, object1));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len1, (byte) 'a')));
      assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(storageObject, len1, null));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectGetInputStreamInBucket(storageObject));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectDeleteInBucket(storageObject));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectsStreamInBucket(storageBucket));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectsStreamInBucket(storageBucket, prefix, null, null));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectsStreamInBucket(storageBucket, prefix, Instant.now(), Instant.now()));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectGetMetadataInBucket(storageObject));
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectsCountInBucket(storageBucket));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectsCountInBucket(storageBucket, prefix, null, null));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectsCountInBucket(storageBucket, prefix, Instant.now(), Instant.now()));
      assertThrows(DriverException.class, () -> driverApi.objectCopy(null, null));
      assertThrows(DriverException.class, () -> driverApi.objectCopy(bucket, object1, bucket, object1, null, null));
      assertThrows(DriverException.class, () -> driverApi.objectCopy(bucket, object1, null, null, null, null));
      assertThrows(DriverException.class,
          () -> driverApi.objectCopy(bucket, object1, bucket + 1, object1 + 1, null, null));
    }
  }

  @Test
  public void testDriverS3WithBucket() throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    var storageBucket = new StorageBucket(bucket, "client", null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketsIterator().hasNext());
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
      assertEquals(storageBucket, driverApi.bucketGet(bucket));
      var iterator = driverApi.bucketsIterator();
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());
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
      assertThrows(DriverNotFoundException.class, () -> driverApi.objectGetInputStreamInBucket(bucket, object1));
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
    }
  }

  @Test
  public void testDriverS3WithBucketAndObjects() throws DriverException {
    testDriverS3WithBucketAndObjectsSha(null, lenBig);
  }

  public void testDriverS3WithBucketAndObjectsSha(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, "client", null);
    final var storageObject = new StorageObject(bucket, object1, sha, length, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));
      assertFalse(driverApi.bucketsIterator().hasNext());

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
      var iterator = driverApi.bucketsIterator();
      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());
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
      // Test with sha given in prepare through StorageObject
      var start = System.nanoTime();
      try {
        driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length, (byte) 'a'));
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
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length, (byte) 'a')));

      try {
        driverApi.objectDeleteInBucket(bucket, object1);
      } catch (final DriverNotAcceptableException | DriverNotFoundException e) {
        fail(e);
      }
      // Test using finalize to give Hash
      final var storageObject2 = new StorageObject(bucket, object1, null, length, null);
      start = System.nanoTime();
      try {
        driverApi.objectPrepareCreateInBucket(storageObject2, new FakeInputStream(length, (byte) 'a'));
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
        driverApi.objectPrepareCreateInBucket(storageObject4, new FakeInputStream(length, (byte) 'a'));
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
        var iteratorObject = driverApi.objectsIteratorInBucket(bucket);
        assertTrue(iteratorObject.hasNext());
        iteratorObject.next();
        assertFalse(iteratorObject.hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, null, null);
        assertEquals(1, objectStream.count());
        var iteratorObject = driverApi.objectsIteratorInBucket(bucket, prefix, null, null);
        assertTrue(iteratorObject.hasNext());
        iteratorObject.next();
        assertFalse(iteratorObject.hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.MIN, Instant.MAX);
        assertEquals(1, objectStream.count());
        var iteratorObject = driverApi.objectsIteratorInBucket(bucket, prefix, Instant.MIN, Instant.MAX);
        assertTrue(iteratorObject.hasNext());
        iteratorObject.next();
        assertFalse(iteratorObject.hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.MIN, Instant.MIN);
        assertEquals(0, objectStream.count());
        var iteratorObject = driverApi.objectsIteratorInBucket(bucket, prefix, Instant.MIN, Instant.MIN);
        assertFalse(iteratorObject.hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, Instant.MAX, null);
        assertEquals(0, objectStream.count());
        var iteratorObject = driverApi.objectsIteratorInBucket(bucket, prefix, Instant.MAX, null);
        assertFalse(iteratorObject.hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var objectStream = driverApi.objectsStreamInBucket(bucket, prefix, null, Instant.MIN);
        assertEquals(0, objectStream.count());
        var iteratorObject = driverApi.objectsIteratorInBucket(bucket, prefix, null, Instant.MIN);
        assertFalse(iteratorObject.hasNext());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
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
  public void testDriverS3WithBucketAndObjectsSha() throws DriverException, NoSuchAlgorithmException, IOException {
    final var digestInputStream =
        new MultipleActionsInputStream(new FakeInputStream(lenBig, (byte) 'a'), DigestAlgo.SHA256);
    FakeInputStream.consumeAll(digestInputStream);
    final var sha = digestInputStream.getDigestBase32();
    testDriverS3WithBucketAndObjectsSha(sha, lenBig);
  }
}
