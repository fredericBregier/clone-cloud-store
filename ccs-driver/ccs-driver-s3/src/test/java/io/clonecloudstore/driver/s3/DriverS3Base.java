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

package io.clonecloudstore.driver.s3;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.quarkus.stream.ChunkInputStream;
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
import io.clonecloudstore.driver.s3.example.client.ApiClientFactory;
import io.clonecloudstore.test.stream.FakeInputStream;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

abstract class DriverS3Base {
  private static final Logger LOG = Logger.getLogger(DriverS3Base.class);
  private static final int len1 = 10 * 1024;
  private static final int lenBig = 20 * 1024 * 1024;
  // Minimal Chunk Size
  private static final int chunk = 5 * 1024 * 1024;
  protected static ApiClientFactory factory;
  protected static boolean oldSha;
  @Inject
  DriverApiFactory driverApiFactory;
  @Inject
  DriverS3Helper driverS3Helper;

  @Test
  public void testDriverS3Empty() throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    final var storageObject = new StorageObject(bucket, object1, null, len1, null);

    try (final var driverApi = driverApiFactory.getInstance()) {
      assertEquals(0, driverApi.bucketsStream().count());
      assertEquals(0, driverApi.bucketsCount());
      assertFalse(driverApi.bucketExists(bucket));

      assertThrows(DriverNotFoundException.class, () -> driverApi.bucketDelete(bucket));
      assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      assertThrows(DriverNotFoundException.class,
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(len1)));
      assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(bucket, object1, len1, null));
      assertThrows(DriverException.class, () -> driverApi.objectGetInputStreamInBucket(bucket, object1));
      assertThrows(DriverException.class,
          () -> driverS3Helper.getS3ObjectBodyInBucket(driverS3Helper.getClient(), bucket, object1, false));
      assertThrows(DriverException.class,
          () -> driverS3Helper.getS3ObjectBodyInBucket(driverS3Helper.getClient(), bucket, object1, true));
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
    }
  }

  @Test
  public void testDriverS3WithBucket() throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    var storageBucket = new StorageBucket(bucket, null);

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
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.getS3ObjectBodyInBucket(driverS3Helper.getClient(), bucket, object1, false));
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.getS3ObjectBodyInBucket(driverS3Helper.getClient(), bucket, object1, true));
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

  void testDriverS3WithBucketAndObjectsSha(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, null);
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
  public void testDriverS3WithMetadata() throws DriverException {
    testDriverS3WithMetadata(null, len1);
  }

  void testDriverS3WithMetadata(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/objectmetadata";
    final var prefix = "dir/";
    final var map = new HashMap<String, String>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    var instant = Instant.now().plusSeconds(1000);
    var storageBucket = new StorageBucket(bucket, null);
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
        final var storageObject1 = driverApi.objectFinalizeCreateInBucket(bucket, object1, length, null);
        assertEquals(bucket, storageObject1.bucket());
        assertEquals(object1, storageObject1.name());
        assertNotNull(storageObject1.creationDate());
        assertEquals(length, storageObject1.size());
        assertEquals(sha, storageObject1.hash());
        assertEquals(2, storageObject1.metadata().size());
        assertEquals(map, storageObject1.metadata());
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
  void testDriverS3WithBucketAndObjectsMultiple() throws DriverException {
    final var bucket = "test1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, null);
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
  public void testDriverS3WithBucketAndObjectsSha() throws DriverException, NoSuchAlgorithmException, IOException {
    final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(lenBig));
    digestInputStream.computeDigest(DigestAlgo.SHA256);
    FakeInputStream.consumeAll(digestInputStream);
    final var sha = digestInputStream.getDigestBase32();
    testDriverS3WithBucketAndObjectsSha(sha, lenBig);
  }

  @Test
  public void testDriverS3WithBucketAndObjectsMultiParts() throws DriverException {
    final var old = DriverS3Properties.getMaxPartSize();
    final var oldUnknown = DriverS3Properties.getMaxPartSizeForUnknownLength();
    try {
      DriverS3Properties.setDynamicPartSize(chunk);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(chunk);
      testDriverS3WithBucketAndObjectsSha(null, lenBig);
    } finally {
      DriverS3Properties.setDynamicPartSize(old);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }
  }

  @Test
  public void testDriverS3WithBucketAndObjectsMultiPartsShaOnTheFly() throws DriverException {
    final var old = DriverS3Properties.getMaxPartSize();
    final var oldUnknown = DriverS3Properties.getMaxPartSizeForUnknownLength();
    try {
      DriverS3Properties.setDynamicPartSize(chunk);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(chunk);
      QuarkusProperties.setServerComputeSha256(true);
      testDriverS3WithBucketAndObjectsSha(null, lenBig);
    } finally {
      DriverS3Properties.setDynamicPartSize(old);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(oldUnknown);
      QuarkusProperties.setServerComputeSha256(oldSha);
    }
  }

  @Test
  public void testDriverS3WithBucketAndObjectsMultiPartsSha()
      throws DriverException, NoSuchAlgorithmException, IOException {
    final var old = DriverS3Properties.getMaxPartSize();
    final var oldUnknown = DriverS3Properties.getMaxPartSizeForUnknownLength();
    try {
      DriverS3Properties.setDynamicPartSize(chunk);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(chunk);
      final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(lenBig));
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      FakeInputStream.consumeAll(digestInputStream);
      final var sha = digestInputStream.getDigestBase32();
      testDriverS3WithBucketAndObjectsSha(sha, lenBig);
    } finally {
      DriverS3Properties.setDynamicPartSize(old);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }
  }

  private Exception uploadMultipart(final StorageObject object, final InputStream inputStream, final long len,
                                    final boolean cancel) {
    final var partSize = Math.min(len > 0 ? len : DriverS3Properties.getMaxPartSizeForUnknownLength(),
        DriverS3Properties.getMaxPartSizeForUnknownLength());
    final var chunkInputStream = new ChunkInputStream(inputStream, len, (int) partSize);
    try (final var client = driverS3Helper.getClient()) {
      final var multipartUploadHelper = new MultipartUploadHelper(client, object);
      try {
        while (chunkInputStream.nextChunk()) {
          final var chunkSize = chunkInputStream.getChunkSize();
          LOG.debugf("Newt ChunkSize: %d", chunkSize);
          multipartUploadHelper.partUpload(chunkInputStream, chunkSize);
          Thread.yield();
        }
        if (cancel) {
          throw new DriverException("test");
        } else {
          multipartUploadHelper.complete();
        }
        Thread.yield();
        return null;
      } catch (final Exception e) {
        LOG.info("Error: " + e.getMessage());
        try {
          LOG.debug("Cancel multipart");
          multipartUploadHelper.cancel();
        } catch (final DriverException e2) {
          LOG.warn("Cancel in error", e2);
        }
        return e;
      }
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
      try {
        chunkInputStream.close();
      } catch (final IOException e) {
        LOG.debug(e);
      }
    }
  }

  void testMultipart(final String sha, final long length) throws DriverException {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";

    var storageBucket = new StorageBucket(bucket, null);
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
      // When cancel
      try (final var inputStream = new FakeInputStream(length)) {
        Exception exception = uploadMultipart(storageObject, inputStream, length, true);
        assertInstanceOf(DriverException.class, exception);
        assertEquals(StorageType.NONE, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      } catch (RuntimeException | IOException e) {
        fail(e);
      }
      var stop = System.nanoTime();
      LOG.infof("Cancel Write Len: %d Duration: %d Speed: %f", length, stop - start,
          length / ((stop - start) / 1000.0));
      start = System.nanoTime();
      // When valid
      try (final var inputStream = new FakeInputStream(length)) {
        Exception exception = uploadMultipart(storageObject, inputStream, length, false);
        assertNull(exception);
        assertEquals(StorageType.OBJECT, driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      } catch (RuntimeException | IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
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
    final var old = DriverS3Properties.getMaxPartSize();
    final var oldUnknown = DriverS3Properties.getMaxPartSizeForUnknownLength();
    try {
      DriverS3Properties.setDynamicPartSize(chunk);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(chunk);
      testMultipart(null, lenBig);
    } finally {
      DriverS3Properties.setDynamicPartSize(old);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }
  }
}
