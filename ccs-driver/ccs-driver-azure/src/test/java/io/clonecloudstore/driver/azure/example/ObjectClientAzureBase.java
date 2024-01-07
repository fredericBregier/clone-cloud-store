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

package io.clonecloudstore.driver.azure.example;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.system.SysErrLogger;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.azure.DriverAzureProperties;
import io.clonecloudstore.driver.azure.example.client.ApiClientFactory;
import io.clonecloudstore.test.stream.FakeInputStream;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.driver.azure.DriverAzureProperties.DEFAULT_MAX_SIZE_NOT_PART;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
abstract class ObjectClientAzureBase {
  protected static final long bigLen = 100 * 1024 * 1024L;
  private static final Logger LOG = Logger.getLogger(ObjectClientAzureBase.class);
  private static final int len1 = 10 * 1024;
  private static final int len2 = 100 * 1024;
  protected static boolean old;
  @Inject
  ApiClientFactory factory;
  @Inject
  DriverApiFactory driverApiFactory;

  protected static String sha256 = null;

  @AfterEach
  public void after() throws InterruptedException {
    DriverAzureProperties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(false);
    // Remove all first
    cleanUp();
    Thread.sleep(200);
  }

  private void cleanUp() {
    LOG.info(SysErrLogger.purple("*** Cleanup ***"));
    try (final var apiClient = factory.newClient(); final var driverApi = driverApiFactory.getInstance()) {
      final var storageBuckets = apiClient.bucketList();
      for (final var bucket : storageBuckets) {
        // API not made
        final var iterator = driverApi.objectsIteratorInBucket(bucket.bucket());
        while (iterator.hasNext()) {
          final var object = iterator.next();
          try {
            apiClient.objectDelete(object.bucket(), object.name());
          } catch (final DriverException e) {
            LOG.error(e.getMessage());
          }
        }
        ;
        apiClient.bucketDelete(bucket.bucket());
      }
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
  }

  @Test
  void test1NoBuckets() {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    try (final var apiClient = factory.newClient()) {
      assertEquals(0, apiClient.bucketList().size());
      assertFalse(apiClient.bucketExists(bucket));
      try {
        apiClient.bucketDelete(bucket);
        fail("Should raised an exception");
      } catch (final DriverNotFoundException e) {
        // OK
      } catch (final DriverException e) {
        LOG.error(e.getMessage());
        fail(e);
      }
      try {
        assertEquals(StorageType.NONE, apiClient.objectOrDirectoryExists(bucket, object1));
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
    final var object1 = "dir/object1";
    try (final var apiClient = factory.newClient()) {
      assertTrue(apiClient.bucketList().isEmpty());
      assertFalse(apiClient.bucketExists(bucket));
      assertEquals(bucket, apiClient.bucketCreate(bucket).bucket());
      assertEquals(bucket, apiClient.bucketList().get(0).bucket());
      assertTrue(apiClient.bucketExists(bucket));
      assertEquals(1, apiClient.bucketList().size());

      // Recreate Bucket but same
      try {
        apiClient.bucketCreate(bucket);
        fail("Should raised an exception");
      } catch (final DriverAlreadyExistException e) {
        // ignore
      }
      assertEquals(1, apiClient.bucketList().size());

      apiClient.bucketDelete(bucket);
      assertEquals(0, apiClient.bucketList().size());
      assertFalse(apiClient.bucketExists(bucket));
    } catch (final Exception e) {
      LOG.error("Exception", e);
      fail(e);
    }
  }

  @Test
  void test3CreateAndDeleteObjectsInBucket() {
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var object2 = "dir/object2";

    try (final var apiClient = factory.newClient()) {
      assertTrue(apiClient.bucketList().isEmpty());
      assertFalse(apiClient.bucketExists(bucket));

      // Create Bucket
      assertEquals(bucket, apiClient.bucketCreate(bucket).bucket());
      assertEquals(bucket, apiClient.bucketList().get(0).bucket());
      assertTrue(apiClient.bucketExists(bucket));
      assertEquals(1, apiClient.bucketList().size());

      // Try reading nonexistent object from bucket
      try {
        apiClient.getObjectMetadata(bucket, object1);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        final var inputStream = apiClient.getInputStream(bucket, object1);
        inputStream.inputStream().read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      }
      try {
        final var inputStream = apiClient.getInputStream(bucket, object2);
        inputStream.inputStream().read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      } catch (final DriverException e) {
        LOG.warn(e.getMessage());
      }
      final var before = Instant.now().minusSeconds(2);
      // Create Object 1
      assertEquals(StorageType.NONE, apiClient.objectOrDirectoryExists(bucket, object1));
      var storageObject = apiClient.postInputStream(bucket, object1, getPseudoInputStream(len1), null, len1);
      assertEquals(len1, storageObject.size());
      assertEquals(object1, storageObject.name());
      assertEquals(bucket, storageObject.bucket());
      assertNull(storageObject.hash());
      assertTrue(storageObject.creationDate().isAfter(before));
      assertEquals(StorageType.OBJECT, apiClient.objectOrDirectoryExists(bucket, object1));
      assertEquals(StorageType.DIRECTORY, apiClient.objectOrDirectoryExists(bucket, "dir/"));
      try {
        storageObject = apiClient.getObjectMetadata(bucket, object1);
        assertEquals(len1, storageObject.size());
        assertEquals(object1, storageObject.name());
        assertEquals(bucket, storageObject.bucket());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var inputStream = apiClient.getInputStream(bucket, object1);
        final var len = FakeInputStream.consumeAll(inputStream.inputStream());
        LOG.debugf("MAI %s", inputStream.inputStream());
        inputStream.inputStream().close();
        assertEquals(len1, len);
        assertEquals(len1, inputStream.dtoOut().size());
        assertEquals(bucket, inputStream.dtoOut().bucket());
        assertEquals(object1, inputStream.dtoOut().name());
        assertNull(inputStream.dtoOut().hash());
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      // Try wrong Delete Bucket/Object
      try {
        apiClient.bucketDelete(bucket);
        fail("Should produces an exception");
      } catch (final DriverNotAcceptableException e) {
        LOG.info(e.getMessage());
      }
      try {
        apiClient.objectDelete(bucket, object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }

      // Try reading nonexistent object from bucket
      try {
        apiClient.getObjectMetadata(bucket, object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        final var inputStream = apiClient.getInputStream(bucket, object2);
        inputStream.inputStream().read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      } catch (final DriverException e) {
        LOG.warn(e.getMessage());
      }

      // Add second Object (wait 2s to keep time correct)
      assertEquals(StorageType.NONE, apiClient.objectOrDirectoryExists(bucket, object2));
      storageObject = apiClient.postInputStream(bucket, object2, getPseudoInputStream(len2), null, len2);
      assertEquals(len2, storageObject.size());
      assertEquals(object2, storageObject.name());
      assertEquals(bucket, storageObject.bucket());
      assertEquals(StorageType.OBJECT, apiClient.objectOrDirectoryExists(bucket, object2));
      assertEquals(StorageType.DIRECTORY, apiClient.objectOrDirectoryExists(bucket, "dir/"));
      try {
        storageObject = apiClient.getObjectMetadata(bucket, object2);
        assertEquals(len2, storageObject.size());
        assertEquals(object2, storageObject.name());
        assertEquals(bucket, storageObject.bucket());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      try {
        final var inputStream = apiClient.getInputStream(bucket, object2);
        final var len = FakeInputStream.consumeAll(inputStream.inputStream());
        inputStream.inputStream().close();
        assertEquals(len2, len);
        assertEquals(len2, inputStream.dtoOut().size());
        assertEquals(bucket, inputStream.dtoOut().bucket());
        assertEquals(object2, inputStream.dtoOut().name());
        assertNull(inputStream.dtoOut().hash());
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }

      // Retry creating Object2
      try {
        LOG.info("try recreate object2");
        storageObject = apiClient.postInputStream(bucket, object2, getPseudoInputStream(len2), null, len2);
        fail("Should produces an exception");
      } catch (final DriverAlreadyExistException e) {
        LOG.info(e.getMessage());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
      }

      // Try creating Object2 in nonexistent bucket
      try {
        storageObject = apiClient.postInputStream(bucket + "no", object2, getPseudoInputStream(len2), null, len2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
      }

      // Try reading object from nonexistent bucket
      try {
        apiClient.getObjectMetadata(bucket + "no", object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        final var inputStream = apiClient.getInputStream(bucket + "no", object2);
        inputStream.inputStream().read();
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      } catch (final IOException e) {
        LOG.warn(e.getMessage());
      }
      try {
        apiClient.bucketDelete(bucket + "no");
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }
      try {
        apiClient.objectDelete(bucket + "no", object2);
        fail("Should produces an exception");
      } catch (final DriverNotFoundException e) {
        LOG.info(e.getMessage());
      }

      // Check All
      assertEquals(1, apiClient.bucketList().size());
      assertTrue(apiClient.bucketExists(bucket));
      try {
        apiClient.bucketDelete(bucket);
        fail("Should produces an exception");
      } catch (final DriverNotAcceptableException e) {
        LOG.info(e.getMessage());
      }
      apiClient.objectDelete(bucket, object2);
      assertEquals(StorageType.NONE, apiClient.objectOrDirectoryExists(bucket, object2));
      apiClient.objectDelete(bucket, object1);
      assertEquals(StorageType.NONE, apiClient.objectOrDirectoryExists(bucket, object1));
      apiClient.bucketDelete(bucket);
      assertEquals(0, apiClient.bucketList().size());
      assertFalse(apiClient.bucketExists(bucket));
    } catch (final Exception e) {
      LOG.error("Exception", e);
      fail(e);
    }
  }

  protected static InputStream getPseudoInputStream(final long len) {
    return new FakeInputStream(len);
  }

  protected static InputStream getPseudoInputStreamForSha(final long len) {
    return new FakeInputStream(len, (byte) 'A');
  }

  @Test
  void test98BigFileNoChunkTest() {
    DriverAzureProperties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(false);
    final var start = System.nanoTime();
    testBigFile(1, bigLen, null);
    final var stop = System.nanoTime();
    LOG.info("Global Time: " + (stop - start) / 1000000 + " MB/s " +
        4 * bigLen / (1024 * 1024.0) / ((stop - start) / 1000000000.0));
  }

  void testBigFile(final int id, final long len, final String shaGiven) {
    final var bucket = "test1";
    final var object = "dir/objectPart" + id;
    final var shaReal = QuarkusProperties.serverComputeSha256() ? sha256 : shaGiven;
    try (final var apiClient = factory.newClient()) {
      assertFalse(apiClient.bucketExists(bucket));
      // Create Bucket
      assertEquals(bucket, apiClient.bucketCreate(bucket).bucket());
      assertTrue(apiClient.bucketExists(bucket));
      final var before = Instant.now().minusSeconds(2);
      var from = System.nanoTime();
      // Create Object 1
      var inputStreamSrc =
          QuarkusProperties.serverComputeSha256() ? getPseudoInputStreamForSha(len) : getPseudoInputStream(len);
      var storageObject = apiClient.postInputStream(bucket, object + 1, inputStreamSrc, shaGiven, len);
      assertEquals(len, storageObject.size());
      assertEquals(object + 1, storageObject.name());
      assertEquals(bucket, storageObject.bucket());
      assertEquals(shaReal, storageObject.hash());
      assertTrue(storageObject.creationDate().isAfter(before));
      var to = System.nanoTime();
      var time = (to - from) / 1000000000.0;
      LOG.info("Time Creation Netty: " + time + " speed: " + len / time / 1024 / 1024.0);

      // Create Object 2
      from = System.nanoTime();
      inputStreamSrc =
          QuarkusProperties.serverComputeSha256() ? getPseudoInputStreamForSha(len) : getPseudoInputStream(len);
      storageObject = apiClient.postInputStream(bucket, object + 2, inputStreamSrc, shaGiven, len);
      assertEquals(len, storageObject.size());
      assertEquals(object + 2, storageObject.name());
      assertEquals(bucket, storageObject.bucket());
      assertEquals(shaReal, storageObject.hash());
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Creation Netty: " + time + " speed: " + len / time / 1024 / 1024.0);

      from = System.nanoTime();
      assertEquals(StorageType.OBJECT, apiClient.objectOrDirectoryExists(bucket, object + 1));
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Check Existence: " + time);

      from = System.nanoTime();
      try {
        storageObject = apiClient.getObjectMetadata(bucket, object + 1);
        assertEquals(len, storageObject.size());
        assertEquals(object + 1, storageObject.name());
        assertEquals(bucket, storageObject.bucket());
        assertEquals(shaReal, storageObject.hash());
      } catch (final DriverNotFoundException e) {
        fail(e);
      }
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Read JSON: " + time);

      from = System.nanoTime();
      try {
        final var inputStream = apiClient.getInputStream(bucket, object + 1);
        final var len2 = FakeInputStream.consumeAll(inputStream.inputStream());
        inputStream.inputStream().close();
        assertEquals(len, len2);
        assertEquals(len, inputStream.dtoOut().size());
        assertEquals(bucket, inputStream.dtoOut().bucket());
        assertEquals(object + 1, inputStream.dtoOut().name());
        assertEquals(shaReal, inputStream.dtoOut().hash());
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Read StreamNetty: " + time + " speed: " + len / time / 1024 / 1024.0);

      from = System.nanoTime();
      try {
        final var inputStream = apiClient.getInputStream(bucket, object + 2);
        final var len2 = FakeInputStream.consumeAll(inputStream.inputStream());
        inputStream.inputStream().close();
        assertEquals(len, len2);
        assertEquals(len, inputStream.dtoOut().size());
        assertEquals(bucket, inputStream.dtoOut().bucket());
        assertEquals(object + 2, inputStream.dtoOut().name());
        assertEquals(shaReal, inputStream.dtoOut().hash());
      } catch (final DriverNotFoundException | IOException e) {
        fail(e);
      }
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Read StreamNetty: " + time + " speed: " + len / time / 1024 / 1024.0);

      // Wrongly ReCreate Object 1
      from = System.nanoTime();
      try {
        storageObject = apiClient.postInputStream(bucket, object + 1, getPseudoInputStream(len), null, len);
        fail("Should failed");
      } catch (final Exception e) {
        // Ignore
      }
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Wrong Creation Netty: " + time + " speed: " + len / time / 1024 / 1024.0);

      from = System.nanoTime();
      try {
        storageObject = apiClient.postInputStream(bucket, object + 2, getPseudoInputStream(len), null, len);
        fail("Should failed");
      } catch (final Exception e) {
        // Ignore
      }
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      LOG.info("Time Wrong Creation Netty: " + time + " speed: " + len / time / 1024 / 1024.0);

      from = System.nanoTime();
      apiClient.objectDelete(bucket, object + 1);
      to = System.nanoTime();
      time = (to - from) / 1000000000.0;
      apiClient.objectDelete(bucket, object + 2);
      LOG.info("Time Delete: " + time);
      apiClient.bucketDelete(bucket);
    } catch (final DriverException e) {
      fail(e);
    }
  }

  @Test
  void test99BigFileChunkedTest() {
    DriverAzureProperties.setDynamicPartSize(10 * 1024 * 1024);
    QuarkusProperties.setServerComputeSha256(false);
    final var start = System.nanoTime();
    testBigFile(3, bigLen, null);
    final var stop = System.nanoTime();
    LOG.info("Global Time: " + (stop - start) / 1000000 + " MB/s " +
        4 * bigLen / (1024 * 1024.0) / ((stop - start) / 1000000000.0));
  }

  @Test
  void test98BigFileNoChunkShaComputeTest() {
    DriverAzureProperties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(true);
    final var start = System.nanoTime();
    testBigFile(2, bigLen, null);
    final var stop = System.nanoTime();
    LOG.info("Global Time: " + (stop - start) / 1000000 + " MB/s " +
        4 * bigLen / (1024 * 1024.0) / ((stop - start) / 1000000000.0));
  }

  @Test
  void test99BigFileChunkedShaComputeTest() {
    DriverAzureProperties.setDynamicPartSize(10 * 1024 * 1024);
    QuarkusProperties.setServerComputeSha256(true);
    final var start = System.nanoTime();
    testBigFile(5, bigLen, null);
    final var stop = System.nanoTime();
    LOG.info("Global Time: " + (stop - start) / 1000000 + " MB/s " +
        4 * bigLen / (1024 * 1024.0) / ((stop - start) / 1000000000.0));
  }

  @Test
  void test98BigFileNoChunkShaTest() {
    DriverAzureProperties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(true);
    final var start = System.nanoTime();
    testBigFile(6, bigLen, sha256);
    final var stop = System.nanoTime();
    LOG.info("Global Time: " + (stop - start) / 1000000 + " MB/s " +
        4 * bigLen / (1024 * 1024.0) / ((stop - start) / 1000000000.0));
  }

  @Test
  void test99BigFileChunkedShaTest() {
    DriverAzureProperties.setDynamicPartSize(10 * 1024 * 1024);
    QuarkusProperties.setServerComputeSha256(true);
    final var start = System.nanoTime();
    testBigFile(4, bigLen, sha256);
    final var stop = System.nanoTime();
    LOG.info("Global Time: " + (stop - start) / 1000000 + " MB/s " +
        4 * bigLen / (1024 * 1024.0) / ((stop - start) / 1000000000.0));
  }
}
