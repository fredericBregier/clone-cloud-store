/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.accessor.server.commons.buffer;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.test.driver.fake.FakeDriver;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.metrics.MetricsCheck;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.micrometer.core.instrument.Counter;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@QuarkusTest
class CcsBufferServiceTest {
  @Inject
  FilesystemHandler filesystemHandler;
  @Inject
  CcsBufferService ccsBufferService;
  @Inject
  CcsBufferJob ccsBufferJob;
  @Inject
  BulkMetrics bulkMetrics;
  @Inject
  DriverApiFactory driverApiFactory;
  @Inject
  CcsBufferSkipPredicate predicate;
  private Counter created;
  private Counter purged;
  private Counter error;

  @BeforeEach
  void beforeEach() throws IOException {
    created = bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    purged = bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_PURGE);
    error =
        bulkMetrics.getCounter(CcsBufferService.BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_WRITE);
    FakeDriverFactory.cleanUp();
    FsTestUtil.cleanUp();
    filesystemHandler.changeHasDatabase(false);
    FakeDriver.shallRaiseAnException = false;
  }

  @Test
  void checkNoJob() throws InterruptedException {
    int countCreated = (int) created.count();
    int countPurged = (int) purged.count();
    int countError = (int) error.count();
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError, 100);
    ccsBufferService.asyncJobRetryImport();
    ccsBufferService.asyncJobCleanup();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError, 100);
    assertFalse(predicate.test(null));
  }

  @Test
  void koWithJobButNoDriver() throws InterruptedException, IOException, DriverException {
    int countCreated = (int) created.count();
    int countPurged = (int) purged.count();
    int countError = (int) error.count();
    final var bucket = "bucket";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    final var accessorObject = new AccessorObject().setBucket(bucket).setName(object).setSize(100)
        .setSite(AccessorProperties.getAccessorSite()).setHash("hash").setId(GuidLike.getGuid())
        .setCreation(Instant.now()).setMetadata(map).setStatus(AccessorStatus.READY);
    // No item at all
    CcsBufferServiceImpl.accessorObjectToReturn = null;
    filesystemHandler.registerItem(bucket, object);
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated + 1, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError, 100);
    assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(0, filesystemHandler.count());

    // Item in FS, in DB but Db not Ready
    accessorObject.setStatus(AccessorStatus.DELETED);
    CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
    filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()), accessorObject.getMetadata(),
        accessorObject.getExpires());
    filesystemHandler.update(bucket, object, accessorObject.getMetadata(), accessorObject.getHash());
    filesystemHandler.registerItem(bucket, object);
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError, 100);
    assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(0, filesystemHandler.count());
    assertEquals(AccessorStatus.DELETED, accessorObject.getStatus());

    // Item in FS, in DB but no Bucket in Driver
    accessorObject.setStatus(AccessorStatus.READY);
    CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
    filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()), accessorObject.getMetadata(),
        accessorObject.getExpires());
    filesystemHandler.update(bucket, object, accessorObject.getMetadata(), accessorObject.getHash());
    filesystemHandler.registerItem(bucket, object);
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError + 1, 100);
    assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(0, filesystemHandler.count());
    assertEquals(AccessorStatus.ERR_UPL, accessorObject.getStatus());

    // Item in FS, in DB but Driver out of business
    FakeDriver.shallRaiseAnException = true;
    accessorObject.setStatus(AccessorStatus.READY);
    CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
    filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()), accessorObject.getMetadata(),
        accessorObject.getExpires());
    filesystemHandler.update(bucket, object, accessorObject.getMetadata(), accessorObject.getHash());
    filesystemHandler.registerItem(bucket, object);
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError + 1, 100);
    assertEquals(1, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(1, filesystemHandler.count());
    assertEquals(AccessorStatus.READY, accessorObject.getStatus());
    filesystemHandler.unregisterItem(bucket, object);
  }

  @Test
  void koWithJobButNoImport() throws InterruptedException, IOException, DriverException {
    int countCreated = (int) created.count();
    int countPurged = (int) purged.count();
    int countError = (int) error.count();
    final var bucket = "bucket";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    final var accessorObject = new AccessorObject().setBucket(bucket).setName(object).setSize(100)
        .setSite(AccessorProperties.getAccessorSite()).setHash("hash").setId(GuidLike.getGuid())
        .setCreation(Instant.now()).setMetadata(map).setStatus(AccessorStatus.READY);
    // Item not in FS, in DB while bucket exists in Driver and not the object
    try (final var driver = driverApiFactory.getInstance()) {
      accessorObject.setStatus(AccessorStatus.READY);
      CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
      driver.bucketCreate(new StorageBucket(bucket, GuidLike.getGuid(), null));
      filesystemHandler.registerItem(bucket, object);
      ccsBufferJob.ccsBufferJob();
      MetricsCheck.waitForValueTest(created, countCreated, 100);
      MetricsCheck.waitForValueTest(purged, countPurged, 100);
      MetricsCheck.waitForValueTest(error, countError + 1, 100);
      assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
      assertEquals(0, filesystemHandler.count());
      assertEquals(AccessorStatus.ERR_UPL, accessorObject.getStatus());
    }

    // Item in FS, in DB but exists also in Driver
    try (final var driver = driverApiFactory.getInstance()) {
      accessorObject.setStatus(AccessorStatus.READY);
      CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
      filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()),
          accessorObject.getMetadata(), accessorObject.getExpires());
      filesystemHandler.update(bucket, object, accessorObject.getMetadata(), accessorObject.getHash());
      var storage = filesystemHandler.readStorageObject(bucket, object);
      driver.objectPrepareCreateInBucket(storage, new FakeInputStream(accessorObject.getSize()));
      driver.objectFinalizeCreateInBucket(storage, storage.size(), storage.hash());
      filesystemHandler.registerItem(bucket, object);
      ccsBufferJob.ccsBufferJob();
      MetricsCheck.waitForValueTest(created, countCreated, 100);
      MetricsCheck.waitForValueTest(purged, countPurged, 100);
      MetricsCheck.waitForValueTest(error, countError + 1, 100);
      assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
      assertEquals(0, filesystemHandler.count());
      driver.objectDeleteInBucket(storage);
      assertEquals(AccessorStatus.READY, accessorObject.getStatus());
    }

    // No item in FS but in DB and not in Driver while Bucket exists
    accessorObject.setStatus(AccessorStatus.READY);
    CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
    filesystemHandler.registerItem(bucket, object);
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError + 2, 100);
    assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(0, filesystemHandler.count());
    assertEquals(AccessorStatus.ERR_UPL, accessorObject.getStatus());

    // Item in FS, in DB, not in DB except bucket but DB not READY
    accessorObject.setStatus(AccessorStatus.DELETED);
    CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
    filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()), accessorObject.getMetadata(),
        accessorObject.getExpires());
    filesystemHandler.update(bucket, object, accessorObject.getMetadata(), accessorObject.getHash());
    filesystemHandler.registerItem(bucket, object);
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError + 2, 100);
    assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(0, filesystemHandler.count());
    assertEquals(AccessorStatus.DELETED, accessorObject.getStatus());
  }

  @Test
  void checkWithJobOk() throws InterruptedException, IOException, DriverException {
    int countCreated = (int) created.count();
    int countPurged = (int) purged.count();
    int countError = (int) error.count();
    final var bucket = "bucket";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    final var accessorObject = new AccessorObject().setBucket(bucket).setName(object).setSize(100)
        .setSite(AccessorProperties.getAccessorSite()).setHash("hash").setId(GuidLike.getGuid())
        .setCreation(Instant.now()).setMetadata(map).setStatus(AccessorStatus.READY);
    // Item in FS, in DB while bucket exists in Driver and not the object
    try (final var driver = driverApiFactory.getInstance()) {
      accessorObject.setStatus(AccessorStatus.READY);
      CcsBufferServiceImpl.accessorObjectToReturn = accessorObject;
      driver.bucketCreate(new StorageBucket(bucket, GuidLike.getGuid(), null));
      filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()),
          accessorObject.getMetadata(), accessorObject.getExpires());
      filesystemHandler.update(bucket, object, accessorObject.getMetadata(), accessorObject.getHash());
      filesystemHandler.registerItem(bucket, object);
      ccsBufferJob.ccsBufferJob();
      MetricsCheck.waitForValueTest(created, countCreated + 1, 100);
      MetricsCheck.waitForValueTest(purged, countPurged, 100);
      MetricsCheck.waitForValueTest(error, countError, 100);
      assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
      assertEquals(0, filesystemHandler.count());
      assertEquals(AccessorStatus.READY, accessorObject.getStatus());
    }
  }

  @Test
  void checkWithPurgeOk() throws InterruptedException, IOException, DriverException {
    int countCreated = (int) created.count();
    int countPurged = (int) purged.count();
    int countError = (int) error.count();
    final var bucket = "bucket";
    final var object = "dir/object";
    final var map = Map.of("key1", "value1");
    final var accessorObject = new AccessorObject().setBucket(bucket).setName(object).setSize(100)
        .setSite(AccessorProperties.getAccessorSite()).setHash("hash").setId(GuidLike.getGuid())
        .setCreation(Instant.now()).setMetadata(map).setStatus(AccessorStatus.READY);
    long old = AccessorProperties.getStorePurgeRetentionSeconds();
    AccessorProperties.setStorePurgeRetentionSeconds(1);
    // Create multiple objects and none must be kept except the last one
    filesystemHandler.save(bucket, object, new FakeInputStream(accessorObject.getSize()), accessorObject.getMetadata(),
        accessorObject.getExpires());
    filesystemHandler.save(bucket, object + 1, new FakeInputStream(accessorObject.getSize()),
        accessorObject.getMetadata(), accessorObject.getExpires());
    assertEquals(0, filesystemHandler.getCurrentRegisteredTasks().size());
    assertEquals(2, filesystemHandler.count());
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged, 100);
    MetricsCheck.waitForValueTest(error, countError, 100);
    assertEquals(2, filesystemHandler.count());

    Thread.sleep(1000);
    filesystemHandler.save(bucket, object + 2, new FakeInputStream(accessorObject.getSize()),
        accessorObject.getMetadata(), accessorObject.getExpires());
    assertEquals(3, filesystemHandler.count());
    ccsBufferJob.ccsBufferJob();
    MetricsCheck.waitForValueTest(created, countCreated, 100);
    MetricsCheck.waitForValueTest(purged, countPurged + 2, 100);
    MetricsCheck.waitForValueTest(error, countError, 100);
    assertEquals(1, filesystemHandler.count());

    AccessorProperties.setStorePurgeRetentionSeconds(old);
  }
}
