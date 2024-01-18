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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageObject;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;

@Dependent
public abstract class CcsBufferService {
  private static final Logger LOGGER = Logger.getLogger(CcsBufferService.class);
  public static final String BUFFERED_IMPORT = "buffered_import";
  protected final DriverApiFactory driverApiFactory;
  private final FilesystemHandler filesystemHandler;
  private final BulkMetrics bulkMetrics;

  protected CcsBufferService() {
    this.driverApiFactory = DriverApiRegistry.getDriverApiFactory();
    this.filesystemHandler = CDI.current().select(FilesystemHandler.class).get();
    this.bulkMetrics = CDI.current().select(BulkMetrics.class).get();
  }

  /**
   * @return the associated DTO AccessorObject from Database
   */
  protected abstract AccessorObject getAccessorObjectFromDb(final String bucket, final String object);

  /**
   * Update into Database the object with the given status
   */
  protected abstract void updateStatusAccessorObject(final AccessorObject object, final AccessorStatus status);

  public void asyncJobRetryImport() {
    try {
      List<BufferedItem> toActOn;
      synchronized (this) {
        toActOn = new ArrayList<>(filesystemHandler.getCurrentRegisteredTasks());
      }
      final AtomicInteger count = new AtomicInteger();
      final AtomicInteger countError = new AtomicInteger();
      final var validItems = executeRegisteredImportsFromLocalBuffer(toActOn, count, countError);
      filesystemHandler.removedValidatedTasks(validItems);
      bulkMetrics.incrementCounter(validItems.size(), BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_UNREGISTER);
      bulkMetrics.incrementCounter(count.get(), BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COPY);
      bulkMetrics.incrementCounter(countError.get(), BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_ERROR_WRITE);
    } catch (final RuntimeException e) {
      LOGGER.warnf("Error during retry of buffered items (%s)", e);
    }
  }

  public void asyncJobCleanup() {
    final var limitDate = Instant.now().minusSeconds(AccessorProperties.getStorePurgeRetentionSeconds());
    try {
      final var count = filesystemHandler.deleteOlderThan(limitDate);
      bulkMetrics.incrementCounter(count, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_PURGE);
    } catch (final Exception e) {
      LOGGER.warnf("Error during purge of buffered items (%s)", e);
    }
  }

  /**
   * @param items the items to try to import
   * @return the list of valid items, i.e. items successfully imported
   */
  private List<BufferedItem> executeRegisteredImportsFromLocalBuffer(final List<BufferedItem> items,
                                                                     final AtomicInteger count,
                                                                     final AtomicInteger countError) {
    final var valid = new ArrayList<BufferedItem>();
    try (final var driver = driverApiFactory.getInstance()) {
      for (final var bufferedItem : items) {
        if (tryReimport(bufferedItem, driver, count, countError)) {
          valid.add(bufferedItem);
          filesystemHandler.delete(bufferedItem.bucket(), bufferedItem.object());
        }
      }
      return valid;
    } catch (DriverServiceNotAvailable e) {
      LOGGER.warnf("Driver service unavailable (%s)", e);
      return valid;
    }
  }

  private boolean tryReimport(final BufferedItem item, final DriverApi driverApi, final AtomicInteger count,
                              final AtomicInteger countError) throws DriverServiceNotAvailable {
    final var accessorObject = getAccessorObjectFromDb(item.bucket(), item.object());
    if (accessorObject == null || !AccessorStatus.READY.equals(accessorObject.getStatus())) {
      // No more in Database so abort item
      return true;
    }
    try {
      if (!driverApi.bucketExists(item.bucket())) {
        // Bucket does not exist, so abort item
        countError.incrementAndGet();
        updateStatusAccessorObject(accessorObject, AccessorStatus.ERR_UPL);
        return true;
      }
      if (StorageType.OBJECT.equals(driverApi.directoryOrObjectExistsInBucket(item.bucket(), item.object()))) {
        // Object already imported, abort item
        return true;
      }
      final var storageObject = QuarkusProperties.hasDatabase() ?
          new StorageObject(accessorObject.getBucket(), accessorObject.getName(), accessorObject.getHash(),
              accessorObject.getSize(), accessorObject.getCreation(), accessorObject.getExpires(),
              accessorObject.getMetadata()) : filesystemHandler.readStorageObject(item.bucket(), item.object());
      final var inputStream = filesystemHandler.readContent(item.bucket(), item.object());
      driverApi.objectPrepareCreateInBucket(storageObject, inputStream);
      driverApi.objectFinalizeCreateInBucket(storageObject.bucket(), storageObject.name(), storageObject.size(),
          storageObject.hash());
      count.incrementAndGet();
      return true;
    } catch (final DriverException e) {
      // Driver service not accessible, fully retry later on
      LOGGER.debugf("Error while using Driver (%s)", e);
      throw new DriverServiceNotAvailable(e);
    } catch (final FileNotFoundException e) {
      // Buffered item no more available, so abort item
      LOGGER.debugf("Error while checking buffer (%s)", e);
      countError.incrementAndGet();
      updateStatusAccessorObject(accessorObject, AccessorStatus.ERR_UPL);
      return true;
    } catch (final RuntimeException | IOException e) {
      // Error while accessing item, will retry later on
      LOGGER.debugf("Error while accessing buffer (%s)", e);
      return false;
    }
  }
}
