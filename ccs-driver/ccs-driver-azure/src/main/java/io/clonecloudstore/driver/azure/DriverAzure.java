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

import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import com.azure.storage.blob.models.BlobItem;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.exception.DriverRuntimeException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.standard.system.SystemTools.STANDARD_EXECUTOR_SERVICE;

/**
 * Azure Driver
 */
public class DriverAzure implements DriverApi {
  private static final Logger LOGGER = Logger.getLogger(DriverAzure.class);
  static final String BUCKET_DOES_NOT_EXIST = "Bucket does not exist: ";
  private static final Map<String, TransferStatus> storageObjectCreations = new ConcurrentHashMap<>();
  private final DriverAzureHelper driverAzureHelper;

  protected DriverAzure(final DriverAzureHelper driverAzureHelper) throws DriverRuntimeException {
    this.driverAzureHelper = driverAzureHelper;
  }

  @Override
  public long bucketsCount() throws DriverException {
    try {
      // Count  buckets
      final var response = driverAzureHelper.getBuckets();
      return response.stream().count();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Stream<StorageBucket> bucketsStream() throws DriverException {
    try {
      // List first level buckets from
      final var response = driverAzureHelper.getBuckets();
      return response.stream().map(driverAzureHelper::fromBlobContainerItem);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() throws DriverException {
    try {
      return bucketsStream().iterator();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      return driverAzureHelper.createBucket(bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      driverAzureHelper.deleteBucket(bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public boolean bucketExists(final String bucket) throws DriverException {
    try {
      return driverAzureHelper.existBucket(bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      // Count  objects from  bucket if it exists
      return driverAzureHelper.countObjectsInBucket(bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public long objectsCountInBucket(final String bucket, final String prefix, final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      final var iterator = driverAzureHelper.getObjectsIteratorFilteredInBucket(bucket, prefix, from, to);
      return SystemTools.consumeAll(iterator);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Stream<StorageObject> objectsStreamInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      return objectsStreamInBucket(bucket, null, null, null);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Stream<StorageObject> objectsStreamInBucket(final String bucket, final String prefix, final Instant from,
                                                     final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      final var stream = driverAzureHelper.getObjectsStreamFilteredInBucket(bucket, prefix, from, to);
      return stream.map(object -> {
        try {
          return driverAzureHelper.fromBlobItem(bucket, object);
        } catch (final DriverException e) {
          // Should not occur except if object is deleted in the middle
          throw new DriverRuntimeException(e.getMessage(), e);
        }
      });
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Iterator<StorageObject> objectsIteratorInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      return objectsIteratorInBucket(bucket, null, null, null);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Iterator<StorageObject> objectsIteratorInBucket(final String bucket, final String prefix, final Instant from,
                                                         final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      final var iterator = driverAzureHelper.getObjectsIteratorFilteredInBucket(bucket, prefix, from, to);
      return new StorageObjectIterator(iterator, bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    try {
      return driverAzureHelper.existDirectoryOrObjectInBucket(bucket, directoryOrObject);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      if (!driverAzureHelper.existBucket(object.bucket())) {
        throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + object.bucket());
      }
      checkExistingObjectOnStorage(object);
      final var countDownLatch = new CountDownLatch(1);
      final var transferStatus = new TransferStatus();
      transferStatus.countDownLatch = countDownLatch;
      transferStatus.exception = null;
      transferStatus.size = 0;
      storageObjectCreations.put(object.bucket() + '/' + object.name(), transferStatus);
      STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          transferStatus.size = driverAzureHelper.objectPrepareCreateInBucket(object, inputStream);
          SystemTools.silentlyCloseNoException(inputStream);
        } catch (final DriverException e) {
          transferStatus.exception = e;
          LOGGER.error(e, e);
        } finally {
          countDownLatch.countDown();
        }
      });
      Thread.yield();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private void checkExistingObjectOnStorage(final StorageObject object)
      throws DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      if (driverAzureHelper.existObjectInBucket(object.bucket(), object.name())) {
        throw new DriverAlreadyExistException("Object already exists: " + object.bucket() + ":" + object.name());
      }
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public StorageObject objectFinalizeCreateInBucket(final String bucket, final String object, final long realLen,
                                                    final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      final var transferStatus = storageObjectCreations.remove(bucket + '/' + object);
      if (transferStatus == null) {
        throw new DriverException("Request not found while finishing creation request: " + bucket + '/' + object);
      }
      await(transferStatus);
      final var exception = transferStatus.exception;
      if (exception != null) {
        if (exception instanceof DriverException e) {
          throw e;
        }
        throw new DriverException("Issue during creation", exception);
      }
      return finalizeStorageObject(bucket, object, realLen, sha256);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private StorageObject finalizeStorageObject(final String bucket, final String object, final long realLen,
                                              final String sha256) throws DriverException {
    try {
      return driverAzureHelper.finalizeObject(bucket, object, sha256, realLen);
    } catch (final DriverNotAcceptableException e) {
      throw new DriverException("Issue during waiting creation ending", e);
    }
  }

  private static void await(final TransferStatus transferStatus) {
    try {
      transferStatus.countDownLatch.await();
    } catch (final InterruptedException e) {
      transferStatus.exception = e;
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public InputStream objectGetInputStreamInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      return driverAzureHelper.getObjectBodyInBucket(bucket, object);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      return driverAzureHelper.getObjectInBucket(bucket, object);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      driverAzureHelper.deleteObjectInBucket(bucket, object);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public void close() {
    // Empty
  }

  private static class TransferStatus {
    CountDownLatch countDownLatch;
    Exception exception;
    long size;
  }

  private class StorageObjectIterator implements Iterator<StorageObject> {
    private final Iterator<BlobItem> iterator;
    private final String bucket;

    public StorageObjectIterator(final Iterator<BlobItem> iterator, final String bucket) {
      this.iterator = iterator;
      this.bucket = bucket;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public StorageObject next() {
      final var item = iterator.next();
      try {
        return driverAzureHelper.fromBlobItem(bucket, item);
      } catch (DriverException e) {
        // Should not occur except if object is deleted in the middle
        throw new DriverRuntimeException(e.getMessage(), e);
      }
    }
  }
}
