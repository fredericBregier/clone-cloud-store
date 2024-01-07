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

package io.clonecloudstore.driver.google;

import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
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

import static io.clonecloudstore.common.standard.properties.StandardProperties.STANDARD_EXECUTOR_SERVICE;

/**
 * Google Driver
 */
public class DriverGoogle implements DriverApi {
  private static final Logger LOGGER = Logger.getLogger(DriverGoogle.class);
  static final String BUCKET_DOES_NOT_EXIST = "Bucket does not exist: ";
  static final String OBJECT_DOES_NOT_EXIST = "Object does not exist: ";
  private static final Map<String, TransferStatus> storageObjectCreations = new ConcurrentHashMap<>();
  private final DriverGoogleHelper driverGoogleHelper;

  protected DriverGoogle(final DriverGoogleHelper driverGoogleHelper) throws DriverRuntimeException {
    this.driverGoogleHelper = driverGoogleHelper;
  }

  @Override
  public long bucketsCount() throws DriverException {
    // Count  buckets
    final var response = driverGoogleHelper.getBuckets();
    return response.streamAll().count();
  }

  @Override
  public Stream<StorageBucket> bucketsStream() throws DriverException {
    // List first level buckets from
    final var response = driverGoogleHelper.getBuckets();
    return response.streamAll().map(driverGoogleHelper::fromBucketInfo);
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() throws DriverException {
    final var response = driverGoogleHelper.getBuckets();
    return new StorageBucketIterator(response.iterateAll().iterator());
  }

  @Override
  public StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    return driverGoogleHelper.createBucket(bucket);
  }

  @Override
  public void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    driverGoogleHelper.deleteBucket(bucket);
  }

  @Override
  public boolean bucketExists(final String bucket) throws DriverException {
    return driverGoogleHelper.existBucket(bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    // Count  objects from  bucket if it exists
    return driverGoogleHelper.countObjectsInBucket(bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket, final String prefix, final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    final var iterator = driverGoogleHelper.getObjectsIteratorFilteredInBucket(bucket, prefix, from, to);
    return SystemTools.consumeAll(iterator);
  }

  @Override
  public Stream<StorageObject> objectsStreamInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsStreamInBucket(bucket, null, null, null);
  }

  @Override
  public Stream<StorageObject> objectsStreamInBucket(final String bucket, final String prefix, final Instant from,
                                                     final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    final var stream = driverGoogleHelper.getObjectsStreamFilteredInBucket(bucket, prefix, from, to);
    return stream.map(object -> {
      try {
        return driverGoogleHelper.fromBlob(object);
      } catch (final DriverException e) {
        // Should not occur except if object is deleted in the middle
        throw new DriverRuntimeException(e.getMessage(), e);
      }
    });
  }

  @Override
  public Iterator<StorageObject> objectsIteratorInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsIteratorInBucket(bucket, null, null, null);
  }

  @Override
  public Iterator<StorageObject> objectsIteratorInBucket(final String bucket, final String prefix, final Instant from,
                                                         final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    final var iterator = driverGoogleHelper.getObjectsIteratorFilteredInBucket(bucket, prefix, from, to);
    return new StorageObjectIterator(iterator);
  }

  @Override
  public StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    return driverGoogleHelper.existDirectoryOrObjectInBucket(bucket, directoryOrObject);
  }

  @Override
  public void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    if (!driverGoogleHelper.existBucket(object.bucket())) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + object.bucket());
    }
    if (driverGoogleHelper.existObjectInBucket(object.bucket(), object.name())) {
      throw new DriverAlreadyExistException("Object exists: " + object.name());
    }
    final var countDownLatch = new CountDownLatch(1);
    final var transferStatus = new TransferStatus();
    transferStatus.countDownLatch = countDownLatch;
    transferStatus.exception = null;
    transferStatus.size = 0;
    storageObjectCreations.put(object.bucket() + '/' + object.name(), transferStatus);
    STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        transferStatus.size = driverGoogleHelper.objectPrepareCreateInBucket(object, inputStream);
        SystemTools.silentlyCloseNoException(inputStream);
      } catch (final DriverException e) {
        transferStatus.exception = e;
        LOGGER.error(e, e);
      } finally {
        countDownLatch.countDown();
      }
    });
    Thread.yield();
  }

  @Override
  public StorageObject objectFinalizeCreateInBucket(final String bucket, final String object, final long realLen,
                                                    final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
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
    return driverGoogleHelper.finalizeObject(bucket, object, sha256);
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
    return driverGoogleHelper.getObjectBodyInBucket(bucket, object);
  }

  @Override
  public StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return driverGoogleHelper.getObjectInBucket(bucket, object);
  }

  @Override
  public void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    driverGoogleHelper.deleteObjectInBucket(bucket, object);
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
    private final Iterator<Blob> iterator;

    public StorageObjectIterator(final Iterator<Blob> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public StorageObject next() {
      final var item = iterator.next();
      try {
        return driverGoogleHelper.fromBlob(item);
      } catch (DriverException e) {
        // Should not occur except if object is deleted in the middle
        throw new DriverRuntimeException(e.getMessage(), e);
      }
    }
  }

  private class StorageBucketIterator implements Iterator<StorageBucket> {
    private final Iterator<Bucket> iterator;

    public StorageBucketIterator(final Iterator<Bucket> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public StorageBucket next() {
      final var item = iterator.next();
      return driverGoogleHelper.fromBucketInfo(item.asBucketInfo());
    }
  }
}
