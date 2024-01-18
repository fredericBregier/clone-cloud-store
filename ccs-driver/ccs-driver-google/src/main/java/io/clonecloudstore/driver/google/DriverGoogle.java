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
import java.util.stream.Stream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
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
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;

/**
 * Google Driver
 */
public class DriverGoogle implements DriverApi {
  private static final Logger LOGGER = Logger.getLogger(DriverGoogle.class);
  static final String BUCKET_DOES_NOT_EXIST = "Bucket does not exist: ";
  static final String OBJECT_DOES_NOT_EXIST = "Object does not exist: ";
  private final DriverGoogleHelper driverGoogleHelper;
  private final BulkMetrics bulkMetrics;

  protected DriverGoogle(final DriverGoogleHelper driverGoogleHelper) throws DriverRuntimeException {
    this.driverGoogleHelper = driverGoogleHelper;
    bulkMetrics = CDI.current().select(BulkMetrics.class).get();
  }

  DriverGoogleHelper getDriverGoogleHelper() {
    return driverGoogleHelper;
  }

  @Override
  public long bucketsCount() throws DriverException {
    // Count  buckets
    final var response = driverGoogleHelper.getBuckets();
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_COUNT);
    return response.streamAll().count();
  }

  @Override
  public Stream<StorageBucket> bucketsStream() throws DriverException {
    // List first level buckets from
    final var response = driverGoogleHelper.getBuckets();
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_STREAM);
    return response.streamAll().map(driverGoogleHelper::fromBucketInfo);
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() throws DriverException {
    final var response = driverGoogleHelper.getBuckets();
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_COUNT);
    return new StorageBucketIterator(response.iterateAll().iterator());
  }

  @Override
  public StorageBucket bucketGet(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_READ);
    return driverGoogleHelper.getBucket(bucket);
  }

  @Override
  public StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_CREATE);
      return driverGoogleHelper.createBucket(bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public StorageBucket bucketImport(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_CREATE);
      return driverGoogleHelper.importBucket(bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_DELETE);
      driverGoogleHelper.deleteBucket(bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_DELETE);
      throw e;
    }
  }

  @Override
  public boolean bucketExists(final String bucket) throws DriverException {
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_EXISTS);
    return driverGoogleHelper.existBucket(bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    // Count  objects from  bucket if it exists
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COUNT);
    return driverGoogleHelper.countObjectsInBucket(bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket, final String prefix, final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    final var iterator = driverGoogleHelper.getObjectsIteratorFilteredInBucket(bucket, prefix, from, to);
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COUNT);
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
    try {
      final var stream = driverGoogleHelper.getObjectsStreamFilteredInBucket(bucket, prefix, from, to);
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_STREAM);
      return stream.map(object -> {
        try {
          return driverGoogleHelper.fromBlob(object);
        } catch (final DriverException e) {
          // Should not occur except if object is deleted in the middle
          throw new DriverRuntimeException(e.getMessage(), e);
        }
      });
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
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
    try {
      final var iterator = driverGoogleHelper.getObjectsIteratorFilteredInBucket(bucket, prefix, from, to);
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_STREAM);
      return new StorageObjectIterator(iterator);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    try {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_EXISTS);
      return driverGoogleHelper.existDirectoryOrObjectInBucket(bucket, directoryOrObject);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      if (!driverGoogleHelper.existBucket(object.bucket())) {
        throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + object.bucket());
      }
      if (driverGoogleHelper.existObjectInBucket(object.bucket(), object.name())) {
        throw new DriverAlreadyExistException("Object exists: " + object.name());
      }
      var size = driverGoogleHelper.objectPrepareCreateInBucket(object, inputStream);
      LOGGER.infof("Imported object %s of size %d", object, size);
      SystemTools.silentlyCloseNoException(inputStream);
      Thread.yield();
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public StorageObject objectFinalizeCreateInBucket(final String bucket, final String object, final long realLen,
                                                    final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    return driverGoogleHelper.finalizeObject(bucket, object, sha256);
  }

  @Override
  public InputStream objectGetInputStreamInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_READ);
      return driverGoogleHelper.getObjectBodyInBucket(bucket, object);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public StorageObject objectCopy(final StorageObject objectSource, final StorageObject objectTarget)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      validCopy(objectSource, objectTarget);
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COPY);
      return driverGoogleHelper.objectCopyToAnother(objectSource, objectTarget);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_READ_MD);
    return driverGoogleHelper.getObjectInBucket(bucket, object);
  }

  @Override
  public void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_DELETE);
      driverGoogleHelper.deleteObjectInBucket(bucket, object);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverGoogle.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_DELETE);
      throw e;
    }
  }

  @Override
  public void close() {
    // Empty
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
      return driverGoogleHelper.fromBucketInfo(item);
    }
  }
}
