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

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.stream.ChunkInputStreamInterface;
import io.clonecloudstore.common.quarkus.stream.ChunkInputStreamOptionalBuffer;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * S3 Driver
 */
public class DriverS3 implements DriverApi {
  private static final Logger LOGGER = Logger.getLogger(DriverS3.class);
  private final S3Client s3Client;
  private final DriverS3Helper driverS3Helper;
  private final BulkMetrics bulkMetrics;

  protected DriverS3(final DriverS3Helper driverS3Helper) throws DriverRuntimeException {
    this.driverS3Helper = driverS3Helper;
    s3Client = driverS3Helper.getClient();
    bulkMetrics = CDI.current().select(BulkMetrics.class).get();
  }

  S3Client getS3Client() {
    return s3Client;
  }

  @Override
  public long bucketsCount() throws DriverException {
    // Count S3 buckets
    final var response = driverS3Helper.getBuckets(s3Client);
    bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_COUNT);
    return response.buckets().size();
  }

  @Override
  public Stream<StorageBucket> bucketsStream() throws DriverException {
    // List first level buckets from S3
    final var response = driverS3Helper.getBuckets(s3Client);
    final var buckets = response.buckets();
    final List<StorageBucket> directories = new ArrayList<>(buckets.size());
    for (final var bucket : buckets) {
      final var clientId = driverS3Helper.getClientIdTag(s3Client, bucket.name());
      directories.add(driverS3Helper.fromBucket(bucket, clientId));
    }
    bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_STREAM);
    return directories.stream();
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() throws DriverException {
    return bucketsStream().iterator();
  }

  @Override
  public StorageBucket bucketGet(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_READ);
    return driverS3Helper.getBucket(s3Client, bucket);
  }

  @Override
  public StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_CREATE);
      return driverS3Helper.createBucket(s3Client, bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public StorageBucket bucketImport(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_CREATE);
      return driverS3Helper.importBucket(s3Client, bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      driverS3Helper.deleteBucket(s3Client, bucket);
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_DELETE);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_DELETE);
      throw e;
    }
  }

  @Override
  public boolean bucketExists(final String bucket) throws DriverException {
    bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_EXISTS);
    return driverS3Helper.existBucket(s3Client, bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      // Count S3 objects from S3 bucket if it exists
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COUNT);
      return driverS3Helper.countObjectsInBucket(s3Client, bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public long objectsCountInBucket(final String bucket, final String prefix, final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      final var iterator = driverS3Helper.getObjectsIteratorFilteredInBucket(s3Client, bucket, prefix, from, to);
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COUNT);
      return SystemTools.consumeAll(iterator);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
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
      final var stream = driverS3Helper.getObjectsStreamFilteredInBucket(s3Client, bucket, prefix, from, to);
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_STREAM);
      return stream.map(s3Object -> {
        try {
          return driverS3Helper.fromS3Object(s3Client, bucket, s3Object);
        } catch (final DriverException e) {
          // Should not occur except if object is deleted in the middle
          throw new DriverRuntimeException(e.getMessage(), e);
        }
      });
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
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
      final var iterator = driverS3Helper.getObjectsIteratorFilteredInBucket(s3Client, bucket, prefix, from, to);
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_STREAM);
      return new StorageObjectIterator(iterator, bucket);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_BUCKET, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    try {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_EXISTS);
      return driverS3Helper.existDirectoryOrObjectInBucket(s3Client, bucket, directoryOrObject);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      if (!driverS3Helper.existBucket(s3Client, object.bucket())) {
        throw new DriverNotFoundException(DriverS3Helper.BUCKET_DOES_NOT_EXIST + object.bucket());
      }
      checkExistingObjectOnStorage(object);
      var exc = objectCreatePreparedAsync(object, inputStream);
      SystemTools.silentlyCloseNoException(inputStream);
      if (exc != null) {
        LOGGER.error(exc, exc);
        if (exc instanceof DriverException e) {
          throw e;
        }
        throw new DriverException("Issue during creation", exc);
      }
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  private void checkExistingObjectOnStorage(final StorageObject object)
      throws DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    if (driverS3Helper.existObjectInBucket(s3Client, object.bucket(), object.name())) {
      throw new DriverAlreadyExistException("Object already exists: " + object.bucket() + ":" + object.name());
    }
  }

  private Exception objectCreatePreparedAsync(final StorageObject object, final InputStream inputStream) {
    if (object.size() > 0 && object.size() <= DriverS3Properties.getMaxPartSize()) {
      return objectCreatePreparedAsyncMonoPart(object, inputStream);
    } else {
      return objectCreatePreparedAsyncMultiParts(object, inputStream);
    }
  }

  private Exception objectCreatePreparedAsyncMultiParts(final StorageObject object, final InputStream inputStream) {
    LOGGER.debugf("Start creation chunked: %s", object.name());
    final var partSize =
        Math.min(object.size() > 0 ? object.size() : DriverS3Properties.getMaxPartSizeForUnknownLength(),
            DriverS3Properties.getMaxPartSizeForUnknownLength());
    final var chunkInputStream = new ChunkInputStreamOptionalBuffer(inputStream, object.size(), (int) partSize);
    try (final var client = driverS3Helper.getClient()) {
      final var multipartUploadHelper = new MultipartUploadHelper(client, object);
      return chunkByChunkAsyncMultiParts(chunkInputStream, multipartUploadHelper);
    } catch (final DriverException e) {
      return e;
    } finally {
      SystemTools.silentlyCloseNoException(inputStream);
      SystemTools.silentlyCloseNoException(chunkInputStream);
    }
  }

  private Exception chunkByChunkAsyncMultiParts(final ChunkInputStreamInterface chunkInputStream,
                                                final MultipartUploadHelper multipartUploadHelper) {
    try {
      while (chunkInputStream.nextChunk()) {
        final var chunkSize = chunkInputStream.getAvailableChunkSize();
        LOGGER.debugf("Newt ChunkSize: %d", chunkSize);
        multipartUploadHelper.partUpload((InputStream) chunkInputStream, chunkSize);
        Thread.yield();
      }
      multipartUploadHelper.complete();
      Thread.yield();
      return null;
    } catch (final Exception e) {
      LOGGER.debugf("Error: %s", e.getMessage());
      try {
        LOGGER.debug("Cancel multipart");
        multipartUploadHelper.cancel();
      } catch (final DriverException e2) {
        LOGGER.warn("Cancel in error", e2);
      }
      return e;
    }
  }

  private Exception objectCreatePreparedAsyncMonoPart(final StorageObject object, final InputStream inputStream) {
    LOGGER.debugf("Start creation direct: %s", object.name());
    try (final var client = driverS3Helper.getClient()) {
      driverS3Helper.createObjectInBucket(client, object, inputStream);
      return null;
    } catch (final DriverException e) {
      return e;
    } finally {
      SystemTools.silentlyCloseNoException(inputStream);
    }
  }

  @Override
  public StorageObject objectFinalizeCreateInBucket(final String bucket, final String object, final long realLen,
                                                    final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    try {
      return driverS3Helper.waitUntilObjectExist(s3Client, bucket, object, sha256);
    } catch (final DriverNotAcceptableException e) {
      throw new DriverException("Issue during waiting creation ending", e);
    }
  }

  @Override
  public InputStream objectGetInputStreamInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_READ);
      return driverS3Helper.getObjectBodyInBucket(s3Client, bucket, object, false);
    } catch (final NoSuchBucketException | NoSuchKeyException e) {
      throw new DriverNotFoundException(e);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_READ);
      throw e;
    }
  }

  @Override
  public StorageObject objectCopy(final StorageObject objectSource, final StorageObject objectTarget)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      validCopy(objectSource, objectTarget);
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_COPY);
      return driverS3Helper.objectCopyToAnother(s3Client, objectSource, objectTarget);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_WRITE);
      throw e;
    }
  }

  @Override
  public StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_READ_MD);
    return driverS3Helper.getObjectInBucket(s3Client, bucket, object);
  }

  @Override
  public void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_DELETE);
      driverS3Helper.deleteObjectInBucket(s3Client, bucket, object);
    } catch (final DriverException e) {
      bulkMetrics.incrementCounter(1, DriverS3.class, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ERROR_DELETE);
      throw e;
    }
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }

  private class StorageObjectIterator implements Iterator<StorageObject> {
    private final Iterator<S3Object> iterator;
    private final String bucket;

    public StorageObjectIterator(final Iterator<S3Object> iterator, final String bucket) {
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
        return driverS3Helper.fromS3Object(s3Client, bucket, item);
      } catch (DriverException e) {
        // Should not occur except if object is deleted in the middle
        throw new DriverRuntimeException(e.getMessage(), e);
      }
    }
  }
}
