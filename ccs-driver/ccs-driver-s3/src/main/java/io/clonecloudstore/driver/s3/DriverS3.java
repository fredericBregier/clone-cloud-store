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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import io.clonecloudstore.common.quarkus.stream.ChunkInputStream;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import static io.clonecloudstore.common.standard.system.SystemTools.STANDARD_EXECUTOR_SERVICE;

/**
 * S3 Driver
 */
public class DriverS3 implements DriverApi {
  private static final Logger LOGGER = Logger.getLogger(DriverS3.class);
  private static final Map<String, TransferStatus> storageObjectCreations = new ConcurrentHashMap<>();

  private final S3Client s3Client;
  private final DriverS3Helper driverS3Helper;

  protected DriverS3(final DriverS3Helper driverS3Helper) throws DriverRuntimeException {
    this.driverS3Helper = driverS3Helper;
    s3Client = driverS3Helper.getClient();
  }

  @Override
  public long bucketsCount() throws DriverException {
    // Count S3 buckets
    final var response = driverS3Helper.getS3Buckets(s3Client);
    return response.buckets().size();
  }

  @Override
  public Stream<StorageBucket> bucketsStream() throws DriverException {
    // List first level buckets from S3
    final var response = driverS3Helper.getS3Buckets(s3Client);
    final var buckets = response.buckets();
    final List<StorageBucket> directories = new ArrayList<>(buckets.size());
    for (final var bucket : buckets) {
      directories.add(driverS3Helper.fromBucket(bucket));
    }
    return directories.stream();
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() throws DriverException {
    return bucketsStream().iterator();
  }

  @Override
  public StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    return driverS3Helper.createS3Bucket(s3Client, bucket);
  }

  @Override
  public void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    driverS3Helper.deleteS3Bucket(s3Client, bucket);
  }

  @Override
  public boolean bucketExists(final String bucket) throws DriverException {
    return driverS3Helper.existS3Bucket(s3Client, bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    // Count S3 objects from S3 bucket if it exists
    return driverS3Helper.countS3ObjectsInBucket(s3Client, bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket, final String prefix, final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    final var iterator = driverS3Helper.getS3ObjectsIteratorFilteredInBucket(s3Client, bucket, prefix, from, to);
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
    final var stream = driverS3Helper.getS3ObjectsStreamFilteredInBucket(s3Client, bucket, prefix, from, to);
    return stream.map(s3Object -> {
      try {
        return driverS3Helper.fromS3Object(s3Client, bucket, s3Object);
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
    final var iterator = driverS3Helper.getS3ObjectsIteratorFilteredInBucket(s3Client, bucket, prefix, from, to);
    return new StorageObjectIterator(iterator, bucket);
  }

  @Override
  public StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    return driverS3Helper.existS3DirectoryOrObjectInBucket(s3Client, bucket, directoryOrObject);
  }

  @Override
  public void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    if (!driverS3Helper.existS3Bucket(s3Client, object.bucket())) {
      throw new DriverNotFoundException(DriverS3Helper.BUCKET_DOES_NOT_EXIST + object.bucket());
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
        final var exc = objectCreatePreparedAsync(object, inputStream, countDownLatch, transferStatus);
        if (exc != null) {
          transferStatus.exception = exc;
          LOGGER.error(exc, exc);
        }
        SystemTools.silentlyCloseNoException(inputStream);
      } finally {
        countDownLatch.countDown();
      }
    });
    Thread.yield();
  }

  private void checkExistingObjectOnStorage(final StorageObject object)
      throws DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    if (driverS3Helper.existS3ObjectInBucket(s3Client, object.bucket(), object.name())) {
      throw new DriverAlreadyExistException("Object already exists: " + object.bucket() + ":" + object.name());
    }
  }

  private Exception objectCreatePreparedAsync(final StorageObject object, final InputStream inputStream,
                                              final CountDownLatch countDownLatch,
                                              final TransferStatus transferStatus) {
    if (object.size() > 0 && object.size() <= DriverS3Properties.getMaxPartSize()) {
      return objectCreatePreparedAsyncMonoPart(object, inputStream, countDownLatch, transferStatus);
    } else {
      return objectCreatePreparedAsyncMultiParts(object, inputStream, countDownLatch, transferStatus);
    }
  }

  private Exception objectCreatePreparedAsyncMultiParts(final StorageObject object, final InputStream inputStream,
                                                        final CountDownLatch countDownLatch,
                                                        final TransferStatus transferStatus) {
    LOGGER.debugf("Start creation chunked: %s", object.name());
    final var partSize =
        Math.min(object.size() > 0 ? object.size() : DriverS3Properties.getMaxPartSizeForUnknownLength(),
            DriverS3Properties.getMaxPartSizeForUnknownLength());
    final var chunkInputStream = new ChunkInputStream(inputStream, object.size(), (int) partSize);
    try (final var client = driverS3Helper.getClient()) {
      final var multipartUploadHelper = new MultipartUploadHelper(client, object);
      return chunkByChunkAsyncMultiParts(transferStatus, chunkInputStream, multipartUploadHelper);
    } catch (final DriverException e) {
      transferStatus.exception = e;
      return e;
    } finally {
      SystemTools.silentlyCloseNoException(inputStream);
      SystemTools.silentlyCloseNoException(chunkInputStream);
      countDownLatch.countDown();
    }
  }

  private Exception chunkByChunkAsyncMultiParts(final TransferStatus transferStatus,
                                                final ChunkInputStream chunkInputStream,
                                                final MultipartUploadHelper multipartUploadHelper) {
    try {
      while (chunkInputStream.nextChunk()) {
        final var chunkSize = chunkInputStream.getChunkSize();
        LOGGER.debugf("Newt ChunkSize: %d", chunkSize);
        multipartUploadHelper.partUpload(chunkInputStream, chunkSize);
        transferStatus.size += chunkSize;
        Thread.yield();
      }
      multipartUploadHelper.complete();
      Thread.yield();
      return null;
    } catch (final Exception e) {
      LOGGER.debugf("Error: %s", e.getMessage());
      transferStatus.exception = e;
      try {
        LOGGER.debug("Cancel multipart");
        multipartUploadHelper.cancel();
      } catch (final DriverException e2) {
        LOGGER.warn("Cancel in error", e2);
      }
      return e;
    }
  }

  private Exception objectCreatePreparedAsyncMonoPart(final StorageObject object, final InputStream inputStream,
                                                      final CountDownLatch countDownLatch,
                                                      final TransferStatus transferStatus) {
    LOGGER.debugf("Start creation direct: %s", object.name());
    try (final var client = driverS3Helper.getClient()) {
      driverS3Helper.createS3ObjectInBucketNoCheck(client, object, inputStream);
      transferStatus.size = object.size();
      Thread.yield();
      return null;
    } catch (final DriverException e) {
      LOGGER.errorf("Error: %s", e);
      transferStatus.exception = e;
      return e;
    } finally {
      SystemTools.silentlyCloseNoException(inputStream);
      countDownLatch.countDown();
    }
  }

  @Override
  public StorageObject objectFinalizeCreateInBucket(final String bucket, final String object, final long realLen,
                                                    final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    final var transferStatus = storageObjectCreations.remove(bucket + '/' + object);
    if (transferStatus == null) {
      throw new DriverException("Request not found while finishing creation request: " + bucket + '/' + object);
    }
    try {
      transferStatus.countDownLatch.await();
    } catch (final InterruptedException e) {
      transferStatus.exception = e;
      Thread.currentThread().interrupt();
    }
    final var exception = transferStatus.exception;
    if (exception != null) {
      if (exception instanceof DriverException e) {
        throw e;
      }
      throw new DriverException("Issue during creation", exception);
    }
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
      return driverS3Helper.getS3ObjectBodyInBucket(s3Client, bucket, object, false);
    } catch (final NoSuchBucketException | NoSuchKeyException e) {
      throw new DriverNotFoundException(e);
    }
  }

  @Override
  public StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return driverS3Helper.getS3ObjectInBucket(s3Client, bucket, object);
  }

  @Override
  public void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    driverS3Helper.deleteS3ObjectInBucket(s3Client, bucket, object);
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }

  private static class TransferStatus {
    CountDownLatch countDownLatch;
    Exception exception;
    long size;
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
