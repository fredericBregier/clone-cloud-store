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

package io.clonecloudstore.driver.api;

import java.io.Closeable;
import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;

/**
 * Driver Java Interface for Object Storage. This shall be retrieved through the DriverApiFactory and closed when
 * usage is over.
 */
public interface DriverApi extends Closeable {
  /**
   * Count Buckets
   */
  long bucketsCount() throws DriverException;

  /**
   * Stream Buckets list
   */
  Stream<StorageBucket> bucketsStream() throws DriverException;

  /**
   * Iterator on Buckets list
   */
  Iterator<StorageBucket> bucketsIterator() throws DriverException;

  /**
   * Get one Bucket and returns it
   *
   * @return the StorageBucket as instantiated within the Object Storage (real values)
   */
  StorageBucket bucketGet(String bucket) throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Create one Bucket and returns it
   *
   * @param bucket contains various information that could be implemented within Object Storage, but, except the name
   *               of the bucket and the client Id, nothing is mandatory
   * @return the StorageBucket as instantiated within the Object Storage (real values)
   */
  StorageBucket bucketCreate(StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException; // NOSONAR Exception details

  /**
   * Import one Bucket and returns it
   *
   * @param bucket contains various information that could be implemented within Object Storage, but, except the name
   *               of the bucket and the client Id, nothing is mandatory
   * @return the StorageBucket as instantiated within the Object Storage (real values)
   */
  StorageBucket bucketImport(StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException; // NOSONAR Exception details

  /**
   * Delete one Bucket if it exists and is empty
   */
  void bucketDelete(String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Delete one Bucket if it exists and is empty
   */
  default void bucketDelete(StorageBucket bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    bucketDelete(bucket.bucket());
  }

  /**
   * Check existence of Bucket
   */
  boolean bucketExists(String bucket) throws DriverException;

  /**
   * Check existence of Bucket
   */
  default boolean bucketExists(StorageBucket bucket) throws DriverException {
    return bucketExists(bucket.bucket());
  }

  /**
   * Count Objects in specified Bucket
   */
  long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Count Objects in specified Bucket
   */
  default long objectsCountInBucket(final StorageBucket bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsCountInBucket(bucket.bucket());
  }

  /**
   * Count Objects in specified Bucket with filters (all optionals)
   */
  long objectsCountInBucket(String bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Count Objects in specified Bucket with filters (all optionals)
   */
  default long objectsCountInBucket(StorageBucket bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsCountInBucket(bucket.bucket(), prefix, from, to);
  }

  /**
   * Stream Objects in specified Bucket
   */
  Stream<StorageObject> objectsStreamInBucket(String bucket)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Stream Objects in specified Bucket
   */
  default Stream<StorageObject> objectsStreamInBucket(StorageBucket bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsStreamInBucket(bucket.bucket());
  }

  /**
   * Stream Objects in specified Bucket with filters (all optionals)
   */
  Stream<StorageObject> objectsStreamInBucket(String bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Stream Objects in specified Bucket with filters (all optionals)
   */
  default Stream<StorageObject> objectsStreamInBucket(StorageBucket bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsStreamInBucket(bucket.bucket(), prefix, from, to);
  }

  /**
   * Iterator on Objects in specified Bucket
   */
  Iterator<StorageObject> objectsIteratorInBucket(String bucket)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Iterator on Objects in specified Bucket
   */
  default Iterator<StorageObject> objectsIteratorInBucket(StorageBucket bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsIteratorInBucket(bucket.bucket());
  }

  /**
   * Iterator on Objects in specified Bucket with filters (all optionals)
   */
  Iterator<StorageObject> objectsIteratorInBucket(String bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Iterator on Objects in specified Bucket with filters (all optionals)
   */
  default Iterator<StorageObject> objectsIteratorInBucket(StorageBucket bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectsIteratorInBucket(bucket.bucket(), prefix, from, to);
  }

  /**
   * Check if Directory or Object exists in specified Bucket (based on prefix)
   */
  StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException;

  /**
   * Check if Directory or Object exists in specified Bucket (based on prefix)
   */
  default StorageType directoryOrObjectExistsInBucket(final StorageBucket bucket, final String directoryOrObject)
      throws DriverException {
    return directoryOrObjectExistsInBucket(bucket.bucket(), directoryOrObject);
  }

  /**
   * First step in creation of an object within a Bucket. The InputStream is ready to be read in
   * a concurrent independent thread to be provided by the driver. Sha256 might be null or empty. Len might be 0,
   * meaning unknown.
   *
   * @param object contains various information that could be implemented within Object Storage, but, except the name
   *               of the bucket and the key of the object, nothing is mandatory
   */
  void objectPrepareCreateInBucket(StorageObject object, InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException; // NOSONAR Exception details

  /**
   * Second step in creation of an object within a Bucket. Sha256 might be null or empty. Reallen must not be 0.
   * This method waits for the prepare method to end and returns the final result.
   *
   * @return the StorageObject as instantiated within the Object Storage (real values)
   */
  StorageObject objectFinalizeCreateInBucket(String bucket, String object, long realLen, String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException; // NOSONAR Exception details

  /**
   * Second step in creation of an object within a Bucket. Sha256 might be null or empty. Reallen must not be 0.
   * This method waits for the prepare method to end and returns the final result.
   *
   * @return the StorageObject as instantiated within the Object Storage (real values)
   */
  default StorageObject objectFinalizeCreateInBucket(StorageObject object, long realLen, String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException {// NOSONAR Exception details
    return objectFinalizeCreateInBucket(object.bucket(), object.name(), realLen, sha256);
  }

  /**
   * Get the content of the specified Object within specified Bucket
   */
  InputStream objectGetInputStreamInBucket(String bucket, String object)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Get the content of the specified Object within specified Bucket
   */
  default InputStream objectGetInputStreamInBucket(StorageObject object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return objectGetInputStreamInBucket(object.bucket(), object.name());
  }

  default void validCopy(StorageObject objectSource, StorageObject objectTarget) throws DriverException {
    if (ParametersChecker.isEmpty(objectSource, objectTarget) ||
        ParametersChecker.isEmpty(objectSource.bucket(), objectSource.name(), objectTarget.bucket(),
            objectTarget.name())) {
      throw new DriverException("Source and Target cannot be null");
    }
    if (Objects.equals(objectSource.bucket(), objectTarget.bucket()) &&
        Objects.equals(objectSource.name(), objectTarget.name())) {
      throw new DriverException("Source and Target cannot be the same object");
    }
  }

  /**
   * Copy one object to another one, possibly in a different bucket.
   * Hash and Size come from given source, while Metadata and expired date come from target
   *
   * @param objectSource source object
   * @param objectTarget target object
   */
  StorageObject objectCopy(StorageObject objectSource, StorageObject objectTarget)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException;

  /**
   * Copy one object to another one, possibly in a different bucket.
   * Hash and Size come from real source, while Metadata and expired date come from target
   */
  default StorageObject objectCopy(String bucketSource, String objectSource, String bucketTarget, String objectTarget,
                                   Map<String, String> targetMetadata, Instant expireDate)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    if (ParametersChecker.isEmpty(bucketSource, objectSource, bucketTarget, objectTarget)) {
      throw new DriverException("Source and Target cannot be null");
    }
    if (Objects.equals(objectSource, objectTarget) && Objects.equals(bucketSource, bucketTarget)) {
      throw new DriverException("Source and Target cannot be the same object");
    }
    final var storageObjectSource = objectGetMetadataInBucket(bucketSource, objectSource);
    StorageObject storageObjectTarget =
        new StorageObject(bucketTarget, objectTarget, storageObjectSource.hash(), storageObjectSource.size(),
            Instant.now(), expireDate, targetMetadata);
    return objectCopy(storageObjectSource, storageObjectTarget);
  }

  /**
   * Get the Object metadata from this Bucket (those available from Object Storage)
   */
  StorageObject objectGetMetadataInBucket(String bucket, String object)
      throws DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Get the Object metadata from this Bucket (those available from Object Storage)
   */
  default StorageObject objectGetMetadataInBucket(StorageObject object)
      throws DriverNotFoundException, DriverException {// NOSONAR Exception details
    return objectGetMetadataInBucket(object.bucket(), object.name());
  }

  /**
   * Delete the Object from this Bucket
   */
  void objectDeleteInBucket(String bucket, String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException; // NOSONAR Exception details

  /**
   * Delete the Object from this Bucket
   */
  default void objectDeleteInBucket(StorageObject object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR Exception details
    objectDeleteInBucket(object.bucket(), object.name());
  }

  /**
   * Close with no exception.
   * Closes this resource, relinquishing any underlying resources. This method is invoked automatically on objects
   * managed by the try-with-resources statement.
   */
  @Override
  void close();
}
