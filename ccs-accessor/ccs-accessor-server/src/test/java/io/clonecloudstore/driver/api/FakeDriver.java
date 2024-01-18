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

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.test.stream.FakeInputStream;
import org.jboss.logging.Logger;

/**
 * Fake Driver that does not store anything except references in memory
 */
public final class FakeDriver implements DriverApi {
  private static final Logger LOGGER = Logger.getLogger(FakeDriver.class);
  private static final Map<String, StorageBucket> STORAGE_BUCKET_MAP = new ConcurrentHashMap<>();
  private static final Map<String, List<StorageObject>> STORAGE_OBJECT_MAP = new ConcurrentHashMap<>();
  private static final Map<String, CountDownLatch> STORAGE_OBJECT_CREATIONS = new ConcurrentHashMap<>();
  private static final String BUCKET_NOT_FOUND = "Bucket not found";
  private static final String OBJECT_NOT_FOUND = "Object not found";
  private final byte[] bytes = new byte[StandardProperties.getBufSize()];
  public static boolean shallRaiseAnException = false;

  public FakeDriver() {
    LOGGER.info("Fake Driver Creation");
  }

  @Override
  public synchronized long bucketsCount() throws DriverException {
    try {
      return STORAGE_BUCKET_MAP.size();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Stream<StorageBucket> bucketsStream() throws DriverException {
    try {
      return STORAGE_BUCKET_MAP.values().stream();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() throws DriverException {
    return bucketsStream().iterator();
  }

  @Override
  public synchronized StorageBucket bucketGet(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    try {
      final var storageBucket = STORAGE_BUCKET_MAP.get(bucket);
      if (storageBucket == null) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      return storageBucket;
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    try {
      if (STORAGE_BUCKET_MAP.containsKey(bucket.bucket())) {
        throw new DriverAlreadyExistException("Already exists");
      }
      final var storageBucket = new StorageBucket(bucket.bucket(), bucket.clientId(), Instant.now());
      STORAGE_BUCKET_MAP.put(bucket.bucket(), storageBucket);
      STORAGE_OBJECT_MAP.put(bucket.bucket(), new ArrayList<>());
      return storageBucket;
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized StorageBucket bucketImport(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    return bucketCreate(bucket);
  }

  @Override
  public synchronized void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      // Check the existence and removes it
      if (STORAGE_BUCKET_MAP.containsKey(bucket)) {
        // Check objects from Bucket
        final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
        if (storageObjectList.isEmpty()) {
          STORAGE_BUCKET_MAP.remove(bucket);
          STORAGE_OBJECT_MAP.remove(bucket);
          return;
        }
        throw new DriverNotAcceptableException("Bucket is not empty");
      }
      throw new DriverNotFoundException(BUCKET_NOT_FOUND);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized boolean bucketExists(final String bucket) throws DriverException {
    if (shallRaiseAnException) {
      throw new DriverException("Issue with Driver");
    }
    try {
      return STORAGE_BUCKET_MAP.containsKey(bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      // Count objects from bucket if it exists
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      return storageObjectList.size();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized long objectsCountInBucket(final String bucket, final String prefix, final Instant from,
                                                final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      // Count objects from bucket if it exists
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (ParametersChecker.isEmpty(prefix) && from == null && to == null) {
        return storageObjectList.size();
      }
      return storageObjectList.stream().filter(
              object -> !(ParametersChecker.isNotEmpty(prefix) && !object.name().startsWith(prefix) ||
                  from != null && from.isAfter(object.creationDate()) || to != null && to.isBefore(object.creationDate())))
          .count();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized Stream<StorageObject> objectsStreamInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      // List objects from bucket
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (storageObjectList == null) {
        storageObjectList = List.of();
      } else {
        storageObjectList = List.copyOf(storageObjectList);
      }
      return storageObjectList.stream();
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized Stream<StorageObject> objectsStreamInBucket(final String bucket, final String prefix,
                                                                  final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      // List objects from bucket
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (ParametersChecker.isEmpty(prefix) && from == null && to == null) {
        return List.copyOf(storageObjectList).stream();
      }
      return storageObjectList.stream().filter(
          object -> !(ParametersChecker.isNotEmpty(prefix) && !object.name().startsWith(prefix) ||
              from != null && from.isAfter(object.creationDate()) || to != null && to.isBefore(object.creationDate())));
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public Iterator<StorageObject> objectsIteratorInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    return objectsStreamInBucket(bucket).iterator();
  }

  @Override
  public Iterator<StorageObject> objectsIteratorInBucket(final String bucket, final String prefix, final Instant from,
                                                         final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    return objectsStreamInBucket(bucket, prefix, from, to).iterator();
  }

  @Override
  public synchronized StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    try {
      // Check object existence within bucket
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        return StorageType.NONE;
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (storageObjectList.isEmpty()) {
        return StorageType.NONE;
      }
      for (final var storageObject : storageObjectList) {
        if (storageObject.name().startsWith(directoryOrObject)) {
          return storageObject.name().equals(directoryOrObject) ? StorageType.OBJECT : StorageType.DIRECTORY;
        }
      }
      return StorageType.NONE;
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    try {
      if (shallRaiseAnException) {
        throw new DriverException("Fake Error");
      }
      if (!STORAGE_BUCKET_MAP.containsKey(object.bucket())) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(object.bucket());
      if (!storageObjectList.isEmpty()) {
        for (final var storageObject : storageObjectList) {
          if (storageObject.name().equals(object.name())) {
            throw new DriverAlreadyExistException("Object already exists");
          }
        }
      }
      final var countDownLatch = new CountDownLatch(1);
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        boolean still = true;
        while (still) {
          try {
            if (inputStream.read(bytes, 0, bytes.length) < 0) {
              inputStream.close();
              still = false;
            }
            Thread.yield();
          } catch (final IOException e) {
            still = false;
          }
        }
        storageObjectList.add(object);
        countDownLatch.countDown();
      });
      Thread.yield();
      STORAGE_OBJECT_CREATIONS.put(object.bucket() + '/' + object.name(), countDownLatch);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized StorageObject objectFinalizeCreateInBucket(final String bucket, final String object,
                                                                 final long realLen, final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    try {
      final var countDownLatch = STORAGE_OBJECT_CREATIONS.remove(bucket + '/' + object);
      if (countDownLatch == null) {
        throw new DriverException("Object not ready while finalize");
      }
      countDownLatch.await();
      // Return object metadata
      final var storageObjects = STORAGE_OBJECT_MAP.get(bucket);
      StorageObject storageObject = null;
      if (storageObjects != null) {
        for (final var object1 : storageObjects) {
          if (object1.name().equals(object)) {
            storageObject = object1;
            break;
          }
        }
      }
      if (storageObject != null) {
        storageObjects.remove(storageObject);
        storageObject = new StorageObject(storageObject.bucket(), storageObject.name(), sha256, realLen,
            storageObject.creationDate() == null ? Instant.now() : storageObject.creationDate(),
            storageObject.expiresDate(), storageObject.metadata());
        storageObjects.add(storageObject);
        return storageObject;
      }
      throw new DriverException("Object not found while finalize");
    } catch (final RuntimeException | InterruptedException e) { // NOSONAR transformed Exception
      throw new DriverException(e);
    }
  }

  /**
   * Copy one object to another one, possibly in a different bucket.
   * Hash and Size come from given source, while Metadata and expired date come from target
   *
   * @param objectSource source object
   * @param objectTarget target object
   */
  @Override
  public synchronized StorageObject objectCopy(final StorageObject objectSource, final StorageObject objectTarget)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      validCopy(objectSource, objectTarget);
      final var updatedTarget =
          new StorageObject(objectTarget.bucket(), objectTarget.name(), objectSource.hash(), objectSource.size(),
              Instant.now(), objectTarget.expiresDate(), objectTarget.metadata());
      if (!STORAGE_BUCKET_MAP.containsKey(objectSource.bucket())) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectSourceList = STORAGE_OBJECT_MAP.get(objectSource.bucket());
      if (!storageObjectSourceList.isEmpty()) {
        boolean found = false;
        for (final var storageObject : storageObjectSourceList) {
          if (storageObject.name().equals(objectSource.name())) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new DriverNotFoundException("Source Object does not exists");
        }
      } else {
        throw new DriverNotFoundException("Source Object does not exists");
      }
      if (!STORAGE_BUCKET_MAP.containsKey(updatedTarget.bucket())) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(updatedTarget.bucket());
      if (!storageObjectList.isEmpty()) {
        for (final var storageObject : storageObjectList) {
          if (storageObject.name().equals(updatedTarget.name())) {
            throw new DriverAlreadyExistException("Target Object already exists");
          }
        }
      }
      storageObjectList.add(updatedTarget);
      return updatedTarget;
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  /**
   * Take the object as base definition, and use name from list
   */
  public synchronized void forTestsOnlyCreateMultipleObjects(final StorageObject object, final long realLen,
                                                             final String sha256, final List<String> names)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    try {
      if (!STORAGE_BUCKET_MAP.containsKey(object.bucket())) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(object.bucket());
      for (var name : names) {
        var newObject = new StorageObject(object.bucket(), name, sha256, realLen,
            object.creationDate() != null ? object.creationDate() : Instant.now(), object.expiresDate(),
            object.metadata());
        storageObjectList.add(newObject);
      }
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized InputStream objectGetInputStreamInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (storageObjectList.isEmpty()) {
        throw new DriverNotFoundException(OBJECT_NOT_FOUND);
      }
      for (final var storageObject : storageObjectList) {
        if (storageObject.name().equals(object)) {
          return new FakeInputStream(storageObject.size(), (byte) 'X');
        }
      }
      throw new DriverNotFoundException(OBJECT_NOT_FOUND);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (storageObjectList.isEmpty()) {
        throw new DriverNotFoundException(OBJECT_NOT_FOUND);
      }
      for (final var storageObject : storageObjectList) {
        if (storageObject.name().equals(object)) {
          return storageObject;
        }
      }
      throw new DriverNotFoundException(OBJECT_NOT_FOUND);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public synchronized void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    try {
      if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
        throw new DriverNotFoundException(BUCKET_NOT_FOUND);
      }
      final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
      if (storageObjectList.isEmpty()) {
        throw new DriverNotFoundException(OBJECT_NOT_FOUND);
      }
      StorageObject found = null;
      for (final var storageObject : storageObjectList) {
        if (storageObject.name().equals(object)) {
          found = storageObject;
          break;
        }
      }
      if (found != null) {
        storageObjectList.remove(found);
        return;
      }
      throw new DriverNotFoundException(OBJECT_NOT_FOUND);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  @Override
  public void close() {
    // Nothing
  }

  public synchronized void cleanUp() {
    STORAGE_BUCKET_MAP.clear();
    STORAGE_OBJECT_CREATIONS.clear();
    for (final var entry : STORAGE_OBJECT_MAP.values()) {
      entry.clear();
    }
    STORAGE_OBJECT_MAP.clear();
  }
}
