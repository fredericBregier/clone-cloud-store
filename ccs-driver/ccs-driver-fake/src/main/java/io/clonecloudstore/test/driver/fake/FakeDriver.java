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

package io.clonecloudstore.test.driver.fake;

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
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.StorageType;
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

  FakeDriver() {
    LOGGER.info("Fake Driver Creation");
  }

  @Override
  public long bucketsCount() {
    return STORAGE_BUCKET_MAP.size();
  }

  @Override
  public Stream<StorageBucket> bucketsStream() {
    return STORAGE_BUCKET_MAP.values().stream();
  }

  @Override
  public Iterator<StorageBucket> bucketsIterator() {
    return bucketsStream().iterator();
  }

  @Override
  public StorageBucket bucketCreate(final StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    if (STORAGE_BUCKET_MAP.containsKey(bucket.bucket())) {
      throw new DriverAlreadyExistException("Already exists");
    }
    final var storageBucket = new StorageBucket(bucket.bucket(), Instant.now());
    STORAGE_BUCKET_MAP.put(bucket.bucket(), storageBucket);
    STORAGE_OBJECT_MAP.put(bucket.bucket(), new ArrayList<>());
    return storageBucket;
  }

  @Override
  public void bucketDelete(final String bucket)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public boolean bucketExists(final String bucket) {
    return STORAGE_BUCKET_MAP.containsKey(bucket);
  }

  @Override
  public long objectsCountInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
    // Count objects from bucket if it exists
    if (!STORAGE_BUCKET_MAP.containsKey(bucket)) {
      throw new DriverNotFoundException(BUCKET_NOT_FOUND);
    }
    final var storageObjectList = STORAGE_OBJECT_MAP.get(bucket);
    return storageObjectList.size();
  }

  @Override
  public long objectsCountInBucket(final String bucket, final String prefix, final Instant from, final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public Stream<StorageObject> objectsStreamInBucket(final String bucket)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public Stream<StorageObject> objectsStreamInBucket(final String bucket, final String prefix, final Instant from,
                                                     final Instant to)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  public StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
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
  }

  @Override
  public void objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public StorageObject objectFinalizeCreateInBucket(final String bucket, final String object, final long realLen,
                                                    final String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR detailed Exceptions
    final var countDownLatch = STORAGE_OBJECT_CREATIONS.remove(bucket + '/' + object);
    if (countDownLatch != null) {
      try {
        countDownLatch.await();
      } catch (final InterruptedException e) { // NOSONAR transformed Exception
        throw new DriverException(e);
      }
    }
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
  }

  @Override
  public InputStream objectGetInputStreamInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public StorageObject objectGetMetadataInBucket(final String bucket, final String object)
      throws DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public void objectDeleteInBucket(final String bucket, final String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException { // NOSONAR detailed Exceptions
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
  }

  @Override
  public void close() {
    // Nothing
  }

  void cleanUp() {
    STORAGE_BUCKET_MAP.clear();
    STORAGE_OBJECT_CREATIONS.clear();
    for (final var entry : STORAGE_OBJECT_MAP.values()) {
      entry.clear();
    }
    STORAGE_OBJECT_MAP.clear();
  }
}
