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

package io.clonecloudstore.accessor.server.simple.application;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Objects;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Accessor Bucket Service
 */
@ApplicationScoped
public class AccessorObjectService {
  private static final String STATUS_STRING = " Status: ";
  private static final String ISSUE_STRING = " issue: ";
  private final DriverApiFactory storageDriverFactory;

  public AccessorObjectService() {
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
  }

  private String mesg(final String bucketName, final String objectName) {
    return "Bucket: " + bucketName + " Object: " + objectName;
  }

  private static AccessorObject fromStorageObject(final StorageObject storageObject) {
    return new AccessorObject().setBucket(storageObject.bucket()).setName(storageObject.name())
        .setSite(ServiceProperties.getAccessorSite()).setCreation(storageObject.creationDate())
        .setSize(storageObject.size()).setHash(storageObject.hash()).setStatus(AccessorStatus.READY)
        .setMetadata(storageObject.metadata()).setExpires(storageObject.expiresDate());
  }

  /**
   * Check if object or directory exists
   */
  public StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName)
      throws CcsOperationException {
    try (final var client = storageDriverFactory.getInstance()) {
      return client.directoryOrObjectExistsInBucket(bucketName, objectOrDirectoryName);
    } catch (final DriverException e) {
      throw new CcsOperationException(mesg(bucketName, objectOrDirectoryName) + ISSUE_STRING + e.getMessage(), e);
    }
  }

  /**
   * Utility to get Object Metadata from Driver Storage
   */
  private StorageObject getObjectMetadata(final String bucketName, final String objectName)
      throws CcsNotExistException, CcsOperationException {
    try (final var driver = storageDriverFactory.getInstance()) {
      return driver.objectGetMetadataInBucket(bucketName, objectName);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(mesg(bucketName, objectName) + ISSUE_STRING + e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(mesg(bucketName, objectName) + ISSUE_STRING + e.getMessage(), e);
    }
  }

  /**
   * Get DB Object DTO
   */
  public AccessorObject getObjectInfo(final String bucketName, final String objectName)
      throws CcsNotExistException, CcsOperationException {
    final var storageObject = getObjectMetadata(bucketName, objectName);
    return fromStorageObject(storageObject).setStatus(AccessorStatus.READY);
  }

  /**
   * Before really creating Object, creates it in DB if possible
   *
   * @throws CcsNotAcceptableException if already in creation step
   */
  public AccessorObject createObject(final AccessorObject accessorObject, final String hash, final long len)
      throws CcsOperationException, CcsAlreadyExistException, CcsNotExistException, CcsNotAcceptableException {
    try {
      // Check non-existence in S3
      try (final var client = storageDriverFactory.getInstance()) {
        if (!client.bucketExists(accessorObject.getBucket())) {
          throw new CcsNotExistException(mesg(accessorObject.getBucket(), accessorObject.getName()));
        }
        final var type = client.directoryOrObjectExistsInBucket(accessorObject.getBucket(), accessorObject.getName());
        if (type.equals(StorageType.NONE)) {
          final var result = new AccessorObject();
          result.setBucket(accessorObject.getBucket()).setName(accessorObject.getName())
              .setExpires(accessorObject.getExpires()).setMetadata(accessorObject.getMetadata())
              .setSite(ServiceProperties.getAccessorSite());
          result.setStatus(AccessorStatus.UPLOAD).setHash(hash).setSize(len).setCreation(Instant.now());
          return result;
        }
        throw new CcsAlreadyExistException(
            mesg(accessorObject.getBucket(), accessorObject.getName()) + STATUS_STRING + AccessorStatus.READY);
      }
      // First Bucket check
    } catch (final DriverException e) {
      throw new CcsOperationException(
          "Error on create object : " + accessorObject.getBucket() + " - " + accessorObject.getName(), e);
    }
  }

  /**
   * Once Object really created in Driver Storage, finalize the Object in DB and Replicator if needed
   */
  public AccessorObject createObjectFinalize(final StorageObject storageObject, final String hash, final long len)
      throws CcsOperationException {
    return fromStorageObject(storageObject).setStatus(AccessorStatus.READY).setSite(ServiceProperties.getAccessorSite())
        .setSize(len).setHash(hash);
  }

  /**
   * Delete object in DB and through Replicator if needed
   */
  public void deleteObject(final String bucketName, final String objectName)
      throws CcsDeletedException, CcsNotExistException, CcsOperationException {
    // Delete in S3
    deleteObjectOnStorage(bucketName, objectName);
  }

  private void deleteObjectOnStorage(final String bucketName, final String objectName) {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.objectDeleteInBucket(bucketName, objectName);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(mesg(bucketName, objectName), e);
    } catch (final DriverNotAcceptableException e) {
      throw new CcsNotAcceptableException(mesg(bucketName, objectName), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(mesg(bucketName, objectName), e);
    }
  }

  private static AccessorObject filter(final AccessorObject accessorObject, final AccessorFilter filter) {
    if (accessorObject == null || filter == null) {
      // Status not checked
      return accessorObject != null ? accessorObject.setStatus(AccessorStatus.READY) : null;
    }
    if (ParametersChecker.isNotEmpty(filter.getNamePrefix()) &&
        !accessorObject.getName().startsWith(filter.getNamePrefix())) {
      return null;
    }
    if (filter.getSizeLessThan() > 0 && accessorObject.getSize() > filter.getSizeLessThan()) {
      return null;
    }
    if (filter.getSizeGreaterThan() > 0 && accessorObject.getSize() < filter.getSizeGreaterThan()) {
      return null;
    }
    if (ParametersChecker.isNotEmpty(accessorObject.getCreation())) {
      if (ParametersChecker.isNotEmpty(filter.getCreationBefore()) &&
          filter.getCreationBefore().isBefore(accessorObject.getCreation())) {
        return null;
      }
      if (ParametersChecker.isNotEmpty(filter.getCreationAfter()) &&
          filter.getCreationAfter().isAfter(accessorObject.getCreation())) {
        return null;
      }
    }
    if (ParametersChecker.isNotEmpty(accessorObject.getExpires())) {
      if (ParametersChecker.isNotEmpty(filter.getExpiresBefore()) &&
          filter.getExpiresBefore().isBefore(accessorObject.getExpires())) {
        return null;
      }
      if (ParametersChecker.isNotEmpty(filter.getExpiresAfter()) &&
          filter.getExpiresAfter().isAfter(accessorObject.getExpires())) {
        return null;
      }
    }
    if (filter.getMetadataFilter() != null && accessorObject.getMetadata() != null &&
        (!filter.getMetadataFilter().isEmpty())) {
      final var map = accessorObject.getMetadata();
      for (final var entry : filter.getMetadataFilter().entrySet()) {
        if (!map.containsKey(entry.getKey())) {
          return null;
        }
        if (!Objects.equals(map.get(entry.getKey()), entry.getValue())) {
          return null;
        }
      }
    }
    // Status not checked
    return accessorObject.setStatus(AccessorStatus.READY);
  }

  public InputStream filterObjects(final String bucket, final AccessorFilter filter, final DriverApi driver) {
    try {
      if (filter == null) {
        final var iterator = driver.objectsIteratorInBucket(bucket);
        return StreamIteratorUtils.getInputStreamFromIterator(iterator,
            source -> filter(fromStorageObject((StorageObject) source), filter), AccessorObject.class);
      }
      final var iterator = driver.objectsIteratorInBucket(bucket, filter.getNamePrefix(), filter.getCreationAfter(),
          filter.getCreationBefore());
      return StreamIteratorUtils.getInputStreamFromIterator(iterator,
          source -> filter(fromStorageObject((StorageObject) source), filter), AccessorObject.class);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverNotAcceptableException e) {
      throw new CcsNotAcceptableException(e.getMessage(), e);
    } catch (final DriverException | IOException e) {
      throw new CcsOperationException(e);
    }
  }
}
