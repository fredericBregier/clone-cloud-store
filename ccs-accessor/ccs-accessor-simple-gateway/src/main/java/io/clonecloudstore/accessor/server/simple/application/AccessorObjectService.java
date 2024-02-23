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
import io.clonecloudstore.accessor.server.commons.AbstractPublicObjectHelper;
import io.clonecloudstore.accessor.server.commons.AccessorObjectServiceInterface;
import io.clonecloudstore.accessor.server.commons.buffer.FilesystemHandler;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
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
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;

/**
 * Accessor Bucket Service
 */
@ApplicationScoped
@Unremovable
public class AccessorObjectService implements AccessorObjectServiceInterface {
  private static final String STATUS_STRING = " Status: ";
  private static final String ISSUE_STRING = " issue: ";
  private final DriverApiFactory storageDriverFactory;
  protected FilesystemHandler filesystemHandler;

  public AccessorObjectService() {
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
    filesystemHandler = CDI.current().select(FilesystemHandler.class).get();
  }

  private String mesg(final String bucketName, final String objectName) {
    return "Bucket: " + bucketName + " Object: " + objectName;
  }

  /**
   * Check if object or directory exists
   */
  @Override
  public StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName,
                                             final boolean fullCheck, final String clientId)
      throws CcsOperationException {
    try (final var client = storageDriverFactory.getInstance()) {
      var result = client.directoryOrObjectExistsInBucket(bucketName, objectOrDirectoryName);
      if (StorageType.NONE.equals(result) && AccessorProperties.isStoreActive() &&
          filesystemHandler.check(bucketName, objectOrDirectoryName)) {
        return StorageType.OBJECT;
      }
      return result;
    } catch (final DriverException e) {
      if (AccessorProperties.isStoreActive() && filesystemHandler.check(bucketName, objectOrDirectoryName)) {
        return StorageType.OBJECT;
      }
      throw new CcsOperationException(mesg(bucketName, objectOrDirectoryName) + ISSUE_STRING + e.getMessage(), e);
    }
  }

  @Override
  public StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName,
                                             final boolean fullCheck, final String clientId, final String opId,
                                             final boolean external) throws CcsOperationException {
    return objectOrDirectoryExists(bucketName, objectOrDirectoryName, fullCheck, clientId);
  }

  /**
   * Utility to get Object Metadata from Driver Storage
   */
  private StorageObject getObjectMetadata(final String bucketName, final String objectName)
      throws CcsNotExistException, CcsOperationException {
    try (final var driver = storageDriverFactory.getInstance()) {
      return driver.objectGetMetadataInBucket(bucketName, objectName);
    } catch (final DriverNotFoundException e) {
      if (AccessorProperties.isStoreActive() && filesystemHandler.check(bucketName, objectName) &&
          !QuarkusProperties.hasDatabase()) {
        try {
          return filesystemHandler.readStorageObject(bucketName, objectName);
        } catch (final IOException ignore) {
          // Ignore
        }
      }
      throw new CcsNotExistException(mesg(bucketName, objectName) + ISSUE_STRING + e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(mesg(bucketName, objectName) + ISSUE_STRING + e.getMessage(), e);
    }
  }

  /**
   * Get DB Object DTO
   */
  @Override
  public AccessorObject getObjectInfo(final String bucketName, final String objectName, final String clientId)
      throws CcsNotExistException, CcsOperationException {
    final var storageObject = getObjectMetadata(bucketName, objectName);
    return AbstractPublicObjectHelper.getFromStorageObject(storageObject).setStatus(AccessorStatus.READY);
  }

  /**
   * Before really creating Object, creates it in DB if possible
   *
   * @throws CcsNotAcceptableException if already in creation step
   */
  @Override
  public AccessorObject createObject(final AccessorObject accessorObject, final String hash, final long len,
                                     final String clientId)
      throws CcsOperationException, CcsAlreadyExistException, CcsNotExistException, CcsNotAcceptableException {
    try {
      // Check non-existence in S3
      try (final var client = storageDriverFactory.getInstance()) {
        // First Bucket check
        if (!client.bucketExists(accessorObject.getBucket())) {
          throw new CcsNotExistException(mesg(accessorObject.getBucket(), accessorObject.getName()));
        }
        final var type = client.directoryOrObjectExistsInBucket(accessorObject.getBucket(), accessorObject.getName());
        if (type.equals(StorageType.NONE)) {
          return createNewAccessorObject(accessorObject, hash, len);
        }
        throw new CcsAlreadyExistException(
            mesg(accessorObject.getBucket(), accessorObject.getName()) + STATUS_STRING + AccessorStatus.READY);
      }
    } catch (final DriverException e) {
      if (AccessorProperties.isStoreActive()) {
        // Act as if the bucket exists and the object doesn't
        return createNewAccessorObject(accessorObject, hash, len);
      }
      throw new CcsOperationException(
          "Error on create object : " + accessorObject.getBucket() + " - " + accessorObject.getName(), e);
    }
  }

  private AccessorObject createNewAccessorObject(final AccessorObject accessorObject, final String hash,
                                                 final long len) {
    if (AccessorProperties.isStoreActive() &&
        filesystemHandler.check(accessorObject.getBucket(), accessorObject.getName())) {
      throw new CcsAlreadyExistException(
          mesg(accessorObject.getBucket(), accessorObject.getName()) + STATUS_STRING + AccessorStatus.READY);
    }
    return accessorObject.cloneInstance().setSite(ServiceProperties.getAccessorSite()).setStatus(AccessorStatus.UPLOAD)
        .setHash(hash).setSize(len).setCreation(Instant.now());
  }

  /**
   * Once Object really created in Driver Storage, finalize the Object in DB and Replicator if needed
   */
  @Override
  public AccessorObject createObjectFinalize(final AccessorObject accessorObject, final String hash, final long len,
                                             final String clientId, final boolean external)
      throws CcsOperationException {
    return accessorObject.setStatus(AccessorStatus.READY).setSite(ServiceProperties.getAccessorSite()).setSize(len)
        .setHash(hash);
  }

  /**
   * Delete object in DB and through Replicator if needed
   */
  @Override
  public void deleteObject(final String bucketName, final String objectName, final String clientId,
                           final boolean external)
      throws CcsDeletedException, CcsNotExistException, CcsOperationException {
    // Delete in S3
    deleteObjectOnStorage(bucketName, objectName);
  }

  private void deleteObjectOnStorage(final String bucketName, final String objectName) {
    var unregistered = false;
    try (final var driver = storageDriverFactory.getInstance()) {
      if (AccessorProperties.isStoreActive()) {
        unregistered = filesystemHandler.unregisterItem(bucketName, objectName);
      }
      driver.objectDeleteInBucket(bucketName, objectName);
    } catch (final DriverNotFoundException e) {
      if (unregistered) {
        return;
      }
      throw new CcsNotExistException(mesg(bucketName, objectName), e);
    } catch (final DriverNotAcceptableException e) {
      throw new CcsNotAcceptableException(mesg(bucketName, objectName), e);
    } catch (final DriverException e) {
      if (unregistered) {
        return;
      }
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

  /**
   * @param bucketName bucket name
   * @param filter     the filter to apply on Objects
   * @return a stream (InputStream) of AccessorObject line by line (newline separated)
   */
  public InputStream filterObjects(final String bucketName, final AccessorFilter filter, final DriverApi driver) {
    try {
      if (filter == null) {
        final var iterator = driver.objectsIteratorInBucket(bucketName);
        return StreamIteratorUtils.getInputStreamFromIterator(iterator,
            source -> filter(AbstractPublicObjectHelper.getFromStorageObject((StorageObject) source), filter),
            AccessorObject.class);
      }
      final var iterator = driver.objectsIteratorInBucket(bucketName, filter.getNamePrefix(), filter.getCreationAfter(),
          filter.getCreationBefore());
      return StreamIteratorUtils.getInputStreamFromIterator(iterator,
          source -> filter(AbstractPublicObjectHelper.getFromStorageObject((StorageObject) source), filter),
          AccessorObject.class);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverNotAcceptableException e) {
      throw new CcsNotAcceptableException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e);
    }
  }

  @Override
  public InputStream filterObjects(final String bucketName, final AccessorFilter filter, final String clientId,
                                   final boolean external) throws CcsOperationException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
