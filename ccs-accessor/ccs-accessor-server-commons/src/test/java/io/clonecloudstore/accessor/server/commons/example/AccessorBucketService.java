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

package io.clonecloudstore.accessor.server.commons.example;

import java.util.Collection;
import java.util.LinkedList;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.AccessorBucketServiceInterface;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Accessor Bucket Service
 */
@ApplicationScoped
public class AccessorBucketService implements AccessorBucketServiceInterface {
  private static final Logger LOGGER = Logger.getLogger(AccessorBucketService.class);
  private static final String BUCKET_STRING = "Bucket ";
  private final DriverApiFactory storageDriverFactory;

  public AccessorBucketService() {
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
  }

  /**
   * Create bucket from client Id and bucketName
   *
   * @param bucketName Bucket Name
   * @param clientId   Format client ID use to identify client
   * @return AccessorBucket add on Database and in object storage
   */
  @Override
  public AccessorBucket createBucket(final String bucketName, final String clientId, final boolean isPublic)
      throws CcsAlreadyExistException, CcsOperationException {
    try {
      //Check format (special char, uppercase...) and size (min, max) for bucket name
      ParametersChecker.checkSanityBucketName(bucketName);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      final var message = String.format("Bucket Name %s is invalid", bucketName);
      throw new CcsOperationException(message);
    }
    AccessorBucket result = new AccessorBucket();
    result.setId(bucketName);
    result.setClientId(clientId);
    result.setSite(ServiceProperties.getAccessorSite());
    result.setStatus(AccessorStatus.UPLOAD);
    //Create Bucket in Object Storage
    final var storageBucket = new StorageBucket(result.getId(), result.getClientId(), null);
    return createBucketOnStorage(storageBucket, result);
  }

  private AccessorBucket createBucketOnStorage(final StorageBucket storageBucket, final AccessorBucket bucket) {
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      // retrieving metadata from storage, would data be duplicated in database
      final var result = storageDriver.bucketCreate(storageBucket);
      bucket.setStatus(AccessorStatus.READY).setCreation(result.creationDate());
      return bucket;
    } catch (final DriverAlreadyExistException e) {
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final DriverNotAcceptableException e) {
      throw new CcsNotAcceptableException(e.getMessage(), e);
    } catch (final CcsInvalidArgumentRuntimeException | DriverException | CcsServerGenericException e) {
      LOGGER.error(e.getMessage());
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  /**
   * Get Bucket information from bucket name
   *
   * @param bucketName Bucket name
   * @return AccessorBucket found
   */
  @Override
  public AccessorBucket getBucket(final String bucketName, final String clientId, final String opId,
                                  final boolean isPublic)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException {
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      // retrieving metadata from storage, would data be duplicated in database
      final var storageBucket = storageDriver.bucketGet(bucketName);
      if (storageBucket != null) {
        AccessorBucket result = new AccessorBucket();
        result.setClientId(storageBucket.clientId());
        result.setId(bucketName);
        result.setSite(ServiceProperties.getAccessorSite());
        result.setCreation(storageBucket.creationDate());
        result.setStatus(AccessorStatus.READY);
        return result;
      }
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(BUCKET_STRING + bucketName + " doesn't exist");
    } catch (final CcsInvalidArgumentRuntimeException | DriverException | CcsServerGenericException e) {
      LOGGER.error(e.getMessage());
      throw new CcsOperationException(e.getMessage(), e);
    }
    //Search bucket in database with ID.
    throw new CcsNotExistException(BUCKET_STRING + bucketName + " doesn't exist");
  }

  /**
   * Get All buckets using Ownership
   *
   * @return the list of Buckets
   */
  @Override
  public Collection<AccessorBucket> getBuckets(final String clientId) throws CcsOperationException {
    //Search buckets.
    final var collection = new LinkedList<AccessorBucket>();
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      final var iterator = storageDriver.bucketsIterator();
      while (iterator.hasNext()) {
        final var storageBucket = iterator.next();
        if (storageBucket.clientId() != null && storageBucket.clientId().equals(clientId)) {
          AccessorBucket result = new AccessorBucket();
          result.setId(storageBucket.bucket());
          result.setClientId(clientId);
          result.setSite(ServiceProperties.getAccessorSite());
          result.setCreation(storageBucket.creationDate());
          result.setStatus(AccessorStatus.READY);
          collection.add(result);
        }
      }
      return collection;
    } catch (final DriverException e) {
      throw new CcsOperationException("Error on get buckets", e);
    }
  }

  /**
   * Check if Bucket exists
   *
   * @param bucketName Bucket name
   * @return True if it exists
   */
  @Override
  public boolean checkBucket(final String bucketName, final boolean fullCheck, final String clientId, final String opId,
                             final boolean isPublic) throws CcsOperationException {
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      return storageDriver.bucketExists(bucketName);
    } catch (final DriverException e) {
      throw new CcsOperationException("Issue while checking bucket : " + bucketName, e);
    }
  }

  /**
   * Delete bucket from bucket name
   *
   * @param bucketName Bucket name
   * @param clientId   Client ID used to identify client
   * @return the associated DTO- deleted
   */
  @Override
  public AccessorBucket deleteBucket(final String bucketName, final String clientId, final boolean isPublic)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException {
    if (checkBucket(bucketName, true, clientId, SimpleClientAbstract.getMdcOpId(), true)) {
      AccessorBucket result = new AccessorBucket();
      result.setId(bucketName);
      result.setClientId(clientId);
      result.setSite(ServiceProperties.getAccessorSite());
      result.setCreation(null);
      result.setStatus(AccessorStatus.DELETING);
      return storageDelete(bucketName, result);
    } else {
      throw new CcsDeletedException(BUCKET_STRING + bucketName + " is already deleted");
    }
  }

  private AccessorBucket storageDelete(final String bucketName, final AccessorBucket bucket) {
    try {
      try (final var storageDriver = storageDriverFactory.getInstance()) {
        // retrieving metadata from storage, would data be duplicated in database
        storageDriver.bucketDelete(bucketName);
      }
      bucket.setStatus(AccessorStatus.DELETED);
      return bucket;
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverNotAcceptableException e) {
      throw new CcsNotAcceptableException(e.getMessage(), e);
    } catch (final DriverException | RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

}

