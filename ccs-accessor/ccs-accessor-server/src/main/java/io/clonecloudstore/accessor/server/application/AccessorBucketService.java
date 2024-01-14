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

package io.clonecloudstore.accessor.server.application;

import java.time.Instant;
import java.util.Collection;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.AccessorBucketServiceInterface;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.BUCKET;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.STATUS;

/**
 * Accessor Bucket Service
 */
@ApplicationScoped
public class AccessorBucketService implements AccessorBucketServiceInterface {
  private static final Logger LOGGER = Logger.getLogger(AccessorBucketService.class);
  private static final String BUCKET_STRING = "Bucket ";
  private final LocalReplicatorService localReplicatorService;
  private final DaoAccessorBucketRepository bucketRepository;
  private final DaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;

  public AccessorBucketService(final LocalReplicatorService localReplicatorService,
                               final Instance<DaoAccessorBucketRepository> bucketRepositoryInstance,
                               final Instance<DaoAccessorObjectRepository> objectRepositoryInstance) {
    this.localReplicatorService = localReplicatorService;
    // Normal injection does not work, probably due to test only dependency
    this.bucketRepository = bucketRepositoryInstance.get();
    this.objectRepository = objectRepositoryInstance.get();
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
  }

  /**
   * Create bucket from client Id and technicalBucketName
   *
   * @param clientId            Format client ID use to identify client
   * @param technicalBucketName Technical Bucket Name
   * @param isPublic            True means replicate with replicator module
   * @return AccessorBucket add on Database and in object storage
   */
  @Override
  public AccessorBucket createBucket(final String clientId, final String technicalBucketName, final boolean isPublic)
      throws CcsAlreadyExistException, CcsOperationException {
    final var bucketName = DaoAccessorBucketRepository.getBucketName(clientId, technicalBucketName);
    try {
      //Check format (special char, uppercase...) and size (min, max) for bucket name and technicalBucketName
      ParametersChecker.checkSanityBucketName(bucketName);
      ParametersChecker.checkSanityBucketName(technicalBucketName);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      final var message = String.format("Bucket Name %s is invalid", technicalBucketName);
      throw new CcsOperationException(message);
    }

    //Check if bucket already exist in database.
    AccessorBucket result;
    try {
      result = bucketRepository.findBucketById(technicalBucketName);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Error on find repository with technical ID", e);
    }
    if (result != null && !AccessorStatus.DELETED.equals(result.getStatus())) {
      final var message = String.format("Bucket %s already exist", technicalBucketName);
      LOGGER.errorf(message);
      throw new CcsAlreadyExistException(message);
    } else if (result == null) {
      //Create Bucket in Database with Status In Progress.
      //Only name and technical name (over value are erased during creation).
      result = new AccessorBucket();
      result.setName(bucketName);
      result.setId(technicalBucketName);
      result.setSite(ServiceProperties.getAccessorSite());
    }
    final AccessorBucket newBucket = createBucketFromDto(result);
    if (isPublic) {
      //Replicate message with replicator (send message to broker).
      LOGGER.infof("Replicate Create Order for %s", newBucket.getId());
      localReplicatorService.create(newBucket.getId(), clientId);
    }
    return newBucket;
  }

  /**
   * Create Bucket from DTO Accessor
   *
   * @param bucket Accessor Bucket with functional data
   * @return AccessorBucket add on Database and in object storage
   */
  private AccessorBucket createBucketFromDto(final AccessorBucket bucket) throws CcsOperationException {

    LOGGER.debugf("Create bucket %s in database and in object storage", bucket.getId());
    try {
      //Insert in Database
      // Since it can be in DELETED status, it might be updated (creation + status)
      bucket.setCreation(Instant.now());
      AccessorBucket newBucket = bucket;
      if (AccessorStatus.DELETED.equals(newBucket.getStatus())) {
        newBucket.setStatus(AccessorStatus.UPLOAD);
        bucketRepository.updateBucketStatus(newBucket, AccessorStatus.UPLOAD, newBucket.getCreation());
      } else {
        newBucket.setStatus(AccessorStatus.UPLOAD);
        newBucket = bucketRepository.insertBucket(newBucket);
      }

      //Create Bucket in Object Storage
      final var storageBucket = new StorageBucket(newBucket.getId(), newBucket.getCreation());
      return createBucketOnStorage(storageBucket, newBucket);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on bucket " + bucket.getName() + " creation", e);
    }
  }

  private AccessorBucket createBucketOnStorage(final StorageBucket storageBucket, final AccessorBucket bucket)
      throws CcsDbException {
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      // retrieving metadata from storage, would data be duplicated in database
      storageDriver.bucketCreate(storageBucket);

      //Update Database in Database with status Available.
      return bucketRepository.updateBucketStatus(bucket, AccessorStatus.READY, null);
    } catch (final CcsInvalidArgumentRuntimeException | DriverException | CcsServerGenericException e) {
      LOGGER.error(e.getMessage());
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.ERR_UPL, null);
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  /**
   * Get Bucket information from bucket technical name
   *
   * @param technicalBucketName Bucket technical name
   * @return AccessorBucket found with technical name
   */
  @Override
  public AccessorBucket getBucket(final String technicalBucketName, final String clientId, final String opId,
                                  final boolean isPublic)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException {
    try {
      //Search bucket in database with ID.
      final var bucket = bucketRepository.findBucketById(technicalBucketName);
      if (bucket != null) {
        if (bucket.getStatus() == AccessorStatus.READY) {
          return bucket;
        } else if (bucket.getStatus() == AccessorStatus.DELETED) {
          throw new CcsDeletedException(BUCKET_STRING + technicalBucketName + "is deleted");
        } else {
          throw new CcsOperationException(BUCKET_STRING + technicalBucketName + " status " + bucket.getStatus());
        }
      } else {
        if (isPublic && AccessorProperties.isRemoteRead()) {
          // Use remote check
          AccessorBucket response = getRemoteAccessorBucket(technicalBucketName, clientId, opId);
          if (response != null) {
            return response;
          }
        }
        throw new CcsNotExistException(BUCKET_STRING + technicalBucketName + " doesn't exist");
      }
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on get bucket : " + technicalBucketName, e);
    }
  }

  private AccessorBucket getRemoteAccessorBucket(final String technicalBucketName, final String clientId,
                                                 final String opId) {
    try {
      final var response = localReplicatorService.remoteGetBucket(technicalBucketName, clientId, opId);
      if (response.response() != null && response.targetId() != null && AccessorProperties.isFixOnAbsent()) {
        localReplicatorService.generateLocalReplicationOrder(
            new ReplicatorOrder(opId, response.targetId(), ServiceProperties.getAccessorSite(), clientId,
                technicalBucketName, ReplicatorConstants.Action.CREATE));
        return response.response();
      }
    } catch (final CcsOperationException ignore) {
      // Ignored
    }
    return null;
  }

  /**
   * Get All buckets in Storgate
   *
   * @return the list of Buckets
   */
  @Override
  public Collection<AccessorBucket> getBuckets(final String clientId) throws CcsOperationException {
    try {
      //Search buckets in Database.
      return bucketRepository.listBuckets(clientId);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on get buckets", e);
    }
  }

  /**
   * Check if Bucket exists
   *
   * @param technicalBucketName Bucket technical name
   * @param fullCheck           True to check Object Storage
   * @param clientId            the clientId
   * @param opId                the OperationId
   * @param isPublic            True to check remotely if not found locally
   * @return True if it exists
   */
  @Override
  public boolean checkBucket(final String technicalBucketName, final boolean fullCheck, final String clientId,
                             final String opId, final boolean isPublic) throws CcsOperationException {
    try {
      // Could use simply COUNT
      final var bucket = bucketRepository.findBucketById(technicalBucketName);
      if (bucket == null && isPublic && AccessorProperties.isRemoteRead()) {
        // Use remote check
        return remoteCheckBucket(technicalBucketName, clientId, opId);
      }
      if (bucket == null || bucket.getStatus() != AccessorStatus.READY) {
        return false;
      }
      if (fullCheck) {
        return checkBucketOnStorage(technicalBucketName);
      }
      return true;
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on check bucket : " + technicalBucketName, e);
    }
  }

  private boolean remoteCheckBucket(final String technicalBucketName, final String clientId, final String opId) {
    try {
      return localReplicatorService.remoteCheckBucket(technicalBucketName, clientId, opId);
    } catch (final CcsOperationException ignore) {
      return false;
    }
  }

  private boolean checkBucketOnStorage(final String technicalBucketName) {
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      return storageDriver.bucketExists(technicalBucketName);
    } catch (final DriverException e) {
      throw new CcsOperationException("Issue while checking bucket : " + technicalBucketName, e);
    }
  }

  /**
   * Delete bucket from technical bucket name
   *
   * @param clientId            Client ID used to identify client
   * @param technicalBucketName Bucket technical name
   * @param isPublic            true to send replication message on replicator
   * @return the associated DTO- deleted
   */
  @Override
  public AccessorBucket deleteBucket(final String clientId, final String technicalBucketName, final boolean isPublic)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException {
    try {
      var bucket = bucketRepository.findBucketById(technicalBucketName);
      if (bucket == null) {
        throw new CcsNotExistException(BUCKET_STRING + technicalBucketName + " doesn't exist");
      } else if (bucket.getStatus() == AccessorStatus.READY) {
        final var count = objectRepository.count(
            new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucket.getId()),
                new DbQuery(RestQuery.QUERY.EQ, STATUS, AccessorStatus.READY)));
        if (count > 0) {
          throw new CcsNotAcceptableException("Bucket is not empty");
        }
        bucketRepository.updateBucketStatus(bucket, AccessorStatus.DELETING, null);
        final var deleted = updateAccessorBucketDelete(technicalBucketName, bucket);
        if (isPublic) {
          LOGGER.infof("Replicate Delete Order for %s ", bucket.getId());
          localReplicatorService.delete(bucket.getId(), clientId);
        }
        return deleted;
      } else if (bucket.getStatus() == AccessorStatus.DELETED) {
        throw new CcsDeletedException(BUCKET_STRING + technicalBucketName + " is already deleted");
      } else {
        throw new CcsOperationException("Bucket Status " + bucket.getStatus());
      }
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on delete bucket " + technicalBucketName, e);
    }
  }

  private AccessorBucket updateAccessorBucketDelete(final String technicalBucketName, final AccessorBucket bucket)
      throws CcsDbException {
    try {
      try (final var storageDriver = storageDriverFactory.getInstance()) {
        // retrieving metadata from storage, would data be duplicated in database
        storageDriver.bucketDelete(technicalBucketName);
      }
      return bucketRepository.updateBucketStatus(bucket, AccessorStatus.DELETED, null);
    } catch (final DriverException | RuntimeException e) {
      LOGGER.error(e.getMessage());
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.ERR_DEL, null);
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

}

