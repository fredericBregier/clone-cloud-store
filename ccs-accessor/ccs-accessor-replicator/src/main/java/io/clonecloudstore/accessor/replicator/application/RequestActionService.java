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

package io.clonecloudstore.accessor.replicator.application;

import java.time.Instant;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
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
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClientFactory;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.jboss.logging.Logger;

/**
 * Request Action Service
 */
@ApplicationScoped
public class RequestActionService {
  private static final Logger LOGGER = Logger.getLogger(RequestActionService.class);
  private static final String STATUS_STRING = " Status: ";
  private final LocalReplicatorApiClientFactory localReplicatorApiClientFactory;
  private final DaoAccessorBucketRepository bucketRepository;
  private final DaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;

  public RequestActionService(final LocalReplicatorApiClientFactory localReplicatorApiClientFactory,
                              final Instance<DaoAccessorBucketRepository> bucketRepositoryInstance,
                              final Instance<DaoAccessorObjectRepository> objectRepositoryInstance) {
    this.localReplicatorApiClientFactory = localReplicatorApiClientFactory;
    // Normal injection does not work, probably due to test only dependency
    this.bucketRepository = bucketRepositoryInstance.get();
    this.objectRepository = objectRepositoryInstance.get();
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
  }

  private String mesg(final String bucketName, final String objectName) {
    return "Bucket: " + bucketName + " Object: " + objectName;
  }

  /**
   * Create bucket from client Id and technicalBucketName
   *
   * @param clientId            Format client ID use to identify client
   * @param technicalBucketName Technical Bucket Name
   * @return AccessorBucket add on Database and in object storage
   */
  public AccessorBucket createBucket(final String clientId, final String technicalBucketName)
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
    return createBucketFromDto(result);
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
    } finally {
      flushBucket();
    }
  }

  private AccessorBucket createBucketOnStorage(final StorageBucket storageBucket, final AccessorBucket bucket)
      throws CcsDbException {
    try (final var storageDriver = storageDriverFactory.getInstance()) {
      // retrieving metadata from storage, would data be duplicated in database
      storageDriver.bucketCreate(storageBucket);
      //Update Database in Database with status Available.
      var newBucket = bucketRepository.updateBucketStatus(bucket, AccessorStatus.READY, null);
      LOGGER.debugf("Bucket %s created in database and in object storage", bucket.getId());
      return newBucket;
    } catch (final CcsOperationException e) {
      LOGGER.error(e.getMessage());
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.ERR_UPL, null);
      throw e;
    } catch (final CcsInvalidArgumentRuntimeException | DriverException | CcsServerGenericException e) {
      LOGGER.error(e.getMessage());
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.ERR_UPL, null);
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  private void flushBucket() {
    try {
      bucketRepository.flushAll();
    } catch (final CcsDbException e) {
      LOGGER.error(e.getMessage());
    }
  }

  /**
   * Delete bucket from technical bucket name
   *
   * @param technicalBucketName Bucket technical name
   * @return the associated DTO- deleted
   */
  public AccessorBucket deleteBucket(final String technicalBucketName)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException {
    try {
      final var bucket = bucketRepository.findBucketById(technicalBucketName);
      if (bucket == null) {
        throw new CcsNotExistException("Bucket " + technicalBucketName + " doesn't exist");
      } else if (bucket.getStatus() == AccessorStatus.READY) {
        final var count = objectRepository.count(new DbQuery(RestQuery.CONJUNCTION.AND,
            new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.BUCKET, bucket.getId()),
            new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, AccessorStatus.READY)));
        if (count > 0) {
          throw new CcsNotAcceptableException("Bucket is not empty");
        }
        bucketRepository.updateBucketStatus(bucket, AccessorStatus.DELETING, null);
        return deleteBucketOnStorage(technicalBucketName, bucket);
      } else if (bucket.getStatus() == AccessorStatus.DELETED) {
        throw new CcsDeletedException("Bucket " + technicalBucketName + " is already deleted");
      } else {
        throw new CcsOperationException("Bucket Status " + bucket.getStatus());
      }
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on delete bucket " + technicalBucketName, e);
    } finally {
      flushBucket();
    }
  }

  private AccessorBucket deleteBucketOnStorage(final String technicalBucketName, final AccessorBucket bucket)
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

  public void createObject(final ReplicatorOrder replicatorOrder)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException {
    final var daoObject = createObjectInternal(replicatorOrder);
    LOGGER.debugf("Debug Log Creation: %s from %s", daoObject.getBucket(), daoObject);
    try (final var driverApi = storageDriverFactory.getInstance();
         final var client = localReplicatorApiClientFactory.newClient()) {
      final var result = client.readRemoteObject(replicatorOrder.bucketName(), replicatorOrder.objectName(),
          replicatorOrder.clientId(), replicatorOrder.fromSite(), GuidLike.getGuid());
      final var dto = result.dtoOut();
      updateObjectFromRemote(daoObject, dto);
      final var objectStorage =
          new StorageObject(daoObject.getBucket(), daoObject.getName(), daoObject.getHash(), daoObject.getSize(),
              daoObject.getCreation(), null, daoObject.getMetadata());
      driverApi.objectPrepareCreateInBucket(objectStorage, result.inputStream());
      driverApi.objectFinalizeCreateInBucket(daoObject.getBucket(), daoObject.getName(), daoObject.getSize(),
          daoObject.getHash());
      createObjectFinalize(daoObject);
    } catch (final DriverAlreadyExistException e) {
      objectInError(daoObject, replicatorOrder);
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final CcsWithStatusException e) {
      objectInError(daoObject, replicatorOrder);
      throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), e.getMessage(), e);
    } catch (final DriverNotFoundException e) {
      objectInError(daoObject, replicatorOrder);
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final CcsNotExistException | CcsServerGenericException e) {
      objectInError(daoObject, replicatorOrder);
      throw e;
    } catch (final DriverException e) {
      objectInError(daoObject, replicatorOrder);
      throw new CcsOperationException(e.getMessage(), e);
      // CcsAlreadyExistException ignored
    } finally {
      flushObject();
    }
  }

  private void flushObject() {
    try {
      objectRepository.flushAll();
    } catch (final CcsDbException e) {
      LOGGER.error(e.getMessage());
    }
  }

  private void objectInError(final DaoAccessorObject daoObject, final ReplicatorOrder replicatorOrder)
      throws CcsOperationException {
    try {
      LOGGER.debugf("Update to Error %s", replicatorOrder);
      objectRepository.updateObjectStatus(daoObject.getBucket(), daoObject.getName(), AccessorStatus.ERR_UPL,
          Instant.now());
    } catch (final CcsDbException ex) {
      throw new CcsOperationException(ex.getMessage(), ex);
    }
  }

  /**
   * Before really creating Object, creates it in DB if possible
   *
   * @throws CcsNotAcceptableException if already in creation step
   */
  private DaoAccessorObject createObjectInternal(final ReplicatorOrder replicatorOrder)
      throws CcsAlreadyExistException, CcsNotExistException, CcsNotAcceptableException, CcsOperationException {
    try {
      // Check non-existence in DB/S3
      // First Bucket check
      final var bucket = bucketRepository.findBucketById(replicatorOrder.bucketName());
      if (bucket == null || !AccessorStatus.READY.equals(bucket.getStatus())) {
        throw new CcsNotExistException("Bucket does not exists");
      }
      // Now Object check
      var daoAccessorObject = objectRepository.getObject(replicatorOrder.bucketName(), replicatorOrder.objectName());
      if (daoAccessorObject != null) {
        if (daoAccessorObject.getStatus() != AccessorStatus.DELETED) {
          if (daoAccessorObject.getStatus() == AccessorStatus.UPLOAD) {
            throw new CcsNotAcceptableException(
                mesg(replicatorOrder.bucketName(), replicatorOrder.objectName()) + " already in creation, Status: " +
                    daoAccessorObject.getStatus());
          }
          throw new CcsAlreadyExistException(
              mesg(replicatorOrder.bucketName(), replicatorOrder.objectName()) + STATUS_STRING +
                  daoAccessorObject.getStatus());
        }
        daoAccessorObject.setStatus(AccessorStatus.UPLOAD).setHash(replicatorOrder.hash())
            .setSize(replicatorOrder.size()).setCreation(Instant.now());
        if (ParametersChecker.isNotEmpty(replicatorOrder.hash()) || replicatorOrder.size() > 0) {
          objectRepository.updateFull(daoAccessorObject);
        } else {
          objectRepository.updateObjectStatus(replicatorOrder.bucketName(), replicatorOrder.objectName(),
              AccessorStatus.UPLOAD, daoAccessorObject.getCreation());
        }
      } else {
        //Create object in database with status "In progress"
        daoAccessorObject = objectRepository.createEmptyItem();
        daoAccessorObject.setBucket(replicatorOrder.bucketName()).setName(replicatorOrder.objectName())
            .setHash(replicatorOrder.hash()).setSize(replicatorOrder.size());
        daoAccessorObject.setId(GuidLike.getGuid()).setSite(ServiceProperties.getAccessorSite())
            .setCreation(Instant.now()).setStatus(AccessorStatus.UPLOAD);
        objectRepository.insert(daoAccessorObject);
      }
      return daoAccessorObject;
    } catch (final CcsDbException e) {
      throw new CcsOperationException(
          "Database error on create object : " + replicatorOrder.bucketName() + " - " + replicatorOrder.objectName(),
          e);
    }
  }

  private void updateObjectFromRemote(final DaoAccessorObject daoObject, final AccessorObject dto) {
    try {
      objectRepository.updateFromDto(daoObject, dto);
    } catch (CcsDbException e) {
      throw new CcsOperationException(e);
    }
  }

  /**
   * Once Object really created in Cloud Cloud Store, finalize the Object in DB
   */
  private DaoAccessorObject createObjectFinalize(final DaoAccessorObject daoObject) throws CcsOperationException {
    try {
      // Update Database with status available and metadata from ObjectStorage
      objectRepository.updateObjectStatusHashLen(daoObject.getBucket(), daoObject.getName(), AccessorStatus.READY,
          daoObject.getHash(), daoObject.getSize());
      return objectRepository.getObject(daoObject.getBucket(), daoObject.getName());
    } catch (final CcsDbException e) {
      throw new CcsOperationException(
          "Database error on create object finalize : " + daoObject.getBucket() + " - " + daoObject.getName(), e);
    }
  }

  /**
   * Delete object in DB and through Replicator if needed
   */
  public void deleteObject(final String bucketName, final String objectName)
      throws CcsDeletedException, CcsNotExistException, CcsOperationException {
    try {
      // Check existence first in DB
      final var daoAccessorObject = objectRepository.getObject(bucketName, objectName);
      if (daoAccessorObject != null) {
        LOGGER.debugf("Dao: %s", daoAccessorObject);
        if (daoAccessorObject.getStatus() != AccessorStatus.READY) {
          if (daoAccessorObject.getStatus().equals(AccessorStatus.DELETED)) {
            throw new CcsDeletedException(mesg(bucketName, objectName) + " is already Deleted");
          }
          throw new CcsOperationException(mesg(bucketName, objectName) + STATUS_STRING + daoAccessorObject.getStatus());
        }
        objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.DELETING, null);
      } else {
        throw new CcsNotExistException(mesg(bucketName, objectName) + STATUS_STRING + AccessorStatus.UNKNOWN);
      }
      // Delete in S3
      deleteObjectOnStorage(bucketName, objectName, daoAccessorObject);
      // Update status in DB to Deleted
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.DELETED, null);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on delete object : " + bucketName + " - " + objectName, e);
    } finally {
      flushObject();
    }
  }

  private void deleteObjectOnStorage(final String bucketName, final String objectName,
                                     final DaoAccessorObject daoAccessorObject) throws CcsDbException {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.objectDeleteInBucket(bucketName, objectName);
    } catch (final DriverNotFoundException e) {
      // Ignore
      LOGGER.debugf("Try to delete but not found: %s", daoAccessorObject);
    } catch (final DriverException e) {
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.ERR_DEL, null);
      throw new CcsOperationException(mesg(bucketName, objectName) + STATUS_STRING + daoAccessorObject.getStatus(), e);
    }
  }
}

