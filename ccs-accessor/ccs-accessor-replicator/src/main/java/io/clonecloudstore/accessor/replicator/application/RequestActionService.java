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
import java.util.concurrent.CompletableFuture;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.administration.client.OwnershipApiClient;
import io.clonecloudstore.administration.client.OwnershipApiClientFactory;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAllowedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerExceptionMapper;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
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
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

/**
 * Request Action Service
 */
@ApplicationScoped
@Unremovable
public class RequestActionService {
  private static final Logger LOGGER = Logger.getLogger(RequestActionService.class);
  private static final String STATUS_STRING = " Status: ";
  private static final String NOT_ALLOWED = " not allowed";
  private final LocalReplicatorApiClientFactory localReplicatorApiClientFactory;
  private final DaoAccessorBucketRepository bucketRepository;
  private final DaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;
  private final OwnershipApiClientFactory ownershipApiClientFactory;

  public RequestActionService(final LocalReplicatorApiClientFactory localReplicatorApiClientFactory,
                              final Instance<DaoAccessorBucketRepository> bucketRepositoryInstance,
                              final Instance<DaoAccessorObjectRepository> objectRepositoryInstance,
                              final OwnershipApiClientFactory ownershipApiClientFactory) {
    this.localReplicatorApiClientFactory = localReplicatorApiClientFactory;
    // Normal injection does not work, probably due to test only dependency
    this.bucketRepository = bucketRepositoryInstance.get();
    this.objectRepository = objectRepositoryInstance.get();
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
    this.ownershipApiClientFactory = ownershipApiClientFactory;
  }

  private String mesg(final String bucketName, final String objectName) {
    return "Bucket: " + bucketName + " Object: " + objectName;
  }

  private void checkReplicatorOrderBucketName(final ReplicatorOrder replicatorOrder) {
    try {
      // Check format (special char, uppercase...) and size (min, max) for bucket name
      ParametersChecker.checkSanityBucketName(replicatorOrder.bucketName());
    } catch (final CcsInvalidArgumentRuntimeException e) {
      final var message = String.format("Bucket Name %s is invalid", replicatorOrder.bucketName());
      throw new CcsOperationException(message);
    }
  }

  /**
   * Create bucket from client Id and BucketName
   */
  public void createBucket(final ReplicatorOrder replicatorOrder)
      throws CcsAlreadyExistException, CcsOperationException {
    checkReplicatorOrderBucketName(replicatorOrder);
    // Check if bucket already exist in database.
    AccessorBucket result;
    try {
      result = bucketRepository.findBucketById(replicatorOrder.bucketName());
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Error on find repository with bucket name", e);
    }
    if (result != null && !AccessorStatus.DELETED.equals(result.getStatus())) {
      final var message = String.format("Bucket %s already exist", replicatorOrder.bucketName());
      LOGGER.errorf(message);
      throw new CcsAlreadyExistException(message);
    } else if (result == null) {
      // Create Bucket in Database with Status UPLOAD.
      // Only name and clientId (other value are erased during creation).
      result = new AccessorBucket();
      result.setId(replicatorOrder.bucketName());
      result.setClientId(replicatorOrder.clientId());
      result.setSite(ServiceProperties.getAccessorSite());
    }
    createBucketFromDto(result);
  }

  /**
   * Create Bucket from DTO Accessor
   *
   * @param bucket Accessor Bucket with functional data
   */
  private void createBucketFromDto(final AccessorBucket bucket) throws CcsOperationException {
    LOGGER.debugf("Create bucket %s in database and in object storage", bucket.getId());
    try {
      // Insert in Database
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
      final var storageBucket = new StorageBucket(newBucket.getId(), newBucket.getClientId(), newBucket.getCreation());
      createBucketOnStorage(storageBucket, newBucket);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on bucket " + bucket.getId() + " creation", e);
    } finally {
      flushBucket();
    }
  }

  private void finalizeAddOwnership(final OwnershipApiClient ownershipClient,
                                    final CompletableFuture<ClientOwnership> completableFuture) {
    try {
      ownershipClient.getClientOwnershipFromAsync(completableFuture);
    } catch (final CcsWithStatusException e) {
      // Ignore
      LOGGER.info(e);
    }
  }

  private void createBucketOnStorage(final StorageBucket storageBucket, final AccessorBucket bucket)
      throws CcsDbException {
    try (final var storageDriver = storageDriverFactory.getInstance();
         final var ownershipClient = ownershipApiClientFactory.newClient()) {
      final var completableFuture =
          ownershipClient.addAsync(storageBucket.clientId(), storageBucket.bucket(), ClientOwnership.OWNER);
      storageDriver.bucketCreate(storageBucket);
      finalizeAddOwnership(ownershipClient, completableFuture);
      // Update Database in Database with status Ready.
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.READY, null);
      LOGGER.debugf("Bucket %s created in database and in object storage", bucket.getId());
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
   * Delete bucket
   */
  public void deleteBucket(final ReplicatorOrder replicatorOrder)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException {
    checkReplicatorOrderBucketName(replicatorOrder);
    try {
      final var bucket = bucketRepository.findBucketById(replicatorOrder.bucketName());
      if (bucket == null) {
        throw new CcsNotExistException("Bucket " + replicatorOrder.bucketName() + " doesn't exist");
      } else if (bucket.getStatus() == AccessorStatus.READY) {
        if (!bucket.getClientId().equals(replicatorOrder.clientId())) {
          throw new CcsNotAllowedException(replicatorOrder.bucketName() + " is not owned by current client");
        }
        final var count = objectRepository.count(new DbQuery(RestQuery.CONJUNCTION.AND,
            new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.BUCKET, bucket.getId()),
            new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.STATUS, AccessorStatus.READY)));
        if (count > 0) {
          throw new CcsNotAcceptableException("Bucket is not empty");
        }
        bucketRepository.updateBucketStatus(bucket, AccessorStatus.DELETING, null);
        deleteBucketOnStorage(replicatorOrder.bucketName(), bucket);
      } else if (bucket.getStatus() == AccessorStatus.DELETED) {
        throw new CcsDeletedException("Bucket " + replicatorOrder.bucketName() + " is already deleted");
      } else {
        throw new CcsOperationException("Bucket Status " + bucket.getStatus());
      }
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on delete bucket " + replicatorOrder.bucketName(), e);
    } finally {
      flushBucket();
    }
  }

  private void finalizeDeleteOwnership(final OwnershipApiClient ownershipClient,
                                       final CompletableFuture<Response> completableFuture) {
    try {
      ownershipClient.getBooleanFromAsync(completableFuture);
    } catch (final CcsWithStatusException e) {
      // Ignore
      LOGGER.info(e);
    }
  }

  private void deleteBucketOnStorage(final String bucketName, final AccessorBucket bucket) throws CcsDbException {
    try {
      try (final var storageDriver = storageDriverFactory.getInstance();
           final var ownershipClient = ownershipApiClientFactory.newClient()) {
        // All clients cannot have anymore ownership on this bucket
        final var completableFuture = ownershipClient.deleteAllClientsAsync(bucketName);
        storageDriver.bucketDelete(bucketName);
        finalizeDeleteOwnership(ownershipClient, completableFuture);
      }
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.DELETED, null);
    } catch (final DriverException | RuntimeException e) {
      LOGGER.error(e.getMessage());
      bucketRepository.updateBucketStatus(bucket, AccessorStatus.ERR_DEL, null);
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  private void checkSanityReplicatorOrder(final ReplicatorOrder replicatorOrder) {
    checkReplicatorOrderBucketName(replicatorOrder);
    try {
      // Check format (special char, uppercase...) and size (min, max) for object name
      ParametersChecker.checkSanityObjectName(replicatorOrder.objectName());
    } catch (final CcsInvalidArgumentRuntimeException e) {
      final var message = String.format("Object Name %s is invalid", replicatorOrder.objectName());
      throw new CcsOperationException(message);
    }
  }

  private void checkOwnership(final ReplicatorOrder replicatorOrder, final ClientOwnership ownership)
      throws CcsNotAllowedException {
    try (final var ownerClient = ownershipApiClientFactory.newClient()) {
      if (!ownerClient.findByBucket(replicatorOrder.clientId(), replicatorOrder.bucketName()).include(ownership)) {
        throw new CcsNotAllowedException(ownership.name() + NOT_ALLOWED);
      }
    } catch (CcsWithStatusException e) {
      if (e.getStatus() < Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
        throw new CcsNotAllowedException(ownership.name() + NOT_ALLOWED, e);
      }
      throw CcsServerExceptionMapper.getCcsException(e.getStatus(), e.getMessage(), e);
    }
  }

  public void createObject(final ReplicatorOrder replicatorOrder)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException {
    checkSanityReplicatorOrder(replicatorOrder);
    checkOwnership(replicatorOrder, ClientOwnership.WRITE);
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
      throw CcsServerExceptionMapper.getCcsException(e.getStatus(), e.getMessage(), e);
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
        // Create object in database with status "UPLOAD"
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
   * Once Object really created in Cloud Clone Store, finalize the Object in DB
   */
  private void createObjectFinalize(final DaoAccessorObject daoObject) throws CcsOperationException {
    try {
      // Update Database with status Ready and metadata from ObjectStorage
      objectRepository.updateObjectStatusHashLen(daoObject.getBucket(), daoObject.getName(), AccessorStatus.READY,
          daoObject.getHash(), daoObject.getSize());
      objectRepository.getObject(daoObject.getBucket(), daoObject.getName());
    } catch (final CcsDbException e) {
      throw new CcsOperationException(
          "Database error on create object finalize : " + daoObject.getBucket() + " - " + daoObject.getName(), e);
    }
  }

  /**
   * Delete object in DB and through Replicator if needed
   */
  public void deleteObject(final ReplicatorOrder replicatorOrder)
      throws CcsDeletedException, CcsNotExistException, CcsOperationException {
    checkSanityReplicatorOrder(replicatorOrder);
    checkOwnership(replicatorOrder, ClientOwnership.DELETE);
    try {
      // Check existence first in DB
      final var daoAccessorObject =
          objectRepository.getObject(replicatorOrder.bucketName(), replicatorOrder.objectName());
      if (daoAccessorObject != null) {
        LOGGER.debugf("Dao: %s", daoAccessorObject);
        if (daoAccessorObject.getStatus() != AccessorStatus.READY) {
          if (daoAccessorObject.getStatus().equals(AccessorStatus.DELETED)) {
            throw new CcsDeletedException(
                mesg(replicatorOrder.bucketName(), replicatorOrder.objectName()) + " is already Deleted");
          }
          throw new CcsOperationException(
              mesg(replicatorOrder.bucketName(), replicatorOrder.objectName()) + STATUS_STRING +
                  daoAccessorObject.getStatus());
        }
        objectRepository.updateObjectStatus(replicatorOrder.bucketName(), replicatorOrder.objectName(),
            AccessorStatus.DELETING, null);
      } else {
        throw new CcsNotExistException(
            mesg(replicatorOrder.bucketName(), replicatorOrder.objectName()) + STATUS_STRING + AccessorStatus.UNKNOWN);
      }
      // Delete in S3
      deleteObjectOnStorage(replicatorOrder.bucketName(), replicatorOrder.objectName(), daoAccessorObject);
      // Update status in DB to Deleted
      objectRepository.updateObjectStatus(replicatorOrder.bucketName(), replicatorOrder.objectName(),
          AccessorStatus.DELETED, null);
    } catch (final CcsDbException e) {
      throw new CcsOperationException(
          "Database error on delete object : " + replicatorOrder.bucketName() + " - " + replicatorOrder.objectName(),
          e);
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

