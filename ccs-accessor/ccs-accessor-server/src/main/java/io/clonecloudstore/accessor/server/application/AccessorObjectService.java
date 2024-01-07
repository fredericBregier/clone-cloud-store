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

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.model.DbQueryAccessorHelper;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.model.ReplicatorResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.BUCKET;

/**
 * Accessor Bucket Service
 */
@ApplicationScoped
public class AccessorObjectService {
  private static final Logger LOGGER = Logger.getLogger(AccessorObjectService.class);
  private static final String STATUS_STRING = " Status: ";
  private static final String ISSUE_STRING = " issue: ";
  private final LocalReplicatorService localReplicatorService;
  private final DaoAccessorBucketRepository bucketRepository;
  private final DaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;

  public AccessorObjectService(final LocalReplicatorService localReplicatorService,
                               final Instance<DaoAccessorBucketRepository> bucketRepositoryInstance,
                               final Instance<DaoAccessorObjectRepository> objectRepositoryInstance) {
    this.localReplicatorService = localReplicatorService;
    // Normal injection does not work, probably due to test only dependency
    this.bucketRepository = bucketRepositoryInstance.get();
    this.objectRepository = objectRepositoryInstance.get();
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
  }

  private String mesg(final String bucketName, final String objectName) {
    return "Bucket: " + bucketName + " Object: " + objectName;
  }

  /**
   * Check if object or directory exists (internal)
   *
   * @param bucketName            technical name
   * @param objectOrDirectoryName prefix or full name
   * @param fullCheck             if True, and if Object, will check on Driver Storage
   * @return the associated StorageType
   */
  public StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName,
                                             final boolean fullCheck) throws CcsOperationException {
    return objectOrDirectoryExists(bucketName, objectOrDirectoryName, fullCheck, null, null, false);
  }

  /**
   * Check if object or directory exists
   */
  public StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName,
                                             final boolean fullCheck, final String clientId, final String opId,
                                             final boolean external) throws CcsOperationException {
    try {
      // Check in DB and associated status
      final var daoAccessorObjectIterator =
          objectRepository.getObjectPrefix(bucketName, objectOrDirectoryName, AccessorStatus.READY);
      var found = false;
      if (daoAccessorObjectIterator != null && (daoAccessorObjectIterator.hasNext())) {
        found = true;
        final var daoAccessorObject = daoAccessorObjectIterator.next();
        if (!daoAccessorObject.getName().equals(objectOrDirectoryName)) {
          return StorageType.DIRECTORY;
        }
      }
      if (!found) {
        final var response = remoteCheckObject(external, bucketName, objectOrDirectoryName, clientId, opId);
        if (response.targetId() != null && AccessorProperties.isFixOnAbsent()) {
          generateReplicationOrderForObject(bucketName, objectOrDirectoryName, clientId, opId, response.targetId(), 0,
              null);
        }
        return response.response();
      }
      SystemTools.consumeAll(daoAccessorObjectIterator);
      if (fullCheck) {
        // Check in S3
        return getStorageType(bucketName, objectOrDirectoryName);
      } else {
        return StorageType.OBJECT;
      }
    } catch (final CcsDbException e) {
      throw new CcsOperationException(
          "Database error on check object or directory : " + bucketName + " - " + objectOrDirectoryName, e);
    }
  }

  public void generateReplicationOrderForObject(final String bucketName, final String objectOrDirectoryName,
                                                final String clientId, final String opId, final String targetId,
                                                long len, String hash) {
    localReplicatorService.generateLocalReplicationOrder(
        new ReplicatorOrder(opId, targetId, ServiceProperties.getAccessorSite(), clientId, bucketName,
            objectOrDirectoryName, len, hash, ReplicatorConstants.Action.CREATE));
  }

  private ReplicatorResponse<StorageType> remoteCheckObject(final boolean remoteCheck, final String bucketName,
                                                            final String objectOrDirectoryName, final String clientId,
                                                            final String opId) {
    try {
      if (remoteCheck && AccessorProperties.isRemoteRead()) {
        return localReplicatorService.remoteCheckObject(bucketName, objectOrDirectoryName, clientId, opId);
      }
    } catch (final CcsOperationException ignore) {
      // Ignore
    }
    return new ReplicatorResponse<>(StorageType.NONE, null);
  }

  private StorageType getStorageType(final String bucketName, final String objectOrDirectoryName) {
    try (final var driver = storageDriverFactory.getInstance()) {
      return driver.directoryOrObjectExistsInBucket(bucketName, objectOrDirectoryName);
    } catch (final DriverException e) {
      throw new CcsOperationException(mesg(bucketName, objectOrDirectoryName) + ISSUE_STRING + e.getMessage(), e);
    }
  }

  /**
   * @param bucketName technical name
   * @param filter     the filter to apply on Objects
   * @return a stream (InputStream) of AccessorObject line by line (newline separated)
   */
  public InputStream filterObjects(final String bucketName, final AccessorFilter filter) throws CcsOperationException {
    try {
      DbQuery query = new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucketName);
      if (filter != null) {
        final var subQuery = DbQueryAccessorHelper.getDbQuery(filter);
        if (subQuery != null) {
          query = new DbQuery(RestQuery.CONJUNCTION.AND, query, subQuery);
        }
      }
      final var iterator = objectRepository.findIterator(query);
      return StreamIteratorUtils.getInputStreamFromIterator(iterator, source -> ((DaoAccessorObject) source).getDto(),
          AccessorObject.class);
    } catch (final CcsDbException | IOException e) {
      throw new CcsOperationException("Database error on filter object : " + bucketName + " - " +
          (filter != null ? filter.toString() : "No Filter"), e);
    }
  }

  /**
   * Check from DB if Object is pullable
   */
  public ReplicatorResponse<AccessorObject> checkPullable(final String bucketName, final String objectName,
                                                          final boolean external, final String clientId,
                                                          final String opId)
      throws CcsNotExistException, CcsOperationException {
    // TODO shall check if bucket is also available
    try {
      // Search objectName in database and check status
      final var daoAccessorObject = objectRepository.getObject(bucketName, objectName, AccessorStatus.READY);
      // TODO check if driver says it exists too
      // If not found or not available or not in driver
      if (daoAccessorObject == null || !AccessorStatus.READY.equals(daoAccessorObject.getStatus())) {
        // Else use remote check.
        final var storageType = remoteCheckObject(external, bucketName, objectName, clientId, opId);
        if (StorageType.OBJECT.equals(storageType.response())) {
          return new ReplicatorResponse<>(new AccessorObject().setBucket(bucketName).setName(objectName),
              storageType.targetId());
        }
        throw new CcsNotExistException("Object not found");
      }
      return new ReplicatorResponse<>(daoAccessorObject.getDto(), null);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on check pullable : " + bucketName + " - " + objectName, e);
    }
  }

  /**
   * When remote read is allowed, will try to read InputStream and DTO from remote
   */
  public InputStreamBusinessOut<AccessorObject> getRemotePullInputStream(final String bucketName,
                                                                         final String objectName, final String clientId,
                                                                         final String targetId, final String opId)
      throws CcsNotExistException {
    try {
      return localReplicatorService.remoteReadObject(bucketName, objectName, clientId, targetId, opId);
    } catch (final CcsOperationException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    }
  }

  // Useful ? Maybe later on with Reconciliator ?

  /**
   * Utility to get Object Metadata from Driver Storage
   */
  public StorageObject getObjectMetadata(final String bucketName, final String objectName)
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
    try {
      final var daoObject = objectRepository.getObject(bucketName, objectName);
      if (daoObject == null) {
        throw new CcsNotExistException(mesg(bucketName, objectName) + " not found");
      }
      return daoObject.getDto();
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on get object info : " + bucketName + " - " + objectName, e);
    }
  }

  /**
   * Before really creating Object, creates it in DB if possible
   *
   * @throws CcsNotAcceptableException if already in creation step
   */
  public DaoAccessorObject createObject(final AccessorObject accessorObject, final String hash, final long len)
      throws CcsOperationException, CcsAlreadyExistException, CcsNotExistException, CcsNotAcceptableException {
    try {
      // Check non-existence in DB/S3
      // First Bucket check
      final var bucket = bucketRepository.findBucketById(accessorObject.getBucket());
      if (bucket == null || !AccessorStatus.READY.equals(bucket.getStatus())) {
        throw new CcsNotExistException("Bucket does not exists");
      }
      // Now Object check
      var daoAccessorObject = objectRepository.getObject(accessorObject.getBucket(), accessorObject.getName());
      if (daoAccessorObject != null) {
        if (daoAccessorObject.getStatus() != AccessorStatus.DELETED) {
          if (daoAccessorObject.getStatus() == AccessorStatus.UPLOAD) {
            throw new CcsNotAcceptableException(
                mesg(accessorObject.getBucket(), accessorObject.getName()) + " already in creation, Status: " +
                    daoAccessorObject.getStatus());
          }
          throw new CcsAlreadyExistException(
              mesg(accessorObject.getBucket(), accessorObject.getName()) + STATUS_STRING +
                  daoAccessorObject.getStatus());
        }
        daoAccessorObject.setStatus(AccessorStatus.UPLOAD).setHash(hash).setSize(len).setCreation(Instant.now());
        if (ParametersChecker.isNotEmpty(hash) || len > 0) {
          objectRepository.updateFull(daoAccessorObject);
        } else {
          objectRepository.updateObjectStatus(accessorObject.getBucket(), accessorObject.getName(),
              AccessorStatus.UPLOAD, daoAccessorObject.getCreation());
        }
      } else {
        // Create object in database with status "In progress"
        accessorObject.setStatus(AccessorStatus.UPLOAD);
        daoAccessorObject = objectRepository.createEmptyItem().fromDto(accessorObject);
        daoAccessorObject.setId(GuidLike.getGuid()).setSite(ServiceProperties.getAccessorSite())
            .setCreation(Instant.now()).setStatus(AccessorStatus.UPLOAD);
        objectRepository.insert(daoAccessorObject);
      }
      return daoAccessorObject;
    } catch (final CcsDbException e) {
      throw new CcsOperationException(
          "Database error on create object : " + accessorObject.getBucket() + " - " + accessorObject.getName(), e);
    }
  }

  /**
   * Once Object really created in Driver Storage, finalize the Object in DB and Replicator if needed
   */
  public DaoAccessorObject createObjectFinalize(final String bucketName, final String objectName, final String hash,
                                                final long len, final String clientId, final boolean external)
      throws CcsOperationException {
    try {
      // Update Database with status available and metadata from ObjectStorage
      objectRepository.updateObjectStatusHashLen(bucketName, objectName, AccessorStatus.READY, hash, len);
      if (external) {
        // Send message to replicator topic.
        localReplicatorService.create(bucketName, objectName, clientId, len, hash);
      }
      return objectRepository.getObject(bucketName, objectName);
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on create object finalize : " + bucketName + " - " + objectName,
          e);
    }
  }

  /**
   * Delete object in DB and through Replicator if needed
   */
  public void deleteObject(final String bucketName, final String objectName, final String clientId,
                           final boolean external)
      throws CcsDeletedException, CcsNotExistException, CcsOperationException {
    try {
      // Check existence first in DB
      final var daoAccessorObject = objectRepository.getObject(bucketName, objectName);
      if (daoAccessorObject != null) {
        LOGGER.infof("Dao: %s", daoAccessorObject);
        if (daoAccessorObject.getStatus() != AccessorStatus.READY) {
          if (daoAccessorObject.getStatus().equals(AccessorStatus.DELETED)) {
            throw new CcsDeletedException(mesg(bucketName, objectName) + STATUS_STRING + daoAccessorObject.getStatus());
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
      if (external) {
        // Send message to replicator topic
        localReplicatorService.delete(bucketName, objectName, clientId);
      }
    } catch (final CcsDbException e) {
      throw new CcsOperationException("Database error on delete object : " + bucketName + " - " + objectName, e);
    }
  }

  private void deleteObjectOnStorage(final String bucketName, final String objectName,
                                     final DaoAccessorObject daoAccessorObject) throws CcsDbException {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.objectDeleteInBucket(bucketName, objectName);
    } catch (final DriverNotFoundException e) {
      // Ignore
      LOGGER.infof("Try to delete but not found: %s", daoAccessorObject);
    } catch (final DriverException e) {
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.ERR_DEL, null);
      throw new CcsOperationException(mesg(bucketName, objectName) + STATUS_STRING + daoAccessorObject.getStatus(), e);
    }
  }

  /**
   * Called only when QuarkusStreamHandler is in Error and Object in status IN_PROGRESS or UNKNOWN
   */
  public void inError(final String bucketName, final String objectName) {
    try {
      objectRepository.updateObjectStatus(bucketName, objectName, AccessorStatus.ERR_UPL, null);
    } catch (final CcsDbException ignore) {
      // Ignore
    }
  }
}