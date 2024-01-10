/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.accessor.server.commons;

import java.util.Collection;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;

public interface AccessorBucketServiceInterface {
  /**
   * Create bucket from client Id and technicalBucketName
   *
   * @param clientId            Format client ID use to identify client
   * @param technicalBucketName Technical Bucket Name
   * @param isPublic            True means replicate with replicator module
   * @return AccessorBucket add on Database and in object storage
   */
  AccessorBucket createBucket(final String clientId, final String technicalBucketName, final boolean isPublic)
      throws CcsAlreadyExistException, CcsOperationException;

  /**
   * Get Bucket information from bucket technical name
   *
   * @param technicalBucketName Bucket technical name
   * @return AccessorBucket found with technical name
   */
  AccessorBucket getBucket(final String technicalBucketName, final String clientId, final String opId,
                           final boolean isPublic)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException;

  /**
   * Get All buckets in Storgate
   *
   * @return the list of Buckets
   */
  Collection<AccessorBucket> getBuckets(final String clientId) throws CcsOperationException;

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
  boolean checkBucket(final String technicalBucketName, final boolean fullCheck, final String clientId,
                      final String opId, final boolean isPublic) throws CcsOperationException;

  /**
   * Delete bucket from technical bucket name
   *
   * @param clientId            Client ID used to identify client
   * @param technicalBucketName Bucket technical name
   * @param isPublic            true to send replication message on replicator
   * @return the associated DTO- deleted
   */
  AccessorBucket deleteBucket(final String clientId, final String technicalBucketName, final boolean isPublic)
      throws CcsNotExistException, CcsDeletedException, CcsOperationException, CcsNotAcceptableException;
}
