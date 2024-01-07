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

package io.clonecloudstore.accessor.server.database.model;

import java.time.Instant;
import java.util.List;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;

/**
 * Bucket DAO Repository
 */
public interface DaoAccessorBucketRepository extends RepositoryBaseInterface<DaoAccessorBucket> {
  String TABLE_NAME = "buckets";
  String SITE = "site";
  String NAME = "name";
  String CREATION = "creation";
  String EXPIRES = "expires";
  String BUCKET_STATUS = "status";
  String BUCKET_RSTATUS = "rstatus";

  /**
   * Generate client-id prefix, used in bucket technical name.
   *
   * @param clientId Client ID used to identify client
   * @return value of client-id prefix.
   */
  static String getPrefix(final String clientId) {
    return clientId + "-";
  }

  /**
   * Generate technical name from client-id and bucket functional name.
   *
   * @param clientId   Client ID used to identify client
   * @param bucketName Functional Bucket Name
   * @return value of bucket technical name
   */
  static String getBucketTechnicalName(final String clientId, final String bucketName) {
    return getPrefix(clientId) + bucketName;
  }

  /**
   * Reverse technical bucket name from client-id to bucket functional name.
   *
   * @param clientId            Client ID used to identify client
   * @param technicalBucketName Technical Bucket Name
   * @return value of bucket Functional name
   */
  static String getBucketName(final String clientId, final String technicalBucketName) {
    return technicalBucketName.replace(getPrefix(clientId), "");
  }

  static String getRealBucketName(final String clientId, final String bucketName, final boolean isPublic) {
    if (isPublic) {
      return getBucketTechnicalName(clientId, bucketName);
    }
    return bucketName;
  }

  /**
   * List all buckets from clientId
   */
  default List<AccessorBucket> listBuckets(final String clientId) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.QUERY.START_WITH, getPkName(), getPrefix(clientId));
      return findStream(query).map(DaoAccessorBucket::getDto).toList();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Update the given Bucket, creation might be null to ignore
   */
  default AccessorBucket updateBucketStatus(final AccessorBucket bucket, final AccessorStatus status,
                                            final Instant creation) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.QUERY.EQ, getPkName(), bucket.getId());
      final var dbUpdate = new DbUpdate().set(BUCKET_STATUS, status);
      if (creation != null) {
        dbUpdate.set(CREATION, creation);
        bucket.setCreation(creation);
      }
      if (this.update(query, dbUpdate) != 1) {
        throw new CcsDbException("Cannot update status for " + bucket.getId(),
            new CcsOperationException("Cannot update status for " + bucket.getId()));
      }
      bucket.setStatus(status);
      return bucket;
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Return the associated bucket
   */
  default AccessorBucket findBucketById(final String bucketTechnicalName) throws CcsDbException {
    try {
      final var dao = this.findOne(new DbQuery(RestQuery.QUERY.EQ, getPkName(), bucketTechnicalName));
      if (dao != null) {
        return dao.getDto();
      }
      return null;
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Insert the given Bucket, returning the modified DTO
   */
  default AccessorBucket insertBucket(final AccessorBucket bucket) throws CcsDbException {
    try {
      final var creation = Instant.now();
      final DaoAccessorBucket datBucket = this.createEmptyItem();
      datBucket.setId(bucket.getId());
      datBucket.setName(bucket.getName());
      datBucket.setStatus(AccessorStatus.UPLOAD);
      datBucket.setCreation(creation);
      datBucket.setSite(ServiceProperties.getAccessorSite());
      this.insert(datBucket);
      return datBucket.getDto();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }
}
