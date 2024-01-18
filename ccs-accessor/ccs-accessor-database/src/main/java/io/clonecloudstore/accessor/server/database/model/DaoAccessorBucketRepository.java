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
  String CLIENT_ID = "clientId";
  String SITE = "site";
  String CREATION = "creation";
  String EXPIRES = "expires";
  String BUCKET_STATUS = "status";
  String BUCKET_RSTATUS = "rstatus";

  /**
   * List all buckets from clientId
   */
  default List<AccessorBucket> listBuckets(final String clientId) throws CcsDbException {
    try {
      final var query = new DbQuery();
      return findStream(query).filter(daoAccessorBucket -> daoAccessorBucket.getClientId().equals(clientId))
          .map(DaoAccessorBucket::getDto).toList();
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
      final var dbUpdate = new DbUpdate().set(BUCKET_STATUS, status).set(CLIENT_ID, bucket.getClientId());
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
  default AccessorBucket findBucketById(final String bucketName) throws CcsDbException {
    try {
      final var dao = this.findOne(new DbQuery(RestQuery.QUERY.EQ, getPkName(), bucketName));
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
      datBucket.setClientId(bucket.getClientId());
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
