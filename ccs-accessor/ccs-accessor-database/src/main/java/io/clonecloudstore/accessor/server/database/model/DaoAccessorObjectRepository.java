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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;

/**
 * Object DAO Repository
 */
public interface DaoAccessorObjectRepository extends RepositoryBaseInterface<DaoAccessorObject> {
  String TABLE_NAME = "objects";

  String BUCKET = "bucket";
  String SITE = "site";
  String NAME = "name";
  String HASH = "hash";
  String STATUS = "status";
  String CREATION = "creation";
  String EXPIRES = "expires";
  String SIZE = "size";
  String METADATA = "metadata";
  String RSTATUS = "rstatus";

  /**
   * Get the Object
   */
  default DaoAccessorObject getObject(final String bucket, final String objectName) throws CcsDbException {
    try {
      return this.findOne(new DbQuery(RestQuery.CONJUNCTION.AND,
          new DbQuery(RestQuery.QUERY.EQ, SITE, ServiceProperties.getAccessorSite()),
          new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucket), new DbQuery(RestQuery.QUERY.EQ, NAME, objectName)));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Get the Object with same status
   */
  default DaoAccessorObject getObject(final String bucket, final String objectName, final AccessorStatus status)
      throws CcsDbException {
    try {
      return this.findOne(new DbQuery(RestQuery.CONJUNCTION.AND,
          new DbQuery(RestQuery.QUERY.EQ, SITE, ServiceProperties.getAccessorSite()),
          new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucket), new DbQuery(RestQuery.QUERY.EQ, NAME, objectName),
          new DbQuery(RestQuery.QUERY.EQ, STATUS, status.name())));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Get Stream of Objects using prefix on name
   */
  default Iterator<DaoAccessorObject> getObjectPrefix(final String bucket, final String objectNamePrefix,
                                                      final AccessorStatus status) throws CcsDbException {
    try {
      final var queries = new ArrayList<>(
          List.of(new DbQuery(RestQuery.QUERY.EQ, SITE, ServiceProperties.getAccessorSite()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucket),
              new DbQuery(RestQuery.QUERY.START_WITH, NAME, objectNamePrefix)));
      if (status != null) {
        queries.add(new DbQuery(RestQuery.QUERY.EQ, STATUS, status.name()));
      }
      return this.findIterator(new DbQuery(RestQuery.CONJUNCTION.AND, queries));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Update the Object, creation time might be null to ignore
   */
  default void updateObjectStatus(final String bucketName, final String objectName, final AccessorStatus status,
                                  final Instant creation) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.CONJUNCTION.AND,
          new DbQuery(RestQuery.QUERY.EQ, SITE, ServiceProperties.getAccessorSite()),
          new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucketName), new DbQuery(RestQuery.QUERY.EQ, NAME, objectName));
      final var update = new DbUpdate().set(STATUS, status);
      if (creation != null) {
        update.set(CREATION, creation);
      }
      if (update(query, update) != 1) {
        throw new CcsDbException("Cannot update status for " + bucketName + ":" + objectName,
            new CcsOperationException("Cannot update status for " + bucketName + ":" + objectName));
      }
    } catch (final CcsDbException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Update the Object
   */
  default void updateObjectStatusHashLen(final String bucketName, final String objectName, final AccessorStatus status,
                                         final String hash, final long len) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.CONJUNCTION.AND,
          new DbQuery(RestQuery.QUERY.EQ, SITE, ServiceProperties.getAccessorSite()),
          new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucketName), new DbQuery(RestQuery.QUERY.EQ, NAME, objectName));
      final var update = new DbUpdate().set(STATUS, status).set(HASH, hash).set(SIZE, len);
      if (update(query, update) != 1) {
        throw new CcsDbException("Cannot update status for " + bucketName + ":" + objectName,
            new CcsOperationException("Cannot update status for " + bucketName + ":" + objectName));
      }
    } catch (final CcsDbException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Update if needed Object from DTO
   */
  default void updateFromDto(final DaoAccessorObject daoAccessorObject, final AccessorObject accessorObject)
      throws CcsDbException {
    try {
      if (daoAccessorObject.updateFromDtoExceptIdSite(accessorObject)) {
        this.updateFull(daoAccessorObject);
      }
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }
}
