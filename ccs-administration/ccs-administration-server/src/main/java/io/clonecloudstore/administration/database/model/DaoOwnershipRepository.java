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

package io.clonecloudstore.administration.database.model;

import java.util.List;

import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;

public interface DaoOwnershipRepository extends RepositoryBaseInterface<DaoOwnership> {
  String TABLE_NAME = "ownerships";
  String CLIENT_ID = "clientId";
  String BUCKET = "bucket";
  String OWNERSHIP = "ownership";

  static String getInternalId(final String client, final String bucket) {
    return client + "_" + bucket;
  }

  default ClientOwnership insertOwnership(final String client, final String bucket, final ClientOwnership ownership)
      throws CcsDbException {
    try {
      final var dao = this.createEmptyItem();
      dao.setClientId(client).setBucket(bucket).setOwnership(ownership).setId(getInternalId(client, bucket));
      this.insert(dao);
      return dao.getOwnership();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default ClientOwnership findByBucket(final String client, final String bucket) throws CcsDbException {
    try {
      final var dao = this.findWithPk(getInternalId(client, bucket));
      return (dao != null ? dao.getOwnership() : null);
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default List<ClientBucketAccess> findAllOwnerships(final String client) throws CcsDbException {
    try {
      return findStream(new DbQuery(RestQuery.QUERY.EQ, CLIENT_ID, client)).map(DaoOwnership::getDto).toList();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default List<ClientBucketAccess> findOwnerships(final String client, final ClientOwnership ownership)
      throws CcsDbException {
    try {
      return findStream(new DbQuery(RestQuery.QUERY.EQ, CLIENT_ID, client)).filter(
          daoOwnership -> daoOwnership.getOwnership().include(ownership)).map(DaoOwnership::getDto).toList();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default ClientOwnership updateOwnership(final String client, final String bucket, final ClientOwnership ownership)
      throws CcsDbException {
    try {
      final var dao = this.findWithPk(getInternalId(client, bucket));
      if (dao != null) {
        dao.setOwnership(dao.getOwnership().fusion(ownership));
        final var query = DbQuery.idEquals(getInternalId(client, bucket));
        final var dbUpdate = new DbUpdate().set(OWNERSHIP, dao.getOwnership());
        if (this.update(query, dbUpdate) != 1) {
          throw new CcsDbException(new CcsOperationException("Could not update Ownership"));
        }
        return dao.getOwnership();
      }
      throw new CcsDbException(new CcsNotExistException("Could not update Ownership"));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default boolean deleteOwnership(final String client, final String bucket) throws CcsDbException {
    try {
      if (this.delete(DbQuery.idEquals(getInternalId(client, bucket))) != 1) {
        throw new CcsDbException(new CcsOperationException("Could not delete Ownership"));
      } else {
        return true;
      }
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default boolean deleteOwnerships(final String bucket) throws CcsDbException {
    try {
      if (this.delete(new DbQuery(RestQuery.QUERY.EQ, BUCKET, bucket)) == 0) {
        throw new CcsDbException(new CcsOperationException("Could not find any Ownership to Delete"));
      } else {
        return true;
      }
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }
}
