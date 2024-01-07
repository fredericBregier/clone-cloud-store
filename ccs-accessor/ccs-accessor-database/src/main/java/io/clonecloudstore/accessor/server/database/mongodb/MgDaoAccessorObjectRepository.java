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

package io.clonecloudstore.accessor.server.database.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.mongo.ExtendedPanacheMongoRepositoryBase;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;

import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;

/**
 * MongoDB DAO Object Repository
 */
@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgDaoAccessorObjectRepository
    extends ExtendedPanacheMongoRepositoryBase<DaoAccessorObject, MgDaoAccessorObject>
    implements DaoAccessorObjectRepository {
  @Override
  public String getTable() {
    return TABLE_NAME;
  }

  @Override
  public DaoAccessorObject createEmptyItem() {
    return new MgDaoAccessorObject();
  }

  public void createIndex() throws CcsDbException {
    try {
      mongoCollection().createIndex(Indexes.ascending(SITE, BUCKET, NAME),
          new IndexOptions().name(TABLE_NAME + "_unique_idx").unique(true));
      mongoCollection().createIndex(Indexes.ascending(SITE, BUCKET, NAME, STATUS),
          new IndexOptions().name(TABLE_NAME + "_filter_idx"));
    } catch (final MongoException e) {
      throw new CcsDbException("Cannot Create Index", e);
    }
  }
}
