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

package io.clonecloudstore.common.database.mongo.impl.codec;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.clonecloudstore.common.database.model.dao.DaoExample;
import io.clonecloudstore.common.database.model.dao.DaoExampleRepository;
import io.clonecloudstore.common.database.mongo.ExtendedPanacheMongoRepositoryBase;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;

import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgDaoExampleRepository extends ExtendedPanacheMongoRepositoryBase<DaoExample, MgDaoExample>
    implements DaoExampleRepository {
  @Override
  public String getTable() {
    return TABLE_NAME;
  }

  @Override
  public DaoExample createEmptyItem() {
    return new MgDaoExample();
  }

  public void createIndex() throws CcsDbException {
    try {
      mongoCollection().createIndex(Indexes.ascending(FIELD1, TIME_FIELD),
          new IndexOptions().name(TABLE_NAME + "_filter_idx"));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Example of Stream usage: note @Transactional is not needed
   *
   * @param dbQuery
   * @return the first item
   */
  public DaoExample findUsingStream(final DbQuery dbQuery) throws CcsDbException {
    final var stream = findStream(dbQuery);
    final var optionalPgDbDtoExample = stream.findFirst();
    if (optionalPgDbDtoExample.isPresent()) {
      return optionalPgDbDtoExample.get();
    }
    return null;
  }
}
