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

package io.clonecloudstore.common.database.postgre.impl.rich;

import io.clonecloudstore.common.database.postgre.ExtendedPanacheRepositoryBase;
import io.clonecloudstore.common.database.postgre.model.rich.DaoRichExample;
import io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.POSTGRE;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = POSTGRE)
@ApplicationScoped
@Transactional
public class PgDaoRichExampleRepository extends ExtendedPanacheRepositoryBase<DaoRichExample, PgDaoRichExample>
    implements DaoRichExampleRepository {
  public PgDaoRichExampleRepository() {
    super(new PgDaoRichExample());
  }

  @Override
  public String getTable() {
    return TABLE_NAME;
  }

  @Override
  public DaoRichExample createEmptyItem() {
    return new PgDaoRichExample();
  }

  /**
   * Example of Stream usage: note that Stream cannot be used outside @Transactional
   *
   * @param dbQuery
   * @return the first item
   */
  public DaoRichExample findUsingStream(final DbQuery dbQuery) throws CcsDbException {
    // Stream cannot be used outside @Transactional
    final var stream = findStream(dbQuery);
    final var optionalPgDbDtoExample = stream.findFirst();
    if (optionalPgDbDtoExample.isPresent()) {
      return optionalPgDbDtoExample.get();
    }
    return null;
  }
}
