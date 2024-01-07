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

package io.clonecloudstore.common.database.postgre;

import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.database.postgre.impl.simple.PgDaoExample;
import io.clonecloudstore.common.database.postgre.impl.simple.PgDaoExampleRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.test.resource.postgres.NoPostgreDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID_PG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(NoPostgreDbProfile.class)
class DbPostgreNoDbTest {
  private static final Logger LOG = Logger.getLogger(DbPostgreNoDbTest.class);
  @Inject
  PgDaoExampleRepository repository;

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertTrue(repository.isSqlRepository());
    assertThrows(CcsDbException.class, () -> repository.countAll());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    assertThrows(CcsDbException.class, () -> repository.insert(new PgDaoExample(dtoExample)));
    assertThrows(CcsDbException.class, () -> repository.flushAll());
    assertThrows(CcsDbException.class, () -> repository.findUsingStream(new DbQuery()));
    streamCommand(new DbQuery());
    assertThrows(CcsDbException.class, () -> repository.findWithPk(opId));
    final var dbQuery = DbQuery.idEquals(opId);
    assertThrows(CcsDbException.class, () -> repository.count(dbQuery));
    var dbUpdate = new DbUpdate().set(FIELD2, "value2");
    final var finalDbUpdate = dbUpdate;
    assertThrows(CcsDbException.class, () -> repository.update(dbQuery, finalDbUpdate));
    assertThrows(CcsDbException.class, () -> repository.findOne(dbQuery));
    dbUpdate = new DbUpdate().set(FIELD2, "value3");
    assertThrows(IllegalStateException.class, () -> repository.update(PostgreSqlHelper.update(finalDbUpdate, dbQuery),
        PostgreSqlHelper.getUpdateParamsAsArray(finalDbUpdate, dbQuery)));
    assertThrows(IllegalStateException.class,
        () -> repository.find(PostgreSqlHelper.query(dbQuery), dbQuery.getSqlParamsAsArray()).firstResult());
    assertThrows(CcsDbException.class, () -> repository.deleteWithPk(opId));
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    assertThrows(CcsDbException.class, () -> repository.insert(example));
    assertThrows(CcsDbException.class, () -> repository.updateFull(example));
    assertThrows(CcsDbException.class, () -> repository.delete(dbQuery));
  }

  /**
   * Example of usage of Stream within a Transactional method
   *
   * @param dbQuery the dbQuery
   */
  void streamCommand(final DbQuery dbQuery) throws CcsDbException {
    assertThrows(CcsDbException.class, () -> repository.findStream(dbQuery));
    assertThrows(CcsDbException.class, () -> repository.findQuery(dbQuery));
  }

  @Test
  void dbBulk() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertThrows(CcsDbException.class, () -> repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    final var start = System.nanoTime();
    for (var i = 0; i < 10; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      assertThrows(CcsDbException.class, () -> repository.insert(pgDbDtoExample));
    }
    assertThrows(CcsDbException.class, () -> repository.flushAll());
    final var stop = System.nanoTime();
    final var start2 = System.nanoTime();
    for (var i = 0; i < 10; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      assertThrows(CcsDbException.class, () -> repository.addToInsertBulk(pgDbDtoExample));
    }
    final var stop2 = System.nanoTime();

    LOG.info("Standard Insert: " + (stop - start) / 1000000);
    LOG.info("Bulk Insert: " + (stop2 - start2) / 1000000);
    assertThrows(CcsDbException.class, () -> repository.deleteAllDb());
  }
}
