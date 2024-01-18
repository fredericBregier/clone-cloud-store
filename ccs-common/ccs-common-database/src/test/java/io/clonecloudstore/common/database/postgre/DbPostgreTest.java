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
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.database.postgre.impl.simple.PgDaoExample;
import io.clonecloudstore.common.database.postgre.impl.simple.PgDaoExampleRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbType;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.resource.postgres.PostgresProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID_PG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(PostgresProfile.class)
class DbPostgreTest {
  private static final Logger LOG = Logger.getLogger(DbPostgreTest.class);
  @Inject
  PgDaoExampleRepository repository;
  private final int MAX_BULK = 1500;

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertTrue(repository.isSqlRepository());
    assertEquals(0, repository.countAll());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    repository.insert(new PgDaoExample(dtoExample));
    repository.flushAll();
    assertEquals(1, repository.countAll());
    var pgDbDtoExample = repository.findUsingStream(new DbQuery());
    assertEquals(opId, pgDbDtoExample.getGuid());
    assertEquals(SystemTools.toMillis(instant), pgDbDtoExample.getTimeField());
    streamCommand(new DbQuery());
    pgDbDtoExample = repository.findWithPk(opId);
    assertEquals(opId, pgDbDtoExample.getGuid());
    final var dbQuery = DbQuery.idEquals(opId);
    assertEquals(1, repository.count(dbQuery));
    var dbUpdate = new DbUpdate().set(FIELD2, "value2");
    assertEquals(1, repository.update(dbQuery, dbUpdate));
    pgDbDtoExample = repository.findOne(dbQuery);
    assertEquals(opId, pgDbDtoExample.getGuid());
    assertEquals("value2", pgDbDtoExample.getField2());
    dbUpdate = new DbUpdate().set(FIELD2, "value3");
    Assertions.assertEquals(1, repository.update(PostgreSqlHelper.update(dbUpdate, dbQuery),
        PostgreSqlHelper.getUpdateParamsAsArray(dbUpdate, dbQuery)));
    pgDbDtoExample = repository.find(PostgreSqlHelper.query(dbQuery), dbQuery.getSqlParamsAsArray()).firstResult();
    assertEquals(opId, pgDbDtoExample.getGuid());
    assertEquals("value3", pgDbDtoExample.getField2());
    assertEquals(1,
        repository.getEntityManager().createNativeQuery(PostgreSqlHelper.select(repository.getTable(), dbQuery))
            .setParameter(1, dbQuery.getSqlParams().get(0)).getResultList().size());
    assertEquals(1,
        repository.getEntityManager().createNativeQuery(PostgreSqlHelper.select(repository.getTable(), new DbQuery()))
            .getResultList().size());
    assertEquals(1,
        repository.getEntityManager().createNativeQuery(PostgreSqlHelper.count(repository.getTable(), dbQuery))
            .setParameter(1, dbQuery.getSqlParams().get(0)).getResultList().size());
    assertEquals(1,
        repository.getEntityManager().createNativeQuery(PostgreSqlHelper.count(repository.getTable(), new DbQuery()))
            .getResultList().size());
    repository.deleteWithPk(opId);
    assertEquals(0, repository.countAll());
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    repository.insert(example);
    final var json = JsonUtil.getInstance().writeValueAsString(dtoExample);
    final var dtoExample1 = JsonUtil.getInstance().readValue(json, DtoExample.class);
    assertEquals(dtoExample, dtoExample1);
    example.setField1("newValue");
    repository.updateFull(example);
    pgDbDtoExample = repository.findOne(DbQuery.idEquals(example.getGuid()));
    assertEquals(opId, pgDbDtoExample.getGuid());
    assertEquals("newValue", pgDbDtoExample.getField1());
    assertEquals(1, repository.countAll());
    assertEquals(1, repository.delete(dbQuery));
    repository.insert(example);
    pgDbDtoExample = repository.findOne(DbQuery.idEquals(example.getGuid()));
    var dbQueryEmpty = new DbQuery();
    Assertions.assertEquals(1, repository.update(PostgreSqlHelper.update(dbUpdate, dbQueryEmpty),
        PostgreSqlHelper.getUpdateParamsAsArray(dbUpdate, dbQueryEmpty)));
    assertEquals(1, deleteFromNativeQuery(dbQueryEmpty));
    assertEquals(0, repository.countAll());
    repository.insert(example);
    pgDbDtoExample = repository.findOne(DbQuery.idEquals(example.getGuid()));
    var dbQueryId = DbQuery.idEquals(example.getGuid());
    Assertions.assertEquals(1, repository.update(PostgreSqlHelper.update(dbUpdate, dbQueryId),
        PostgreSqlHelper.getUpdateParamsAsArray(dbUpdate, dbQueryId)));
    assertEquals(1, deleteFromNativeQuery(dbQueryId));
    assertEquals(0, repository.countAll());
  }

  @Transactional
  protected int deleteFromNativeQuery(final DbQuery dbQuery) {
    if (dbQuery.isEmpty()) {
      return repository.getEntityManager().createNativeQuery(PostgreSqlHelper.delete(repository.getTable(), dbQuery))
          .executeUpdate();
    }
    return repository.getEntityManager().createNativeQuery(PostgreSqlHelper.delete(repository.getTable(), dbQuery))
        .setParameter(1, dbQuery.getSqlParams().get(0)).executeUpdate();
  }

  /**
   * Example of usage of Stream within a Transactional method
   *
   * @param dbQuery the dbQuery
   */
  @Transactional
  void streamCommand(final DbQuery dbQuery) throws CcsDbException {
    final var stream = repository.findStream(dbQuery);
    final var optionalPgDbDtoExample = stream.findFirst();
    assertTrue(optionalPgDbDtoExample.isPresent());
    final var pgDbDtoExample = optionalPgDbDtoExample.get();
    assertNotNull(pgDbDtoExample);
    assertEquals(1, repository.findQuery(dbQuery).list().size());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 30, 50, 100, 500})
  void benchInsertBulk(int bulkSize) throws CcsDbException {
    LOG.infof("Bulk %d", bulkSize);
    repository.changeBulkSize(bulkSize);
    dbInsertBulk();
  }

  void dbInsertBulk() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertFalse(DbType.getInstance().isMongoDbType());
    assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    final var start = System.nanoTime();
    for (var i = 0; i < MAX_BULK; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      repository.insert(pgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    final var start2 = System.nanoTime();
    insertBulk(dtoExample);
    final var stop2 = System.nanoTime();
    LOG.info("Standard Insert: " + (stop - start) / 1000000);
    LOG.info("Bulk Insert: " + (stop2 - start2) / 1000000);
    final var start3 = System.nanoTime();
    streamAllCommand(new DbQuery());
    final var stop3 = System.nanoTime();
    final var start4 = System.nanoTime();
    countAll();
    final var stop4 = System.nanoTime();

    LOG.info("Stream all: " + (stop3 - start3) / 1000000 + " vs " + (stop4 - start4) / 1000000);
    repository.deleteAllDb();
    if (repository.getBulkSize() > 1) {
      assertTrue(stop - start >= stop2 - start2);
    }
  }

  @Transactional
  void insertBulk(final DtoExample dtoExample) throws CcsDbException {
    for (var i = 0; i < MAX_BULK; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(pgDbDtoExample);
    }
    repository.flushAll();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 30, 50, 100, 500})
  void benchUpdateBulk(int bulkSize) throws CcsDbException {
    LOG.infof("Bulk %d", bulkSize);
    repository.changeBulkSize(bulkSize);
    dbUpsertBulk();
  }

  void dbUpsertBulk() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    List<PgDaoExample> example2List = new ArrayList<>(MAX_BULK);
    insertBulkForUpdate(dtoExample, example2List);
    final var start = System.nanoTime();
    for (final var pgDaoExample : example2List) {
      pgDaoExample.setField1("value2");
      repository.updateFull(pgDaoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    final var start2 = System.nanoTime();
    updateBulk(example2List);
    final var stop2 = System.nanoTime();

    LOG.info("Standard Update: " + (stop - start) / 1000000);
    LOG.info("Bulk Update: " + (stop2 - start2) / 1000000);
    repository.deleteAllDb();
    repository.flushAll();

    final var start3 = System.nanoTime();
    upsertBulk(example2List);
    final var stop3 = System.nanoTime();

    LOG.info("Bulk Update Insert: " + (stop3 - start3) / 1000000);
    repository.deleteAllDb();

    if (repository.getBulkSize() > 1) {
      assertTrue(stop - start >= stop2 - start2);
    }
  }

  @Transactional
  void upsertBulk(final List<PgDaoExample> example2List) throws CcsDbException {
    for (final var pgDaoExample : example2List) {
      pgDaoExample.setField1("value3");
      repository.addToUpdateBulk(pgDaoExample);
    }
    repository.flushAll();
  }

  @Transactional
  void updateBulk(final List<PgDaoExample> example2List) throws CcsDbException {
    for (final var pgDaoExample : example2List) {
      pgDaoExample.setField1("value2");
      repository.addToUpdateBulk(pgDaoExample);
    }
    repository.flushAll();
  }

  @Transactional
  void insertBulkForUpdate(final DtoExample dtoExample, final List<PgDaoExample> example2List) throws CcsDbException {
    for (var i = 0; i < MAX_BULK; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(pgDbDtoExample);
      example2List.add(pgDbDtoExample);
    }
    repository.flushAll();
  }

  void countAll() {
    final var count = repository.findAll().stream().count();
  }

  @Transactional
  void streamAllCommand(final DbQuery dbQuery) throws CcsDbException {
    final var countReal = repository.count(dbQuery);
    final var stream = repository.findStream(dbQuery);
    final var count = stream.count();
    assertEquals(count, countReal);
  }
}
