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
import io.clonecloudstore.common.database.model.dao.DaoExampleRepository;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.database.postgre.impl.simple.PgDaoExample;
import io.clonecloudstore.common.database.postgre.impl.simple.PgDaoExampleRepository;
import io.clonecloudstore.common.database.postgre.impl.simple.PgTestStream;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.resource.postgres.PostgresProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID_PG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(PostgresProfile.class)
class DbPostgreInjectTest {
  private static final Logger LOG = Logger.getLogger(DbPostgreInjectTest.class);
  @Inject
  Instance<DaoExampleRepository> repositoryInstance;
  DaoExampleRepository repository;
  @Inject
  PgTestStream testStream;

  @BeforeEach
  void setup() {
    repository = repositoryInstance.get();
  }

  @Test
  void checkDbIterator() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertTrue(repository.isSqlRepository());
    assertEquals(0, repository.countAll());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    final var dtoExample2 =
        new DtoExample().setGuid(GuidLike.getGuid()).setField1("field2").setField2("field3").setTimeField(instant);
    repository.insert(new PgDaoExample(dtoExample));
    var pgDbDtoExample = repository.findOne(new DbQuery());
    assertEquals(opId, pgDbDtoExample.getGuid());
    repository.insert(new PgDaoExample(dtoExample2));
    repository.flushAll();
    assertEquals(2, repository.countAll());
    pgDbDtoExample = repository.findUsingStream(DbQuery.idEquals(opId));
    assertEquals(opId, pgDbDtoExample.getGuid());
    assertEquals(SystemTools.toMillis(instant), pgDbDtoExample.getTimeField());
    streamCommand(new DbQuery());
    LOG.infof("Test transaction");
    assertTrue(manualTransaction());
    LOG.infof("Test with Annotation Transaction");
    assertTrue(annotationTransaction());
    LOG.infof("Test with Annotation Transaction and Iterator");
    assertTrue(annotationTransactionIterator());
    LOG.infof("Test with Annotation Transaction and Iterator with error but suspended");
    assertTrue(annotationTransactionIteratorDbException());
    LOG.infof("Test transaction with error");
    assertFalse(manualTransactionWithGlobalRollback());
    LOG.infof("Test transaction with error but suspended");
    assertTrue(manualTransactionWithRollbackOnlyOnSubMethod());
    LOG.infof("Test transaction Iterator");
    assertTrue(manualTransactionIterator());
    LOG.infof("Test transaction iterator global rollback");
    assertFalse(manualTransactionIteratorWithGlobalRollback());
    LOG.infof("Test transaction iterator with error but suspended");
    assertTrue(manualTransactionIteratorWithRollbackOnlyOnSubMethod());
    pgDbDtoExample = repository.findWithPk(opId);
    assertEquals(opId, pgDbDtoExample.getGuid());
    final var dbQuery = DbQuery.idEquals(opId);
    assertEquals(1, repository.count(dbQuery));
    assertEquals(2, repository.deleteAllDb());
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
    stream.close();
    assertEquals(2, repository.findStream(dbQuery).toList().size());
  }

  @Transactional
  boolean annotationTransaction() {
    try {
      final var stream = testStream.getStream();
      testStream.updateEach(stream);
      stream.close();
      return true;
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    }
    return false;
  }

  @Transactional
  boolean annotationTransactionIterator() {
    try {
      final var iterator = testStream.getIterator();
      while (testStream.suspendTransaction(iterator)) {
        // Nothing
      }
      return true;
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    }
    return false;
  }

  @Transactional
  boolean annotationTransactionIteratorDbException() {
    try {
      final var iterator = testStream.getIterator();
      while (testStream.suspendTransactionWithDbException(iterator)) {
        // Nothing
      }
      iterator.close();
      return true;
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    }
    return false;
  }

  boolean manualTransaction() {
    try {
      testStream.createTransaction();
      final var stream = testStream.getStream();
      testStream.updateEach(stream);
      stream.close();
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    } finally {
      return testStream.finalizeTransaction();
    }
  }

  boolean manualTransactionWithGlobalRollback() {
    try {
      testStream.createTransaction();
      final var stream = testStream.getStream();
      testStream.updateEach(stream);
      testStream.simulateDbException();
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    } finally {
      return testStream.finalizeTransaction();
    }
  }

  boolean manualTransactionWithRollbackOnlyOnSubMethod() {
    try {
      testStream.createTransaction();
      final var stream = testStream.getStream();
      testStream.suspendTransactionWithDbException();
      testStream.updateEach(stream);
      stream.close();
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    } finally {
      return testStream.finalizeTransaction();
    }
  }

  boolean manualTransactionIterator() {
    try {
      testStream.createTransaction();
      final var iterator = testStream.getIterator();
      testStream.updateEach(iterator);
      iterator.close();
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    } finally {
      return testStream.finalizeTransaction();
    }
  }

  boolean manualTransactionIteratorWithGlobalRollback() {
    try {
      testStream.createTransaction();
      final var iterator = testStream.getIterator();
      testStream.updateEach(iterator);
      testStream.simulateDbException();
      iterator.close();
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    } finally {
      return testStream.finalizeTransaction();
    }
  }

  boolean manualTransactionIteratorWithRollbackOnlyOnSubMethod() {
    try {
      testStream.createTransaction();
      final var iterator = testStream.getIterator();
      testStream.suspendTransactionWithDbException();
      testStream.suspendTransaction(iterator);
      iterator.close();
    } catch (final CcsDbException e) {
      LOG.infof(e.getMessage());
    } finally {
      return testStream.finalizeTransaction();
    }
  }

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
    pgDbDtoExample = repository.findWithPk(opId);
    assertEquals(opId, pgDbDtoExample.getGuid());
    final var dbQuery = DbQuery.idEquals(opId);
    assertEquals(1, repository.count(dbQuery));
    var dbUpdate = new DbUpdate().set(FIELD2, "value2");
    assertEquals(1, repository.update(dbQuery, dbUpdate));
    pgDbDtoExample = repository.findOne(dbQuery);
    assertEquals(opId, pgDbDtoExample.getGuid());
    assertEquals("value2", pgDbDtoExample.getField2());
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
  }

  @Test
  void dbBulk() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    final var start = System.nanoTime();
    for (var i = 0; i < 1500; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      repository.insert(pgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    repository.delete(new DbQuery());
    final var start2 = System.nanoTime();
    bulkInsert(dtoExample);
    final var stop2 = System.nanoTime();
    final var start2B = System.nanoTime();
    bulkUpdate(dtoExample);
    final var stop2B = System.nanoTime();
    LOG.info("Standard Insert: " + (stop - start) / 1000000);
    LOG.info("Bulk Insert: " + (stop2 - start2) / 1000000);
    LOG.info("Bulk Update: " + (stop2B - start2B) / 1000000);
    final var start3 = System.nanoTime();
    streamAllCommand(new DbQuery());
    final var stop3 = System.nanoTime();
    final var start4 = System.nanoTime();
    countAll();
    final var stop4 = System.nanoTime();

    LOG.info("Stream all: " + (stop3 - start3) / 1000000 + " vs " + (stop4 - start4) / 1000000);
    repository.deleteAllDb();
  }

  @Transactional
  void bulkInsert(final DtoExample dtoExample) throws CcsDbException {
    for (var i = 0; i < 1500; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(pgDbDtoExample);
    }
    repository.flushAll();
  }

  @Transactional
  void bulkUpdate(final DtoExample dtoExample) throws CcsDbException {
    for (var i = 0; i < 1500; i++) {
      final var pgDbDtoExample = new PgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      ((PgDaoExampleRepository) repository).addToUpdateBulk(pgDbDtoExample);
    }
    repository.flushAll();
  }

  void countAll() throws CcsDbException {
    final var count = repository.countAll();
  }

  @Transactional
  void streamAllCommand(final DbQuery dbQuery) throws CcsDbException {
    final var countReal = repository.count(dbQuery);
    final var stream = repository.findStream(dbQuery);
    final var count = stream.count();
    assertEquals(count, countReal);
  }
}
