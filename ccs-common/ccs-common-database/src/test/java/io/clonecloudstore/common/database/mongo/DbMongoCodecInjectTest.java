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

package io.clonecloudstore.common.database.mongo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.database.model.dao.DaoExampleRepository;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.database.mongo.impl.codec.MgDaoExample;
import io.clonecloudstore.common.database.mongo.impl.codec.MgDaoExampleRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.ID;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test with a Codec
 */
@QuarkusTest
@TestProfile(MongoDbProfile.class)
class DbMongoCodecInjectTest {
  private static final Logger LOG = Logger.getLogger(DbMongoCodecInjectTest.class);
  @Inject
  Instance<DaoExampleRepository> repositoryInstance;
  DaoExampleRepository repository;

  @BeforeEach
  void beforeEach() throws CcsDbException {
    repository = repositoryInstance.get();
    repository.deleteAllDb();
  }

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID, repository.getPkName());
    Assertions.assertFalse(repository.isSqlRepository());
    ((MgDaoExampleRepository) repository).createIndex();
    Assertions.assertEquals(0, repository.countAll());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    repository.insert(new MgDaoExample(dtoExample));
    repository.flushAll();
    Assertions.assertEquals(1, repository.countAll());
    var mgDbDtoExample = repository.findUsingStream(new DbQuery());
    assertEquals(opId, mgDbDtoExample.getGuid());
    assertEquals(SystemTools.toMillis(instant), mgDbDtoExample.getTimeField());
    streamCommand(new DbQuery());
    mgDbDtoExample = repository.findWithPk(opId);
    assertEquals(opId, mgDbDtoExample.getGuid());
    final var dbQuery = DbQuery.idEquals(opId);
    Assertions.assertEquals(1, repository.count(dbQuery));
    final var dbUpdate = new DbUpdate().set(FIELD2, "value2");
    Assertions.assertEquals(1, repository.update(dbQuery, dbUpdate));
    mgDbDtoExample = repository.findOne(dbQuery);
    assertEquals(opId, mgDbDtoExample.getGuid());
    assertEquals("value2", mgDbDtoExample.getField2());
    mgDbDtoExample.setField1("newVal");
    repository.updateFull(mgDbDtoExample);
    mgDbDtoExample = repository.findOne(dbQuery);
    assertEquals("newVal", mgDbDtoExample.getField1());
    repository.deleteWithPk(opId);
    Assertions.assertEquals(0, repository.countAll());
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    repository.insert(example);
    final var json = JsonUtil.getInstance().writeValueAsString(dtoExample);
    final var dtoExample1 = JsonUtil.getInstance().readValue(json, DtoExample.class);
    assertEquals(dtoExample, dtoExample1);
    Assertions.assertEquals(1, repository.countAll());
    try (final var cursor = repository.findIterator(dbQuery)) {
      var cpt = SystemTools.consumeAll(cursor);
      assertEquals(1, cpt);
    }

    final var findIterable = repository.findIterator(dbQuery);
    mgDbDtoExample = findIterable.hasNext() ? findIterable.next() : null;
    assertNotNull(mgDbDtoExample);
    findIterable.close();

    final var list = repository.findStream(dbQuery).toList();
    Assertions.assertEquals(1, list.size());
    Assertions.assertEquals(1, repository.delete(dbQuery));
  }

  /**
   * Example of usage of Stream
   *
   * @param dbQuery the dbQuery
   */
  //No @Transactional needed
  void streamCommand(final DbQuery dbQuery) throws CcsDbException {
    final var stream = repository.findStream(dbQuery);
    final var optionalMgDbDtoExample = stream.findFirst();
    Assertions.assertTrue(optionalMgDbDtoExample.isPresent());
    final var mgDbDtoExample = optionalMgDbDtoExample.get();
    assertNotNull(mgDbDtoExample);
  }

  @Test
  void dbBulk() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID, repository.getPkName());
    Assertions.assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    List<MgDaoExample> list = new ArrayList<>(1500);
    for (var i = 0; i < 1500; i++) {
      final var mgDbDtoExample = new MgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      list.add(mgDbDtoExample);
    }
    final var start = System.nanoTime();
    for (final var mgDbDtoExample : list) {
      repository.insert(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    repository.delete(new DbQuery());
    repository.flushAll();
    final var start2 = System.nanoTime();
    for (final var mgDbDtoExample : list) {
      repository.addToInsertBulk(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop2 = System.nanoTime();

    LOG.info("Standard Insert: " + (stop - start) / 1000000);
    LOG.info("Bulk Insert: " + (stop2 - start2) / 1000000);

    final var start3 = System.nanoTime();
    streamCommandAll(new DbQuery());
    final var stop3 = System.nanoTime();

    LOG.info("Stream all: " + (stop3 - start3) / 1000000);

    final var start4 = System.nanoTime();
    repository.update(new DbQuery(), new DbUpdate().set(DaoExampleRepository.FIELD1, "newvalue"));
    final var stop4 = System.nanoTime();

    LOG.info("Update all: " + (stop4 - start4) / 1000000);
    repository.deleteAllDb();
    assertTrue(stop - start > stop2 - start2);
  }

  //No @Transactional needed
  void streamCommandAll(final DbQuery dbQuery) throws CcsDbException {
    final var stream = repository.findStream(dbQuery);
    final var count = stream.count();
    Assertions.assertEquals(count, repository.count(dbQuery));
  }
}
