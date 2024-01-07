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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.database.mongo.impl.codec.MgDaoExample;
import io.clonecloudstore.common.database.mongo.impl.codec.MgDaoExampleRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.ID;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test with a Codec
 */
@QuarkusTest
@TestProfile(NoMongoDbProfile.class)
class DbMongoCodecNoDbTest {
  private static final Logger LOG = Logger.getLogger(DbMongoCodecNoDbTest.class);
  @Inject
  MgDaoExampleRepository repository;

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID, repository.getPkName());
    Assertions.assertFalse(repository.isSqlRepository());
    assertThrows(CcsDbException.class, () -> repository.createIndex());
    assertThrows(CcsDbException.class, () -> repository.countAll());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    assertThrows(CcsDbException.class, () -> repository.insert(new MgDaoExample(dtoExample)));
    assertThrows(CcsDbException.class, () -> repository.flushAll());
    assertThrows(CcsDbException.class, () -> repository.findUsingStream(new DbQuery()));
    streamCommand(new DbQuery());
    assertThrows(CcsDbException.class, () -> repository.findWithPk(opId));
    final var dbQuery = DbQuery.idEquals(opId);
    final var dbUpdate = new DbUpdate().set(FIELD2, "value2");
    assertThrows(CcsDbException.class, () -> repository.update(dbQuery, dbUpdate));
    assertThrows(CcsDbException.class, () -> repository.findOne(dbQuery));
    final var mgDbDtoExample = new MgDaoExample();
    assertThrows(CcsDbException.class, () -> repository.updateFull(mgDbDtoExample));
    assertThrows(CcsDbException.class, () -> repository.deleteWithPk(opId));
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    assertThrows(CcsDbException.class, () -> repository.insert(example));
    assertThrows(CcsDbException.class, () -> repository.findIterator(dbQuery));
    repository.find(MongoSqlHelper.query(dbQuery), dbQuery.getSqlParamsAsArray());
    assertThrows(CcsDbException.class, () -> repository.delete(dbQuery));
    assertThrows(CcsDbException.class, () -> repository.deleteAllDb());
  }

  /**
   * Example of usage of Stream
   *
   * @param dbQuery the dbQuery
   */
  //No @Transactional needed
  void streamCommand(final DbQuery dbQuery) throws CcsDbException {
    assertThrows(CcsDbException.class, () -> repository.findStream(dbQuery));
  }

  @Test
  void dbBulk() throws CcsDbException {
    assertEquals(TABLE_NAME, repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertThrows(CcsDbException.class, () -> repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    final var start = System.nanoTime();
    for (var i = 0; i < 10; i++) {
      final var mgDbDtoExample = new MgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      assertThrows(CcsDbException.class, () -> repository.insert(mgDbDtoExample));
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    final var start2 = System.nanoTime();
    for (var i = 0; i < 10; i++) {
      final var mgDbDtoExample = new MgDaoExample(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(mgDbDtoExample);
    }
    assertThrows(CcsDbException.class, () -> repository.flushAll());
    final var stop2 = System.nanoTime();

    LOG.info("Standard Insert: " + (stop - start) / 1000000);
    LOG.info("Bulk Insert: " + (stop2 - start2) / 1000000);
    assertTrue(stop - start > stop2 - start2);
  }
}
