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
import io.clonecloudstore.common.database.mongo.impl.nocodec.MgDaoExample2;
import io.clonecloudstore.common.database.mongo.impl.nocodec.MgDaoExample2Repository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test without a Codec
 */
@QuarkusTest
@TestProfile(MongoDbProfile.class)
class DbMongoNoCodecTest {
  private static final Logger LOG = Logger.getLogger(DbMongoNoCodecTest.class);
  @Inject
  MgDaoExample2Repository repository;

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    assertEquals("tbl_ex2", repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertFalse(repository.isSqlRepository());
    repository.createIndex();
    assertEquals(0, repository.count());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    repository.insert(new MgDaoExample2(dtoExample));
    repository.flushAll();
    assertEquals(1, repository.count());
    var mgDbDtoExample = repository.findUsingStream(new DbQuery());
    assertEquals(opId, mgDbDtoExample.getGuid());
    assertEquals(SystemTools.toMillis(instant), mgDbDtoExample.getTimeField());
    streamCommand(new DbQuery());
    mgDbDtoExample = repository.findWithPk(opId);
    assertEquals(opId, mgDbDtoExample.getGuid());
    final var dbQuery = new DbQuery(RestQuery.QUERY.EQ, ID, opId);
    assertEquals(1, repository.count(dbQuery));
    final var dbUpdate = new DbUpdate().set(FIELD2, "value2");
    assertEquals(1, repository.update(dbQuery, dbUpdate));
    mgDbDtoExample = repository.findOne(dbQuery);
    assertEquals(opId, mgDbDtoExample.getGuid());
    assertEquals("value2", mgDbDtoExample.getField2());
    repository.deleteWithPk(opId);
    assertEquals(0, repository.count());
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    repository.insert(example);
    final var json = JsonUtil.getInstance().writeValueAsString(dtoExample);
    final var dtoExample1 = JsonUtil.getInstance().readValue(json, DtoExample.class);
    assertEquals(dtoExample, dtoExample1);
    assertEquals(1, repository.count());
    assertEquals(1, repository.delete(dbQuery));
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
    assertTrue(optionalMgDbDtoExample.isPresent());
    final var mgDbDtoExample = optionalMgDbDtoExample.get();
    assertNotNull(mgDbDtoExample);
  }

  @Test
  void dbBulk() throws CcsDbException {
    assertEquals("tbl_ex2", repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertEquals(0, repository.count());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    final var start = System.nanoTime();
    for (var i = 0; i < 100; i++) {
      final var mgDbDtoExample = new MgDaoExample2(dtoExample).setGuid(GuidLike.getGuid());
      repository.insert(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    final var start2 = System.nanoTime();
    for (var i = 0; i < 100; i++) {
      final var mgDbDtoExample = new MgDaoExample2(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop2 = System.nanoTime();

    LOG.info("Standard Insert: " + (stop - start) / 1000000);
    LOG.info("Bulk Insert: " + (stop2 - start2) / 1000000);
    repository.deleteAllDb();
    assertTrue(stop - start > stop2 - start2);
  }
}
