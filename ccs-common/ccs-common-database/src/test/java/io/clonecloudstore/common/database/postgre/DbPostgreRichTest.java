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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.database.postgre.impl.rich.PgDaoRichExample;
import io.clonecloudstore.common.database.postgre.impl.rich.PgDaoRichExampleRepository;
import io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository;
import io.clonecloudstore.common.database.postgre.model.rich.DtoRichExample;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.test.resource.postgres.PostgresProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID_PG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(PostgresProfile.class)
class DbPostgreRichTest {
  private static final Logger LOG = Logger.getLogger(DbPostgreRichTest.class);
  @Inject
  PgDaoRichExampleRepository repository;

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    Assertions.assertEquals(DaoRichExampleRepository.TABLE_NAME, repository.getTable());
    assertEquals(ID_PG, repository.getPkName());
    assertTrue(repository.isSqlRepository());
    assertEquals(0, repository.countAll());
    final var opId = GuidLike.getGuid();
    final Map<String, String> map = new HashMap<>();
    map.put("key1", "val1");
    map.put("key2", "val2");
    final Set<String> set = new HashSet<>();
    set.add("setval1");
    set.add("setval2");
    final var dtoExample = new DtoRichExample().setGuid(opId).setField1("field1").setMap1(map).setSet1(set);
    repository.insert(new PgDaoRichExample(dtoExample));
    repository.flushAll();
    assertEquals(1, repository.countAll());
    var pgDbDtoExample = repository.findUsingStream(new DbQuery());
    Assertions.assertEquals(opId, pgDbDtoExample.getGuid());
    Assertions.assertEquals(map, pgDbDtoExample.getMap1());
    Assertions.assertEquals(set, pgDbDtoExample.getSet1());
    streamCommand(new DbQuery());
    pgDbDtoExample = repository.findWithPk(opId);
    Assertions.assertEquals(opId, pgDbDtoExample.getGuid());
    final var dbQuery = new DbQuery(RestQuery.QUERY.EQ, ID_PG, opId);
    assertEquals(1, repository.count(dbQuery));
    final var dbUpdate = new DbUpdate().addToSet(DaoRichExampleRepository.ARRAY1, "setval3");
    assertEquals(1, repository.update(dbQuery, dbUpdate));
    pgDbDtoExample = repository.findOne(dbQuery);
    Assertions.assertEquals(opId, pgDbDtoExample.getGuid());
    Assertions.assertTrue(pgDbDtoExample.getSet1().contains("setval3"));
    final var dbUpdate2 = new DbUpdate().setArray(DaoRichExampleRepository.ARRAY1, "setval4", set);
    assertEquals(1, repository.update(dbQuery, dbUpdate2));
    pgDbDtoExample = repository.findOne(dbQuery);
    Assertions.assertEquals(opId, pgDbDtoExample.getGuid());
    Assertions.assertTrue(pgDbDtoExample.getSet1().contains("setval4"));
    pgDbDtoExample.addMapValue("key3", "val3");
    assertTrue(repository.updateFull(pgDbDtoExample));
    pgDbDtoExample = repository.find(PostgreSqlHelper.query(dbQuery), dbQuery.getSqlParamsAsArray()).firstResult();
    Assertions.assertEquals(opId, pgDbDtoExample.getGuid());
    Assertions.assertEquals("val3", pgDbDtoExample.getMapValue("key3"));
    repository.deleteWithPk(opId);
    assertEquals(0, repository.countAll());
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    repository.insert(example);
    final var json = JsonUtil.getInstance().writeValueAsString(dtoExample);
    final var dtoExample1 = JsonUtil.getInstance().readValue(json, DtoRichExample.class);
    assertEquals(dtoExample, dtoExample1);
    assertEquals(1, repository.countAll());
    assertEquals(1, repository.delete(dbQuery));
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
}
