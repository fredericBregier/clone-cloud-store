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
import io.clonecloudstore.common.database.model.dao.DaoExample2Repository;
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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
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
class DbMongoNoCodecInjectTest {
  private static final Logger LOG = Logger.getLogger(DbMongoNoCodecInjectTest.class);
  @Inject
  Instance<DaoExample2Repository> repositoryInstance;
  DaoExample2Repository repository;

  @BeforeEach
  void beforeEach() throws CcsDbException {
    repository = repositoryInstance.get();
  }

  @Test
  void checkDb() throws JsonProcessingException, CcsDbException {
    assertEquals("tbl_ex2", repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertFalse(repository.isSqlRepository());
    ((MgDaoExample2Repository) repository).createIndex();
    assertEquals(0, repository.countAll());
    final var instant = Instant.now();
    final var opId = GuidLike.getGuid();
    final var dtoExample = new DtoExample().setGuid(opId).setField1("field1").setField2("field2").setTimeField(instant);
    repository.insert(new MgDaoExample2(dtoExample));
    repository.flushAll();
    assertEquals(1, repository.countAll());
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
    assertEquals(0, repository.countAll());
    final var example = repository.createEmptyItem();
    example.fromDto(dtoExample);
    repository.insert(example);
    final var json = JsonUtil.getInstance().writeValueAsString(dtoExample);
    final var dtoExample1 = JsonUtil.getInstance().readValue(json, DtoExample.class);
    assertEquals(dtoExample, dtoExample1);
    assertEquals(1, repository.countAll());
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
  void dbInsertBulk() throws CcsDbException {
    assertEquals("tbl_ex2", repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    final var start = System.nanoTime();
    for (var i = 0; i < 1500; i++) {
      final var mgDbDtoExample = new MgDaoExample2(dtoExample).setGuid(GuidLike.getGuid());
      repository.insert(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    final var start2 = System.nanoTime();
    for (var i = 0; i < 1500; i++) {
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

  @Test
  void dbUpsertBulk() throws CcsDbException {
    assertEquals("tbl_ex2", repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    List<MgDaoExample2> example2List = new ArrayList<>(1500);
    final var start0 = System.nanoTime();
    for (var i = 0; i < 1500; i++) {
      final var mgDbDtoExample = new MgDaoExample2(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(mgDbDtoExample);
      example2List.add(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop0 = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("field1", daoExample2.getField1());
        });
    LOG.info("Bulk Insert: " + (stop0 - start0) / 1000000);
    final var start = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      mgDbDtoExample.setField1("value2");
      repository.updateFull(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value2", daoExample2.getField1());
        });
    LOG.info("Standard Update: " + (stop - start) / 1000000);
    final var start2 = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      final var guid = mgDbDtoExample.getGuid();
      mgDbDtoExample.setField1("value3");
      ((MgDaoExample2Repository) repository).addToUpsertBulk(new Document("_id", guid), mgDbDtoExample);
    }
    repository.flushAll();
    final var stop2 = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value3", daoExample2.getField1());
        });
    LOG.info("Bulk Update: " + (stop2 - start2) / 1000000);
    final var start2A = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      final var guid = mgDbDtoExample.getGuid();
      mgDbDtoExample.setField1("value0");
      final var update = getDocumentFromObject(mgDbDtoExample);
      var document = new Document("$set", update);
      ((MgDaoExample2Repository) repository).addToUpdateBulk(new Document("_id", guid), document);
    }
    repository.flushAll();
    final var stop2A = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value0", daoExample2.getField1());
        });

    LOG.info("Bulk UpdateBson: " + (stop2A - start2A) / 1000000);

    final var start2B = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      final var guid = mgDbDtoExample.getGuid();
      mgDbDtoExample.setField1("value4");
      ((MgDaoExample2Repository) repository).addToUpsertBulk(new Document("_id", guid), mgDbDtoExample);
    }
    repository.flushAll();
    final var stop2B = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value4", daoExample2.getField1());
        });

    LOG.info("Bulk Upsert: " + (stop2B - start2B) / 1000000);

    var firstGuid = example2List.getFirst().getGuid();
    final var start3 = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      final var guid = GuidLike.getGuid();
      mgDbDtoExample.setField1("value5");
      mgDbDtoExample.setGuid(guid);
      ((MgDaoExample2Repository) repository).addToUpsertBulk(new Document("_id", guid), mgDbDtoExample);
    }
    repository.flushAll();
    final var stop3 = System.nanoTime();
    var temp2 = repository.findOne(DbQuery.idEquals(firstGuid));
    assertEquals("value4", temp2.getField1());
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value5", daoExample2.getField1());
        });

    LOG.info("Bulk Update Insert: " + (stop3 - start3) / 1000000);

    repository.deleteAllDb();
    repository.flushAll();

    final var start4 = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      final var guid = GuidLike.getGuid();
      mgDbDtoExample.setField1("value6");
      mgDbDtoExample.setGuid(guid);
      ((MgDaoExample2Repository) repository).addToUpsertBulk(new Document("_id", guid), mgDbDtoExample);
    }
    repository.flushAll();
    final var stop4 = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value6", daoExample2.getField1());
        });
    LOG.info("Bulk Update Insert2: " + (stop4 - start4) / 1000000);
    repository.deleteAllDb();

    assertTrue(stop - start > stop2 - start2);
  }

  private Document getDocumentFromObject(final MgDaoExample2 update) {
    BsonDocument unwrapped = new BsonDocument();
    BsonWriter jsonWriter = new BsonDocumentWriter(unwrapped);
    final var encoder =
        (Codec<MgDaoExample2>) ((MgDaoExample2Repository) repository).mongoCollection().getCodecRegistry()
            .get(update.getClass());
    encoder.encode(jsonWriter, update, EncoderContext.builder().build());
    final var json = unwrapped.toJson();
    return Document.parse(json);
  }

  @Test
  void dbUpdateBsonBulk() throws CcsDbException {
    assertEquals("tbl_ex2", repository.getTable());
    assertEquals(ID, repository.getPkName());
    assertEquals(0, repository.countAll());
    final var dtoExample = new DtoExample();
    dtoExample.setField1("field1").setField2("field2").setTimeField(Instant.now());
    List<MgDaoExample2> example2List = new ArrayList<>(1500);
    for (var i = 0; i < 1500; i++) {
      final var mgDbDtoExample = new MgDaoExample2(dtoExample).setGuid(GuidLike.getGuid());
      repository.addToInsertBulk(mgDbDtoExample);
      example2List.add(mgDbDtoExample);
    }
    repository.flushAll();
    final var start = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      mgDbDtoExample.setField1("value2");
      repository.updateFull(mgDbDtoExample);
    }
    repository.flushAll();
    final var stop = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value2", daoExample2.getField1());
        });
    LOG.info("Standard Update: " + (stop - start) / 1000000);
    final var start2 = System.nanoTime();
    for (final var mgDbDtoExample : example2List) {
      final var guid = mgDbDtoExample.getGuid();
      mgDbDtoExample.setField1("value3");
      final var update = getDocumentFromObject(mgDbDtoExample);
      var document = new Document("$set", update);
      ((MgDaoExample2Repository) repository).addToUpdateBulk(new Document("_id", guid), document);
    }
    repository.flushAll();
    final var stop2 = System.nanoTime();
    repository.findStream(
            new DbQuery(RestQuery.QUERY.IN, ID, example2List.stream().map(MgDaoExample2::getGuid).toList()))
        .forEach(daoExample2 -> {
          assertEquals("value3", daoExample2.getField1());
        });

    LOG.info("Bulk Update Bson: " + (stop2 - start2) / 1000000);
    repository.deleteAllDb();
    repository.flushAll();
    repository.deleteAllDb();

    assertTrue(stop - start > stop2 - start2);
  }
}
