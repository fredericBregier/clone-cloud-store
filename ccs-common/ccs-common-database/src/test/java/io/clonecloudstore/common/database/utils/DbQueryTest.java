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

package io.clonecloudstore.common.database.utils;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clonecloudstore.common.database.utils.RestQuery.CONJUNCTION;
import io.clonecloudstore.common.database.utils.RestQuery.QUERY;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class DbQueryTest {
  static ObjectMapper objectMapper;

  @BeforeAll
  static void beforeAll() {
    objectMapper = JsonUtil.getInstance();
  }

  @Test
  void testPkId() {
    assertEquals(ID, DbQuery.idEquals("myvalue").getFieldName());
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void testEmptyCreationJson() throws JsonProcessingException {
    final var dbQuery = new DbQuery();
    final var json = objectMapper.writeValueAsString(dbQuery);
    final var parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    assertNull(parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertNull(parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
    Log.debug(parsed.getSqlQueryString().toString());
    assertTrue(parsed.getSqlQueryString().isEmpty());
    Log.debug(parsed.getBson().toBsonDocument().toJson());
    Log.debug(parsed.getMgQueryString().toString());
    assertNotNull(parsed.getBson());
    assertEquals("{}", parsed.getBson().toBsonDocument().toJson());
    assertTrue(parsed.getMgQueryString().isEmpty());
  }

  @Test
  void testQueryCreationJson() throws JsonProcessingException {
    var dbQuery = new DbQuery(QUERY.EQ, "field", "value");
    var json = objectMapper.writeValueAsString(dbQuery);
    var parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemString(parsed, QUERY.EQ);

    dbQuery = new DbQuery(QUERY.GTE, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.GTE);

    dbQuery = new DbQuery(QUERY.LTE, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    assertEquals(QUERY.LTE, parsed.getQUERY());
    testItemString(parsed, QUERY.LTE);

    dbQuery = new DbQuery(QUERY.NEQ, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.NEQ);

    dbQuery = new DbQuery(QUERY.START_WITH, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.START_WITH);

    dbQuery = new DbQuery(QUERY.JSON_EQ, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.JSON_EQ);

    dbQuery = new DbQuery(QUERY.JSON_NEQ, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.JSON_NEQ);

    final var object = Integer.valueOf(0);

    dbQuery = new DbQuery(QUERY.EQ, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemObject(parsed, QUERY.EQ, object);

    dbQuery = new DbQuery(QUERY.GTE, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.GTE, object);

    dbQuery = new DbQuery(QUERY.LTE, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.LTE, object);

    dbQuery = new DbQuery(QUERY.NEQ, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.NEQ, object);

    final List<String> values = new ArrayList<>();
    values.add("val1");
    values.add("val2");
    final var array = values.toArray(new String[0]);
    final List<Long> valuesLong = new ArrayList<>();
    valuesLong.add(1L);
    valuesLong.add(2L);
    final var arrayLong = valuesLong.toArray(new Long[0]);

    dbQuery = new DbQuery(QUERY.IN, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemList(parsed, QUERY.IN, values);

    dbQuery = new DbQuery(QUERY.CONTAINS, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINS, values);

    dbQuery = new DbQuery(QUERY.CONTAINED, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINED, values);

    dbQuery = new DbQuery(QUERY.ANY, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.ANY, values);

    dbQuery = new DbQuery(QUERY.IN, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemArray(parsed, QUERY.IN, array);

    dbQuery = new DbQuery(QUERY.CONTAINS, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINS, array);

    dbQuery = new DbQuery(QUERY.CONTAINED, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINED, array);

    dbQuery = new DbQuery(QUERY.ANY, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.ANY, array);

    dbQuery = new DbQuery(QUERY.IN, "field", valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemList(parsed, QUERY.IN, valuesLong);

    dbQuery = new DbQuery(QUERY.CONTAINS, "field", valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINS, valuesLong);

    dbQuery = new DbQuery(QUERY.CONTAINED, "field", valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINED, valuesLong);

    dbQuery = new DbQuery(QUERY.ANY, "field", valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.ANY, valuesLong);

    dbQuery = new DbQuery(QUERY.IN, "field", (Object[]) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemArray(parsed, QUERY.IN, arrayLong);

    dbQuery = new DbQuery(QUERY.CONTAINS, "field", (Object[]) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINS, arrayLong);

    dbQuery = new DbQuery(QUERY.CONTAINED, "field", (Object[]) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINED, arrayLong);

    dbQuery = new DbQuery(QUERY.ANY, "field", (Object[]) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.ANY, arrayLong);

    dbQuery = new DbQuery(QUERY.IN, "field", (Object) valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemList(parsed, QUERY.IN, valuesLong);

    dbQuery = new DbQuery(QUERY.CONTAINS, "field", (Object) valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINS, valuesLong);

    dbQuery = new DbQuery(QUERY.CONTAINED, "field", (Object) valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINED, valuesLong);

    dbQuery = new DbQuery(QUERY.ANY, "field", (Object) valuesLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.ANY, valuesLong);

    dbQuery = new DbQuery(QUERY.IN, "field", (Object) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testItemArray(parsed, QUERY.IN, arrayLong);

    dbQuery = new DbQuery(QUERY.CONTAINS, "field", (Object) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINS, arrayLong);

    dbQuery = new DbQuery(QUERY.CONTAINED, "field", (Object) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINED, arrayLong);

    dbQuery = new DbQuery(QUERY.ANY, "field", (Object) arrayLong);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.ANY, arrayLong);

    dbQuery = new DbQuery(QUERY.EQ, RepositoryBaseInterface.ID, "value");
    json = objectMapper.writeValueAsString(dbQuery);
    dbQuery = DbQuery.idEquals("value");
    assertEquals(json, objectMapper.writeValueAsString(dbQuery));
  }

  @Test
  void testQueryCreationError() {
    final var object = Integer.valueOf(0);
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.IN, "field", "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.CONTAINED, "field", "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.CONTAINS, "field", "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.ANY, "field", "value"));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.IN, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.CONTAINED, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.CONTAINS, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.ANY, "field", object));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.START_WITH, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_NEQ, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_EQ, "field", object));

    final List<String> values = new ArrayList<>();
    values.add("val1");
    values.add("val2");
    final var array = values.toArray(new String[0]);
    final List<Long> valuesLong = new ArrayList<>();
    valuesLong.add(1L);
    valuesLong.add(2L);
    final var arrayLong = valuesLong.toArray(new Long[0]);

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.EQ, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.NEQ, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.GTE, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.LTE, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.START_WITH, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_EQ, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_NEQ, "field", values));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.EQ, "field", valuesLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.NEQ, "field", valuesLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.GTE, "field", valuesLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.LTE, "field", valuesLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.START_WITH, "field", valuesLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_EQ, "field", valuesLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_NEQ, "field", valuesLong));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.EQ, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.NEQ, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.GTE, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.LTE, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new DbQuery(QUERY.START_WITH, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.JSON_EQ, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new DbQuery(QUERY.JSON_NEQ, "field", (Object[]) array));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.EQ, "field", (Object[]) arrayLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.NEQ, "field", (Object[]) arrayLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.GTE, "field", (Object[]) arrayLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new DbQuery(QUERY.LTE, "field", (Object[]) arrayLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new DbQuery(QUERY.START_WITH, "field", (Object[]) arrayLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new DbQuery(QUERY.JSON_EQ, "field", (Object[]) arrayLong));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new DbQuery(QUERY.JSON_NEQ, "field", (Object[]) arrayLong));
  }

  private void testItemString(final DbQuery parsed, final QUERY query) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertEquals("value", parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
    Log.debug(parsed.getSqlQueryString().toString());
    assertFalse(parsed.getSqlQueryString().isEmpty());
    Log.debug(parsed.getBson().toBsonDocument().toJson());
    Log.debug(parsed.getMgQueryString().toString());
    assertNotNull(parsed.getBson());
    assertNotEquals("{}", parsed.getBson().toBsonDocument().toJson());
    assertFalse(parsed.getMgQueryString().isEmpty());
  }

  private void testItemObject(final DbQuery parsed, final QUERY query, final Object object) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertEquals(object, parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
    Log.debug(parsed.getSqlQueryString().toString());
    assertFalse(parsed.getSqlQueryString().isEmpty());
    Log.debug(parsed.getBson().toBsonDocument().toJson());
    Log.debug(parsed.getMgQueryString().toString());
    assertNotNull(parsed.getBson());
    assertNotEquals("{}", parsed.getBson().toBsonDocument().toJson());
    assertFalse(parsed.getMgQueryString().isEmpty());
  }

  private void testItemList(final DbQuery parsed, final QUERY query, final List<?> values) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getSvalue());
    final var array = parsed.getValueCollection().toArray();
    for (int i = 0; i < values.size(); i++) {
      assertEquals(values.get(i).toString(), array[i].toString());
    }
    assertEquals(2, parsed.getValueCollection().size());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
    Log.debug(parsed.getSqlQueryString().toString());
    assertFalse(parsed.getSqlQueryString().isEmpty());
    Log.debug(parsed.getBson().toBsonDocument().toJson());
    Log.debug(parsed.getMgQueryString().toString());
    assertNotNull(parsed.getBson());
    assertNotEquals("{}", parsed.getBson().toBsonDocument().toJson());
    assertFalse(parsed.getMgQueryString().isEmpty());
  }

  private void testItemArray(final DbQuery parsed, final QUERY query, final Object[] array) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getValueCollection());
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i].toString(), parsed.getAvalues()[i].toString());
    }
    assertEquals(2, parsed.getAvalues().length);
    assertNull(parsed.getRestQueries());
    Log.debug(parsed.getSqlQueryString().toString());
    assertFalse(parsed.getSqlQueryString().isEmpty());
    Log.debug(parsed.getBson().toBsonDocument().toJson());
    Log.debug(parsed.getMgQueryString().toString());
    assertNotNull(parsed.getBson());
    assertNotEquals("{}", parsed.getBson().toBsonDocument().toJson());
    assertFalse(parsed.getMgQueryString().isEmpty());
  }

  @Test
  void testQueryConjunctionCreationJson() throws JsonProcessingException {
    final var first = new DbQuery(QUERY.NEQ, "field", "value");
    final var second = new DbQuery(QUERY.EQ, "field2", "value2");

    var dbQuery = new DbQuery(CONJUNCTION.AND, first, second);
    var json = objectMapper.writeValueAsString(dbQuery);
    var parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testConjunctionItem(parsed, CONJUNCTION.AND);
    testConjunctionItems(parsed, second);

    dbQuery = new DbQuery(CONJUNCTION.OR, first, second);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.OR);
    testConjunctionItems(parsed, second);

    dbQuery = new DbQuery(CONJUNCTION.NOT, first, second);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.NOT);
    testConjunctionItems(parsed, second);

    dbQuery = new DbQuery(CONJUNCTION.AND, first);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.AND);
    assertEquals(1, parsed.getRestQueries().length);
    var secRead = parsed.getRestQueries()[0];
    assertEquals(first.getQUERY(), secRead.getQUERY());

    dbQuery = new DbQuery(CONJUNCTION.OR, first);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.OR);
    assertEquals(1, parsed.getRestQueries().length);
    secRead = parsed.getRestQueries()[0];
    assertEquals(first.getQUERY(), secRead.getQUERY());

    dbQuery = new DbQuery(CONJUNCTION.NOT, first);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.NOT);
    assertEquals(1, parsed.getRestQueries().length);
    secRead = parsed.getRestQueries()[0];
    assertEquals(first.getQUERY(), secRead.getQUERY());
  }

  private void testConjunctionItem(final DbQuery parsed, final CONJUNCTION conjunction) {
    assertNull(parsed.getQUERY());
    assertEquals(conjunction, parsed.getConjunction());
    assertNull(parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNotNull(parsed.getRestQueries());
    Log.debug(parsed.getSqlQueryString().toString());
    assertFalse(parsed.getSqlQueryString().isEmpty());
    Log.debug(parsed.getBson().toBsonDocument().toJson());
    Log.debug(parsed.getMgQueryString().toString());
    assertNotNull(parsed.getBson());
    assertNotEquals("{}", parsed.getBson().toBsonDocument().toJson());
    assertFalse(parsed.getMgQueryString().isEmpty());
  }

  private void testConjunctionItems(final DbQuery parsed, final DbQuery second) {
    assertEquals(2, parsed.getRestQueries().length);
    // Valid since Parsed is already a DbQuery
    final var secRead = (DbQuery) parsed.getRestQueries()[1];
    assertEquals(second.getQUERY(), secRead.getQUERY());
    assertEquals(second.getConjunction(), secRead.getConjunction());
    assertEquals(second.getFieldName(), secRead.getFieldName());
    assertEquals(second.getOvalue(), secRead.getOvalue());
    assertEquals(second.getValueCollection(), secRead.getValueCollection());
    assertEquals(second.getSvalue(), secRead.getSvalue());
    assertArrayEquals(second.getAvalues(), secRead.getAvalues());
    assertArrayEquals(second.getRestQueries(), secRead.getRestQueries());
    Log.debug(secRead.getSqlQueryString().toString());
    assertFalse(secRead.getSqlQueryString().isEmpty());
    Log.debug(secRead.getBson().toBsonDocument().toJson());
    Log.debug(secRead.getMgQueryString().toString());
    assertNotNull(secRead.getBson());
    assertNotEquals("{}", secRead.getBson().toBsonDocument().toJson());
    assertFalse(secRead.getMgQueryString().isEmpty());
  }

  @Test
  void testQueryReplaceArg() {
    final List<String> values = new ArrayList<>();
    values.add("val1");
    values.add("val2");
    final var array = values.toArray(new String[0]);
    var dbQuery = new DbQuery(QUERY.ANY, "field", values);
    assertEquals(2, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParam("valnew");
    assertEquals(1, dbQuery.getSqlParams().size());
    final List<Object> list = new ArrayList<>();
    list.add("val3");
    list.add("val4");
    dbQuery.replaceSqlParams(list);
    assertEquals(2, dbQuery.getSqlParams().size());

    dbQuery = new DbQuery(QUERY.ANY, "field", (Object[]) array);
    assertEquals(2, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParam("valnew");
    assertEquals(1, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParams(list);
    assertEquals(2, dbQuery.getSqlParams().size());

    dbQuery = new DbQuery(QUERY.EQ, "field", "val0");
    assertEquals(1, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParam("valnew");
    assertEquals(1, dbQuery.getSqlParams().size());

    final var finalDbQuery = dbQuery;
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> finalDbQuery.replaceSqlParams(list));
  }

  @Test
  void testQueryReplaceArgNotString() {
    final List<Long> values = new ArrayList<>();
    values.add(1L);
    values.add(2L);
    final var array = values.toArray(new Long[0]);
    var dbQuery = new DbQuery(QUERY.ANY, "field", values);
    assertEquals(2, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParam(3);
    assertEquals(1, dbQuery.getSqlParams().size());
    final List<Object> list = new ArrayList<>();
    list.add(4L);
    list.add(5L);
    dbQuery.replaceSqlParams(list);
    assertEquals(2, dbQuery.getSqlParams().size());

    dbQuery = new DbQuery(QUERY.ANY, "field", (Object[]) array);
    assertEquals(2, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParam(3);
    assertEquals(1, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParams(list);
    assertEquals(2, dbQuery.getSqlParams().size());

    dbQuery = new DbQuery(QUERY.EQ, "field", 0);
    assertEquals(1, dbQuery.getSqlParams().size());
    dbQuery.replaceSqlParam(1);
    assertEquals(1, dbQuery.getSqlParams().size());

    final var finalDbQuery = dbQuery;
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> finalDbQuery.replaceSqlParams(list));
  }

  @Test
  void testQueryConjunctionCollectionCreationJson() throws JsonProcessingException {
    final var first = new DbQuery(QUERY.NEQ, "field", "value");
    final var second = new DbQuery(QUERY.EQ, "field2", "value2");
    List<DbQuery> list = new ArrayList<>();
    list.add(first);
    list.add(second);

    var dbQuery = new DbQuery(CONJUNCTION.AND, list);
    var json = objectMapper.writeValueAsString(dbQuery);
    var parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    Log.debug(dbQuery.getSqlQueryString().toString());
    Log.debug(json);
    testConjunctionItem(parsed, CONJUNCTION.AND);
    testConjunctionItems(parsed, second);

    dbQuery = new DbQuery(CONJUNCTION.OR, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.OR);
    testConjunctionItems(parsed, second);

    dbQuery = new DbQuery(CONJUNCTION.NOT, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.NOT);
    testConjunctionItems(parsed, second);

    list = new ArrayList<>();
    list.add(first);
    dbQuery = new DbQuery(CONJUNCTION.AND, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.AND);
    assertEquals(1, parsed.getRestQueries().length);
    var secRead = parsed.getRestQueries()[0];
    assertEquals(first.getQUERY(), secRead.getQUERY());

    dbQuery = new DbQuery(CONJUNCTION.OR, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.OR);
    assertEquals(1, parsed.getRestQueries().length);
    secRead = parsed.getRestQueries()[0];
    assertEquals(first.getQUERY(), secRead.getQUERY());

    dbQuery = new DbQuery(CONJUNCTION.NOT, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.NOT);
    assertEquals(1, parsed.getRestQueries().length);
    secRead = parsed.getRestQueries()[0];
    assertEquals(first.getQUERY(), secRead.getQUERY());
  }
}
