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
import java.util.Collection;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class RestQueryTest {
  static ObjectMapper objectMapper;

  @BeforeAll
  static void beforeAll() {
    objectMapper = JsonUtil.getInstance();
  }

  @Test
  void testEmptyCreationJson() throws JsonProcessingException {
    final var dbQuery = new RestQuery();
    final var json = objectMapper.writeValueAsString(dbQuery);
    final RestQuery parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    assertNull(parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertNull(parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
  }

  @Test
  void testQueryInjection() {
    final var object = Integer.valueOf(0);

    final var err1 = "va<!ENTITYl2";
    final List<String> values1 = new ArrayList<>();
    values1.add("val1");
    values1.add(err1);
    final var array1 = values1.toArray(new String[0]);
    final var err2 = "va<script>l2";
    final List<String> values2 = new ArrayList<>();
    values2.add("val1");
    values2.add(err2);
    final var array2 = values2.toArray(new String[0]);
    final var err3 = "va;l2";
    final List<String> values3 = new ArrayList<>();
    values3.add("val1");
    values3.add(err3);
    final var array3 = values3.toArray(new String[0]);

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, err1, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, "value", err1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, err2, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, "value", err2));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, err3, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, "value", err3));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, err1, object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, err2, object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, err3, object));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, null, object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, null, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, null, "value", "value2"));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "value", values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "value", (Object[]) array1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "value", values2));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "value", (Object[]) array2));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "value", values3));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "value", (Object[]) array3));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, null, values1));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(CONJUNCTION.AND, (RestQuery[]) null));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new RestQuery(CONJUNCTION.AND, (Collection<RestQuery>) null));
  }

  @Test
  void testQueryCreationJson() throws JsonProcessingException {
    var dbQuery = new RestQuery(QUERY.EQ, "field", "value");
    var json = objectMapper.writeValueAsString(dbQuery);
    RestQuery parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.EQ);
    Log.warn(json);

    dbQuery = new RestQuery(QUERY.GTE, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.GTE);

    dbQuery = new RestQuery(QUERY.LTE, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.LTE);

    dbQuery = new RestQuery(QUERY.NEQ, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.NEQ);

    dbQuery = new RestQuery(QUERY.START_WITH, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.START_WITH);

    dbQuery = new RestQuery(QUERY.JSON_EQ, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.JSON_EQ);

    dbQuery = new RestQuery(QUERY.JSON_NEQ, "field", "value");
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemString(parsed, QUERY.JSON_NEQ);

    final var object = Integer.valueOf(0);

    dbQuery = new RestQuery(QUERY.EQ, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.EQ, object);
    Log.warn(json);

    dbQuery = new RestQuery(QUERY.GTE, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.GTE, object);

    dbQuery = new RestQuery(QUERY.LTE, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.LTE, object);

    dbQuery = new RestQuery(QUERY.NEQ, "field", object);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemObject(parsed, QUERY.NEQ, object);

    final List<String> values = new ArrayList<>();
    values.add("val1");
    values.add("val2");
    final var array = values.toArray(new String[0]);

    dbQuery = new RestQuery(QUERY.IN, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.IN, values);
    Log.warn(json);

    dbQuery = new RestQuery(QUERY.CONTAINS, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINS, values);

    dbQuery = new RestQuery(QUERY.CONTAINED, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.CONTAINED, values);

    dbQuery = new RestQuery(QUERY.ANY, "field", values);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemList(parsed, QUERY.ANY, values);

    dbQuery = new RestQuery(QUERY.IN, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.IN, array);
    Log.warn(json);

    dbQuery = new RestQuery(QUERY.CONTAINS, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINS, array);

    dbQuery = new RestQuery(QUERY.CONTAINED, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.CONTAINED, array);

    dbQuery = new RestQuery(QUERY.ANY, "field", (Object[]) array);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testItemArray(parsed, QUERY.ANY, array);
  }

  @Test
  void testRequestError() {
    final var object = Integer.valueOf(0);

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "field", "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.CONTAINED, "field", "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.CONTAINS, "field", "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.ANY, "field", "value"));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.IN, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.CONTAINED, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.CONTAINS, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.ANY, "field", object));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.START_WITH, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.JSON_NEQ, "field", object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.JSON_EQ, "field", object));

    final List<String> values = new ArrayList<>();
    values.add("val1");
    values.add("val2");
    final var array = values.toArray(new String[0]);

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.NEQ, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.GTE, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.LTE, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.START_WITH, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.JSON_EQ, "field", values));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.JSON_NEQ, "field", values));

    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.EQ, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.NEQ, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.GTE, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new RestQuery(QUERY.LTE, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new RestQuery(QUERY.START_WITH, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new RestQuery(QUERY.JSON_EQ, "field", (Object[]) array));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new RestQuery(QUERY.JSON_NEQ, "field", (Object[]) array));
  }

  private void testItemString(final RestQuery parsed, final QUERY query) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertEquals("value", parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
  }

  private void testItemObject(final RestQuery parsed, final QUERY query, final Object object) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertEquals(object, parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
  }

  private void testItemList(final RestQuery parsed, final QUERY query, final List<String> values) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getSvalue());
    assertEquals(values, parsed.getValueCollection());
    assertEquals(2, parsed.getValueCollection().size());
    assertNull(parsed.getAvalues());
    assertNull(parsed.getRestQueries());
  }

  private void testItemArray(final RestQuery parsed, final QUERY query, final String[] array) {
    assertEquals(query, parsed.getQUERY());
    assertNull(parsed.getConjunction());
    assertEquals("field", parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getValueCollection());
    assertArrayEquals(array, parsed.getAvalues());
    assertEquals(2, parsed.getAvalues().length);
    assertNull(parsed.getRestQueries());
  }

  @Test
  void testQueryConjunctionCreationJson() throws JsonProcessingException {
    final var first = new RestQuery(QUERY.NEQ, "field", "value");
    final var second = new RestQuery(QUERY.EQ, "field2", "value2");

    var dbQuery = new RestQuery(CONJUNCTION.AND, first, second);
    var json = objectMapper.writeValueAsString(dbQuery);
    RestQuery parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.AND);
    testConjunctionItems(parsed, second);
    Log.warn(json);

    dbQuery = new RestQuery(CONJUNCTION.OR, first, second);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.OR);
    testConjunctionItems(parsed, second);

    dbQuery = new RestQuery(CONJUNCTION.NOT, first, second);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.NOT);
    testConjunctionItems(parsed, second);
  }

  private void testConjunctionItem(final RestQuery parsed, final CONJUNCTION conjunction) {
    assertNull(parsed.getQUERY());
    assertEquals(conjunction, parsed.getConjunction());
    assertNull(parsed.getFieldName());
    assertNull(parsed.getOvalue());
    assertNull(parsed.getValueCollection());
    assertNull(parsed.getSvalue());
    assertNull(parsed.getAvalues());
    assertNotNull(parsed.getRestQueries());
  }

  private void testConjunctionItems(final RestQuery parsed, final RestQuery second) {
    assertEquals(2, parsed.getRestQueries().length);
    final var secRead = parsed.getRestQueries()[1];
    assertEquals(second.getQUERY(), secRead.getQUERY());
    assertEquals(second.getConjunction(), secRead.getConjunction());
    assertEquals(second.getFieldName(), secRead.getFieldName());
    assertEquals(second.getOvalue(), secRead.getOvalue());
    assertEquals(second.getValueCollection(), secRead.getValueCollection());
    assertEquals(second.getSvalue(), secRead.getSvalue());
    assertArrayEquals(second.getAvalues(), secRead.getAvalues());
    assertArrayEquals(second.getRestQueries(), secRead.getRestQueries());
  }

  @Test
  void testQueryConjunctionCollectionCreationJson() throws JsonProcessingException {
    final var first = new RestQuery(QUERY.NEQ, "field", "value");
    final var second = new RestQuery(QUERY.EQ, "field2", "value2");
    final List<RestQuery> list = new ArrayList<>();
    list.add(first);
    list.add(second);

    var dbQuery = new RestQuery(CONJUNCTION.AND, list);
    var json = objectMapper.writeValueAsString(dbQuery);
    RestQuery parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.AND);
    testConjunctionItems(parsed, second);
    Log.warn(json);

    dbQuery = new RestQuery(CONJUNCTION.OR, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.OR);
    testConjunctionItems(parsed, second);

    dbQuery = new RestQuery(CONJUNCTION.NOT, list);
    json = objectMapper.writeValueAsString(dbQuery);
    parsed = DbQuery.fromRestQuery(objectMapper.readValue(json, RestQuery.class));
    testConjunctionItem(parsed, CONJUNCTION.NOT);
    testConjunctionItems(parsed, second);
  }
}
