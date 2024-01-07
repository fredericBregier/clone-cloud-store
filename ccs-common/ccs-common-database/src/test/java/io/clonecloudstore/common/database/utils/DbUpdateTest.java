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

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class DbUpdateTest {

  @Test
  void checkDbUpdate() {
    final var dbUpdate = new DbUpdate();
    assertEquals(0, dbUpdate.getSqlParams().size());
    assertEquals(0, dbUpdate.getSqlQuery().length());
    dbUpdate.addToSet("array", "val1");
    assertEquals(0, dbUpdate.getSqlParams().size());
    assertTrue(dbUpdate.getSqlQuery().indexOf("array = array ||") >= 0);
    dbUpdate.set("field", "val2");
    dbUpdate.set("field2", "val3");
    assertEquals(2, dbUpdate.getSqlParams().size());
    assertTrue(dbUpdate.getSqlQuery().indexOf("field = #") >= 0);
    var pos = dbUpdate.getSqlQuery().indexOf("field = #");
    assertTrue(dbUpdate.getSqlQuery().indexOf("field2 = #", pos + 1) >= 0);
    final List<String> oldValues = new ArrayList<>();
    oldValues.add("val1old");
    oldValues.add("val2old");
    dbUpdate.setArray("array", "val3", oldValues);
    assertEquals(2, dbUpdate.getSqlParams().size());
    assertTrue(dbUpdate.getSqlQuery().indexOf("array = '{") >= 0);
    assertTrue(dbUpdate.getSqlQuery().indexOf("val1old") >= 0);
    assertTrue(dbUpdate.getSqlQuery().indexOf("val2old") >= 0);
  }

  @Test
  void testQueryInjection() {
    final var object = Integer.valueOf(0);

    final var err1 = "va<!ENTITYl2";
    final List<String> values1 = new ArrayList<>();
    values1.add("val1");
    values1.add(err1);
    final var err2 = "va<script>l2";
    final List<String> values2 = new ArrayList<>();
    values2.add("val1");
    values2.add(err2);
    final var err3 = "va;l2";
    final List<String> values3 = new ArrayList<>();
    values3.add("val1");
    values3.add(err3);

    final var dbUpdate = new DbUpdate();
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.set(err1, object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.set(err2, object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.set(err3, object));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.addToSet(err1, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.addToSet(err2, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.addToSet(err3, "value"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.addToSet("key", err1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.addToSet("key", err2));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.addToSet("key", err3));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray(err1, "value", values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray(err2, "value", values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray(err3, "value", values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray("key", err1, values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray("key", err2, values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray("key", err3, values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray("key", "value", values1));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray("key", "value", values2));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> dbUpdate.setArray("key", "value", values3));
  }
}
