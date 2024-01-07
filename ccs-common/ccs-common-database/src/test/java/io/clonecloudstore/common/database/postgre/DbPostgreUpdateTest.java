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

import java.util.ArrayList;
import java.util.List;

import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.test.resource.postgres.NoPostgreDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(NoPostgreDbProfile.class)
class DbPostgreUpdateTest {

  @Test
  void checkDbUpdate() {
    final var dbUpdate = new DbUpdate();
    assertEquals(0, dbUpdate.getSqlParams().size());
    assertEquals(0, dbUpdate.getSqlQuery().length());
    dbUpdate.addToSet("array", "val1");
    assertEquals(0, dbUpdate.getSqlParams().size());
    assertTrue(dbUpdate.getSqlQuery().indexOf("array = array ||") >= 0);
    dbUpdate.set("field", "val2");
    assertEquals(1, dbUpdate.getSqlParams().size());
    assertTrue(dbUpdate.getSqlQuery().indexOf("field = #") >= 0);
    final List<String> oldValues = new ArrayList<>();
    oldValues.add("val1old");
    oldValues.add("val2old");
    dbUpdate.setArray("array", "val3", oldValues);
    assertEquals(1, dbUpdate.getSqlParams().size());
    assertTrue(dbUpdate.getSqlQuery().indexOf("array = '{") >= 0);
    assertTrue(dbUpdate.getSqlQuery().indexOf("val1old") >= 0);
    assertTrue(dbUpdate.getSqlQuery().indexOf("val2old") >= 0);
  }
}
