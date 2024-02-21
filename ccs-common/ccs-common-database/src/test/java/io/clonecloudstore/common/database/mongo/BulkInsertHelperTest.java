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

import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(NoMongoDbProfile.class)
class BulkInsertHelperTest {
  @Test
  void checkBulkInsert() {
    final var bulkHelperEx = new BulkInsertHelperEx(null);
    Assertions.assertFalse(bulkHelperEx.addToInsertBulk("item"));
    Assertions.assertFalse(bulkHelperEx.addToInsertBulk("item2"));
    Assertions.assertTrue(bulkHelperEx.addToInsertBulk("item3"));
    bulkHelperEx.bulkPersist();
    Assertions.assertFalse(bulkHelperEx.addToInsertBulk("item"));
    bulkHelperEx.bulkPersist();
  }

  @Test
  void checkBulkUpsert() {
    final var bulkHelperEx = new BulkUpsertHelperEx(null);
    Assertions.assertFalse(bulkHelperEx.addToUpsertBulk(new Document(), ""));
    Assertions.assertFalse(bulkHelperEx.addToUpsertBulk(new Document(), ""));
    Assertions.assertTrue(bulkHelperEx.addToUpsertBulk(new Document(), ""));
    bulkHelperEx.bulkUpsert();
    Assertions.assertFalse(bulkHelperEx.addToUpsertBulk(new Document(), ""));
    bulkHelperEx.bulkUpsert();
  }

  @Test
  void simpleExceptionTest() {
    final var throwable = new Throwable("test");
    final var exception = new CcsDbException(throwable);
    assertEquals(throwable, exception.getCause());
  }

  private static class BulkInsertHelperEx extends MongoBulkInsertHelper<String, String> {

    /**
     * Constructor
     *
     * @param repositoryBase
     */
    public BulkInsertHelperEx(final ExtendedPanacheMongoRepositoryBase<String, String> repositoryBase) {
      super(repositoryBase);
    }

    @Override
    protected int getMaxBatch() {
      return 3;
    }

    @Override
    public MongoBulkInsertHelper bulkPersist() {
      listInsert.clear();
      return null;
    }
  }

  private static class BulkUpsertHelperEx extends MongoBulkInsertHelper<String, String> {

    /**
     * Constructor
     *
     * @param repositoryBase
     */
    public BulkUpsertHelperEx(final ExtendedPanacheMongoRepositoryBase<String, String> repositoryBase) {
      super(repositoryBase);
    }

    @Override
    protected int getMaxBatch() {
      return 3;
    }

    @Override
    public MongoBulkInsertHelper<String, String> bulkUpsert() {
      listFindQuery.clear();
      listUpdate.clear();
      return null;
    }
  }
}
