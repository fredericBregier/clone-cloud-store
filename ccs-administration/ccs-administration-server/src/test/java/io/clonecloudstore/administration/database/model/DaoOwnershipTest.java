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

package io.clonecloudstore.administration.database.model;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.administration.test.fake.FakeDaoOwnership;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.test.conf.Constants.OWNERSHIP_BUCKET;
import static io.clonecloudstore.administration.test.conf.Constants.OWNERSHIP_CLIENT_ID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class DaoOwnershipTest {
  private static final Logger logger = Logger.getLogger(DaoOwnershipTest.class);

  @Test
  void checkDaoDtoConversion() {
    logger.infof("\n\n Testing dao bean initialisation from dto");
    final var dto =
        assertDoesNotThrow(() -> new ClientBucketAccess(OWNERSHIP_CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.READ));

    final var dao = assertDoesNotThrow(() -> new FakeDaoOwnership().fromDto(dto));

    logger.infof("\n\n Testing dao and dto bean comparison");
    assertNotEquals(dao, dto);
    assertEquals(dao.getDto(), dto);
    assertEquals(dao, new FakeDaoOwnership().fromDto(dto));
    assertEquals(dao, dao);

    logger.infof("\n\n Testing dao bean hash code calculation");
    assertEquals(dao.hashCode(), new FakeDaoOwnership().fromDto(dto).hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoOwnership().fromDto(dto).setBucket("xxx").hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoOwnership().fromDto(dto).setId("xx").hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoOwnership().fromDto(dto).setClientId("xx").hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoOwnership().fromDto(dto).setOwnership(ClientOwnership.WRITE).hashCode());
    logger.infof("dao: %s", dao);
    assertTrue(dao.toString().contains(OWNERSHIP_CLIENT_ID + "_" + OWNERSHIP_BUCKET));
  }

  @Test
  void checkStructure() {
    var dto = new ClientBucketAccess(OWNERSHIP_CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.READ);
    var dao = new FakeDaoOwnership().fromDto(dto);
    assertEquals(ClientOwnership.READ, dao.getOwnership());
    dto = new ClientBucketAccess(OWNERSHIP_CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.READ);
    var daoPartial = new FakeDaoOwnership().fromDto(dto);
    assertEquals(ClientOwnership.READ, daoPartial.getOwnership());
    assertEquals(dao, daoPartial);
    dto = new ClientBucketAccess(OWNERSHIP_CLIENT_ID, OWNERSHIP_BUCKET + "differ", null);
    daoPartial = new FakeDaoOwnership().fromDto(dto);
    assertNotEquals(dao, daoPartial);
    dto = new ClientBucketAccess(OWNERSHIP_CLIENT_ID + "differ", OWNERSHIP_BUCKET, null);
    daoPartial = new FakeDaoOwnership().fromDto(dto);
    assertNotEquals(dao, daoPartial);
  }

  @Test
  void checkEquals() {
    DaoOwnership Ownership = new FakeDaoOwnership();
    DaoOwnership Ownership1 = new FakeDaoOwnership();
    AccessorBucket bucket = new AccessorBucket();
    assertEquals(Ownership, Ownership1);
    assertEquals(Ownership.hashCode(), Ownership1.hashCode());
    assertEquals(Ownership, Ownership);
    assertEquals(Ownership.hashCode(), Ownership.hashCode());
    assertNotEquals(Ownership, bucket);
    Ownership1.setId("id");
    assertNotEquals(Ownership1, Ownership);
    assertNotEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership.setId("id");
    assertEquals(Ownership1, Ownership);
    assertEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership1.setClientId("name");
    assertNotEquals(Ownership1, Ownership);
    assertNotEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership.setClientId("name");
    assertEquals(Ownership1, Ownership);
    assertEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership1.setOwnership(ClientOwnership.READ);
    assertNotEquals(Ownership1, Ownership);
    assertNotEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership.setOwnership(ClientOwnership.READ);
    assertEquals(Ownership1, Ownership);
    assertEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership1.setBucket("bucket");
    assertNotEquals(Ownership1, Ownership);
    assertNotEquals(Ownership.hashCode(), Ownership1.hashCode());
    Ownership.setBucket("bucket");
    assertEquals(Ownership1, Ownership);
    assertEquals(Ownership.hashCode(), Ownership1.hashCode());
  }
}
