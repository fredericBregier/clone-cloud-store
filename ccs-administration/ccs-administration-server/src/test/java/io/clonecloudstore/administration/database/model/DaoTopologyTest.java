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
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.administration.test.fake.FakeDaoTopology;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.model.TopologyStatus.DOWN;
import static io.clonecloudstore.administration.model.TopologyStatus.UNKNOWN;
import static io.clonecloudstore.administration.model.TopologyStatus.UP;
import static io.clonecloudstore.administration.test.conf.Constants.TOPOLOGY_NAME;
import static io.clonecloudstore.administration.test.conf.Constants.TOPOLOGY_SITE;
import static io.clonecloudstore.administration.test.conf.Constants.TOPOLOGY_URI;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class DaoTopologyTest {
  private static final Logger logger = Logger.getLogger(DaoTopologyTest.class);

  @Test
  void checkDaoDtoConversion() {
    logger.infof("\n\n Testing dao bean initialisation from dto");
    final var dto = assertDoesNotThrow(() -> new Topology(TOPOLOGY_SITE, TOPOLOGY_NAME, TOPOLOGY_URI, UP));

    final var dao = assertDoesNotThrow(() -> new FakeDaoTopology().fromDto(dto));

    logger.infof("\n\n Testing dao and dto bean comparison");
    assertNotEquals(dao, dto);
    assertEquals(dao.getDto(), dto);
    assertEquals(dao, new FakeDaoTopology().fromDto(dto));
    assertEquals(dao, dao);

    logger.infof("\n\n Testing dao bean hash code calculation");
    assertEquals(dao.hashCode(), new FakeDaoTopology().fromDto(dto).hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoTopology().fromDto(dto).setName("xx").hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoTopology().fromDto(dto).setId("xx").hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoTopology().fromDto(dto).setUri("xx").hashCode());
    assertNotEquals(dao.hashCode(), new FakeDaoTopology().fromDto(dto).setStatus(DOWN).hashCode());

    assertTrue(dao.toString().contains(TOPOLOGY_URI));
  }

  @Test
  void checkStructure() {
    var dto = new Topology(TOPOLOGY_SITE, TOPOLOGY_NAME, TOPOLOGY_URI, UNKNOWN);
    var dao = new FakeDaoTopology().fromDto(dto);
    assertEquals(TopologyStatus.UNKNOWN, dao.getStatus());
    dto = new Topology(TOPOLOGY_SITE, TOPOLOGY_NAME, TOPOLOGY_URI, null);
    var daoPartial = new FakeDaoTopology().fromDto(dto);
    assertEquals(TopologyStatus.UNKNOWN, daoPartial.getStatus());
    assertEquals(dao, daoPartial);
    dto = new Topology(TOPOLOGY_SITE, TOPOLOGY_NAME, TOPOLOGY_URI + "0", null);
    daoPartial = new FakeDaoTopology().fromDto(dto);
    assertNotEquals(dao, daoPartial);
    dto = new Topology(TOPOLOGY_SITE, TOPOLOGY_NAME + "differ", TOPOLOGY_URI, null);
    daoPartial = new FakeDaoTopology().fromDto(dto);
    assertNotEquals(dao, daoPartial);
    dto = new Topology(TOPOLOGY_SITE + "differ", TOPOLOGY_NAME, TOPOLOGY_URI, null);
    daoPartial = new FakeDaoTopology().fromDto(dto);
    assertNotEquals(dao, daoPartial);
  }

  @Test
  void checkEquals() {
    DaoTopology topology = new FakeDaoTopology();
    DaoTopology topology1 = new FakeDaoTopology();
    AccessorBucket bucket = new AccessorBucket();
    assertTrue(topology.equals(topology1));
    assertEquals(topology.hashCode(), topology1.hashCode());
    assertTrue(topology.equals(topology));
    assertEquals(topology.hashCode(), topology.hashCode());
    assertFalse(topology.equals(bucket));
    topology1.setId("id");
    assertFalse(topology1.equals(topology));
    assertNotEquals(topology.hashCode(), topology1.hashCode());
    topology.setId("id");
    assertTrue(topology1.equals(topology));
    assertEquals(topology.hashCode(), topology1.hashCode());
    topology1.setName("name");
    assertFalse(topology1.equals(topology));
    assertNotEquals(topology.hashCode(), topology1.hashCode());
    topology.setName("name");
    assertTrue(topology1.equals(topology));
    assertEquals(topology.hashCode(), topology1.hashCode());
    topology1.setStatus(UP);
    assertFalse(topology1.equals(topology));
    assertNotEquals(topology.hashCode(), topology1.hashCode());
    topology.setStatus(UP);
    assertTrue(topology1.equals(topology));
    assertEquals(topology.hashCode(), topology1.hashCode());
    topology1.setUri("http://1.1.1.1");
    assertFalse(topology1.equals(topology));
    assertNotEquals(topology.hashCode(), topology1.hashCode());
    topology.setUri("http://1.1.1.1");
    assertTrue(topology1.equals(topology));
    assertEquals(topology.hashCode(), topology1.hashCode());
  }
}
