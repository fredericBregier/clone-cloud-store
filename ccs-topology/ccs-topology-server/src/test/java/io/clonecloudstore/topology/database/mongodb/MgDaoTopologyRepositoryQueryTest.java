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

package io.clonecloudstore.topology.database.mongodb;

import io.clonecloudstore.common.database.utils.DbType;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.clonecloudstore.topology.database.model.DaoTopologyRepository;
import io.clonecloudstore.topology.database.mongodb.conf.Constants;
import io.clonecloudstore.topology.model.Topology;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.topology.model.TopologyStatus.DOWN;
import static io.clonecloudstore.topology.model.TopologyStatus.UNKNOWN;
import static io.clonecloudstore.topology.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
class MgDaoTopologyRepositoryQueryTest {
  private static final Logger logger = Logger.getLogger(MgDaoTopologyRepositoryQueryTest.class);

  @Inject
  Instance<DaoTopologyRepository> repositoryInstance;
  MgDaoTopologyRepository repository;

  @BeforeEach
  void beforeEach() throws CcsDbException {
    repository = (MgDaoTopologyRepository) repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
    repository.deleteAllDb();
  }

  @Test
  void checkConf() {
    logger.debugf("\n\nTesting dao topology repository conf");

    assertEquals("topology", repository.getTable());
    assertDoesNotThrow(() -> repository.createIndex());
  }

  @Test
  void repositoryQueryValidation() {
    final var topologyWrong =
        new Topology(Constants.TOPOLOGY_SITE + "wrong", Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, null);
    {
      logger.debugf("\n\nTesting topology creation with status UP");
      final var topology = new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP);
      final var createdTopology = assertDoesNotThrow(() -> {
        return repository.insertTopology(topology);
      });
      assertEquals(topology, createdTopology);
    }
    {
      logger.debugf("\n\nTesting cannot create duplicate topology");
      final var topology = new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP);
      assertThrows(CcsDbException.class, () -> repository.insertTopology(topology));
    }
    {
      logger.debugf("\n\nTesting topology creation with status DOWN");
      final var topology =
          new Topology(Constants.TOPOLOGY_SITE_2, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, DOWN);
      final var createdTopology = assertDoesNotThrow(() -> {
        return repository.insertTopology(topology);
      });
      assertEquals(topology, createdTopology);
    }
    {
      logger.debugf("\n\nTesting topology retrieval by siteId");
      final var retrievedTopology = assertDoesNotThrow(() -> {
        return repository.findBySite(Constants.TOPOLOGY_SITE);
      });
      assertEquals(Constants.TOPOLOGY_NAME, retrievedTopology.name());
      assertDoesNotThrow(() -> assertNull(repository.findBySite(Constants.TOPOLOGY_SITE + "wrong")));
    }
    {
      logger.debugf("\n\nTesting topology list retrieval");
      final var topologies = assertDoesNotThrow(() -> {
        return repository.findAllTopologies();
      });
      assertNotNull(topologies);
      assertEquals(2, topologies.size());
      assertEquals(Constants.TOPOLOGY_SITE, topologies.get(0).id());
      assertEquals(Constants.TOPOLOGY_SITE_2, topologies.get(1).id());
    }
    {
      logger.debugf("\n\nTesting available topology list retrieval");
      final var topologies = assertDoesNotThrow(() -> repository.findTopologies(UP));
      assertNotNull(topologies);
      assertEquals(1, topologies.size());
      assertEquals(Constants.TOPOLOGY_SITE, topologies.get(0).id());
      final var topologies2 = assertDoesNotThrow(() -> repository.findTopologies(UNKNOWN));
      assertNotNull(topologies2);
      assertEquals(0, topologies2.size());
    }
    {
      logger.debugf("\n\nTesting find topology by site");
      final var topology = assertDoesNotThrow(() -> {
        return repository.findBySite(Constants.TOPOLOGY_SITE);
      });
      assertEquals(Constants.TOPOLOGY_NAME, topology.name());

      logger.debugf("\n\nTesting update topology status");
      final var updatedTopology = assertDoesNotThrow(() -> {
        return repository.updateTopologyStatus(topology, DOWN);
      });
      assertEquals(updatedTopology.id(), topology.id());
      assertEquals(DOWN, updatedTopology.status());
      assertThrows(CcsDbException.class, () -> repository.updateTopologyStatus(topologyWrong, DOWN));
    }
    {
      logger.debugf("\n\nTesting available topologies after topology deletion");
      final var topologies = assertDoesNotThrow(() -> repository.findTopologies(UP));
      assertNotNull(topologies);
      assertEquals(0, topologies.size());
    }
    {
      logger.debugf("\n\nTesting delete topology");
      assertDoesNotThrow(() -> repository.deleteTopology(Constants.TOPOLOGY_SITE));
      final var all = assertDoesNotThrow(() -> {
        return repository.findAllTopologies();
      });
      assertNotNull(all);
      assertEquals(1, all.size());

      final var available = assertDoesNotThrow(() -> repository.findTopologies(UP));
      assertNotNull(available);
      assertEquals(0, available.size());
    }
    {
      logger.debugf("\n\nTesting delete topology that do not exists");
      assertThrows(CcsDbException.class, () -> repository.deleteTopology("xx"));
    }
    {
      logger.debugf("\n\nTesting find topology by site after deletion");
      final var topology = assertDoesNotThrow(() -> {
        return repository.findBySite(Constants.TOPOLOGY_SITE);
      });
      assertNull(topology);
    }
  }
}
