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
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.clonecloudstore.topology.database.model.DaoTopologyRepository;
import io.clonecloudstore.topology.database.mongodb.conf.Constants;
import io.clonecloudstore.topology.model.Topology;
import io.clonecloudstore.topology.model.TopologyStatus;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.topology.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
class MgDaoTopologyRepositoryDataTest {
  private static final Logger logger = Logger.getLogger(MgDaoTopologyRepositoryDataTest.class);

  @Inject
  Instance<DaoTopologyRepository> repositoryInstance;
  DaoTopologyRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void constraintViolationEmptyData() throws CcsDbException {
    logger.debugf("\n\nTesting topology creation failure with empty attributes");
    final var count = repository.findAllTopologies().size();
    // insert empty topology should not be possible
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> {
      repository.insertTopology(new Topology(null, null, null, null));
    });

    // insert topology with empty site should not be possible
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> {
      repository.insertTopology(new Topology(null, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP));
    });

    // insert topology with empty name should not be possible
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> {
      repository.insertTopology(new Topology(Constants.TOPOLOGY_SITE, null, Constants.TOPOLOGY_URI, UP));
    });

    // insert topology with empty uri should not be possible
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> {
      repository.insertTopology(new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, null, UP));
    });

    // insert topology with empty status leads to a UNKNOWN STATUS
    assertEquals(TopologyStatus.UNKNOWN, repository.insertTopology(
        new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, null)).status());

    // double check no topology was inserted except 1
    final var topologies = assertDoesNotThrow(() -> repository.findAllTopologies());
    assertEquals(topologies.size(), count + 1);
  }

  @Test
  void constraintViolationWrongData() throws CcsDbException {
    logger.debugf("\n\nTesting topology creation with name too long");
    final var count = repository.findAllTopologies().size();
    {
      // insert name too long should not be possible
      assertThrows(CcsInvalidArgumentRuntimeException.class, () -> {
        repository.insertTopology(
            new Topology(Constants.TOPOLOGY_SITE, Constants.INVALID_NAME, Constants.TOPOLOGY_URI, UP));
      });
    }
    {
      // double check no topology was inserted
      final var topologies = assertDoesNotThrow(() -> {
        return repository.findAllTopologies();
      });
      assertEquals(topologies.size(), count);
    }
    {
      // insert uri too long should not be possible
      assertThrows(CcsDbException.class, () -> {
        repository.insertTopology(new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, "/Bad", UP));
      });
    }
    {
      // double check no topology was inserted
      final var topologies = assertDoesNotThrow(() -> {
        return repository.findAllTopologies();
      });
      assertEquals(topologies.size(), count);
    }
  }

  @Test
  void constraintViolationDuplicateKey() {
    logger.debugf("\n\nTesting topology duplicate creation failure with same attributes");

    final var topologies = assertDoesNotThrow(() -> repository.findAllTopologies());
    // insert first topology
    final var topology = assertDoesNotThrow(() -> repository.insertTopology(
        new Topology(Constants.TOPOLOGY_SITE + "new", Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP)));
    assertNotNull(topology);

    // insert same topology should not be possible
    assertThrows(CcsDbException.class, () -> {
      repository.insertTopology(
          new Topology(Constants.TOPOLOGY_SITE + "new", Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP));
    });

    // double check only one topology is stored in database
    final var topologies2 = assertDoesNotThrow(() -> repository.findAllTopologies());
    assertEquals(topologies.size() + 1, topologies2.size());
  }
}
