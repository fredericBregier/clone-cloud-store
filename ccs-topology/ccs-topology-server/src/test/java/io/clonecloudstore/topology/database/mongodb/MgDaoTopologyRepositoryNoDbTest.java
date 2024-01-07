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
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.clonecloudstore.topology.database.model.DaoTopologyRepository;
import io.clonecloudstore.topology.database.mongodb.conf.Constants;
import io.clonecloudstore.topology.model.Topology;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.topology.model.TopologyStatus.DOWN;
import static io.clonecloudstore.topology.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(NoMongoDbProfile.class)
class MgDaoTopologyRepositoryNoDbTest {
  @Inject
  Instance<DaoTopologyRepository> repositoryInstance;
  DaoTopologyRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void checkNoDbException() {
    final var topology = new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP);

    assertThrows(CcsDbException.class, () -> ((MgDaoTopologyRepository) repository).createIndex());
    assertThrows(CcsDbException.class, () -> repository.findTopologies(UP));
    assertThrows(CcsDbException.class, () -> repository.findAllTopologies());
    assertThrows(CcsDbException.class, () -> repository.insertTopology(topology));
    assertThrows(CcsDbException.class, () -> repository.findTopologies(UP));
    assertThrows(CcsDbException.class, () -> repository.deleteTopology(Constants.TOPOLOGY_SITE));
    assertThrows(CcsDbException.class, () -> repository.updateTopologyStatus(topology, DOWN));
    assertThrows(CcsDbException.class, () -> repository.updateTopology(topology));
    assertThrows(CcsDbException.class, () -> repository.findBySite(Constants.TOPOLOGY_SITE));
  }
}
