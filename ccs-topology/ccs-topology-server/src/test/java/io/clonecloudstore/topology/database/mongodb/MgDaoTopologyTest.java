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

import io.clonecloudstore.topology.database.mongodb.conf.Constants;
import io.clonecloudstore.topology.model.Topology;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.topology.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class MgDaoTopologyTest {
  private static final Logger logger = Logger.getLogger(MgDaoTopologyTest.class);


  @Test
  void checkDaoInitialisationFromDto() {
    logger.debugf("\n\nTesting topology dao bean");
    final var dto = new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP);
    final var dao = new MgDaoTopology().setId(Constants.TOPOLOGY_SITE).setName(Constants.TOPOLOGY_NAME)
        .setUri(Constants.TOPOLOGY_URI).setStatus(UP);

    final var daoFromDto = new MgDaoTopology(dto);

    assertEquals(daoFromDto, dao);
    assertEquals(daoFromDto.hashCode(), dao.hashCode());
  }
}
