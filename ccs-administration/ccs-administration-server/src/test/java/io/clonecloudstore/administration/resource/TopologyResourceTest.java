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

package io.clonecloudstore.administration.resource;

import io.clonecloudstore.administration.client.TopologyApiClientFactory;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.resource.conf.Constants;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
class TopologyResourceTest {
  private static final Logger logger = Logger.getLogger(TopologyResourceTest.class);

  @Inject
  TopologyApiClientFactory factory;

  @BeforeAll
  static void setup() {
  }

  @Test
  void topologyApiClientValidation() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting add topology");
        final var topology = new Topology(Constants.SITE, Constants.TOPOLOGY_NAME, Constants.URI_SERVER, UP);
        final var createdTopology = assertDoesNotThrow(() -> client.add(topology));
        assertNotNull(createdTopology);
        assertEquals(createdTopology, topology);
      }
      {
        logger.debugf("\n\nTesting list topology");
        final var topologies = assertDoesNotThrow(client::listAll);
        assertNotNull(topologies);
        assertEquals(1, topologies.size());
      }
      {
        logger.debugf("\n\nTesting list topology");
        final var topologies = assertDoesNotThrow(() -> client.listWithStatus(UP));
        assertNotNull(topologies);
        assertEquals(1, topologies.size());
      }
      {
        logger.debugf("\n\nTesting get topology");
        final var topology = new Topology(Constants.SITE, Constants.TOPOLOGY_NAME, Constants.URI_SERVER, UP);
        final var getTopology = assertDoesNotThrow(() -> client.findBySite(Constants.SITE));
        assertNotNull(getTopology);
        assertEquals(getTopology, topology);
      }
      {
        logger.debugf("\n\nTesting update topology");
        final var topology = new Topology(Constants.SITE, Constants.TOPOLOGY_NAME + "xx", Constants.URI_SERVER, UP);
        final var updatedTopology = assertDoesNotThrow(() -> client.update(topology));
        assertNotNull(updatedTopology);
        assertEquals(updatedTopology, topology);
      }
      {
        logger.debugf("\n\nTesting delete topology");
        final var deleted = assertDoesNotThrow(() -> client.delete(Constants.SITE));
        assertTrue(deleted);
      }
      {
        logger.debugf("\n\nTesting list topology");
        final var topologies = assertDoesNotThrow(client::listAll);
        assertNotNull(topologies);
        assertEquals(0, topologies.size());
      }
    }
  }

  @Test
  void topologyApiClientNotFound() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting get topology not found ");
        final var exception =
            assertThrows(CcsWithStatusException.class, () -> client.findBySite(Constants.SITE_NOT_FOUND));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting update topology not found ");
        final var topology = new Topology(Constants.SITE_NOT_FOUND, Constants.TOPOLOGY_NAME, Constants.URI_SERVER, UP);
        final var exception = assertThrows(CcsWithStatusException.class, () -> client.update(topology));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting delete topology not found ");
        final var exception = assertThrows(CcsWithStatusException.class, () -> client.delete(Constants.SITE_NOT_FOUND));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getStatus());
      }
    }

  }
}
