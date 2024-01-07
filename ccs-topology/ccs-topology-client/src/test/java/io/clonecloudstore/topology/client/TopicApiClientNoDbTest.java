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

package io.clonecloudstore.topology.client;

import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.topology.client.conf.Constants;
import io.clonecloudstore.topology.client.fake.FakeTopologyResource;
import io.clonecloudstore.topology.model.Topology;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.topology.model.TopologyStatus.DOWN;
import static io.clonecloudstore.topology.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class TopicApiClientNoDbTest {
  private static final Logger logger = Logger.getLogger(TopicApiClientNoDbTest.class);
  @Inject
  TopologyApiClientFactory factory;

  @Test
  void checkClientWithFakeServer() {
    logger.debugf("\n\nTesting topology api");

    final var topology = new Topology(Constants.SITE, Constants.TOPOLOGY_NAME, Constants.URI_SERVER, UP);
    final var inactive = Constants.SITE + "Inactive";
    final var topologyInactive = new Topology(inactive, Constants.TOPOLOGY_NAME, Constants.URI_SERVER, DOWN);
    try (final var client = factory.newClient()) {
      assertTrue(client.listAll().isEmpty());
      assertTrue(client.listWithStatus(UP).isEmpty());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      FakeTopologyResource.errorCode = 400;
      // Ensure cache is empty
      factory.clearCache();
      assertEquals(400, assertThrows(CcsWithStatusException.class, () -> client.listAll()).getStatus());
      assertEquals(400, assertThrows(CcsWithStatusException.class, () -> client.listWithStatus(UP)).getStatus());
    } finally {
      FakeTopologyResource.errorCode = 0;
    }
    try (final var client = factory.newClient()) {
      assertEquals(topology, client.add(topology));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(409, assertThrows(CcsWithStatusException.class, () -> client.add(topology)).getStatus());
      assertEquals(1, client.listAll().size());
      assertEquals(1, client.listWithStatus(UP).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(topologyInactive, client.add(topologyInactive));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(409, assertThrows(CcsWithStatusException.class, () -> client.add(topologyInactive)).getStatus());
      assertEquals(2, client.listAll().size());
      assertEquals(1, client.listWithStatus(UP).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(topology, client.findBySite(Constants.SITE));
      assertEquals(topologyInactive, client.findBySite(inactive));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(topology, client.update(topology));
      assertEquals(topologyInactive, client.update(topologyInactive));
      assertEquals(2, client.listAll().size());
      assertEquals(1, client.listWithStatus(UP).size());
      final var topologyUpdated = new Topology(topologyInactive, UP);
      assertEquals(topologyUpdated, client.update(topologyUpdated));
      assertEquals(topologyUpdated, client.findBySite(inactive));
      assertEquals(2, client.listAll().size());
      assertEquals(2, client.listWithStatus(UP).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertTrue(client.delete(Constants.SITE));
      assertEquals(1, client.listAll().size());
      assertEquals(1, client.listWithStatus(UP).size());
      assertTrue(client.delete(inactive));
      assertEquals(0, client.listAll().size());
      assertEquals(0, client.listWithStatus(UP).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.findBySite(Constants.SITE)).getStatus());
      assertEquals(404, assertThrows(CcsWithStatusException.class, () -> client.findBySite(inactive)).getStatus());
      assertEquals(404, assertThrows(CcsWithStatusException.class, () -> client.update(topologyInactive)).getStatus());
      assertEquals(404, assertThrows(CcsWithStatusException.class, () -> client.delete(inactive)).getStatus());
    }
  }
}
