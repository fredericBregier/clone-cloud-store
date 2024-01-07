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

package io.clonecloudstore.topology.model;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.topology.model.conf.Constants;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.topology.model.TopologyStatus.UP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class TopologyTest {
  private static final Logger logger = Logger.getLogger(TopologyTest.class);

  @Test
  void checkDtoInit() {
    logger.debugf("Testing bean dto initialisation...");
    final var topology = assertDoesNotThrow(
        () -> new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP));

    var partialTopology = new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, null);
    assertNotEquals(topology, partialTopology);
    partialTopology =
        new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI + "0", null);
    assertNotEquals(topology, partialTopology);
    partialTopology =
        new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME + "differ", Constants.TOPOLOGY_URI, null);
    assertNotEquals(topology, partialTopology);
    partialTopology =
        new Topology(Constants.TOPOLOGY_SITE + "differ", Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP);
    assertNotEquals(topology, partialTopology);
    logger.debugf("Testing dto cloning and comparison");
    final var fullAllocated =
        new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP);
    assertTrue(topology.equals(topology));
    assertEquals(topology, fullAllocated);
    assertFalse(topology.equals(logger));

    logger.debugf("Testing dto attributes format");
    assertDoesNotThrow(
        () -> new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, "http://localhost:8081", UP));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, "https:/// localhost::90yd", UP));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, "test string", UP));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new Topology(null, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, UP));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new Topology(Constants.TOPOLOGY_SITE, null, Constants.TOPOLOGY_URI, UP));
    assertEquals(TopologyStatus.UNKNOWN,
        new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, Constants.TOPOLOGY_URI, null).status());
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new Topology(Constants.TOPOLOGY_SITE, Constants.TOPOLOGY_NAME, null, UP));

    assertEquals(topology.hashCode(), fullAllocated.hashCode());

    assertTrue(topology.toString().contains(Constants.TOPOLOGY_URI));
  }
}
