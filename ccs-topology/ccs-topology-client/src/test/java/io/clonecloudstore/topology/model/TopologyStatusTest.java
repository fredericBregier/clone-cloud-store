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

import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class TopologyStatusTest {
  private static final Logger logger = Logger.getLogger(TopologyStatusTest.class);

  @Test
  void checkStatusConversion() {
    logger.debugf("Testing status conversion...");
    assertEquals(TopologyStatus.UP, TopologyStatus.fromStatusCode(TopologyStatus.UP.getStatus()));
    assertEquals(TopologyStatus.DOWN, TopologyStatus.fromStatusCode(TopologyStatus.DOWN.getStatus()));
    assertEquals(TopologyStatus.DELETED, TopologyStatus.fromStatusCode(TopologyStatus.DELETED.getStatus()));
    assertEquals(TopologyStatus.UNKNOWN, TopologyStatus.fromStatusCode(TopologyStatus.UNKNOWN.getStatus()));
    assertEquals(TopologyStatus.UNKNOWN, TopologyStatus.fromStatusCode((short) -1));
  }

}
