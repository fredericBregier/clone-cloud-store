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

package io.clonecloudstore.common.quarkus.client;

import io.clonecloudstore.common.quarkus.client.example.SimpleApiClient;
import io.clonecloudstore.common.quarkus.client.example.SimpleApiClientFactory;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.CONFLICT_NAME;
import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.NOT_ACCEPTABLE_NAME;
import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.NOT_FOUND_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class SimpleApiClientTest {
  private static final Logger LOG = Logger.getLogger(SimpleApiClientTest.class);

  @Inject
  SimpleApiClientFactory factory;

  @Test
  void check99Service() {
    try (final var client = factory.newClient()) {
      assertEquals(factory.getUri(), client.getFactory().getUri());
      assertEquals("localhost", factory.getUri().getHost());
      assertTrue(client.checkName("test"));
    }
    try (final var client = (SimpleApiClient) factory.newClient(factory.getUri())) {
      assertEquals(factory.getUri(), client.getUri());
      assertTrue(client.checkName("test"));
      client.setOpId("opid");
      assertEquals("opid", client.getOpId());
    }
    assertFalse(factory.isTls());
    factory.close();
  }

  @Test
  void check01CallInError() {
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(NOT_FOUND_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(CONFLICT_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(NOT_ACCEPTABLE_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
    }
  }

  @Test
  void check06GetObjectMetadata() {
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata("test");
      assertEquals("test", businessOut.name);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
  }
}
