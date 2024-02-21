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

import java.io.IOException;

import io.clonecloudstore.common.quarkus.client.example.ApiClient;
import io.clonecloudstore.common.quarkus.client.example.ApiClientFactory;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.CONFLICT_NAME;
import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.LEN;
import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.NOT_ACCEPTABLE_NAME;
import static io.clonecloudstore.common.quarkus.client.example.server.ApiService.NOT_FOUND_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class ApiClientTest {
  private static final Logger LOG = Logger.getLogger(ApiClientTest.class);

  @Inject
  ApiClientFactory factory;

  @Test
  void check99Service() {
    try (final var client = factory.newClient()) {
      assertEquals(factory.getUri(), client.getFactory().getUri());
      assertEquals("localhost", factory.getUri().getHost());
      assertTrue(client.checkName("test"));
    }
    try (final var client = (ApiClient) factory.newClient(factory.getUri())) {
      assertEquals(factory.getUri(), client.getUri());
      assertTrue(client.checkName("test"));
    }
    factory.close();
  }

  @Test
  void check40CallInError() {
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(NOT_FOUND_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getStatus());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(CONFLICT_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
      assertEquals(Response.Status.CONFLICT.getStatusCode(), e.getStatus());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(NOT_ACCEPTABLE_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
      assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), e.getStatus());
    }
  }

  @Test
  void check20WrongPostInputStream() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(CONFLICT_NAME, new FakeInputStream(LEN, (byte) 'A'), LEN);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check30WrongGetInputStream() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStream = client.getInputStreamBusinessOut(NOT_FOUND_NAME, LEN);
      final var len = FakeInputStream.consumeAll(inputStream.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException | IOException e) {
      LOG.info("Error received: " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check04PostInputStream() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream("test", new FakeInputStream(LEN, (byte) 'A'), LEN);
      assertEquals("test", businessOut.name);
      assertEquals(LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check05GetInputStream() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStream = client.getInputStreamBusinessOut("test", LEN);
      final var len = FakeInputStream.consumeAll(inputStream.inputStream());
      assertEquals(LEN, len);
      assertEquals("test", inputStream.dtoOut().name);
      assertEquals(LEN, inputStream.dtoOut().len);
      if (inputStream.inputStream() instanceof MultipleActionsInputStream mai) {
        LOG.infof("DEBUG %s", mai);
      }
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
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

  @Test
  void check90Retry() {
    check04PostInputStream();
    check05GetInputStream();
    check20WrongPostInputStream();
    check04PostInputStream();
    check05GetInputStream();
    check30WrongGetInputStream();
    check04PostInputStream();
    check05GetInputStream();
  }
}
