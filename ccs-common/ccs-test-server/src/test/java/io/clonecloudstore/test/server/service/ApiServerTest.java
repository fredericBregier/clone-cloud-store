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

package io.clonecloudstore.test.server.service;

import java.io.IOException;
import java.util.Arrays;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.server.service.example.client.ApiClientFactory;
import io.clonecloudstore.test.server.service.example.client.ApiConstants;
import io.clonecloudstore.test.server.service.example.model.ApiBusinessOut;
import io.clonecloudstore.test.server.service.example.server.ApiService;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class ApiServerTest {
  private static final Logger LOG = Logger.getLogger(ApiServerTest.class);

  @Inject
  ApiClientFactory factory;

  @BeforeAll
  static void beforeAll() {
    QuarkusProperties.setServerComputeSha256(false);
  }

  @BeforeEach
  void beforeEach() {
    QuarkusProperties.setServerComputeSha256(false);
    try {
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      // Ignore
    }
  }

  @Test
  void check99Service() {
    try (final var client = factory.newClient()) {
      assertTrue(client.checkName("test"));
    }
    factory.close();
  }

  @Test
  void check03GetObjectMetadata() {
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
  void check10PostInputStream() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiService.LEN), ApiService.LEN, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check11PostInputStreamNoSize() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream("test", new FakeInputStream(ApiService.LEN), 0, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check11PostInputStreamNotChunked() {
    final var len = 5 * 1024 * 1024; // Max 10 MB by default
    {
      // Content-Length set to empty or -1
      final var start = System.nanoTime();
      final var businessOut =
          given().header(ApiConstants.X_NAME, "test").contentType(MediaType.APPLICATION_OCTET_STREAM)
              .header(X_OP_ID, "1").body(new FakeInputStream(len)).when()
              .post(ApiConstants.API_ROOT + ApiConstants.API_COLLECTIONS).then().statusCode(201).extract()
              .as(ApiBusinessOut.class);
      assertEquals("test", businessOut.name);
      assertEquals(len, businessOut.len);
      assertNotNull(businessOut.creationDate);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ((float) len) / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0));
    }
    if (false) {
      // Content-Length set array size
      final var start = System.nanoTime();
      final var content = new byte[len];
      Arrays.fill(content, 0, len, (byte) 'A');
      final var businessOut =
          given().header(ApiConstants.X_NAME, "test").contentType(MediaType.APPLICATION_OCTET_STREAM)
              .header(X_OP_ID, "2").body(content).when().post(ApiConstants.API_ROOT + ApiConstants.API_COLLECTIONS)
              .then().statusCode(201).extract().as(ApiBusinessOut.class);
      assertEquals("test", businessOut.name);
      assertEquals(len, businessOut.len);
      assertNotNull(businessOut.creationDate);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ((float) len) / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0));
    }
  }

  @Test
  void check16GetInputStream() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", ApiService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }
}
