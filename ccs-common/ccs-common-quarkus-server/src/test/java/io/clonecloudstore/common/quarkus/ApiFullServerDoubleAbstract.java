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

package io.clonecloudstore.common.quarkus;

import java.io.IOException;

import io.clonecloudstore.common.quarkus.example.client.ApiQuarkusClientFactory;
import io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.stream.FakeInputStream;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.PROXY_COMP_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.PROXY_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.ULTRA_COMPRESSION_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

abstract class ApiFullServerDoubleAbstract {
  private static final Logger LOG = Logger.getLogger(ApiFullServerDoubleAbstract.class);

  @Inject
  ApiQuarkusClientFactory factory;

  @BeforeAll
  static void beforeAll() {
    QuarkusProperties.setServerComputeSha256(false);
  }

  @BeforeEach
  void beforeEach() {
    QuarkusProperties.setServerComputeSha256(false);
    slowdown();
  }

  private void slowdown() {
    try {
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      // Ignore
    }
  }

  void check30PostInputStreamQuarkusDoubleTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'),
              ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check31PostInputStreamQuarkusDoubleNoSizeTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), 0, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true,
              false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check32PostInputStreamQuarkusShaDoubleTest() {
    QuarkusProperties.setServerComputeSha256(true);
    slowdown();
    slowdown();
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'),
              ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check33PostInputStreamQuarkusShaDoubleNoSizeTest() {
    QuarkusProperties.setServerComputeSha256(true);
    slowdown();
    slowdown();
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), 0, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true,
              false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check34GetInputStreamQuarkusDoubleTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + "test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(PROXY_TEST + "test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + ULTRA_COMPRESSION_TEST + "test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check35GetInputStreamQuarkusDoubleNoSizeTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(PROXY_TEST + "test", 0, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(PROXY_TEST + "test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + ULTRA_COMPRESSION_TEST + "test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check35WrongPostInputStreamQuarkusDoubleTest() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    slowdown();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
  }

  void check36WrongPostInputStreamQuarkusNoSizeDoubleTest() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN), 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    slowdown();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
  }

  void check37WrongGetInputStreamQuarkusDoubleTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    // Redo to check multiple access
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
  }

  void check38WrongGetInputStreamQuarkusNoSizeDoubleTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + ApiQuarkusService.NOT_FOUND_NAME, 0, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    // Redo to check multiple access
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + ApiQuarkusService.NOT_FOUND_NAME, 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
  }

  void check41PostInputStreamQuarkusDoubleCompressedIntraTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'),
              ApiQuarkusService.LEN, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN),
              ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'),
              ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check42PostInputStreamQuarkusDoubleNoSizeCompressedIntraTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check43PostInputStreamQuarkusShaDoubleCompressedIntraTest() {
    QuarkusProperties.setServerComputeSha256(true);
    slowdown();
    slowdown();
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'),
              ApiQuarkusService.LEN, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN),
              ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'),
              ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check44PostInputStreamQuarkusShaDoubleNoSizeCompressedIntraTest() {
    QuarkusProperties.setServerComputeSha256(true);
    slowdown();
    slowdown();
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_COMP_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check45WrongPostInputStreamQuarkusDoubleCompressedIntraTest() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_COMP_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    slowdown();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_COMP_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_COMP_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
  }

  void check46WrongPostInputStreamQuarkusNoSizeDoubleCompressedIntraTest() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_COMP_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    slowdown();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_COMP_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(PROXY_COMP_TEST + ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
  }

  void check47WrongGetInputStreamQuarkusDoubleCompressedIntraTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, false,
              false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    // Redo to check multiple access
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
  }

  void check48WrongGetInputStreamQuarkusNoSizeDoubleCompressedIntraTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + ApiQuarkusService.NOT_FOUND_NAME, 0, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    // Redo to check multiple access
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + ApiQuarkusService.NOT_FOUND_NAME, 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
  }

  void check50GetInputStreamQuarkusDoubleCompressedIntraTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + "test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + "test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + ULTRA_COMPRESSION_TEST + "test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  void check51GetInputStreamQuarkusDoubleNoSizeCompressedIntraTest() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(PROXY_COMP_TEST + "test", 0, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(PROXY_COMP_TEST + "test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    slowdown();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_COMP_TEST + ULTRA_COMPRESSION_TEST + "test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

}
