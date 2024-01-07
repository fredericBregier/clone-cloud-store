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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.clonecloudstore.common.quarkus.example.client.ApiQuarkusClientFactory;
import io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.PROXY_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@Disabled("Only for checking")
public class ApiServerBenchRunIT {
  private static final Logger LOG = Logger.getLogger(ApiServerBenchRunIT.class);
  final Random random = new Random();
  @Inject
  ApiQuarkusClientFactory factory;

  public static void beforeAll0() {
    QuarkusProperties.setServerComputeSha256(false);
  }

  @BeforeEach
  public void beforeEach() {
    QuarkusProperties.setServerComputeSha256(false);
    try {
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      // Ignore
    }
  }

  void benchQ(final Map<String, Double> result) {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, false,
              false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      // fail(e);
    }
    var stop = System.nanoTime();
    double maxSpeed = 0;
    var speed = ApiQuarkusService.LEN / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0);
    LOG.info("Speed (MB/s): " + speed);
    maxSpeed += speed;

    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      // fail(e);
    }
    stop = System.nanoTime();
    speed = ApiQuarkusService.LEN / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0);
    LOG.info("Speed (MB/s): " + speed);
    maxSpeed += speed;
    final var name = "S_" + (QuarkusProperties.getBufSize() / 1024) + "K";
    if (!result.containsKey(name) || result.get(name) < maxSpeed) {
      result.put(name, maxSpeed);
    }
  }

  @Test
  void check92QuarkusBenchmark() {
    final Map<String, Double> result = new HashMap<>();
    final var oldBufSize = QuarkusProperties.getBufSize();
    try {
      var bufSize = 64 * 1024;
      QuarkusProperties.setBufSize(bufSize);
      LOG.infof("Warm up with bufSize %d", QuarkusProperties.getBufSize());
      benchQ(result);
      benchQ(result);
      for (var j = 0; j < 5; j++) {
        for (var i = 0; i < 3; i++) {
          bufSize = i * 32 * 1024 + 64 * 1024;
          QuarkusProperties.setBufSize(bufSize);
          LOG.infof("Start with bufSize %d", QuarkusProperties.getBufSize());
          benchQ(result);
        }
      }

      final Map<String, Double> sorted =
          result.entrySet().stream().sorted(Comparator.comparingDouble(e -> e.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k, v) -> k, LinkedHashMap::new));
      LOG.info(sorted);
    } finally {
      QuarkusProperties.setBufSize(oldBufSize);
    }
  }

  private void threadWaitRandom() {
    try {
      Thread.sleep(random.nextInt(10, 50));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void check95QuarkusConcurrentBenchmark() throws InterruptedException {
    final Map<String, Double> result = new ConcurrentHashMap<>();
    final var oldBufSize = QuarkusProperties.getBufSize();
    try {
      benchQ(result);
      benchQ(result);

      ExecutorService executorService = Executors.newFixedThreadPool(4);
      for (var j = 0; j < 4; j++) {
        executorService.execute(() -> {
          for (var i = 0; i < 3; i++) {
            var bufSize = i * 32 * 1024 + 64 * 1024;
            QuarkusProperties.setBufSize(bufSize);
            for (int k = 0; k < 3; k++) {
              benchQ(result);
            }
          }
        });
      }
      executorService.shutdown();
      executorService.awaitTermination(1000, TimeUnit.SECONDS);

      final Map<String, Double> sorted =
          result.entrySet().stream().sorted(Comparator.comparingDouble(e -> e.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k, v) -> k, LinkedHashMap::new));
      LOG.info(sorted);
    } finally {
      QuarkusProperties.setBufSize(oldBufSize);
    }
  }

  @Test
  void check96QuarkusDoubleConcurrentBenchmark() throws InterruptedException {
    final Map<String, Double> result = new ConcurrentHashMap<>();
    final var oldBufSize = QuarkusProperties.getBufSize();
    try {
      bench2Q(result);
      bench2Q(result);

      ExecutorService executorService = Executors.newFixedThreadPool(4);
      for (var j = 0; j < 4; j++) {
        executorService.execute(() -> {
          for (int k = 0; k < 5; k++) {
            bench2Q(result);
          }
        });
      }
      executorService.shutdown();
      executorService.awaitTermination(1000, TimeUnit.SECONDS);

      final Map<String, Double> sorted =
          result.entrySet().stream().sorted(Comparator.comparingDouble(e -> e.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k, v) -> k, LinkedHashMap::new));
      LOG.info(sorted);
    } finally {
      QuarkusProperties.setBufSize(oldBufSize);
    }
  }

  void bench2Q(final Map<String, Double> result) {
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
      // fail(e);
    }
    var stop = System.nanoTime();
    double maxSpeed = 0;
    var speed = ApiQuarkusService.LEN / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0);
    LOG.info("Speed (MB/s): " + speed);
    maxSpeed += speed;
    threadWaitRandom();
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + "test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      // fail(e);
    }
    stop = System.nanoTime();
    speed = ApiQuarkusService.LEN / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0);
    LOG.info("Speed (MB/s): " + speed);
    maxSpeed += speed;
    final var name = "S_" + (QuarkusProperties.getBufSize() / 1024) + "K";
    if (!result.containsKey(name) || result.get(name) < maxSpeed) {
      result.put(name, maxSpeed);
    }
    threadWaitRandom();
  }

  @Test
  void check93QuarkusDoubleBenchmark() {
    final Map<String, Double> result = new HashMap<>();
    final var oldBufSize = QuarkusProperties.getBufSize();
    try {
      var bufSize = 64 * 1024;
      QuarkusProperties.setBufSize(bufSize);
      LOG.infof("Warm up with bufSize %d", QuarkusProperties.getBufSize());
      bench2Q(result);
      bench2Q(result);
      for (var j = 0; j < 5; j++) {
        for (var i = 0; i < 3; i++) {
          bufSize = i * 32 * 1024 + 64 * 1024;
          QuarkusProperties.setBufSize(bufSize);
          LOG.infof("Start with bufSize %d", QuarkusProperties.getBufSize());
          bench2Q(result);
        }
      }

      final Map<String, Double> sorted =
          result.entrySet().stream().sorted(Comparator.comparingDouble(e -> e.getValue()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (k, v) -> k, LinkedHashMap::new));
      LOG.info(sorted);
    } finally {
      QuarkusProperties.setBufSize(oldBufSize);
    }
  }
}
