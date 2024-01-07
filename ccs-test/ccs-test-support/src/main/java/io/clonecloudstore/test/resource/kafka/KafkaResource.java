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

package io.clonecloudstore.test.resource.kafka;

import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Resource Lifecycle Manager for Kafka
 */
public class KafkaResource implements QuarkusTestResourceLifecycleManager {
  // As in Kafka, uses Redpanda since faster as test container
  private static final String IMAGE_NAME = "confluentinc/cp-kafka";
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse(IMAGE_NAME));

  public static String getBootstrapServers() {
    return KAFKA_CONTAINER.getBootstrapServers();
  }

  @Override
  public Map<String, String> start() {
    if (!KAFKA_CONTAINER.isRunning()) {
      KAFKA_CONTAINER.start();
    }
    final Map<String, String> conf = new HashMap<>();
    conf.put(ResourcesConstants.KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONTAINER.getBootstrapServers());
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(KAFKA_CONTAINER.getEnvMap());
    return conf;
  }

  @Override
  public void stop() {
    KAFKA_CONTAINER.stop();
  }
}
