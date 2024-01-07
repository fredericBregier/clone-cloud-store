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

package io.clonecloudstore.test.resource.pulsar;

import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Resource Lifecycle Manager for Pulsar
 */
public class PulsarResource implements QuarkusTestResourceLifecycleManager {
  private static final String IMAGE_NAME = "apachepulsar/pulsar";
  private static final PulsarContainer PULSAR_CONTAINER = new PulsarContainer(DockerImageName.parse(IMAGE_NAME));

  public static String getPulsarBrokerUrl() {
    return PULSAR_CONTAINER.getPulsarBrokerUrl();
  }

  @Override
  public Map<String, String> start() {
    if (!PULSAR_CONTAINER.isRunning()) {
      PULSAR_CONTAINER.start();
    }
    final Map<String, String> conf = new HashMap<>();
    conf.put(ResourcesConstants.PULSAR_CLIENT_SERVICE_URL, PULSAR_CONTAINER.getPulsarBrokerUrl());
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(PULSAR_CONTAINER.getEnvMap());
    return conf;
  }

  @Override
  public void stop() {
    PULSAR_CONTAINER.stop();
  }
}
