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

package io.clonecloudstore.test.resource.mongodb;

import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Resource Lifecycle Manager for MongoDB
 */
public class MongoDbResource implements QuarkusTestResourceLifecycleManager {
  private static final String IMAGE_NAME = "mongo:latest";
  private static final MongoDBContainer MONGO_DB_CONTAINER = new MongoDBContainer(DockerImageName.parse(IMAGE_NAME));

  public static String getConnectionString() {
    return MONGO_DB_CONTAINER.getConnectionString();
  }

  @Override
  public Map<String, String> start() {
    if (!MONGO_DB_CONTAINER.isRunning()) {
      MONGO_DB_CONTAINER.start();
    }
    final Map<String, String> conf = new HashMap<>();
    conf.put(ResourcesConstants.QUARKUS_MONGODB_CONNECTION_STRING, MONGO_DB_CONTAINER.getConnectionString());
    conf.put(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, "false");
    conf.put(ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, "false");
    conf.put(ResourcesConstants.CCS_DB_TYPE, ResourcesConstants.MONGO);
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(MONGO_DB_CONTAINER.getEnvMap());
    return conf;
  }

  @Override
  public void stop() {
    MONGO_DB_CONTAINER.stop();
  }
}
