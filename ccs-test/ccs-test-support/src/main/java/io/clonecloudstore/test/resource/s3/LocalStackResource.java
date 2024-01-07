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

package io.clonecloudstore.test.resource.s3;

import java.util.Map;

import io.quarkus.logging.Log;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class LocalStackResource implements QuarkusTestResourceLifecycleManager {
  public static final LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest")).withServices(S3)
          .withEnv(Map.of("EAGER_SERVICE_LOADING", "1", "LS_LOG", "warn"));

  public static String getAccessKey() {
    return localstack.getAccessKey();
  }

  public static String getSecretKey() {
    return localstack.getSecretKey();
  }

  public static String getUrlString() {
    return localstack.getEndpointOverride(S3).toString();
  }

  public static String getRegion() {
    return localstack.getRegion();
  }

  @Override
  public Map<String, String> start() {
    if (!localstack.isRunning()) {
      localstack.start();
    }
    // Ensure container is started
    while (!(localstack.isRunning() && localstack.isCreated() && localstack.isHealthy())) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Log.infof("LS %b %b %b %b %s", localstack.isRunning(), localstack.isHostAccessible(), localstack.isCreated(),
        localstack.isHealthy(), localstack.getEnvMap());
    return localstack.getEnvMap();
  }

  @Override
  public void stop() {
    localstack.stop();
  }
}
