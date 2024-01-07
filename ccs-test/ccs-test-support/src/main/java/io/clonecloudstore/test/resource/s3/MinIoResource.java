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

import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Resource Lifecycle Manager for MinIo (for S3)
 */
public class MinIoResource implements QuarkusTestResourceLifecycleManager {
  private static final String ACCESS_KEY = "accessKey";
  private static final String SECRET_KEY = "secretKey";
  private static final String EU_WEST_1 = "eu-west-1";
  private static final MinIOContainer MINIO_CONTAINER =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withUserName(ACCESS_KEY).withPassword(SECRET_KEY);
  private static final Map<String, String> conf = new HashMap<>();

  static {
    conf.put(ResourcesConstants.QUARKUS_S_3_AWS_CREDENTIALS_TYPE, "static");
    conf.put(ResourcesConstants.QUARKUS_S_3_AWS_CREDENTIALS_STATIC_PROVIDER_ACCESS_KEY_ID,
        MINIO_CONTAINER.getUserName());
    conf.put(ResourcesConstants.QUARKUS_S_3_AWS_CREDENTIALS_STATIC_PROVIDER_SECRET_ACCESS_KEY,
        MINIO_CONTAINER.getPassword());
    conf.put(ResourcesConstants.QUARKUS_S_3_AWS_REGION, getRegion());
  }

  public static String getAccessKey() {
    return MINIO_CONTAINER.getUserName();
  }

  public static String getSecretKey() {
    return MINIO_CONTAINER.getPassword();
  }

  public static String getUrlString() {
    return MINIO_CONTAINER.getS3URL();
  }

  public static String getRegion() {
    return EU_WEST_1;
  }

  @Override
  public Map<String, String> start() {
    if (!MINIO_CONTAINER.isRunning()) {
      MINIO_CONTAINER.start();
    }
    conf.put(ResourcesConstants.QUARKUS_S_3_ENDPOINT_OVERRIDE, MINIO_CONTAINER.getS3URL());
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(MINIO_CONTAINER.getEnvMap());
    return conf;
  }

  @Override
  public void stop() {
    MINIO_CONTAINER.stop();
  }
}
