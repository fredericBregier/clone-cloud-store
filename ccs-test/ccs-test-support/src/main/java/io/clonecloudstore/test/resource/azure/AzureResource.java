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

package io.clonecloudstore.test.resource.azure;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.utility.DockerImageName;

/**
 * Resource Lifecycle Manager for Azurite (for Azure Blob Storage)
 */
public class AzureResource implements QuarkusTestResourceLifecycleManager {
  public static final String IMAGE = "mcr.microsoft.com/azure-storage/azurite:latest";
  static final int EXPOSED_PORT = 10000;
  private static final String PROTOCOL = "http";
  private static final String ACCOUNT_NAME = "devstoreaccount1";
  private static final String ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  /**
   * Label to add to shared Dev Services for Azurite storage blob service running in containers.
   * This allows other applications to discover the running service and use it instead of starting a new instance.
   */
  static final String DEV_SERVICE_LABEL = "quarkus-dev-service-azure-storage-blob";
  private static final AzureContainer AZURE_CONTAINER =
      new AzureContainer(DockerImageName.parse(IMAGE), OptionalInt.empty(), DEV_SERVICE_LABEL);
  private static final Map<String, String> conf = new HashMap<>();

  private static String getBlobEndpoint(String host, int port) {
    return String.format("%s://%s:%s/%s", PROTOCOL, host, port, ACCOUNT_NAME);
  }

  private static String getConnectionString(String host, int port) {
    String blobEndpoint = getBlobEndpoint(host, port);
    return String.format("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;", PROTOCOL,
        ACCOUNT_NAME, ACCOUNT_KEY, blobEndpoint);
  }

  public static String getConnectionString() {
    return getConnectionString(AZURE_CONTAINER.getHost(), AZURE_CONTAINER.getPort());
  }

  @Override
  public Map<String, String> start() {
    if (!AZURE_CONTAINER.isRunning()) {
      AZURE_CONTAINER.start();
    }
    conf.put(ResourcesConstants.QUARKUS_AZURE_CONNECTION_STRING, getConnectionString());
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(AZURE_CONTAINER.getEnvMap());
    return conf;
  }

  @Override
  public void stop() {
    AZURE_CONTAINER.stop();
  }
}
