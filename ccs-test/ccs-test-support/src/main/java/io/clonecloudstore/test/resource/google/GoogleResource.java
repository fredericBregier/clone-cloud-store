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

package io.clonecloudstore.test.resource.google;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.utility.DockerImageName;

/**
 * Resource Lifecycle Manager for Fake Gcs Server (for Google Cloud Storage)
 */
public class GoogleResource implements QuarkusTestResourceLifecycleManager {
  public static final String IMAGE = "fsouza/fake-gcs-server:latest";
  static final int EXPOSED_PORT = 4443;
  private static final String PROTOCOL = "http";
  private static final GoogleContainer GOOGLE_CONTAINER =
      new GoogleContainer(DockerImageName.parse(IMAGE), OptionalInt.empty());
  private static final Map<String, String> conf = new HashMap<>();

  private static String getConnectionString(String host, int port) {
    return String.format("%s://%s:%d", PROTOCOL, host, port);
  }

  public static String getConnectionString() {
    return getConnectionString(GOOGLE_CONTAINER.getHost(), GOOGLE_CONTAINER.getPort());
  }

  private static void updateExternalUrlWithContainerUrl(String fakeGcsExternalUrl)
      throws IOException, InterruptedException {
    String modifyExternalUrlRequestUri = fakeGcsExternalUrl + "/_internal/config";
    String updateExternalUrlJson = "{" + "\"externalUrl\": \"" + fakeGcsExternalUrl + "\"" + "}";

    HttpRequest req =
        HttpRequest.newBuilder().uri(URI.create(modifyExternalUrlRequestUri)).header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(updateExternalUrlJson)).build();
    HttpResponse<Void> response = HttpClient.newBuilder().build().send(req, HttpResponse.BodyHandlers.discarding());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "error updating fake-gcs-server with external url, response status code " + response.statusCode() +
              " != 200");
    }
  }

  @Override
  public Map<String, String> start() {
    if (!GOOGLE_CONTAINER.isRunning()) {
      GOOGLE_CONTAINER.start();
    }
    try {
      updateExternalUrlWithContainerUrl(getConnectionString());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    conf.put(ResourcesConstants.QUARKUS_GOOGLE_HOST, getConnectionString());
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(GOOGLE_CONTAINER.getEnvMap());

    return conf;
  }

  @Override
  public void stop() {
    GOOGLE_CONTAINER.stop();
  }
}
