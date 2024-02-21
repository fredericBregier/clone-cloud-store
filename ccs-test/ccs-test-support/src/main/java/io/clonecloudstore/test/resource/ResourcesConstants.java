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

package io.clonecloudstore.test.resource;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * All constants for Resources
 */
public class ResourcesConstants {
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String PULSAR_CLIENT_SERVICE_URL = "pulsar.client.serviceUrl";
  public static final String QUARKUS_MONGODB_CONNECTION_STRING = "quarkus.mongodb.connection-string";
  public static final String QUARKUS_DEVSERVICES_ENABLED = "quarkus.devservices.enabled";
  public static final String QUARKUS_HIBERNATE_ORM_ENABLED = "quarkus.hibernate-orm.enabled";
  public static final String QUARKUS_DATASOURCE_JDBC_URL = "quarkus.datasource.jdbc.url";
  public static final String QUARKUS_DATASOURCE_USERNAME = "quarkus.datasource.username";
  public static final String QUARKUS_DATASOURCE_PASSWORD = "quarkus.datasource.password";
  public static final String QUARKUS_DATASOURCE_DB_KIND = "quarkus.datasource.db-kind";
  public static final String QUARKUS_HIBERNATE_ORM_DATABASE_GENERATION = "quarkus.hibernate-orm.database.generation";
  public static final String DROP_AND_CREATE = "drop-and-create";
  public static final String QUARKUS_S_3_ENDPOINT_OVERRIDE = "quarkus.s3.endpoint-override";
  public static final String QUARKUS_S_3_AWS_CREDENTIALS_TYPE = "quarkus.s3.aws.credentials.type";
  public static final String QUARKUS_S_3_AWS_CREDENTIALS_STATIC_PROVIDER_ACCESS_KEY_ID =
      "quarkus.s3.aws.credentials.static-provider.access-key-id";
  public static final String QUARKUS_S_3_AWS_CREDENTIALS_STATIC_PROVIDER_SECRET_ACCESS_KEY =
      "quarkus.s3.aws.credentials.static-provider.secret-access-key";
  public static final String QUARKUS_S_3_AWS_REGION = "quarkus.s3.aws.region";
  public static final String QUARKUS_AZURE_DEVSERVICES = "quarkus.azure.storage.blob.devservices.enabled";
  public static final String QUARKUS_AZURE_CONNECTION_STRING = "quarkus.azure.storage.blob.connection-string";
  public static final String QUARKUS_GOOGLE_PROJECT = "quarkus.google.cloud.project-id";
  public static final String QUARKUS_GOOGLE_HOST = "quarkus.google.cloud.storage.host-override";
  public static final String CCS_DB_TYPE = "ccs.db.type";
  public static final String MONGO = "mongo";
  public static final String POSTGRE = "postgre";
  public static final String TMP_DATA = "/tmp/data";

  private ResourcesConstants() {
    // Empty
  }

  public static void createIfNeededTmpDataDirectory(final String path) {
    File data = new File(path);
    if (!data.isDirectory()) {
      data.mkdirs(); // NOSONAR
    }
  }

  public static void cleanTmpDataDirectory(final String path) {
    File data = new File(path);
    if (data.isDirectory()) {
      File[] allContents = data.listFiles();
      if (allContents != null) {
        for (final var other : allContents) {
          if (other.isDirectory()) {
            try {
              FileUtils.deleteDirectory(other);
            } catch (final IOException ignore) {
              // Ignore
            }
          } else {
            other.delete(); // NOSONAR
          }
        }
      }
    }
  }
}
