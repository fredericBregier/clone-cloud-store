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

package io.clonecloudstore.test.resource.postgres;

import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Resource Lifecycle Manager for PostgreSQL
 */
public class PostgresResource implements QuarkusTestResourceLifecycleManager {
  public static final String DB_KIND = "postgresql";
  private static final String IMAGE_NAME = "postgres";
  private static final String DATABASE_NAME = "mydb";
  private static final String USERNAME = "myuser";
  private static final String PASSWORD = "mypwd";
  private static final PostgreSQLContainer<?> POSTGRE_SQL_CONTAINER =
      new PostgreSQLContainer<>(IMAGE_NAME).withDatabaseName(DATABASE_NAME).withUsername(USERNAME)
          .withPassword(PASSWORD);

  public static String getJdbcUrl() {
    return POSTGRE_SQL_CONTAINER.getJdbcUrl();
  }

  public static String getDatabaseName() {
    return POSTGRE_SQL_CONTAINER.getDatabaseName();
  }

  public static String getUsername() {
    return POSTGRE_SQL_CONTAINER.getUsername();
  }

  public static String getPassword() {
    return POSTGRE_SQL_CONTAINER.getPassword();
  }

  @Override
  public Map<String, String> start() {
    POSTGRE_SQL_CONTAINER.start();
    final Map<String, String> conf = new HashMap<>();
    conf.put(ResourcesConstants.QUARKUS_DATASOURCE_JDBC_URL, POSTGRE_SQL_CONTAINER.getJdbcUrl());
    conf.put(ResourcesConstants.QUARKUS_DATASOURCE_USERNAME, POSTGRE_SQL_CONTAINER.getUsername());
    conf.put(ResourcesConstants.QUARKUS_DATASOURCE_PASSWORD, POSTGRE_SQL_CONTAINER.getPassword());
    conf.put(ResourcesConstants.QUARKUS_DATASOURCE_DB_KIND, DB_KIND);
    conf.put(ResourcesConstants.QUARKUS_HIBERNATE_ORM_DATABASE_GENERATION, ResourcesConstants.DROP_AND_CREATE);
    conf.put(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, "false");
    conf.put(ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, "true");
    conf.put(ResourcesConstants.CCS_DB_TYPE, ResourcesConstants.POSTGRE);
    for (final var entry : conf.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
    conf.putAll(POSTGRE_SQL_CONTAINER.getEnvMap());
    return conf;
  }

  @Override
  public void stop() {
    POSTGRE_SQL_CONTAINER.stop();
  }
}
