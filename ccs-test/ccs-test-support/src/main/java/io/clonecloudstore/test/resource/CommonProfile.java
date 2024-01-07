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

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public abstract class CommonProfile implements QuarkusTestProfile {

  public static final String FALSE = "false";

  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, FALSE,
        ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, FALSE, ResourcesConstants.CCS_DB_TYPE,
        ResourcesConstants.MONGO, ResourcesConstants.QUARKUS_AZURE_DEVSERVICES, FALSE,
        ResourcesConstants.QUARKUS_AZURE_CONNECTION_STRING, "http", ResourcesConstants.QUARKUS_GOOGLE_PROJECT,
        "test-ccs", ResourcesConstants.QUARKUS_GOOGLE_HOST, "http://localhost:10000");
  }

  @Override
  public boolean disableGlobalTestResources() {
    return true;
  }
}
