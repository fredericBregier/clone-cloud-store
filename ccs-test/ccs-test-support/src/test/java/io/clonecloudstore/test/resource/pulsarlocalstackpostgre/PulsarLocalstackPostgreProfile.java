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

package io.clonecloudstore.test.resource.pulsarlocalstackpostgre;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.clonecloudstore.test.resource.CommonProfile;
import io.clonecloudstore.test.resource.ResourcesConstants;
import io.clonecloudstore.test.resource.postgres.PostgresResource;
import io.clonecloudstore.test.resource.pulsar.PulsarResource;
import io.clonecloudstore.test.resource.s3.LocalStackResource;

public class PulsarLocalstackPostgreProfile extends CommonProfile {
  @Override
  public String getConfigProfile() {
    return "test-pulsar-localstack-postgre";
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, "false",
        ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, "true", ResourcesConstants.CCS_DB_TYPE,
        ResourcesConstants.POSTGRE);
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return List.of(new TestResourceEntry(LocalStackResource.class, Collections.emptyMap(), true),
        new TestResourceEntry(PulsarResource.class, Collections.emptyMap(), true),
        new TestResourceEntry(PostgresResource.class, Collections.emptyMap(), true));
  }
}
