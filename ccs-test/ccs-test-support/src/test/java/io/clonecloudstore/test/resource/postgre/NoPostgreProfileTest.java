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

package io.clonecloudstore.test.resource.postgre;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.clonecloudstore.test.resource.postgres.NoPostgreDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(NoPostgreDbProfile.class)
class NoPostgreProfileTest {

  @Test
  void checkEmpty() {
    Assertions.assertEquals(ResourcesConstants.POSTGRE,
        ConfigProvider.getConfig().getOptionalValue(ResourcesConstants.CCS_DB_TYPE, String.class).get());
    assertEquals("false",
        ConfigProvider.getConfig().getValue(ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, String.class));
  }
}
