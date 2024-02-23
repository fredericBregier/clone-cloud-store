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

package io.clonecloudstore.driver.google;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.google.example.client.ApiClientFactory;
import io.clonecloudstore.test.resource.google.GoogleProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.spi.CDI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@TestProfile(GoogleProfile.class)
public class DriverGoogleTest extends DriverGoogleBase {

  @BeforeAll
  static void setup() throws InterruptedException {
    // Sometime, test failed on DriverApiFactory not setup
    if (DriverApiRegistry.getDriverApiFactory() == null) {
      // Ensure container is started
      Thread.sleep(100);
      DriverApiRegistry.setDriverApiFactory(CDI.current().select(DriverGoogleApiFactory.class).get());
    }
    oldSha = QuarkusProperties.serverComputeSha256();
    factory = new ApiClientFactory();
  }

  @AfterAll
  static void endTests() {
    factory.close();
    DriverGoogleProperties.setDynamicPartSize(DriverGoogleProperties.DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(oldSha);
  }
}
