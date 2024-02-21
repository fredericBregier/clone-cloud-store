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

package io.clonecloudstore.driver.azure.example;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.azure.DriverAzureApiFactory;
import io.clonecloudstore.driver.azure.DriverAzureProperties;
import io.clonecloudstore.test.resource.azure.AzureProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.spi.CDI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.driver.azure.DriverAzureProperties.DEFAULT_MAX_SIZE_NOT_PART;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@TestProfile(AzureProfile.class)
class ObjectClientAzureTest extends ObjectClientAzureBase {

  @BeforeAll
  static void setup() throws InterruptedException {
    // Sometime, test failed on DriverApiFactory not setup
    if (DriverApiRegistry.getDriverApiFactory() == null) {
      // Ensure container is started
      Thread.sleep(500);
      DriverApiRegistry.setDriverApiFactory(CDI.current().select(DriverAzureApiFactory.class).get());
    }
    old = QuarkusProperties.serverComputeSha256();
    QuarkusProperties.setServerComputeSha256(false);
    try (final var digestInputStream = new MultipleActionsInputStream(getPseudoInputStreamForSha(bigLen),
        DigestAlgo.SHA256)) {
      FakeInputStream.consumeAll(digestInputStream);
      sha256 = digestInputStream.getDigestBase32();
    } catch (IOException | NoSuchAlgorithmException e) {
      fail(e);
    }
  }

  @AfterAll
  static void endTests() {
    DriverAzureProperties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(old);
  }
}
