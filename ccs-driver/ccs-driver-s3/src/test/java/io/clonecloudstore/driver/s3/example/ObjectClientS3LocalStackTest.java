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

package io.clonecloudstore.driver.s3.example;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.system.SysErrLogger;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.s3.DriverS3ApiFactory;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.s3.LocalStackProfile;
import io.clonecloudstore.test.resource.s3.LocalStackResource;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.spi.CDI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.localstack.LocalStackContainer;

import static io.clonecloudstore.driver.s3.DriverS3Properties.DEFAULT_MAX_SIZE_NOT_PART;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@TestProfile(LocalStackProfile.class)
class ObjectClientS3LocalStackTest extends ObjectClientS3Base {
  @BeforeAll
  static void setup() throws InterruptedException {
    // Sometime, test failed on DriverApiFactory not setup
    if (DriverApiRegistry.getDriverApiFactory() == null) {
      DriverApiRegistry.setDriverApiFactory(CDI.current().select(DriverS3ApiFactory.class).get());
    }
    // Simulate loading the Factory from Class name
    DriverS3Properties.setDynamicS3Parameters(
        LocalStackResource.localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
        LocalStackResource.localstack.getAccessKey(), LocalStackResource.localstack.getSecretKey(),
        LocalStackResource.localstack.getRegion());
    old = QuarkusProperties.serverComputeSha256();
    try (final var digestInputStream = new MultipleActionsInputStream(getPseudoInputStreamForSha(bigLen),
        DigestAlgo.SHA256)) {
      FakeInputStream.consumeAll(digestInputStream);
      sha256 = digestInputStream.getDigestBase32();
    } catch (IOException | NoSuchAlgorithmException e) {
      fail(e);
    }
    if (DriverApiRegistry.getDriverApiFactory() == null) {
      Log.error(SysErrLogger.red("Driver not initialized"));
    }
  }

  @AfterAll
  static void endTests() {
    DriverS3Properties.setDynamicPartSize(DEFAULT_MAX_SIZE_NOT_PART);
    QuarkusProperties.setServerComputeSha256(old);
  }
}
