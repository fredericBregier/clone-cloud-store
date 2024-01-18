/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.clonecloudstore.test.resource.azure.AzureProfile;
import io.clonecloudstore.test.resource.google.GoogleProfile;
import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.clonecloudstore.test.resource.postgres.NoPostgreDbProfile;
import io.clonecloudstore.test.resource.postgres.PostgresProfile;
import io.clonecloudstore.test.resource.pulsar.PulsarProfile;
import io.clonecloudstore.test.resource.s3.LocalStackProfile;
import io.clonecloudstore.test.resource.s3.MinioProfile;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class FakeProfileTest {

  @Test
  void testOnProfiles() {
    CommonProfile commonProfile = new AzureProfile();
    assertEquals("test-azure", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());
    assertFalse(commonProfile.getConfigOverrides().isEmpty());
    assertTrue(commonProfile.disableGlobalTestResources());

    commonProfile = new GoogleProfile();
    assertEquals("test-google", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new KafkaProfile();
    assertEquals("test-kafka", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new MongoDbProfile();
    assertEquals("test-mongo", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new NoMongoDbProfile();
    assertEquals("test-no-mongo", commonProfile.getConfigProfile());
    assertTrue(commonProfile.testResources().isEmpty());

    commonProfile = new PostgresProfile();
    assertEquals("test-db-postgre", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new NoPostgreDbProfile();
    assertEquals("test-no-postgre", commonProfile.getConfigProfile());
    assertTrue(commonProfile.testResources().isEmpty());

    commonProfile = new PulsarProfile();
    assertEquals("test-pulsar", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new MinioProfile();
    assertEquals("test-minio", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new LocalStackProfile();
    assertEquals("test-localstack", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new MongoKafkaProfile();
    assertEquals("test-mongo-kafka", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new MinioMongoProfile();
    assertEquals("test-minio-mongo", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new MinioMongoKafkaProfile();
    assertEquals("test-minio-mongo-kafka", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new MinioKafkaProfile();
    assertEquals("test-minio-kafka", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new AzureMongoProfile();
    assertEquals("test-azure-mongo", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new AzureMongoKafkaProfile();
    assertEquals("test-azure-mongo-kafka", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new AzureKafkaProfile();
    assertEquals("test-azure-kafka", commonProfile.getConfigProfile());
    assertFalse(commonProfile.testResources().isEmpty());

    commonProfile = new NoResourceProfile();
    assertEquals("test-noresource", commonProfile.getConfigProfile());
    assertTrue(commonProfile.testResources().isEmpty());
  }

  @Test
  void testTmpDir() {
    final var tmp = System.getProperty("java.io.tmpdir");
    final var subTmp = tmp + "/CCS";
    ResourcesConstants.createIfNeededTmpDataDirectory(subTmp);
    assertTrue(Files.isDirectory(Path.of(subTmp)));
    File testFile = new File(new File(subTmp), "testfile");
    try (var os = new FileOutputStream(testFile)) {
      os.write('A');
      os.flush();
    } catch (final IOException e) {
      Log.error(e);
    }
    assertTrue(Files.isRegularFile(testFile.toPath()));
    ResourcesConstants.cleanTmpDataDirectory(subTmp);
    assertFalse(Files.isRegularFile(testFile.toPath()));
    assertTrue(Files.isDirectory(Path.of(subTmp)));
    new File(subTmp).delete();
    assertFalse(Files.isDirectory(Path.of(subTmp)));
  }
}
