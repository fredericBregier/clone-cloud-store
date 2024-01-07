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

package io.clonecloudstore.accessor.server.resource;

import java.time.Instant;
import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.MongoKafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
class AccessorBucketResourceNoS3Test {
  private static final Logger LOG = Logger.getLogger(AccessorBucketResourceNoS3Test.class);
  @Inject
  AccessorBucketApiFactory factory;
  @Inject
  Instance<DaoAccessorBucketRepository> repositoryInstance;
  DaoAccessorBucketRepository repository;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();
    DriverS3Properties.setDynamicS3Parameters("http://127.0.0.1:9999", "AccessKey", "SecretKey", "Region");
  }

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
  }

  @Test
  void testInternalExceptions() {
    final var bucketName1 = "testinternalexception1";
    final var bucketName2 = "testinternalexception2";
    // save S3 parameters for later
    try (final var client = factory.newClient()) {
      // THEN
      // Only case where No S3 leads to issue
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName1, clientId)).getStatus());
      // No S3 usage from here
      assertEquals(StorageType.NONE, assertDoesNotThrow(() -> client.checkBucket(bucketName1, clientId)));
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName2, clientId)).getStatus());
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName1, clientId)).getStatus());
      // Now same but having Bucket in DB, except creation
      final var fakebucket = "fakebucket";
      final var dao = repository.createEmptyItem();
      dao.setSite(AccessorProperties.getAccessorSite()).setCreation(Instant.now()).setName(fakebucket)
          .setId(DaoAccessorBucketRepository.getBucketTechnicalName(clientId, fakebucket))
          .setStatus(AccessorStatus.READY);
      repository.insert(dao);
      assertEquals(StorageType.NONE, client.checkBucket(bucketName1, clientId));
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName2, clientId)).getStatus());
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName1, clientId)).getStatus());
      // Now check non-existing bucket
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.createBucket(bucketName1 + "New", clientId)).getStatus());
    } catch (final CcsWithStatusException | CcsDbException e) {
      fail(e);
    }
  }
}
