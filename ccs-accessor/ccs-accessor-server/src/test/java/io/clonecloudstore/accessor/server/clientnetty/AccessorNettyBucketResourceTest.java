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

package io.clonecloudstore.accessor.server.clientnetty;

import java.time.Instant;
import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.MinioMongoKafkaProfile;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(MinioMongoKafkaProfile.class)
@Disabled("Wait for Netty client implementation")
class AccessorNettyBucketResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorNettyBucketResourceTest.class);
  @Inject
  AccessorBucketApiFactory factory;
  @Inject
  Instance<DaoAccessorBucketRepository> repositoryInstance;
  DaoAccessorBucketRepository repository;
  @Inject
  DriverApiFactory driverApiFactory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();

    // Bug fix on "localhost"
    var url = MinIoResource.getUrlString();
    if (url.contains("localhost")) {
      url = url.replace("localhost", "127.0.0.1");
    }
    DriverS3Properties.setDynamicS3Parameters(url, MinIoResource.getAccessKey(), MinIoResource.getSecretKey(),
        MinIoResource.getRegion());
  }

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
  }

  @Test
  void createBucket() {
    final var bucketName = "testcreatebucket1";
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      Assertions.assertEquals(bucketName, bucket.getName());
      //Test already exist Exception
      assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName, clientId));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getBucket() {
    final var bucketName = "testgetbucket1";
    try (final var client = factory.newClient()) {
      client.createBucket(bucketName, clientId);
      final var bucket = client.getBucket(bucketName, clientId);
      Assertions.assertEquals(bucketName, bucket.getName());

      // Check bucket not exist
      final var bucketUnknownName = "unknown";
      final var unknownException =
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketUnknownName, clientId));
      assertEquals(404, unknownException.getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getBuckets() {
    try (final var client = factory.newClient()) {
      final var bucketsBeforeTest = client.getBuckets(clientId);
      final var numberBucketBeforeInsert = bucketsBeforeTest.size();
      client.createBucket("testgetbuckets1", clientId);
      client.createBucket("testgetbuckets2", clientId);
      client.createBucket("testgetbuckets3", clientId);
      final var buckets = client.getBuckets(clientId);
      assertEquals(numberBucketBeforeInsert + 3, buckets.size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void deleteBucket() {
    final var bucketName = "testdeletebucket1";
    try (final var client = factory.newClient()) {
      client.createBucket(bucketName, clientId);
      assertTrue(client.deleteBucket(bucketName, clientId));
      // 2nd delete doesn't fail since GONE ~ TRUE
      assertTrue(client.deleteBucket(bucketName, clientId));
      // get deleted bucket should fail
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName, clientId));
      // Try to delete not existing Bucket shall fail
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket("notexist", clientId)).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void checkBucket() {
    final var bucketName = "testcheckbucket1";
    try (final var client = factory.newClient()) {
      // check non-existing bucket
      var res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);

      // simple check existing bucket
      client.createBucket(bucketName, clientId);
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);

      // manually delete bucket for full check
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.bucketDelete(DaoAccessorBucketRepository.getPrefix(clientId) + bucketName);
      } catch (final DriverException e) {
        fail(e);
      }

      // simple check non-existing bucket
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);

      // No full check non-existing bucket
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void testInternalExceptions() {
    final var bucketName1 = "testinternalexception1";
    final var bucketName2 = "testinternalexception2";
    try (final var client = factory.newClient()) {
      // we first need a bucket to exist
      client.createBucket(bucketName1, clientId);
      // THEN
      assertEquals(409,
          assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName1, clientId)).getStatus());
      assertEquals(StorageType.BUCKET, assertDoesNotThrow(() -> client.checkBucket(bucketName1, clientId)));
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName2, clientId)).getStatus());
      assertEquals(400,
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
      assertEquals(400,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName1, clientId)).getStatus());
      // Now check non-existing bucket
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.createBucket(bucketName1 + "new", clientId)).getStatus());
    } catch (final CcsWithStatusException | CcsDbException e) {
      fail(e);
    }
  }
}