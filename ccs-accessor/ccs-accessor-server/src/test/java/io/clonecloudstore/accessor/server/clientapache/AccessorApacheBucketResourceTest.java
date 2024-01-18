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

package io.clonecloudstore.accessor.server.clientapache;

import java.util.UUID;

import io.clonecloudstore.accessor.apache.client.AccessorApiFactory;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.resource.AzureMongoKafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(AzureMongoKafkaProfile.class)
class AccessorApacheBucketResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorApacheBucketResourceTest.class);
  AccessorApiFactory factory;
  @Inject
  Instance<DaoAccessorBucketRepository> repositoryInstance;
  DaoAccessorBucketRepository repository;
  @Inject
  DriverApiFactory driverApiFactory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();

    FakeCommonBucketResourceHelper.errorCode = 404;
    FakeCommonObjectResourceHelper.errorCode = 404;
  }

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
    factory = new AccessorApiFactory("http://127.0.0.1:8081", clientId);
    // Clean all
    CleanupTestUtil.cleanUp();
  }

  @AfterEach
  void afterEach() {
    factory.close();
  }

  @Test
  void createBucket() {
    final var bucketName = "testcreatebucket1";
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName);
      Assertions.assertEquals(bucketName, bucket.getId());
      //Test already exist Exception
      assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName));

      //Check Error
      assertThrows(CcsWithStatusException.class, () -> client.createBucket("notValidBucket"));

      assertTrue(client.deleteBucket(bucketName));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getBucket() {
    final var bucketName = "testgetbucket1";
    try (final var client = factory.newClient()) {
      client.createBucket(bucketName);
      final var bucket = client.getBucket(bucketName);
      Assertions.assertEquals(bucketName, bucket.getId());

      // Check bucket not exist
      final var bucketUnknownName = "unknown";
      final var unknownException =
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketUnknownName));
      assertEquals(404, unknownException.getStatus());
      assertTrue(client.deleteBucket(bucketName));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getBuckets() {
    try (final var client = factory.newClient()) {
      final var bucketsBeforeTest = client.getBuckets();
      final var numberBucketBeforeInsert = bucketsBeforeTest.size();
      client.createBucket("testcreatebuckets13");
      client.createBucket("testcreatebuckets23");
      client.createBucket("testcreatebuckets33");
      final var buckets = client.getBuckets();
      assertEquals(numberBucketBeforeInsert + 3, buckets.size());
      for (var item : buckets) {
        assertTrue(client.deleteBucket(item.getId()));
      }
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void deleteBucket() {
    final var bucketName = "testdeletebucket1";
    try (final var client = factory.newClient()) {
      client.createBucket(bucketName);
      assertTrue(client.deleteBucket(bucketName));
      // 2nd delete doesn't fail since GONE ~ TRUE
      assertTrue(client.deleteBucket(bucketName));
      // get deleted bucket should fail
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName));
      // Try to delete not existing Bucket shall fail
      assertEquals(404, assertThrows(CcsWithStatusException.class, () -> client.deleteBucket("notexist")).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void checkBucket() {
    final var bucketName = "testcheckbucket12";
    try (final var client = factory.newClient()) {
      // check non-existing bucket
      var res = client.checkBucket(bucketName);
      assertEquals(StorageType.NONE, res);

      // simple check existing bucket
      var bucket = client.createBucket(bucketName);
      res = client.checkBucket(bucketName);
      assertEquals(StorageType.BUCKET, res);

      // manually delete bucket for full check
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.bucketDelete(bucketName);
      } catch (final DriverException e) {
        fail(e);
      }

      // simple check non-existing bucket
      res = client.checkBucket(bucketName);
      assertEquals(StorageType.BUCKET, res);

      // No full check non-existing bucket
      res = client.checkBucket(bucketName);
      assertEquals(StorageType.BUCKET, res);
      repository.deleteWithPk(bucket.getId());
    } catch (final CcsWithStatusException | CcsDbException e) {
      fail(e);
    }
  }
}
