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

package io.clonecloudstore.accessor.server.resource.internal;

import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.test.resource.AzureMongoKafkaProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
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
class AccessorBucketInternalResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorBucketInternalResourceTest.class);
  @Inject
  AccessorBucketInternalApiFactory factory;
  @Inject
  AccessorBucketApiFactory factoryExternal;
  @Inject
  DriverApiFactory driverApiFactory;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();
  }

  @BeforeEach
  void beforeEach() {
    bucketRepository = bucketRepositoryInstance.get();
    // Clean all
    CleanupTestUtil.cleanUp();
  }

  @Test
  void getBucketReplicator() {
    final var bucketName = "testgetbucket2";
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      clientExternal.createBucket(bucketName, clientId);
      final var bucket = client.getBucket(bucketName, clientId);
      Assertions.assertEquals(bucketName, bucket.getId());
      Assertions.assertEquals(clientId, bucket.getClientId());

      final var bucket2 = client.getBucket(bucketName, GuidLike.getGuid());
      Assertions.assertEquals(bucketName, bucket2.getId());
      Assertions.assertEquals(clientId, bucket2.getClientId());

      // Check bucket not exist
      final var bucketUnknownName = "unknown";
      final var unknownException =
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketUnknownName, clientId));
      assertEquals(404, unknownException.getStatus());
      assertTrue(clientExternal.deleteBucket(bucketName, clientId));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getBuckets() {
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      final var bucketsBeforeTest = client.getBuckets(clientId);
      final var numberBucketBeforeInsert = bucketsBeforeTest.size();
      clientExternal.createBucket("testcreatebuckets11", clientId);
      clientExternal.createBucket("testcreatebuckets12", clientId);
      clientExternal.createBucket("testcreatebuckets13", clientId);
      final var buckets = client.getBuckets(clientId);
      assertEquals(numberBucketBeforeInsert + 3, buckets.size());
      for (var item : buckets) {
        assertTrue(clientExternal.deleteBucket(item.getId(), clientId));
      }
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void checkBucketReplicator() {
    final var bucketName = "testcheckbucket2";
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      // check non-existing bucket
      var res = client.checkBucket(bucketName, clientId, true);
      assertEquals(StorageType.NONE, res);

      // simple check existing bucket
      clientExternal.createBucket(bucketName, clientId);
      res = client.checkBucket(bucketName, clientId, false);
      assertEquals(StorageType.BUCKET, res);

      // manually delete bucket for full check
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.bucketDelete(bucketName);
      } catch (final DriverException e) {
        fail(e);
      }

      // simple check non-existing bucket
      res = client.checkBucket(bucketName, clientId, false);
      assertEquals(StorageType.BUCKET, res);

      // full check non-existing bucket
      res = client.checkBucket(bucketName, clientId, true);
      assertEquals(StorageType.NONE, res);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }
}
