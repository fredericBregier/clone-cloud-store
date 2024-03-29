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

package io.clonecloudstore.test.accessor.serverpublic;

import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiClient;
import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class AccessorBucketPublicResourceTest {

  private static final Logger LOG = Logger.getLogger(AccessorBucketApiClient.class);
  @Inject
  AccessorBucketApiFactory factory;
  @Inject
  DriverApiFactory driverApiFactory;

  private static final String clientId = UUID.randomUUID().toString();

  @BeforeAll
  static void beforeAll() {
    FakeDriverFactory.cleanUp();
  }

  @BeforeEach
  void beforeEach() {
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
  }

  @Test
  void invalidApi() {
    FakeCommonBucketResourceHelper.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkBucket("bucket", clientId));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.createBucket("bucket", clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBucket("bucket", clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket", clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBuckets(clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void createBucket() {
    final var bucketName = "testcreatebucket1";
    try (final var client = factory.newClient()) {
      final var bucket = client.createBucket(bucketName, clientId);
      assertEquals(bucketName, bucket.getId());
      //Test already exist Exception
      assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName, clientId));
    } catch (CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getBucket() {
    final var bucketName = "testgetbucket1";
    try (final var client = factory.newClient()) {
      var bucket0 = client.createBucket(bucketName, clientId);
      assertEquals(bucketName, bucket0.getId());
      final var bucket = client.getBucket(bucketName, clientId);
      assertEquals(bucketName, bucket.getId());

      // Check bucket not exist
      final var bucketUnknownName = "unknown";
      final var unknownException =
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketUnknownName, clientId));
      assertEquals(404, unknownException.getStatus());
    } catch (CcsWithStatusException e) {
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
      final var buckets2 = client.getBuckets(GuidLike.getGuid());
      assertEquals(0, buckets2.size());
    } catch (CcsWithStatusException e) {
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
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, clientId)).getStatus());
      // get deleted bucket should fail
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName, clientId));
      // Try to delete not existing Bucket shall fail
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket("notexist", clientId)).getStatus());
    } catch (CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void checkBucket() {
    final var bucketName = "testcheckbucket18";
    try (final var client = factory.newClient()) {
      // check non-existing bucket
      var res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);

      FakeCommonBucketResourceHelper.errorCode = 404;
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);

      FakeCommonBucketResourceHelper.errorCode = 204;
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);
      FakeCommonBucketResourceHelper.errorCode = 0;

      // simple check existing bucket
      client.createBucket(bucketName, clientId);
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);

      // manually delete bucket for full check
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.bucketDelete(bucketName);
      } catch (DriverException e) {
        fail(e);
      }

      // simple check non-existing bucket (No DB so no GONE)
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);

      // full check non-existing bucket
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void checkBucketWithDifferentClientId() {
    final var bucketName = "testcheckbucket10";
    final var otherClient = GuidLike.getGuid();
    try (final var client = factory.newClient()) {
      // check non-existing bucket
      var res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);
      res = client.checkBucket(bucketName, otherClient);
      assertEquals(StorageType.NONE, res);

      // simple check existing bucket
      client.createBucket(bucketName, clientId);
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);
      res = client.checkBucket(bucketName, otherClient);
      assertEquals(StorageType.BUCKET, res);

      var bucket = client.getBucket(bucketName, clientId);
      assertEquals(bucketName, bucket.getId());
      assertEquals(clientId, bucket.getClientId());
      bucket = client.getBucket(bucketName, otherClient);
      assertEquals(bucketName, bucket.getId());
      assertEquals(clientId, bucket.getClientId());

      // Check only clientId can delete
      assertEquals(406,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, otherClient)).getStatus());
      assertTrue(client.deleteBucket(bucketName, clientId));

      // simple check non-existing bucket
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);
      res = client.checkBucket(bucketName, otherClient);
      assertEquals(StorageType.NONE, res);

      // Check otherClient can recreate
      client.createBucket(bucketName, otherClient);
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.BUCKET, res);
      res = client.checkBucket(bucketName, otherClient);
      assertEquals(StorageType.BUCKET, res);

      bucket = client.getBucket(bucketName, clientId);
      assertEquals(bucketName, bucket.getId());
      assertEquals(otherClient, bucket.getClientId());
      bucket = client.getBucket(bucketName, otherClient);
      assertEquals(bucketName, bucket.getId());
      assertEquals(otherClient, bucket.getClientId());

      // Check only clientId can delete
      assertEquals(406,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, clientId)).getStatus());
      assertTrue(client.deleteBucket(bucketName, otherClient));

      // simple check non-existing bucket
      res = client.checkBucket(bucketName, clientId);
      assertEquals(StorageType.NONE, res);
      res = client.checkBucket(bucketName, otherClient);
      assertEquals(StorageType.NONE, res);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }
}
