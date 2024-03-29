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

package io.clonecloudstore.test.accessor.serverprivate;

import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
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
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class AccessorBucketPrivateResourceTest {

  private static final Logger LOG = Logger.getLogger(AccessorBucketPrivateResourceTest.class);
  @Inject
  AccessorBucketInternalApiFactory factory;
  @Inject
  AccessorBucketApiFactory factoryExternal;
  @Inject
  DriverApiFactory driverApiFactory;

  private static final String clientId = UUID.randomUUID().toString();

  @BeforeEach
  void beforeEach() {
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
  }

  @BeforeAll
  static void beforeAll() {
    FakeDriverFactory.cleanUp();
  }

  @Test
  void invalidApi() {
    FakeCommonBucketResourceHelper.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkBucket("bucket", clientId, false));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
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
      client.getBuckets(clientId);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void getBucket() {
    final var bucketName = "testgetbucket1";
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      clientExternal.createBucket(bucketName, clientId);
      final var realBucketName = bucketName;
      final var bucket = client.getBucket(realBucketName, clientId);
      assertEquals(realBucketName, bucket.getId());
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
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      final var bucketsBeforeTest = client.getBuckets(clientId);
      final var numberBucketBeforeInsert = bucketsBeforeTest.size();
      clientExternal.createBucket("testgetbuckets1", clientId);
      clientExternal.createBucket("testgetbuckets2", clientId);
      clientExternal.createBucket("testgetbuckets3", clientId);
      final var buckets = client.getBuckets(clientId);
      assertEquals(numberBucketBeforeInsert + 3, buckets.size());
    } catch (CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void checkBucket() {
    final var bucketName = "testcheckbucket17";
    try (final var client = factory.newClient(); final var clientExternal = factoryExternal.newClient()) {
      // check non-existing bucket
      var res = client.checkBucket(bucketName, clientId, false);
      assertEquals(StorageType.NONE, res);
      res = client.checkBucket(bucketName, clientId, true);
      assertEquals(StorageType.NONE, res);

      final var realBucketName = bucketName;
      // simple check existing bucket
      clientExternal.createBucket(bucketName, clientId);
      res = client.checkBucket(realBucketName, clientId, true);
      assertEquals(StorageType.BUCKET, res);

      // manually delete bucket for full check
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.bucketDelete(bucketName);
      } catch (DriverException e) {
        fail(e);
      }

      // simple check non-existing bucket (No DB so no GONE)
      res = client.checkBucket(realBucketName, clientId, true);
      assertEquals(StorageType.NONE, res);

      // full check non-existing bucket
      res = client.checkBucket(realBucketName, clientId, true);
      assertEquals(StorageType.NONE, res);
    } catch (CcsWithStatusException e) {
      fail(e);
    }
  }
}
