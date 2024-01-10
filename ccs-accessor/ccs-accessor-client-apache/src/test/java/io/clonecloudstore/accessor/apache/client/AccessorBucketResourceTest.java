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

package io.clonecloudstore.accessor.apache.client;

import java.util.UUID;

import io.clonecloudstore.accessor.apache.client.fakeserver.FakeBucketService;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class AccessorBucketResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorBucketResourceTest.class);
  static AccessorApiFactory factory;
  private static final String clientId = UUID.randomUUID().toString();

  @BeforeAll
  static void beforeAll() {
    factory = new AccessorApiFactory("http://127.0.0.1:8081", clientId);
  }

  @AfterAll
  static void afterAll() {
    factory.close();
  }

  @Test
  void invalidApi() {
    FakeBucketService.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkBucket("bucket"));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.createBucket("bucket");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBucket("bucket");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBuckets();
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn("Status: " + e.getStatus(), e);
        fail(e);
      }
    }
    FakeBucketService.errorCode = 406;
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    FakeBucketService.errorCode = 409;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkBucket("bucket"));
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.createBucket("bucket");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBucket("bucket");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBuckets();
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn("Status: " + e.getStatus(), e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    FakeBucketService.errorCode = 410;
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeBucketService.errorCode) {
        LOG.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void validApi() {
    FakeBucketService.errorCode = 0;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.BUCKET, client.checkBucket("bucket"));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.createBucket("bucket");
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getBucket("bucket");
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.deleteBucket("bucket");
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getBuckets();
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
  }
}
