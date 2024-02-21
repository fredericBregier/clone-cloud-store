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

package io.clonecloudstore.accessor.client.internal.resource;

import java.util.UUID;

import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiClient;
import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.accessor.client.internal.resource.fakeserver.FakeBucketInternalService;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class AccessorBucketInternalResourceTest {

  private static final Logger LOG = Logger.getLogger(AccessorBucketInternalApiClient.class);
  @Inject
  AccessorBucketInternalApiFactory factory;

  private static final String clientId = UUID.randomUUID().toString();

  @Test
  void invalidApiReplicator() {
    FakeBucketInternalService.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkBucket("bucket", clientId, false));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkBucket("bucket", clientId, true));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getBucket("bucket", clientId);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getBuckets(clientId);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient(factory.getUri())) {
      client.getBuckets(clientId);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        LOG.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void validApiReplicator() {
    FakeBucketInternalService.errorCode = 0;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.BUCKET, client.checkBucket("bucket", clientId, false));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.BUCKET, client.checkBucket("bucket", clientId, true));
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getBucket("bucket", clientId);
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getBuckets(clientId);
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient(factory.getUri())) {
      client.getBuckets(clientId);
    } catch (final CcsWithStatusException e) {
      LOG.warn(e, e);
      fail(e);
    }
  }
}
