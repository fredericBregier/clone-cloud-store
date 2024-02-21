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

package io.clonecloudstore.administration.client;

import java.util.concurrent.ExecutionException;

import io.clonecloudstore.administration.client.conf.Constants;
import io.clonecloudstore.administration.client.fake.FakeOwnershipResource;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class OwnershipApiClientNoDbTest {
  @Inject
  OwnershipApiClientFactory factory;

  @Test
  void checkClientWithFakeServer() {
    Log.debugf("\n\nTesting ownership api");
    try (final var client = factory.newClient()) {
      assertTrue(client.listAll(Constants.CLIENT_ID).isEmpty());
      assertTrue(client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.UNKNOWN).isEmpty());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      FakeOwnershipResource.errorCode = 400;
      // Ensure cache is empty
      factory.clearCache(Constants.CLIENT_ID);
      assertEquals(400,
          assertThrows(CcsWithStatusException.class, () -> client.listAll(Constants.CLIENT_ID)).getStatus());
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.UNKNOWN)).getStatus());
    } finally {
      FakeOwnershipResource.errorCode = 0;
    }
    try (final var client = factory.newClient()) {
      assertEquals(ClientOwnership.READ, client.add(Constants.CLIENT_ID, Constants.BUCKET, ClientOwnership.READ));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(409, assertThrows(CcsWithStatusException.class,
          () -> client.add(Constants.CLIENT_ID, Constants.BUCKET, ClientOwnership.WRITE)).getStatus());
      assertEquals(1, client.listAll(Constants.CLIENT_ID).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.READ).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var completableFuture = client.addAsync(Constants.CLIENT_ID, Constants.BUCKET + 1, ClientOwnership.WRITE);
      assertEquals(ClientOwnership.WRITE, completableFuture.get());
    } catch (final ExecutionException | InterruptedException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var completableFuture = client.addAsync(Constants.CLIENT_ID, Constants.BUCKET + 1, ClientOwnership.WRITE);
      assertThrows(CcsWithStatusException.class, () -> client.getClientOwnershipFromAsync(completableFuture));
    } catch (final RuntimeException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(409, assertThrows(CcsWithStatusException.class,
          () -> client.add(Constants.CLIENT_ID, Constants.BUCKET + 1, ClientOwnership.DELETE)).getStatus());
      assertEquals(2, client.listAll(Constants.CLIENT_ID).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.READ).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.WRITE).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(ClientOwnership.READ, client.findByBucket(Constants.CLIENT_ID, Constants.BUCKET));
      assertEquals(ClientOwnership.WRITE, client.findByBucket(Constants.CLIENT_ID, Constants.BUCKET + 1));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(ClientOwnership.READ, client.update(Constants.CLIENT_ID, Constants.BUCKET, ClientOwnership.READ));
      assertEquals(ClientOwnership.READ_WRITE,
          client.update(Constants.CLIENT_ID, Constants.BUCKET + 1, ClientOwnership.READ));
      assertEquals(ClientOwnership.OWNER,
          client.update(Constants.CLIENT_ID, Constants.BUCKET + 1, ClientOwnership.DELETE));
      assertEquals(2, client.listAll(Constants.CLIENT_ID).size());
      assertEquals(2, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.READ).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.WRITE).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.DELETE).size());
      assertEquals(ClientOwnership.READ_WRITE,
          client.update(Constants.CLIENT_ID, Constants.BUCKET, ClientOwnership.READ_WRITE));
      assertEquals(2, client.listAll(Constants.CLIENT_ID).size());
      assertEquals(2, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.READ).size());
      assertEquals(2, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.WRITE).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.DELETE).size());
      assertEquals(2, client.listAll(Constants.CLIENT_ID).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertTrue(client.delete(Constants.CLIENT_ID, Constants.BUCKET));
      assertEquals(1, client.listAll(Constants.CLIENT_ID).size());
      assertEquals(1, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.READ).size());
      final var completable = client.deleteAllClientsAsync(Constants.BUCKET + 1);
      assertTrue(client.getBooleanFromAsync(completable));
      assertEquals(0, client.listAll(Constants.CLIENT_ID).size());
      assertEquals(0, client.listWithOwnership(Constants.CLIENT_ID, ClientOwnership.READ).size());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.findByBucket(Constants.CLIENT_ID, Constants.BUCKET)).getStatus());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.findByBucket(Constants.CLIENT_ID, Constants.BUCKET + 1)).getStatus());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.update(Constants.CLIENT_ID, Constants.BUCKET, ClientOwnership.READ)).getStatus());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.delete(Constants.CLIENT_ID, Constants.BUCKET)).getStatus());
      final var completable = client.deleteAllClientsAsync(Constants.BUCKET);
      assertEquals(404,
          assertThrows(CcsWithStatusException.class, () -> client.getBooleanFromAsync(completable)).getStatus());
    }
  }
}
