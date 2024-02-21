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

package io.clonecloudstore.administration.resource;

import io.clonecloudstore.administration.client.OwnershipApiClientFactory;
import io.clonecloudstore.administration.database.mongodb.conf.Constants;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.resource.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.administration.resource.conf.Constants.OWNERSHIP_BUCKET;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
class OwnershipResourceTest {
  private static final Logger logger = Logger.getLogger(OwnershipResourceTest.class);

  @Inject
  OwnershipApiClientFactory factory;

  @BeforeAll
  static void setup() {
  }

  @Test
  void getOpenAPI() {
    final var openAPI = given().get("/q/openapi").then().statusCode(200).extract().response().asString();
    Log.infof("OpenAPI: \n%s", openAPI);
  }

  @Test
  void topologyApiClientValidation() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting add ownership");
        final var clientOwnership =
            assertDoesNotThrow(() -> client.add(CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.READ));
        assertNotNull(clientOwnership);
        assertEquals(ClientOwnership.READ, clientOwnership);
        assertDoesNotThrow(() -> client.getClientOwnershipFromAsync(
            client.addAsync(CLIENT_ID + 1, Constants.OWNERSHIP_BUCKET, ClientOwnership.WRITE)));

      }
      {
        logger.debugf("\n\nTesting list ownership");
        final var clientBucketAccesses = assertDoesNotThrow(() -> client.listAll(CLIENT_ID));
        assertNotNull(clientBucketAccesses);
        assertEquals(1, clientBucketAccesses.size());
      }
      {
        logger.debugf("\n\nTesting list ownership");
        final var clientBucketAccesses =
            assertDoesNotThrow(() -> client.listWithOwnership(CLIENT_ID, ClientOwnership.READ));
        assertNotNull(clientBucketAccesses);
        assertEquals(1, clientBucketAccesses.size());
      }
      {
        logger.debugf("\n\nTesting get ownership");
        final var clientOwnership = assertDoesNotThrow(() -> client.findByBucket(CLIENT_ID, OWNERSHIP_BUCKET));
        assertNotNull(clientOwnership);
        assertEquals(ClientOwnership.READ, clientOwnership);
      }
      {
        logger.debugf("\n\nTesting update ownership");
        final var clientOwnership =
            assertDoesNotThrow(() -> client.update(CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.WRITE));
        assertNotNull(clientOwnership);
        assertEquals(ClientOwnership.READ_WRITE, clientOwnership);
      }
      {
        logger.debugf("\n\nTesting delete ownership");
        final var deleted = assertDoesNotThrow(() -> client.delete(CLIENT_ID, OWNERSHIP_BUCKET));
        assertTrue(deleted);
      }
      {
        logger.debugf("\n\nTesting list ownership");
        final var clientBucketAccesses = assertDoesNotThrow(() -> client.listAll(CLIENT_ID));
        assertNotNull(clientBucketAccesses);
        assertEquals(0, clientBucketAccesses.size());
        assertDoesNotThrow(() -> client.getBooleanFromAsync(client.deleteAllClientsAsync(Constants.OWNERSHIP_BUCKET)));
        assertThrows(CcsWithStatusException.class,
            () -> client.getBooleanFromAsync(client.deleteAllClientsAsync(Constants.OWNERSHIP_BUCKET)));
      }
    }
  }

  @Test
  void topologyApiClientNotFound() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting get ownership not found ");
        final var exception =
            assertThrows(CcsWithStatusException.class, () -> client.findByBucket(CLIENT_ID, OWNERSHIP_BUCKET));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting update ownership not found ");
        final var exception = assertThrows(CcsWithStatusException.class,
            () -> client.update(CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.DELETE));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting delete ownership not found ");
        final var exception =
            assertThrows(CcsWithStatusException.class, () -> client.delete(CLIENT_ID, OWNERSHIP_BUCKET));
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), exception.getStatus());
      }
    }

  }
}
