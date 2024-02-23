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
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.resource.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.administration.resource.conf.Constants.OWNERSHIP_BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(NoMongoDbProfile.class)
class OwnershipResourceNoDbTest {
  private static final Logger logger = Logger.getLogger(OwnershipResourceNoDbTest.class);

  @Inject
  OwnershipApiClientFactory factory;

  @BeforeAll
  static void setup() {
  }

  @Test
  void topologyApiClientNotFound() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting list ownership not found ");
        final var exception = assertThrows(CcsWithStatusException.class, () -> client.listAll(CLIENT_ID));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting list ownership not found ");
        final var exception =
            assertThrows(CcsWithStatusException.class, () -> client.listWithOwnership(CLIENT_ID, ClientOwnership.READ));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting get ownership not found ");
        final var exception =
            assertThrows(CcsWithStatusException.class, () -> client.findByBucket(CLIENT_ID, OWNERSHIP_BUCKET));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting add ownership not found ");
        final var exception = assertThrows(CcsWithStatusException.class,
            () -> client.add(CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.READ));
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), exception.getStatus());
      }
      {
        logger.debugf("\n\nTesting update ownership not found ");
        final var exception = assertThrows(CcsWithStatusException.class,
            () -> client.update(CLIENT_ID, OWNERSHIP_BUCKET, ClientOwnership.WRITE));
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
