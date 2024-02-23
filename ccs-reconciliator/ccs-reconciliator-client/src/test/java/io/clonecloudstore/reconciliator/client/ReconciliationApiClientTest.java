/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.reconciliator.client;

import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.reconciliator.client.fake.FakeReconciliatorService;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ReconciliationApiClientTest {
  @Inject
  ReconciliatorApiFactory factory;

  @Test
  void checkUsage() {
    FakeReconciliatorService.errorCode = 0;
    AccessorProperties.setInternalCompression(false);
    ReconciliationRequest request =
        new ReconciliationRequest(GuidLike.getGuid(), "client", "bucket", null, "from", "current", null, false);

    try (final var client = factory.newClient()) {
      assertNotNull(assertDoesNotThrow(() -> client.createRequestCentral(request)));
      assertEquals(request.id(), assertDoesNotThrow(() -> client.getRequestStatus(request.id())).id());
      assertDoesNotThrow(() -> client.createRequestLocal(request));
      assertDoesNotThrow(() -> client.endRequestLocal(request.id(), "site2"));
      var iterator = assertDoesNotThrow(() -> client.getSitesListing(request.id()));
      assertTrue(iterator.hasNext());
      assertEquals(request.id(), iterator.next().requestId());
      assertFalse(iterator.hasNext());
      assertDoesNotThrow(() -> client.endRequestCentral(request.id()));
      var iteratorAction = assertDoesNotThrow(() -> client.getActionsListing(request.id(), "site2"));
      assertTrue(iteratorAction.hasNext());
      var action = iteratorAction.next();
      assertEquals(request.id(), action.requestId());
      assertTrue(action.sites().contains("site2"));
      assertFalse(iteratorAction.hasNext());
      assertEquals(request.id(), assertDoesNotThrow(() -> client.getLocalRequestStatus(request.id())).id());
      assertNotNull(assertDoesNotThrow(() -> client.launchPurge("clientid", 100)));
      // Later on real object
      assertNotNull(assertDoesNotThrow(() -> client.getPurgeStatus("idPurge")));
      assertNotNull(assertDoesNotThrow(() -> client.launchImport("bucket", "clientid", 100, "metadata")));
      // Later on real object
      assertNotNull(assertDoesNotThrow(() -> client.getImportStatus("bucket", "idImport")));
      assertNotNull(assertDoesNotThrow(() -> client.launchSync("bucket", "clientid", "site2")));
    }
  }

  @Test
  void checkUsageCompressed() {
    FakeReconciliatorService.errorCode = 0;
    AccessorProperties.setInternalCompression(true);
    ReconciliationRequest request =
        new ReconciliationRequest(GuidLike.getGuid(), "client", "bucket", null, "from", "current", null, false);

    try (final var client = factory.newClient()) {
      assertNotNull(assertDoesNotThrow(() -> client.createRequestCentral(request)));
      assertEquals(request.id(), assertDoesNotThrow(() -> client.getRequestStatus(request.id())).id());
      assertDoesNotThrow(() -> client.createRequestLocal(request));
      assertDoesNotThrow(() -> client.endRequestLocal(request.id(), "site2"));
      var iterator = assertDoesNotThrow(() -> client.getSitesListing(request.id()));
      assertTrue(iterator.hasNext());
      assertEquals(request.id(), iterator.next().requestId());
      assertFalse(iterator.hasNext());
      assertDoesNotThrow(() -> client.endRequestCentral(request.id()));
      var iteratorAction = assertDoesNotThrow(() -> client.getActionsListing(request.id(), "site2"));
      assertTrue(iteratorAction.hasNext());
      var action = iteratorAction.next();
      assertEquals(request.id(), action.requestId());
      assertTrue(action.sites().contains("site2"));
      assertFalse(iteratorAction.hasNext());
      assertEquals(request.id(), assertDoesNotThrow(() -> client.getLocalRequestStatus(request.id())).id());
      assertNotNull(assertDoesNotThrow(() -> client.launchPurge("clientid", 100)));
      // Later on real object
      assertNotNull(assertDoesNotThrow(() -> client.getPurgeStatus("idPurge")));
      assertNotNull(assertDoesNotThrow(() -> client.launchImport("bucket", "clientid", 100, "metadata")));
      // Later on real object
      assertNotNull(assertDoesNotThrow(() -> client.getImportStatus("bucket", "idImport")));
      assertNotNull(assertDoesNotThrow(() -> client.launchSync("bucket", "clientid", "site2")));
    }
  }

  @Test
  void invalidUsage() {
    FakeReconciliatorService.errorCode = 404;
    AccessorProperties.setInternalCompression(false);
    ReconciliationRequest request =
        new ReconciliationRequest(GuidLike.getGuid(), "client", "bucket", null, "from", "current", null, false);

    try (final var client = factory.newClient()) {
      assertThrows(CcsWithStatusException.class, () -> client.createRequestCentral(request));
      assertThrows(CcsWithStatusException.class, () -> client.getRequestStatus(request.id()));
      assertThrows(CcsWithStatusException.class, () -> client.createRequestLocal(request));
      assertThrows(CcsWithStatusException.class, () -> client.endRequestLocal(request.id(), "site2"));
      assertThrows(CcsWithStatusException.class, () -> client.getSitesListing(request.id()));
      assertThrows(CcsWithStatusException.class, () -> client.endRequestCentral(request.id()));
      assertThrows(CcsWithStatusException.class, () -> client.getActionsListing(request.id(), "site2"));
      assertThrows(CcsWithStatusException.class, () -> client.getLocalRequestStatus(request.id()));
      assertThrows(CcsWithStatusException.class, () -> client.launchPurge("clientid", 100));
      // Later on real object
      assertThrows(CcsWithStatusException.class, () -> client.getPurgeStatus("idPurge"));
      assertThrows(CcsWithStatusException.class, () -> client.launchImport("bucket", "clientid", 100, "metadata"));
      // Later on real object
      assertThrows(CcsWithStatusException.class, () -> client.getImportStatus("bucket", "idImport"));
      assertThrows(CcsWithStatusException.class, () -> client.launchSync("bucket", "clientid", "site2"));
    }
  }
}
