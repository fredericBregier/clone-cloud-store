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

package io.clonecloudstore.reconciliator.model;

import java.time.Instant;
import java.util.List;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ModelTest {
  @Test
  void checkSimpleRecord() {
    ReconciliationRequest requestStartUp =
        new ReconciliationRequest("idRequest", "client", "bucket", null, "from", "current", null, false);
    ReconciliationRequest request =
        new ReconciliationRequest("idRequest", "client", "bucket", null, "from", "current", null,
            requestStartUp.start(), 0, 0, 0, 0, 0, false, ReconciliationStep.CREATE, null);
    assertEquals(request, requestStartUp);
    SingleSiteObject local = new SingleSiteObject(request.currentSite(), (short) 1, Instant.now());
    ReconciliationSitesListing sitesListing =
        new ReconciliationSitesListing("id", request.id(), request.bucket(), "name", List.of(local));
    assertEquals("id", sitesListing.id());
    assertEquals(request.id(), sitesListing.requestId());
    assertEquals(request.bucket(), sitesListing.bucket());
    assertEquals(local, sitesListing.local().getFirst());
    ReconciliationSitesAction sitesAction =
        new ReconciliationSitesAction("id", request.id(), request.bucket(), sitesListing.name(), local.nstatus(),
            List.of(request.fromSite()), List.of(request.currentSite()));
    assertEquals("id", sitesAction.id());
    assertEquals(request.id(), sitesAction.requestId());
    assertEquals(request.bucket(), sitesAction.bucket());
    assertEquals(sitesListing.name(), sitesAction.name());
    assertEquals(local.nstatus(), sitesAction.needAction());
    assertEquals(request.fromSite(), sitesAction.needActionFrom().getFirst());
    assertEquals(request.currentSite(), sitesAction.sites().getFirst());
    for (var action : ReconciliationAction.values()) {
      assertEquals(action, ReconciliationAction.fromStatusCode(action.getStatus()));
    }
  }

  @Test
  void checkEquals() {
    SingleSiteObject local = new SingleSiteObject("site", (short) 1, Instant.now());
    SingleSiteObject local2 = new SingleSiteObject("site", (short) 1, local.event());
    assertTrue(local.equals(local2));
    assertEquals(local.hashCode(), local2.hashCode());
    assertTrue(local.equals(local));
    assertEquals(local.hashCode(), local.hashCode());
    assertFalse(local.equals(local2.event()));
    SingleSiteObject local3 = new SingleSiteObject("site2", (short) 1, local.event());
    assertFalse(local.equals(local3));
    assertNotEquals(local.hashCode(), local3.hashCode());
    SingleSiteObject local4 = new SingleSiteObject("site", (short) 2, local.event());
    assertFalse(local.equals(local4));
    assertNotEquals(local.hashCode(), local4.hashCode());
    SingleSiteObject local5 = new SingleSiteObject("site", (short) 1, Instant.now().plusMillis(10));
    assertFalse(local.equals(local5));
    assertNotEquals(local.hashCode(), local5.hashCode());
    assertTrue(local.toString().contains("site"));
  }
}
