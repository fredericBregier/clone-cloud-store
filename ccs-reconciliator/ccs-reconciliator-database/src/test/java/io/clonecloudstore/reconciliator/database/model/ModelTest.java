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

package io.clonecloudstore.reconciliator.database.model;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoNativeListing;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoRequest;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoSitesAction;
import io.clonecloudstore.reconciliator.database.mongodb.MgDaoSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ModelTest {
  @Test
  void checkSimpleRecord() {
    DaoRequest request = new MgDaoRequest().setId("idRequest").setClientId("client").setBucket("bucket").setFilter(null)
        .setFromSite("from").setCurrentSite("current").setStart(Instant.now()).setDryRun(false);
    SingleSiteObject local = new SingleSiteObject(request.getCurrentSite(), (short) 1, Instant.now());
    SingleSiteObject local2 = new SingleSiteObject(request.getCurrentSite(), (short) 2, Instant.now());
    DaoNativeListing nativeListing =
        new MgDaoNativeListing().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName("name").setDriver(local).setDb(local2);
    assertEquals("id", nativeListing.getId());
    assertEquals(request.getId(), nativeListing.getRequestId());
    assertEquals(request.getBucket(), nativeListing.getBucket());
    assertEquals("name", nativeListing.getName());
    assertEquals(local, nativeListing.getDriver());
    assertEquals(local2, nativeListing.getDb());
    DaoSitesListing sitesListing =
        new MgDaoSitesListing().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName(nativeListing.getName()).setLocal(List.of(local));
    assertEquals("id", sitesListing.getId());
    assertEquals(request.getId(), sitesListing.getRequestId());
    assertEquals(request.getBucket(), sitesListing.getBucket());
    assertEquals(nativeListing.getName(), sitesListing.getName());
    assertEquals(local, sitesListing.getLocal().getFirst());
    DaoSitesAction sitesAction =
        new MgDaoSitesAction().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName(nativeListing.getName()).setNeedAction(local.nstatus()).setNeedActionFrom(request.getFromSite())
            .setActions(Set.of(request.getCurrentSite()));
    assertEquals("id", sitesAction.getId());
    assertEquals(request.getId(), sitesAction.getRequestId());
    assertEquals(request.getBucket(), sitesAction.getBucket());
    assertEquals(nativeListing.getName(), sitesAction.getName());
    assertEquals(local.nstatus(), sitesAction.getNeedAction());
    assertEquals(request.getFromSite(), sitesAction.getNeedActionFrom());
    assertTrue(sitesAction.getActions().contains(request.getCurrentSite()));
  }

  @Test
  void checkEquals() {
    DaoRequest request = new MgDaoRequest().setId("idRequest").setClientId("client").setBucket("bucket").setFilter(null)
        .setFromSite("from").setCurrentSite("current").setStart(Instant.now()).setDryRun(false);
    SingleSiteObject local = new SingleSiteObject(request.getCurrentSite(), (short) 1, Instant.now());
    SingleSiteObject local2 = new SingleSiteObject(request.getCurrentSite(), (short) 2, Instant.now());
    DaoRequest request1 = new MgDaoRequest().fromDto(request.getDto());
    assertTrue(request.equals(request));
    assertEquals(request.hashCode(), request.hashCode());
    assertTrue(request.equals(request1));
    assertEquals(request.hashCode(), request1.hashCode());
    DaoRequest request2 = new MgDaoRequest(request.getDto());
    assertTrue(request.equals(request2));
    assertEquals(request.hashCode(), request2.hashCode());
    assertFalse(request.equals(local2.event()));
    assertTrue(request.toString().contains("idRequest"));

    DaoNativeListing nativeListing =
        new MgDaoNativeListing().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName("name").setDriver(local).setDb(local2);
    DaoNativeListing nativeListing1 =
        new MgDaoNativeListing().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName("name").setDriver(local).setDb(local2);
    assertTrue(nativeListing.equals(nativeListing));
    assertEquals(nativeListing.hashCode(), nativeListing.hashCode());
    assertTrue(nativeListing.equals(nativeListing1));
    assertEquals(nativeListing.hashCode(), nativeListing1.hashCode());
    assertFalse(nativeListing.equals(local2.event()));
    assertTrue(nativeListing.toString().contains("idRequest"));

    DaoSitesListing sitesListing =
        new MgDaoSitesListing().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName(nativeListing.getName()).setLocal(List.of(local));
    DaoSitesListing sitesListing1 = new MgDaoSitesListing().fromDto(sitesListing.getDto());
    assertTrue(sitesListing.equals(sitesListing));
    assertEquals(sitesListing.hashCode(), sitesListing.hashCode());
    assertTrue(sitesListing.equals(sitesListing1));
    assertEquals(sitesListing.hashCode(), sitesListing1.hashCode());
    DaoSitesListing sitesListing2 = new MgDaoSitesListing(sitesListing.getDto());
    assertTrue(sitesListing.equals(sitesListing2));
    assertEquals(sitesListing.hashCode(), sitesListing2.hashCode());
    assertFalse(sitesListing.equals(local2.event()));
    assertTrue(sitesListing.toString().contains("idRequest"));

    DaoSitesAction sitesAction =
        new MgDaoSitesAction().setId("id").setRequestId(request.getId()).setBucket(request.getBucket())
            .setName(nativeListing.getName()).setNeedAction(local.nstatus()).setNeedActionFrom(request.getFromSite())
            .setActions(Set.of(request.getCurrentSite()));
    DaoSitesAction sitesAction1 = new MgDaoSitesAction().fromDto(sitesAction.getDto());
    assertTrue(sitesAction.equals(sitesAction));
    assertEquals(sitesAction.hashCode(), sitesAction.hashCode());
    assertTrue(sitesAction.equals(sitesAction1));
    assertEquals(sitesAction.hashCode(), sitesAction1.hashCode());
    DaoSitesAction sitesAction2 = new MgDaoSitesAction(sitesAction.getDto());
    assertTrue(sitesAction.equals(sitesAction2));
    assertEquals(sitesAction.hashCode(), sitesAction2.hashCode());
    assertFalse(sitesAction.equals(local2.event()));
    assertTrue(sitesAction.toString().contains("idRequest"));
  }

  @Test
  void checkService() {
    assertEquals(DaoService.TO_UPDATE_RANK, DaoService.STATUS_NAME_ORDERED.size());
    assertEquals(AccessorStatus.READY.name(), DaoService.STATUS_NAME_ORDERED.get(DaoService.READY_RANK));
  }
}
