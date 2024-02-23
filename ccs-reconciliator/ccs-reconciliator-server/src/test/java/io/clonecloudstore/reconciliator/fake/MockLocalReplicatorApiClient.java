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

package io.clonecloudstore.reconciliator.fake;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Semaphore;

import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.reconciliator.client.ReconciliatorApiClient;
import io.clonecloudstore.reconciliator.client.ReconciliatorApiFactory;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClient;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClientFactory;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;

public class MockLocalReplicatorApiClient extends LocalReplicatorApiClient {
  private static final Logger LOGGER = Logger.getLogger(MockLocalReplicatorApiClient.class);
  public static boolean runClient = false;
  public static Semaphore semaphore = new Semaphore(0);
  private final ReconciliatorApiFactory factory;
  private final ReconciliatorApiClient client;

  public MockLocalReplicatorApiClient() {
    super(CDI.current().select(LocalReplicatorApiClientFactory.class).get());
    factory = CDI.current().select(ReconciliatorApiFactory.class).get();
    client = factory.newClient();
  }

  @Override
  public void createRequestLocal(final ReconciliationRequest request, final String clientId, final String targetId,
                                 final String opId) throws CcsWithStatusException {
    LOGGER.infof("Mock %b", runClient);
    semaphore.release();
    if (runClient) {
      client.createRequestLocal(request);
    }
  }

  @Override
  public void endRequestLocal(final String idRequest, final String remoteId, final String clientId,
                              final String targetId, final String opId) throws CcsWithStatusException {
    LOGGER.infof("Mock %b", runClient);
    semaphore.release();
    if (runClient) {
      client.endRequestLocal(idRequest, remoteId);
    }
  }

  @Override
  public Iterator<ReconciliationSitesListing> getSitesListing(final String idRequest, final String clientId,
                                                              final String targetId, final String opId)
      throws CcsWithStatusException {
    LOGGER.infof("Mock %s %s", idRequest, targetId);
    LOGGER.infof("Mock %b", runClient);
    semaphore.release();
    if (runClient) {
      return new ReconciliationSitesListingIterator(client.getSitesListing(idRequest), targetId);
    }
    return null;
  }

  private record ReconciliationSitesListingIterator(Iterator<ReconciliationSitesListing> iterator, String remoteId)
      implements Iterator<ReconciliationSitesListing> {

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public ReconciliationSitesListing next() {
      final var next = iterator.next();
      if (next != null) {
        final var list = next.local();
        final var newList = new ArrayList<SingleSiteObject>(list.size());
        for (final var local : list) {
          newList.add(new SingleSiteObject(remoteId, local.nstatus(), local.event()));
        }
        return new ReconciliationSitesListing(next.id(), next.requestId(), next.bucket(), next.name(), newList);
      }
      return null;
    }
  }

  @Override
  public void endRequestCentral(final String idRequest, final String clientId, final String targetId, final String opId)
      throws CcsWithStatusException {
    LOGGER.infof("Mock %b", runClient);
    semaphore.release();
    if (runClient) {
      client.endRequestCentral(idRequest);
    }
  }

  @Override
  public Iterator<ReconciliationSitesAction> getActionsListing(final String idRequest, final String remoteId,
                                                               final String clientId, final String targetId,
                                                               final String opId) throws CcsWithStatusException {
    LOGGER.infof("Mock %b", runClient);
    semaphore.release();
    if (runClient) {
      return client.getActionsListing(idRequest, remoteId);
    }
    return null;
  }
}
