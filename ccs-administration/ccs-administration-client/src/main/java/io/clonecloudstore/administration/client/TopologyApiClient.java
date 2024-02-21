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

import java.util.Collection;

import io.clonecloudstore.administration.client.api.TopologyApi;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import jakarta.ws.rs.core.Response;

public class TopologyApiClient extends SimpleClientAbstract<TopologyApi> {
  /**
   * Constructor used by the Factory
   */
  protected TopologyApiClient(final TopologyApiClientFactory factory) {
    super(factory, factory.getUri());
  }

  /**
   * No cache
   */
  public Collection<Topology> listAll() throws CcsWithStatusException {
    final var uni = getService().listAll(TopologyStatus.UNKNOWN);
    return (Collection<Topology>) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * Cache on ALL_UP_KEY
   */
  public Collection<Topology> listWithStatus(final TopologyStatus status) throws CcsWithStatusException {
    final var factory = (TopologyApiClientFactory) getFactory();
    if (TopologyStatus.UP.equals(status)) {
      final var cached = factory.getFromCache();
      if (cached != null) {
        return cached;
      }
    }
    final var uni = getService().listAll(status);
    final var result = (Collection<Topology>) exceptionMapper.handleUniObject(this, uni);
    if (TopologyStatus.UP.equals(status)) {
      factory.addToCache(result);
    }
    return result;
  }

  /**
   * Cache if possible
   */
  public Topology findBySite(final String site) throws CcsWithStatusException {
    final var factory = (TopologyApiClientFactory) getFactory();
    final var cached = factory.getFromCache();
    if (cached != null) {
      var optional = cached.stream().filter(topology -> site.equals(topology.name())).findFirst();
      if (optional.isPresent()) {
        return optional.get();
      }
    }
    final var uni = getService().findBySite(site);
    return (Topology) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * Invalidate Cache
   */
  public Topology add(final Topology topology) throws CcsWithStatusException {
    ((TopologyApiClientFactory) getFactory()).clearCache();
    final var uni = getService().add(topology);
    return (Topology) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * Invalidate Cache
   */
  public Topology update(final Topology topology) throws CcsWithStatusException {
    ((TopologyApiClientFactory) getFactory()).clearCache();
    final var uni = getService().update(topology);
    return (Topology) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * Invalidate Cache
   */
  public boolean delete(final String site) throws CcsWithStatusException {
    ((TopologyApiClientFactory) getFactory()).clearCache();
    final var uni = getService().delete(site);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return response.getStatus() == Response.Status.NO_CONTENT.getStatusCode();
    }
  }
}
