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

import java.net.URI;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.clonecloudstore.administration.client.api.TopologyApi;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.common.quarkus.client.SimpleClientFactoryAbstract;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.quarkus.arc.Unremovable;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CaffeineCache;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Unremovable
public class TopologyApiClientFactory extends SimpleClientFactoryAbstract<TopologyApi> {
  private static final String ALL_UP_KEY = "ALL_UP_KEY";
  Cache cache;

  public TopologyApiClientFactory(@CacheName("topology-cache") final Cache cache) {
    this.cache = cache;
  }

  @Override
  public TopologyApiClient newClient() {
    return new TopologyApiClient(this);
  }

  @Override
  public synchronized TopologyApiClient newClient(final URI uri) {
    return (TopologyApiClient) super.newClient(uri);
  }

  @Override
  protected Class<?> getServiceClass() {
    return TopologyApi.class;
  }

  public Collection<Topology> getFromCache() {
    try {
      final var result = cache.as(CaffeineCache.class).getIfPresent(ALL_UP_KEY);
      if (result == null) {
        return null; // NOSONAR intentional for Cache
      }
      return (Collection<Topology>) result.get();
    } catch (final InterruptedException | ExecutionException e) {// NOSONAR intentional
      return null; // NOSONAR intentional for Cache
    }
  }

  public void addToCache(final Collection<Topology> topologies) {
    cache.as(CaffeineCache.class).put(ALL_UP_KEY, CompletableFuture.completedFuture(topologies));
  }

  public void clearCache() {
    cache.as(CaffeineCache.class).invalidate(ALL_UP_KEY).await().atMost(QuarkusProperties.getDurationResponseTimeout());
  }
}
