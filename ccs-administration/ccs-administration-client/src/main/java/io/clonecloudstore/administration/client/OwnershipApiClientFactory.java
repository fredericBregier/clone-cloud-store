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

import io.clonecloudstore.administration.client.api.OwnershipApi;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.common.quarkus.client.SimpleClientFactoryAbstract;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.quarkus.arc.Unremovable;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CaffeineCache;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@Unremovable
public class OwnershipApiClientFactory extends SimpleClientFactoryAbstract<OwnershipApi> {
  private static final String ALL_FROM = "ALL_FROM_";
  Cache cache;

  public OwnershipApiClientFactory(@CacheName("ownership-cache") final Cache cache) {
    this.cache = cache;
  }

  @Override
  public OwnershipApiClient newClient() {
    return new OwnershipApiClient(this);
  }

  @Override
  public synchronized OwnershipApiClient newClient(final URI uri) {
    return (OwnershipApiClient) super.newClient(uri);
  }

  @Override
  protected Class<?> getServiceClass() {
    return OwnershipApi.class;
  }

  public Collection<ClientBucketAccess> getFromCache(final String client) {
    try {
      final var result = cache.as(CaffeineCache.class).getIfPresent(ALL_FROM + client);
      if (result == null) {
        return null; // NOSONAR intentional for Cache
      }
      return (Collection<ClientBucketAccess>) result.get();
    } catch (final InterruptedException | ExecutionException e) {// NOSONAR intentional
      return null; // NOSONAR intentional for Cache
    }
  }

  public void addToCache(final String client, final Collection<ClientBucketAccess> clientBuckets) {
    cache.as(CaffeineCache.class).put(ALL_FROM + client, CompletableFuture.completedFuture(clientBuckets));
  }

  public void clearCache(final String client) {
    cache.as(CaffeineCache.class).invalidate(ALL_FROM + client).await()
        .atMost(QuarkusProperties.getDurationResponseTimeout());
  }

  public void clearCache() {
    cache.as(CaffeineCache.class).invalidateAll().await().atMost(QuarkusProperties.getDurationResponseTimeout());
  }
}
