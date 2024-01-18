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
import java.util.concurrent.CompletableFuture;

import io.clonecloudstore.administration.client.api.OwnershipApi;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;

public class OwnershipApiClient extends SimpleClientAbstract<OwnershipApi> {
  /**
   * Constructor used by the Factory
   */
  protected OwnershipApiClient(final OwnershipApiClientFactory factory) {
    super(factory, factory.getUri());
  }

  /**
   * Cache on ALL_FROM + clientId
   */
  public Collection<ClientBucketAccess> listAll(final String clientId) throws CcsWithStatusException {
    final var factory = (OwnershipApiClientFactory) getFactory();
    final var cached = factory.getFromCache(clientId);
    if (cached != null) {
      return cached;
    }
    final var uni = getService().listAll(clientId, ClientOwnership.UNKNOWN);
    final var result = (Collection<ClientBucketAccess>) exceptionMapper.handleUniObject(this, uni);
    factory.addToCache(clientId, result);
    return result;
  }

  /**
   * Cache if possible
   */
  public Collection<ClientBucketAccess> listWithOwnership(final String clientId, final ClientOwnership ownership)
      throws CcsWithStatusException {
    final var cached = listAll(clientId);
    if (cached != null) {
      return cached.stream().filter(relationClientBuckets -> relationClientBuckets.ownership().include(ownership))
          .toList();
    }
    final var uni = getService().listAll(clientId, ownership);
    return (Collection<ClientBucketAccess>) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * Cache if possible
   */
  public ClientOwnership findByBucket(final String clientId, final String bucket) throws CcsWithStatusException {
    final var cached = listAll(clientId);
    if (cached != null) {
      var optional =
          cached.stream().filter(relationClientBuckets -> bucket.equals(relationClientBuckets.bucket())).findFirst();
      if (optional.isPresent()) {
        return optional.get().ownership();
      }
    }
    final var uni = getService().findByBucket(clientId, bucket);
    return (ClientOwnership) exceptionMapper.handleUniObject(this, uni);
  }

  private Uni<ClientOwnership> internalAdd(final String clientId, final String bucket,
                                           final ClientOwnership ownership) {
    ((OwnershipApiClientFactory) getFactory()).clearCache(clientId);
    return getService().add(clientId, bucket, ownership);
  }

  /**
   * Invalidate Cache
   */
  public ClientOwnership add(final String clientId, final String bucket, final ClientOwnership ownership)
      throws CcsWithStatusException {
    final var uni = internalAdd(clientId, bucket, ownership);
    return (ClientOwnership) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * Invalidate Cache and async method
   */
  public CompletableFuture<ClientOwnership> addAsync(final String clientId, final String bucket,
                                                     final ClientOwnership ownership) {
    final var uni = internalAdd(clientId, bucket, ownership);
    return uni.subscribe().asCompletionStage();
  }

  /**
   * @return the final result
   * @throws CcsWithStatusException if any error occurs
   */
  public ClientOwnership getClientOwnershipFromAsync(final CompletableFuture<ClientOwnership> completableFuture)
      throws CcsWithStatusException {
    return (ClientOwnership) exceptionMapper.handleCompletableObject(this, completableFuture);
  }

  /**
   * Invalidate Cache
   */
  public ClientOwnership update(final String clientId, final String bucket, final ClientOwnership ownership)
      throws CcsWithStatusException {
    ((OwnershipApiClientFactory) getFactory()).clearCache(clientId);
    final var uni = getService().update(clientId, bucket, ownership);
    return (ClientOwnership) exceptionMapper.handleUniObject(this, uni);
  }

  private Uni<Response> internalDelete(final String clientId, final String bucket) {
    ((OwnershipApiClientFactory) getFactory()).clearCache(clientId);
    return getService().delete(clientId, bucket);
  }

  /**
   * Invalidate Cache
   */
  public boolean delete(final String clientId, final String bucket) throws CcsWithStatusException {
    final var uni = internalDelete(clientId, bucket);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return response.getStatus() == Response.Status.NO_CONTENT.getStatusCode();
    }
  }

  /**
   * Invalidate Cache and async method
   */
  public CompletableFuture<Response> deleteAllClientsAsync(final String bucket) {
    ((OwnershipApiClientFactory) getFactory()).clearCache();
    final var uni = getService().deleteAllClient(bucket);
    return uni.subscribe().asCompletionStage();
  }

  /**
   * @return the final result
   * @throws CcsWithStatusException if any error occurs
   */
  public boolean getBooleanFromAsync(final CompletableFuture<Response> completableFuture)
      throws CcsWithStatusException {
    try (final var response = exceptionMapper.handleCompletableResponse(completableFuture)) {
      return response.getStatus() == Response.Status.NO_CONTENT.getStatusCode();
    }
  }
}
