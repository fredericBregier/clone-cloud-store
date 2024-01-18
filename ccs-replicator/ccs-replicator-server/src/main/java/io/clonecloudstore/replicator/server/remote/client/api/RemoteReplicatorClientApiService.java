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

package io.clonecloudstore.replicator.server.remote.client.api;

import java.net.URI;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.server.remote.client.RemoteReplicatorApiClient;
import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.ClientWebApplicationException;

import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Note: URI is needed for the cache
 */
@ApplicationScoped
@Unremovable
public class RemoteReplicatorClientApiService {
  // Keep arguments needed by ApiKeyGenerator
  private static final ClientResponseExceptionMapper exceptionMapper = new ClientResponseExceptionMapper();
  public static final String REMOTE_CHECK_BUCKET = "remote-check-bucket";
  public static final String REMOTE_CHECK_OBJECT = "remote-check-object";

  @CacheResult(cacheName = REMOTE_CHECK_BUCKET, keyGenerator = RemoteReplicatorApiKeyGenerator.class)
  public StorageType checkBucketCache(final RemoteReplicatorApi client, final URI uri, final String bucket,
                                      final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    return checkBucket(client, uri, bucket, fullCheck, clientId, opId);
  }

  public StorageType checkBucket(final RemoteReplicatorApi client, final String bucket, final boolean fullCheck,
                                 final String clientId, final String opId) throws CcsWithStatusException {
    return checkBucket(client, null, bucket, fullCheck, clientId, opId);
  }

  private StorageType checkBucket(final RemoteReplicatorApi client, final URI ignoredUri,// NOSONAR for cache
                                  final String bucket, final boolean fullCheck, final String clientId,
                                  final String opId) throws CcsWithStatusException {
    final var uni = client.checkBucket(bucket, fullCheck, clientId, opId);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return AccessorHeaderDtoConverter.getStorageTypeFromResponse(response);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == NOT_FOUND.getStatusCode()) {
        return StorageType.NONE;
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(bucket, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public AccessorBucket getBucket(final RemoteReplicatorApiClient upperClient, final RemoteReplicatorApi client,
                                  final String bucket, final String clientId, final String opId)
      throws CcsWithStatusException {
    final var uni = client.getBucket(bucket, clientId, opId);
    return (AccessorBucket) exceptionMapper.handleUniObject(upperClient, uni);
  }

  @CacheInvalidate(cacheName = REMOTE_CHECK_BUCKET, keyGenerator = RemoteReplicatorApiKeyGenerator.class)
  public void clearCacheBucket(final URI ignoredUri, final String ignoredBucket) {
    // Empty
  }

  @CacheInvalidateAll(cacheName = REMOTE_CHECK_BUCKET)
  public void invalidateCacheBucket() {
    // Empty
  }

  @CacheResult(cacheName = REMOTE_CHECK_OBJECT, keyGenerator = RemoteReplicatorApiKeyGenerator.class)
  public StorageType checkObjectOrDirectoryCache(final RemoteReplicatorApi client, final URI uri, final String bucket,
                                                 final String pathDirectoryOrObject, final boolean fullCheck,
                                                 final String clientId, final String opId)
      throws CcsWithStatusException {
    return checkObjectOrDirectory(client, uri, bucket, pathDirectoryOrObject, fullCheck, clientId, opId);
  }

  public StorageType checkObjectOrDirectory(final RemoteReplicatorApi client, final String bucket,
                                            final String pathDirectoryOrObject, final boolean fullCheck,
                                            final String clientId, final String opId) throws CcsWithStatusException {
    return checkObjectOrDirectory(client, null, bucket, pathDirectoryOrObject, fullCheck, clientId, opId);
  }

  private StorageType checkObjectOrDirectory(final RemoteReplicatorApi client, final URI ignoredUri,// NOSONAR for cache
                                             final String bucket, final String pathDirectoryOrObject,
                                             final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    final var uni = client.checkObjectOrDirectory(bucket, pathDirectoryOrObject, fullCheck, clientId, opId);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return AccessorHeaderDtoConverter.getStorageTypeFromResponse(response);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == NOT_FOUND.getStatusCode()) {
        return StorageType.NONE;
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(pathDirectoryOrObject, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }

  @CacheInvalidate(cacheName = REMOTE_CHECK_OBJECT, keyGenerator = RemoteReplicatorApiKeyGenerator.class)
  public void clearCacheObject(final URI ignoredUri, final String ignoredBucket,
                               final String ignoredPathDirectoryOrObject) {
    // Empty
  }

  @CacheInvalidateAll(cacheName = REMOTE_CHECK_OBJECT)
  public void invalidateCacheObject() {
    // Empty
  }
}
