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

package io.clonecloudstore.replicator.server.remote.client;

import java.util.List;
import java.util.Map;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.server.remote.client.api.RemoteReplicatorApi;
import io.clonecloudstore.replicator.server.remote.client.api.RemoteReplicatorClientApiService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.core.Response;

public class RemoteReplicatorApiClient extends ClientAbstract<ReplicatorOrder, AccessorObject, RemoteReplicatorApi> {
  private final RemoteReplicatorClientApiService apiService;

  /**
   * Constructor used by the Factory
   */
  protected RemoteReplicatorApiClient(final RemoteReplicatorApiClientFactory factory) {
    super(factory, factory.getUri());
    apiService = CDI.current().select(RemoteReplicatorClientApiService.class).get();
  }

  /**
   * Cached response
   */
  public StorageType checkBucketCache(final String bucket, final boolean fullCheck, final String clientId,
                                      final String opId) throws CcsWithStatusException {
    return apiService.checkBucketCache(getService(), getUri(), bucket, fullCheck, clientId, opId);
  }

  /**
   * Cached response
   */
  public StorageType checkObjectOrDirectoryCache(final String bucket, final String pathDirectoryOrObject,
                                                 final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    return apiService.checkObjectOrDirectoryCache(getService(), getUri(), bucket, pathDirectoryOrObject, fullCheck,
        clientId, opId);
  }

  /**
   * Not Cached
   */
  public StorageType checkBucket(final String bucket, final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    return apiService.checkBucket(getService(), bucket, fullCheck, clientId, opId);
  }

  /**
   * Not Cached
   */
  public AccessorBucket getBucket(final String bucket, final String clientId, final String opId)
      throws CcsWithStatusException {
    return apiService.getBucket(this, getService(), bucket, clientId, opId);
  }

  /**
   * Not Cached
   */
  public StorageType checkObjectOrDirectory(final String bucket, final String pathDirectoryOrObject,
                                            final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    return apiService.checkObjectOrDirectory(getService(), bucket, pathDirectoryOrObject, fullCheck, clientId, opId);
  }

  public InputStreamBusinessOut<AccessorObject> readRemoteObject(final String bucket, final String object,
                                                                 final String clientId, final String opId,
                                                                 final long len) throws CcsWithStatusException {
    this.setOpId(opId);
    final var request =
        new ReplicatorOrder(opId, ServiceProperties.getAccessorSite(), null, clientId, bucket, object, len, null,
            ReplicatorConstants.Action.UNKNOWN);
    prepareInputStreamToReceive(AccessorProperties.isInternalCompression(), request);
    final var uni =
        getService().remoteReadObject(AccessorProperties.isInternalCompression(), bucket, object, clientId, getOpId());
    return getInputStreamBusinessOutFromUni(true, uni);
  }

  public Uni<Response> createOrder(final ReplicatorOrder replicatorOrder) {
    return getService().createOrder(replicatorOrder);
  }

  public Uni<Response> createOrders(final List<ReplicatorOrder> replicatorOrders) {
    return getService().createOrders(replicatorOrders);
  }

  @Override
  protected AccessorObject getApiBusinessOutFromResponseForCreate(final Response response) {
    // No Push
    return null;
  }

  @Override
  protected Map<String, String> getHeadersFor(final ReplicatorOrder businessIn, final int context) {
    return businessIn.getHeaders();
  }
}
