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

package io.clonecloudstore.replicator.client;

import java.util.Map;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.client.api.LocalReplicatorApi;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.model.ReplicatorResponse;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.ClientWebApplicationException;

import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

public class LocalReplicatorApiClient extends ClientAbstract<ReplicatorOrder, AccessorObject, LocalReplicatorApi> {
  /**
   * Constructor used by the Factory
   */
  protected LocalReplicatorApiClient(final LocalReplicatorApiClientFactory factory) {
    super(factory, factory.getUri());
  }

  public ReplicatorResponse<AccessorBucket> getBucket(final String bucket, final String clientId, final String opId)
      throws CcsWithStatusException {
    return getBucket(bucket, clientId, "", opId);
  }

  public ReplicatorResponse<AccessorBucket> getBucket(final String bucket, final String clientId, final String targetId,
                                                      final String opId) throws CcsWithStatusException {
    final var uni = getService().getBucket(bucket, clientId, targetId, opId);
    try {
      return (ReplicatorResponse<AccessorBucket>) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(), e);
    }
  }

  public ReplicatorResponse<StorageType> checkBucket(final String bucket, final boolean fullCheck,
                                                     final String clientId, final String opId)
      throws CcsWithStatusException {
    return checkBucket(bucket, fullCheck, clientId, "", opId);
  }

  private static String getTargetFromResponse(final Response response) {
    var target = response.getHeaderString(AccessorConstants.Api.X_TARGET_ID);
    if (ParametersChecker.isEmpty(target)) {
      return null;
    }
    return target;
  }

  public ReplicatorResponse<StorageType> checkBucket(final String bucket, final boolean fullCheck,
                                                     final String clientId, final String targetId, final String opId)
      throws CcsWithStatusException {
    final var uni = getService().checkBucket(bucket, fullCheck, clientId, targetId, opId);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return new ReplicatorResponse<>(AccessorHeaderDtoConverter.getStorageTypeFromResponse(response),
          getTargetFromResponse(response));
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == NOT_FOUND.getStatusCode()) {
        return new ReplicatorResponse<>(StorageType.NONE, null);
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(), e);
    }
  }

  public ReplicatorResponse<StorageType> checkObjectOrDirectory(final String bucket, final String pathDirectoryOrObject,
                                                                final boolean fullCheck, final String clientId,
                                                                final String opId) throws CcsWithStatusException {
    return checkObjectOrDirectory(bucket, pathDirectoryOrObject, fullCheck, clientId, "", opId);
  }

  public ReplicatorResponse<StorageType> checkObjectOrDirectory(final String bucket, final String pathDirectoryOrObject,
                                                                final boolean fullCheck, final String clientId,
                                                                final String targetId, final String opId)
      throws CcsWithStatusException {
    final var uni =
        getService().checkObjectOrDirectory(bucket, pathDirectoryOrObject, fullCheck, clientId, targetId, opId);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return new ReplicatorResponse<>(AccessorHeaderDtoConverter.getStorageTypeFromResponse(response),
          getTargetFromResponse(response));
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == NOT_FOUND.getStatusCode()) {
        return new ReplicatorResponse<>(StorageType.NONE, null);
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(), e);
    }
  }

  public InputStreamBusinessOut<AccessorObject> readRemoteObject(final String bucket, final String object,
                                                                 final String clientId, final String targetId,
                                                                 final String opId) throws CcsWithStatusException {
    return readRemoteObject(bucket, object, clientId, targetId, opId, true);
  }

  public InputStreamBusinessOut<AccessorObject> readRemoteObject(final String bucket, final String object,
                                                                 final String clientId, final String targetId,
                                                                 final String opId, final boolean decompress)
      throws CcsWithStatusException {
    this.setOpId(opId);
    final var request =
        new ReplicatorOrder(opId, ServiceProperties.getAccessorSite(), targetId, clientId, bucket, object, 0, null,
            ReplicatorConstants.Action.UNKNOWN);
    prepareInputStreamToReceive(AccessorProperties.isInternalCompression(), request);
    final var uni =
        getService().remoteReadObject(AccessorProperties.isInternalCompression(), bucket, object, clientId, getOpId(),
            targetId);
    return getInputStreamBusinessOutFromUni(decompress, uni);
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
  // FIXME Add PUT to get Listing
}
