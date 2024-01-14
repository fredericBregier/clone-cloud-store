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

package io.clonecloudstore.replicator.server.remote.resource;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.client.internal.AccessorObjectInternalApiClient;
import io.clonecloudstore.accessor.client.internal.AccessorObjectInternalApiFactory;
import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.server.service.NativeStreamHandlerAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;

@RequestScoped
public class RemoteReplicatorNativeStreamHandler extends NativeStreamHandlerAbstract<ReplicatorOrder, AccessorObject> {
  private static final Logger LOGGER = Logger.getLogger(RemoteReplicatorNativeStreamHandler.class);
  private static final String COULD_NOT_REMOTE_READ = "Could not remote read from any of remote replicators: %s";
  AccessorObjectInternalApiFactory accessorObjectInternalApiFactory;
  private AccessorObject fromInputStream = null;

  @Override
  protected void postSetup() {
    super.postSetup();
    accessorObjectInternalApiFactory = CDI.current().select(AccessorObjectInternalApiFactory.class).get();
  }

  @Override
  protected boolean checkDigestToCompute(final ReplicatorOrder businessIn) {
    return false;
  }

  @Override
  protected AccessorObject getAnswerPushInputStream(final ReplicatorOrder replicatorOrder, final String finalHash,
                                                    final long size) {
    // NO PUSH
    return null;
  }

  @Override
  protected Map<String, String> getHeaderPushInputStream(final ReplicatorOrder replicatorOrder, final String finalHash,
                                                         final long size, final AccessorObject accessorObject) {
    // NO PUSH
    return new HashMap<>();
  }

  @Override
  protected void checkPushAble(final ReplicatorOrder payload, final MultipleActionsInputStream inputStream) {
    // NO PUSH
  }

  @Override
  protected boolean checkPullAble(final ReplicatorOrder replicatorOrder, final MultiMap headers) {
    // Check shall be done before and will be done implicitly during access
    return true;
  }

  @Override
  protected InputStream getPullInputStream(final ReplicatorOrder replicatorOrder) {
    AccessorObjectInternalApiClient client = null;
    try {
      client = accessorObjectInternalApiFactory.newClient();
      getCloser().add(client);
      // Store output object for getHeaderPullInputStream
      client.setOpId(getOpId());
      final var result =
          client.getObject(replicatorOrder.bucketName(), replicatorOrder.objectName(), replicatorOrder.clientId(),
              false);
      fromInputStream = result.dtoOut();
      setResponseCompressed(result.compressed());
      return result.inputStream();
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      LOGGER.errorf(COULD_NOT_REMOTE_READ, e.getMessage());
      throw e;
    } catch (final CcsWithStatusException e) {
      LOGGER.errorf(COULD_NOT_REMOTE_READ, e.getMessage());
      throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), e.getMessage(), e);
    } catch (final RuntimeException e) {
      LOGGER.errorf(COULD_NOT_REMOTE_READ, e.getMessage());
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final ReplicatorOrder replicatorOrder) {
    if (fromInputStream != null) {
      final Map<String, String> map = new HashMap<>();
      AccessorHeaderDtoConverter.objectToMap(fromInputStream, map);
      return map;
    }
    return replicatorOrder.getHeaders();
  }

  @Override
  protected Map<String, String> getHeaderError(final ReplicatorOrder replicatorOrder, final int status) {
    return replicatorOrder.getHeaders();
  }
}
