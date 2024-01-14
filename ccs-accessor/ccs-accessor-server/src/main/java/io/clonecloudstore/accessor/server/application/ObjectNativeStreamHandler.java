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

package io.clonecloudstore.accessor.server.application;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.AbstractObjectNativeStreamHandler;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@RequestScoped
public class ObjectNativeStreamHandler extends AbstractObjectNativeStreamHandler {
  private static final Logger LOGGER = Logger.getLogger(ObjectNativeStreamHandler.class);
  private AtomicReference<String> remoteTargetId;

  protected ObjectNativeStreamHandler() {
    super(CDI.current().select(AccessorObjectService.class).get());
  }

  protected ObjectNativeStreamHandler(final AccessorObjectService service) {
    super(service);
  }

  @Override
  protected void postSetup() {
    super.postSetup();
    remoteTargetId = new AtomicReference<>();
    LOGGER.debugf("External ? %b %s", external, this.getRequest().uri());
  }

  @Override
  protected boolean checkPullAble(final AccessorObject object, final MultiMap headers) {
    try {
      if (isListing) {
        checked.set(null);
        LOGGER.debug("Filter able: " + object.getBucket() + " = " + filter);
        return true;
      }
      final var response =
          ((AccessorObjectService) service).checkPullable(object.getBucket(), object.getName(), external, clientId,
              getOpId());
      checked.set(response.response());
      remoteTargetId.set(response.targetId());
      LOGGER.debug("Pull able: " + object.getBucket() + "/" + object.getName() + " = " + object);
      return true;
    } catch (final CcsNotExistException e) {
      LOGGER.debug("Not Pull able: " + object.getBucket() + "/" + object.getName());
      checked.set(null);
      return false;
    }
  }

  @Override
  protected InputStream getPullInputStream(final AccessorObject object) {
    try {
      if (isListing) {
        return service.filterObjects(object.getBucket(), filter);
      }
      LOGGER.debugf("Debug Log Read: %s %s", object.getBucket(), object.getName());
      return driverApi.objectGetInputStreamInBucket(object.getBucket(), object.getName());
    } catch (final DriverNotFoundException e) {
      if (external && AccessorProperties.isRemoteRead() && remoteTargetId.get() != null) {
        //Else use remote read.
        // Replicator client to remote read (which will raised NOT_FOUND if necessary)
        final InputStreamBusinessOut<AccessorObject> inputStreamBusinessOut;
        inputStreamBusinessOut =
            ((AccessorObjectService) service).getRemotePullInputStream(object.getBucket(), object.getName(), clientId,
                remoteTargetId.get(), getOpId());
        checked.set(inputStreamBusinessOut.dtoOut());
        setResponseCompressed(inputStreamBusinessOut.compressed());
        if (AccessorProperties.isFixOnAbsent()) {
          ((AccessorObjectService) service).generateReplicationOrderForObject(checked.get().getBucket(),
              checked.get().getName(), clientId, getOpId(), remoteTargetId.get(), checked.get().getSize(),
              checked.get().getHash());
        }
        return inputStreamBusinessOut.inputStream();
      }
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderError(final AccessorObject object, final int status) {
    try {
      final var currentStatus = service.getObjectInfo(object.getBucket(), object.getName()).getStatus();
      LOGGER.infof("Will send error %d while object status in %s for %s", status, currentStatus, object);
      // Write only and If not NOT_ACCEPTABLE (push), can change status
      // And If IN_PROGRESS or UNKNOWN or NULL, can change status
      if (isUpload() && status != Response.Status.NOT_ACCEPTABLE.getStatusCode() &&
          (AccessorStatus.UPLOAD.equals(currentStatus) || AccessorStatus.UNKNOWN.equals(currentStatus) ||
              currentStatus == null)) {
        ((AccessorObjectService) service).inError(object.getBucket(), object.getName());
      }
    } catch (final CcsNotExistException | CcsServerGenericException ignore) {
      // Ignore
    }
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(object, map);
    return map;
  }
}
