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
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.server.service.NativeServerResponseException;
import io.clonecloudstore.common.quarkus.server.service.NativeStreamHandlerAbstract;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.API_ROOT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;

@RequestScoped
public class ObjectNativeStreamHandler extends NativeStreamHandlerAbstract<AccessorObject, AccessorObject> {
  private static final Logger LOGGER = Logger.getLogger(ObjectNativeStreamHandler.class);
  private final AccessorObjectService service;
  private DriverApi driverApi;
  private AccessorFilter filter;
  private volatile boolean isListing;
  private String clientId;
  private boolean external;
  private AtomicReference<AccessorObject> checked;
  private AtomicReference<String> remoteTargetId;

  protected ObjectNativeStreamHandler(final AccessorObjectService service) {
    this.service = service;
  }

  @Override
  protected void postSetup() {
    super.postSetup();
    setDriverApi();
    getCloser().add(driverApi);
    remoteTargetId = new AtomicReference<>();
    checked = new AtomicReference<>();
    final var headers = this.getRequest().headers();
    clientId = headers.get(X_CLIENT_ID);
    external = this.getRequest().uri().startsWith(API_ROOT);
    LOGGER.debugf("External ? %b %s", external, this.getRequest().uri());
    final var currentBucketName = getBusinessIn().getBucket();
    final var currentObjectName = getBusinessIn().getName();
    AccessorHeaderDtoConverter.objectFromMap(getBusinessIn(), headers);
    // Force for Replicator already having the right bucket name and object name, but not others while already computed
    LOGGER.debugf("Previous bucket %s => %s", getBusinessIn().getBucket(), currentBucketName);
    getBusinessIn().setBucket(currentBucketName);
    if (ParametersChecker.isNotEmpty(currentObjectName)) {
      getBusinessIn().setName(currentObjectName);
    }
    // Force local site
    getBusinessIn().setSite(ServiceProperties.getAccessorSite());
    LOGGER.debugf("Object to create/return: %s", getBusinessIn());
    isListing = false;
    filter = null;
  }

  @Override
  protected boolean checkDigestToCumpute(final AccessorObject businessIn) {
    return true;
  }

  private void setDriverApi() {
    if (driverApi == null) {
      // No native CDI here
      driverApi = DriverApiRegistry.getDriverApiFactory().getInstance();
    }
  }

  @Override
  protected void checkPushAble(final AccessorObject object, final MultipleActionsInputStream inputStream) {
    try {
      // Size may differ on Object and InputStream if compression
      final var daoObject = service.createObject(object, object.getHash(), object.getSize());
      final var objectStorage =
          new StorageObject(daoObject.getBucket(), daoObject.getName(), daoObject.getHash(), daoObject.getSize(),
              daoObject.getCreation());
      LOGGER.debugf("Debug Log Creation: %s %s from %s", object.getBucket(), objectStorage, daoObject);
      driverApi.objectPrepareCreateInBucket(objectStorage, inputStream);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverAlreadyExistException e) {
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected AccessorObject getAnswerPushInputStream(final AccessorObject object, final String finalHash,
                                                    final long size) {
    // Hash from request
    var hash = object.getHash();
    if (finalHash != null) {
      // Hash from NettyToInputStream (on the fly)
      hash = finalHash;
    }
    try {
      LOGGER.debugf("Debug Log End Creation: %s %s from %s", object.getBucket(), object.getName(), object);
      // Size here is the "real" uncompressed size
      driverApi.objectFinalizeCreateInBucket(object.getBucket(), object.getName(), size, hash);
      final var dao =
          service.createObjectFinalize(object.getBucket(), object.getName(), hash, size, clientId, external);
      return dao.getDto();
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverAlreadyExistException e) {
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderPushInputStream(final AccessorObject objectIn, final String finalHash,
                                                         final long size, final AccessorObject objectOut) {
    return new HashMap<>();
  }

  public Response pullList() throws NativeServerResponseException {
    try {
      isListing = true;
      final var accessorFilter = new AccessorFilter();
      final var foundFilter = AccessorHeaderDtoConverter.filterFromMap(accessorFilter, this.getRequest().headers());
      if (foundFilter) {
        accessorFilter.setNamePrefix(ParametersChecker.getSanitizedName(accessorFilter.getNamePrefix()));
        filter = accessorFilter;
      } else {
        filter = new AccessorFilter();
      }
      preparePull();
      // Proxy operation is possible, so getting first InputStream in order to get correct Headers then
      return doGetInputStream();
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      // Change to create an exception with response that will be used in case of error
      sendError(e.getStatus(), e);
    } catch (final NativeServerResponseException e) {
      throw e;
    } catch (final Exception e) {
      sendError(Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      clear();
    }
    throw getNativeClientResponseException(Response.Status.INTERNAL_SERVER_ERROR);
  }

  @Override
  protected boolean checkPullAble(final AccessorObject object, final MultiMap headers) {
    try {
      if (isListing) {
        checked.set(null);
        LOGGER.debug("Filter able: " + object.getBucket() + " = " + filter);
        return true;
      }
      final var response = service.checkPullable(object.getBucket(), object.getName(), external, clientId, getOpId());
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
            service.getRemotePullInputStream(object.getBucket(), object.getName(), clientId, remoteTargetId.get(),
                getOpId());
        checked.set(inputStreamBusinessOut.dtoOut());
        if (AccessorProperties.isFixOnAbsent()) {
          service.generateReplicationOrderForObject(checked.get().getBucket(), checked.get().getName(), clientId,
              getOpId(), remoteTargetId.get(), checked.get().getSize(), checked.get().getHash());
        }
        return inputStreamBusinessOut.inputStream();
      }
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final AccessorObject objectIn) {
    final Map<String, String> map = new HashMap<>();
    if (isListing) {
      return map;
    }
    if (checked.get() != null) {
      AccessorHeaderDtoConverter.objectToMap(checked.get(), map);
      return map;
    }
    AccessorHeaderDtoConverter.objectToMap(objectIn, map);
    return map;
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
        service.inError(object.getBucket(), object.getName());
      }
    } catch (final CcsNotExistException | CcsServerGenericException ignore) {
      // Ignore
    }
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(object, map);
    return map;
  }
}
