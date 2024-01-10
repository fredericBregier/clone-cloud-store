/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.accessor.server.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
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
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.API_ROOT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;

@Dependent
public abstract class AbstractObjectNativeStreamHandler
    extends NativeStreamHandlerAbstract<AccessorObject, AccessorObject> {
  private static final Logger LOGGER = Logger.getLogger(AbstractObjectNativeStreamHandler.class);
  protected final AccessorObjectServiceInterface service;
  protected DriverApi driverApi;
  protected AccessorFilter filter;
  protected volatile boolean isListing;
  protected String clientId;
  protected boolean external;
  protected AtomicReference<AccessorObject> checked;

  protected AbstractObjectNativeStreamHandler(final AccessorObjectServiceInterface service) {
    this.service = service;
  }

  @Override
  protected void postSetup() {
    super.postSetup();
    setDriverApi();
    getCloser().add(driverApi);
    checked = new AtomicReference<>();
    final var headers = this.getRequest().headers();
    clientId = headers.get(X_CLIENT_ID);
    external = this.getRequest().uri().startsWith(API_ROOT);
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
      final var accessorObject = service.createObject(object, object.getHash(), object.getSize());
      final var objectStorage =
          new StorageObject(accessorObject.getBucket(), accessorObject.getName(), accessorObject.getHash(),
              accessorObject.getSize(), accessorObject.getCreation(), accessorObject.getExpires(),
              accessorObject.getMetadata());
      LOGGER.debugf("Debug Log Creation: %s %s from %s", object.getBucket(), objectStorage, accessorObject);
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
      // Hash from MultipleActionsInputStream (on the fly)
      hash = finalHash;
    }
    try {
      LOGGER.debugf("Debug Log End Creation: %s %s from %s", object.getBucket(), object.getName(), object);
      // Size here is the "real" uncompressed size
      final var storage = driverApi.objectFinalizeCreateInBucket(object.getBucket(), object.getName(), size, hash);
      final var accessorObject = object.cloneInstance().setHash(storage.hash()).setCreation(storage.creationDate())
          .setExpires(storage.expiresDate()).setSite(ServiceProperties.getAccessorSite());
      return service.createObjectFinalize(accessorObject, hash, size, clientId, external);
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

  @Override
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
}
