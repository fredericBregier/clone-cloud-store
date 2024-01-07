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

package io.clonecloudstore.test.accessor.common;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.server.service.NativeStreamHandlerAbstract;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_HASH;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_SIZE;

@Dependent
public abstract class FakeNativeStreamHandlerAbstract
    extends NativeStreamHandlerAbstract<AccessorObject, AccessorObject> {
  private static final Logger LOGGER = Logger.getLogger(FakeNativeStreamHandlerAbstract.class);
  private DriverApi driverApi;
  private AccessorObject checked;
  private String clientId;

  @Override
  protected void postSetup() {
    internalInitBeforeAction();
  }

  protected abstract boolean isPublic();

  /**
   * Initialize information from Headers
   */
  protected void internalInitBeforeAction() {
    setDriverApi();
    getCloser().add(driverApi);
    final var headers = getRequest().headers();
    clientId = headers.get(X_CLIENT_ID);
    final var currentBucketName = getBusinessIn().getBucket();
    final var currentObjectName = getBusinessIn().getName();
    AccessorHeaderDtoConverter.objectFromMap(getBusinessIn(), headers);
    // Force for Replicator already having the right bucket name and object name, but not others while already computed
    LOGGER.debugf("Previous bucket %s => %s", getBusinessIn().getBucket(), currentBucketName);
    getBusinessIn().setBucket(currentBucketName);
    if (ParametersChecker.isNotEmpty(currentObjectName)) {
      getBusinessIn().setName(currentObjectName);
    }
  }

  private void setDriverApi() {
    if (driverApi == null) {
      driverApi = DriverApiRegistry.getDriverApiFactory().getInstance();
    }
  }

  @Override
  protected void checkPushAble(final AccessorObject object, final MultipleActionsInputStream nettyToInputStream) {
    if (FakeObjectServiceAbstract.errorCode > 0) {
      throw CcsServerGenericExceptionMapper.getCcsException(FakeObjectServiceAbstract.errorCode);
    }
    try {
      final var objectStorage =
          new StorageObject(object.getBucket(), object.getName(), object.getHash(), object.getSize(),
              object.getCreation());
      driverApi.objectPrepareCreateInBucket(objectStorage, nettyToInputStream);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverAlreadyExistException e) {
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  /**
   * Could be overridden to take into account remote creation
   */
  protected void remoteCreation(final AccessorObject objectOut, final String clientId) {
    // Nothing
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
      final var storageObject =
          driverApi.objectFinalizeCreateInBucket(object.getBucket(), object.getName(), size, hash);
      final var accessorObject = FakeObjectServiceAbstract.fromStorageObject(storageObject);
      remoteCreation(accessorObject, clientId);
      return accessorObject;
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
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(objectOut, map);
    // Hash from request
    if (ParametersChecker.isNotEmpty(finalHash)) {
      // Hash from NettyToInputStream (on the fly)
      map.put(X_OBJECT_HASH, finalHash);
    }
    if (size > 0) {
      map.put(X_OBJECT_SIZE, Long.toString(size));
    }
    return map;
  }

  /**
   * Could be overridden to take into account remote check
   */
  protected boolean checkRemotePullable(final AccessorObject object, final MultiMap headers, final String clientId) {
    return false;
  }

  @Override
  protected boolean checkPullAble(final AccessorObject object, final MultiMap headers) {
    return true;
  }

  protected boolean internalCheckPullable(final AccessorObject object) {
    LOGGER.infof("Check FakeApi %s/%s", object.getBucket(), object.getName());
    if (FakeObjectServiceAbstract.errorCode > 0) {
      if (FakeObjectServiceAbstract.errorCode == 404) {
        return false;
      }
      throw CcsServerGenericExceptionMapper.getCcsException(FakeObjectServiceAbstract.errorCode);
    }
    try {
      final var storageObject = driverApi.objectGetMetadataInBucket(object.getBucket(), object.getName());
      if (storageObject != null) {
        checked = FakeObjectServiceAbstract.fromStorageObject(storageObject);
        LOGGER.infof("Checked FakeApi %s/%s", object.getBucket(), object.getName());
        return true;
      }
      if (isPublic() && checkRemotePullable(object, getRequest().headers(), clientId)) {
        checked = object.setStatus(AccessorStatus.READY).cloneInstance().setHash("hash").setCreation(Instant.now())
            .setId(GuidLike.getGuid()).setSize(FakeObjectServiceAbstract.length);
      } else {
        checked = null;
      }
      LOGGER.infof("Checked failed FakeApi %s/%s", object.getBucket(), object.getName());
      return false;
    } catch (final DriverNotFoundException e) {
      if (isPublic()) {
        return checkRemotePullable(object, getRequest().headers(), clientId);
      }
      LOGGER.infof("Checked failed FakeApi %s/%s", object.getBucket(), object.getName());
      return false;
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  /**
   * Could be overridden to use remote access
   */
  protected InputStream getRemotePullInputStream(final AccessorObject object, final String clientId) {
    throw new CcsNotExistException(object.getName());
  }

  @Override
  protected InputStream getPullInputStream(final AccessorObject object) {
    try {
      if (FakeObjectServiceAbstract.errorCode > 0) {
        if (FakeObjectServiceAbstract.errorCode >= 400) {
          throw CcsServerGenericExceptionMapper.getCcsException(FakeObjectServiceAbstract.errorCode);
        } else {
          return new FakeInputStream(100);
        }
      }
      if (!internalCheckPullable(object)) {
        LOGGER.infof("Pull FakeApi %s/%s", object.getBucket(), object.getName());
      }
      return driverApi.objectGetInputStreamInBucket(object.getBucket(), object.getName());
    } catch (final DriverNotFoundException e) {
      LOGGER.infof("Pull failed but try remote FakeApi %s/%s", object.getBucket(), object.getName());
      if (isPublic()) {
        // Else use remote read
        return getRemotePullInputStream(object, clientId);
      }
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final AccessorObject objectIn) {
    final Map<String, String> map = new HashMap<>();
    if (checked != null) {
      AccessorHeaderDtoConverter.objectToMap(checked, map);
      return map;
    }
    AccessorHeaderDtoConverter.objectToMap(objectIn, map);
    return map;
  }

  @Override
  protected Map<String, String> getHeaderError(final AccessorObject object, final int status) {
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(object, map);
    return map;
  }

  @Override
  protected boolean checkDigestToCumpute(final AccessorObject businessIn) {
    return true;
  }
}

