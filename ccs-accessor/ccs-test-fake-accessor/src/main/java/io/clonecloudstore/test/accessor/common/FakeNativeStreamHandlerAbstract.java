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
import io.clonecloudstore.accessor.server.commons.AbstractObjectNativeStreamHandler;
import io.clonecloudstore.accessor.server.commons.AccessorObjectServiceInterface;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public abstract class FakeNativeStreamHandlerAbstract extends AbstractObjectNativeStreamHandler {
  private static final Logger LOGGER = Logger.getLogger(FakeNativeStreamHandlerAbstract.class);

  protected FakeNativeStreamHandlerAbstract() {
    super((AccessorObjectServiceInterface) null);
  }

  @Override
  protected void postSetup() {
    super.postSetup();
    internalInitBeforeAction();
  }

  protected abstract boolean isPublic();

  /**
   * Initialize information from Headers
   */
  protected void internalInitBeforeAction() {
  }

  @Override
  protected void checkPushAble(final AccessorObject object, final MultipleActionsInputStream inputStream) {
    if (FakeCommonObjectResourceHelper.errorCode > 0) {
      throw CcsServerGenericExceptionMapper.getCcsException(FakeCommonObjectResourceHelper.errorCode);
    }
    try {
      final var objectStorage =
          new StorageObject(object.getBucket(), object.getName(), object.getHash(), object.getSize(),
              object.getCreation());
      driverApi.objectPrepareCreateInBucket(objectStorage, inputStream);
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
      // Hash from MultipleActionsInputStream (on the fly)
      hash = finalHash;
    }
    try {
      final var storageObject =
          driverApi.objectFinalizeCreateInBucket(object.getBucket(), object.getName(), size, hash);
      final var accessorObject = FakeCommonObjectResourceHelper.fromStorageObject(storageObject);
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
    if (FakeCommonObjectResourceHelper.errorCode > 0) {
      if (FakeCommonObjectResourceHelper.errorCode == 404) {
        return false;
      }
      throw CcsServerGenericExceptionMapper.getCcsException(FakeCommonObjectResourceHelper.errorCode);
    }
    try {
      final var storageObject = driverApi.objectGetMetadataInBucket(object.getBucket(), object.getName());
      if (storageObject != null) {
        checked.set(FakeCommonObjectResourceHelper.fromStorageObject(storageObject));
        LOGGER.infof("Checked FakeApi %s/%s", object.getBucket(), object.getName());
        return true;
      }
      if (isPublic() && checkRemotePullable(object, getRequest().headers(), clientId)) {
        checked.set(object.setStatus(AccessorStatus.READY).cloneInstance().setHash("hash").setCreation(Instant.now())
            .setId(GuidLike.getGuid()).setSize(FakeCommonObjectResourceHelper.length));
      } else {
        checked.set(null);
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
      if (FakeCommonObjectResourceHelper.errorCode > 0) {
        if (FakeCommonObjectResourceHelper.errorCode >= 400) {
          throw CcsServerGenericExceptionMapper.getCcsException(FakeCommonObjectResourceHelper.errorCode);
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
  protected Map<String, String> getHeaderError(final AccessorObject object, final int status) {
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(object, map);
    return map;
  }
}

