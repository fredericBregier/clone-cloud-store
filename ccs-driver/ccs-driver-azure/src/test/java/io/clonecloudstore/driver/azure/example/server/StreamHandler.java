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

package io.clonecloudstore.driver.azure.example.server;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.server.service.StreamHandlerAbstract;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.driver.azure.example.client.ApiConstants;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import org.jboss.logging.Logger;

import static io.clonecloudstore.driver.azure.example.server.ApiService.NOT_ACCEPTABLE_NAME;

@RequestScoped
public class StreamHandler extends StreamHandlerAbstract<StorageObject, StorageObject> {
  private static final Logger LOG = Logger.getLogger(StreamHandler.class);
  private volatile StorageObject pullAble;
  private DriverApi driverApi;

  private void setDriverApi() {
    if (driverApi == null) {
      // No native CDI here
      driverApi = DriverApiRegistry.getDriverApiFactory().getInstance();
    }
  }

  @Override
  protected void postSetup() {
    super.postSetup();
    setDriverApi();
    getCloser().add(driverApi);
  }

  @Override
  protected void checkPushAble(final StorageObject apiBusinessIn, final MultipleActionsInputStream inputStream) {
    // Business code should come here (example: check through Object Storage that object does not exist yet, and if so
    // add the consumption of the stream for the Object Storage object creation)
    if (NOT_ACCEPTABLE_NAME.equals(apiBusinessIn.name())) {
      throw new CcsNotAcceptableException("Not Acceptable test");
    }
    try {
      driverApi.objectPrepareCreateInBucket(apiBusinessIn, inputStream);
    } catch (final DriverNotFoundException e) {
      LOG.debugf("Not Pushable since not found: %s/%s", apiBusinessIn.bucket(), apiBusinessIn.name());
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverAlreadyExistException e) {
      LOG.debugf("Not Pushable since already exists: %s/%s", apiBusinessIn.bucket(), apiBusinessIn.name());
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected StorageObject getAnswerPushInputStream(final StorageObject apiBusinessIn, final String finalHash,
                                                   final long size) {
    // Business code should come here (example: getting the StorageObject object)
    // Hash from request
    var hash = apiBusinessIn.hash();
    if (finalHash != null) {
      // Hash from InputStream (on the fly)
      hash = finalHash;
    }
    try {
      return driverApi.objectFinalizeCreateInBucket(apiBusinessIn.bucket(), apiBusinessIn.name(), size, hash);
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverAlreadyExistException e) {
      throw new CcsAlreadyExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderPushInputStream(final StorageObject apiBusinessIn, final String finalHash,
                                                         final long size, final StorageObject apiBusinessOut) {
    // Business code should come here (example: headers for object name, object size, ...)
    return new HashMap<>();
  }

  @Override
  protected boolean checkPullAble(final StorageObject apiBusinessIn, final MultiMap headers) {
    // Business code should come here
    // Put here business logic to check if the GET is valid (example: ObjectStorage check of existence of object)
    try {
      pullAble = driverApi.objectGetMetadataInBucket(apiBusinessIn.bucket(), apiBusinessIn.name());
      LOG.debugf("Pullable: %s/%s = %s => %s", apiBusinessIn.bucket(), apiBusinessIn.name(), apiBusinessIn, pullAble);
      return true;
    } catch (final DriverNotFoundException e) {
      pullAble = null;
      LOG.debugf("Not Pullable: %s/%s ", apiBusinessIn.bucket(), apiBusinessIn.name());
      return false;
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected InputStream getPullInputStream(final StorageObject apiBusinessIn) {
    // Business code should come here (example: getting the Object Storage object stream)
    try {
      return driverApi.objectGetInputStreamInBucket(apiBusinessIn.bucket(), apiBusinessIn.name());
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final StorageObject apiBusinessIn) {
    // Business code should come here (example: headers for object name, object size...)
    LOG.debugf("Pullable %s", pullAble);
    if (pullAble != null) {
      return getHeaderMap(pullAble);
    }
    try {
      return getHeaderMap(driverApi.objectGetMetadataInBucket(apiBusinessIn.bucket(), apiBusinessIn.name()));
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException(e.getMessage(), e);
    } catch (final DriverException e) {
      throw new CcsOperationException(e.getMessage(), e);
    }
  }

  @Override
  protected Map<String, String> getHeaderError(final StorageObject apiBusinessIn, final int status) {
    // Business code should come here (example: get headers in case of error as Object name, Bucket name...)
    return getHeaderMap(apiBusinessIn);
  }

  private static Map<String, String> getHeaderMap(final StorageObject apiBusinessIn) {
    final Map<String, String> map = new HashMap<>();
    map.put(ApiConstants.X_BUCKET, apiBusinessIn.bucket());
    map.put(ApiConstants.X_OBJECT, apiBusinessIn.name());
    final var creationDate = apiBusinessIn.creationDate();
    if (creationDate != null) {
      map.put(ApiConstants.X_CREATION_DATE, creationDate.toString());
    }
    final var hash = apiBusinessIn.hash();
    if (hash != null) {
      map.put(ApiConstants.X_HASH, hash);
    }
    if (apiBusinessIn.size() > 0) {
      map.put(ApiConstants.X_LEN, Long.toString(apiBusinessIn.size()));
    }
    LOG.debugf("Debug Map: %s", map);
    return map;
  }
}
