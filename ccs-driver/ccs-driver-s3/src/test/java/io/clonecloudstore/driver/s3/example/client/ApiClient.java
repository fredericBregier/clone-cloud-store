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

package io.clonecloudstore.driver.s3.example.client;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import jakarta.ws.rs.core.Response;

public class ApiClient extends ClientAbstract<StorageObject, StorageObject, ApiServiceInterface> {
  /**
   * Constructor used by the Factory
   */
  protected ApiClient(final ApiClientFactory factory) {
    super(factory, factory.getUri());
  }

  public List<StorageBucket> bucketList() throws DriverException {
    final var uni = getService().bucketList();
    var businessIn = new StorageObject(null, null, null, 0, null);
    try {
      return (List<StorageBucket>) exceptionMapper.handleUniObject(this, uni);
    } catch (CcsWithStatusException e) {
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      throw new DriverException(e);
    }
  }

  public boolean bucketExists(String bucket) throws DriverException {
    final var uni = getService().bucketExists(bucket);
    var businessIn = new StorageObject(bucket, null, null, 0, null);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      switch (response.getStatus()) {
        case 200, 204 -> {
          return true;
        }
      }
      return false;
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        return false;
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        return false;
      }
      throw new DriverException(e);
    }
  }

  public StorageBucket bucketCreate(String bucket)
      throws DriverException, DriverAlreadyExistException, DriverNotAcceptableException {
    final var uni = getService().bucketCreate(bucket);
    var businessIn = new StorageObject(bucket, null, null, 0, null);
    try {
      return (StorageBucket) exceptionMapper.handleUniObject(this, uni);
    } catch (CcsWithStatusException e) {
      switch (e.getStatus()) {
        case 406 -> throw new DriverNotAcceptableException(e);
        case 409 -> throw new DriverAlreadyExistException(e);
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      switch (e.getStatus()) {
        case 406 -> throw new DriverNotAcceptableException(e);
        case 409 -> throw new DriverAlreadyExistException(e);
      }
      throw new DriverException(e);
    }
  }

  public void bucketDelete(String bucket)
      throws DriverNotFoundException, DriverException, DriverNotAcceptableException {
    final var uni = getService().bucketDelete(bucket);
    var businessIn = new StorageObject(bucket, null, null, 0, null);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        throw new DriverNotFoundException("Bucket not found: " + bucket);
      }
      if (response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
        throw new DriverNotAcceptableException("Bucket not empty: " + bucket);
      }
    } catch (final CcsWithStatusException e) {
      switch (e.getStatus()) {
        case 404 -> throw new DriverNotFoundException(e);
        case 406 -> throw new DriverNotAcceptableException(e);
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      switch (e.getStatus()) {
        case 404 -> throw new DriverNotFoundException(e);
        case 406 -> throw new DriverNotAcceptableException(e);
      }
      throw new DriverException(e);
    }
  }

  public StorageType objectOrDirectoryExists(String bucket, String objectOrDirectory) throws DriverException {
    final var uni = getService().objectOrDirectoryExists(bucket, objectOrDirectory);
    var businessIn = new StorageObject(bucket, objectOrDirectory, null, 0, null);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      var type = (String) response.getHeaders().get(ApiConstants.X_TYPE).get(0);
      if (ParametersChecker.isNotEmpty(type)) {
        return StorageType.valueOf(type);
      }
      return StorageType.NONE;
    } catch (CcsWithStatusException e) {
      if (e.getStatus() == 404) {
        return StorageType.NONE;
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      if (e.getStatus() == 404) {
        return StorageType.NONE;
      }
      throw new DriverException(e);
    }
  }

  // Example of service out of any InputStream operations, including using the same URI but not same Accept header
  public StorageObject getObjectMetadata(final String bucket, final String name)
      throws DriverNotFoundException, DriverException {
    // Business code should come here
    var businessIn = new StorageObject(bucket, name, null, 0, null);
    final var uni = getService().getObjectMetadata(bucket, name);
    try {
      return (StorageObject) exceptionMapper.handleUniObject(this, uni);
    } catch (CcsWithStatusException e) {
      if (e.getStatus() == 404) {
        throw new DriverNotFoundException(e);
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      if (e.getStatus() == 404) {
        throw new DriverNotFoundException(e);
      }
      throw new DriverException(e);
    }
  }

  public void objectDelete(String bucket, String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException {
    final var uni = getService().objectDelete(bucket, object);
    var businessIn = new StorageObject(bucket, object, null, 0, null);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        throw new DriverNotFoundException("Object not found: " + bucket + ":" + object);
      }
      if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
        throw new DriverNotAcceptableException("Object cannot be deleted: " + bucket + ":" + object);
      }
    } catch (final CcsWithStatusException e) {
      switch (e.getStatus()) {
        case 404 -> throw new DriverNotFoundException(e);
        case 406 -> throw new DriverNotAcceptableException(e);
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      switch (e.getStatus()) {
        case 404 -> throw new DriverNotFoundException(e);
        case 406 -> throw new DriverNotAcceptableException(e);
      }
      throw new DriverException(e);
    }
  }

  // Example of service for Post InputStream
  public StorageObject postInputStream(final String bucket, final String name, final InputStream content,
                                       final String hash, final long len)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverNotAcceptableException, DriverException {
    // Business code should come here
    var businessIn = new StorageObject(bucket, name, hash, len, null);
    try {
      // TODO choose compression model
      final var inputStream = prepareInputStreamToSend(content, false, false, businessIn);
      final var uni = getService().createObject(bucket, name, len, hash, content);
      return getResultFromPostInputStreamUni(uni, inputStream);
    } catch (CcsWithStatusException e) {
      switch (e.getStatus()) {
        case 404 -> throw new DriverNotFoundException(e);
        case 406 -> throw new DriverNotAcceptableException(e);
        case 409 -> throw new DriverAlreadyExistException(e);
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      switch (e.getStatus()) {
        case 404 -> throw new DriverNotFoundException(e);
        case 406 -> throw new DriverNotAcceptableException(e);
        case 409 -> throw new DriverAlreadyExistException(e);
      }
      throw new DriverException(e);
    }
  }

  // Example of service for Get InputStream
  public InputStreamBusinessOut<StorageObject> getInputStream(final String bucket, final String name)
      throws DriverNotFoundException, DriverException {
    // Business code should come here
    var businessIn = new StorageObject(bucket, name, null, 0, null);
    try {
      // TODO choose compression model
      prepareInputStreamToReceive(false, businessIn);
      final var uni = getService().readObject(bucket, name);
      return getInputStreamBusinessOutFromUni(false, false, uni);
    } catch (CcsWithStatusException e) {
      if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        throw new DriverNotFoundException(e);
      }
      throw new DriverException(e);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        throw new DriverNotFoundException(e);
      }
      throw new DriverException(e);
    }
  }

  private Map<String, String> getHeaders(final StorageObject apiBusinessIn, final long len) {
    // Here simple example of Headers for GET/POST InputStream
    Map<String, String> map = new HashMap<>();
    map.put(ApiConstants.X_LEN, Long.toString(len));
    map.put(ApiConstants.X_BUCKET, apiBusinessIn.bucket());
    map.put(ApiConstants.X_OBJECT, apiBusinessIn.name());
    map.put(ApiConstants.X_HASH, apiBusinessIn.hash());
    return map;
  }

  @Override
  protected StorageObject getApiBusinessOutFromResponse(final Response response) {
    try {
      final var storageObject = response.readEntity(StorageObject.class);
      if (storageObject != null) {
        return storageObject;
      }
    } catch (final RuntimeException ignore) {
      // Nothing
    }
    final var headers = response.getStringHeaders();
    long len = 0;
    var slen = headers.getFirst(ApiConstants.X_LEN);
    if (ParametersChecker.isNotEmpty(slen)) {
      len = Long.parseLong(slen);
    }
    Instant instant = null;
    var sInstant = headers.getFirst(ApiConstants.X_CREATION_DATE);
    if (ParametersChecker.isNotEmpty(sInstant)) {
      instant = Instant.parse(sInstant);
    }
    var bucket = headers.getFirst(ApiConstants.X_BUCKET);
    var sobject = headers.getFirst(ApiConstants.X_OBJECT);
    return new StorageObject(bucket, sobject, headers.getFirst(ApiConstants.X_HASH), len, instant);
  }

  @Override
  protected Map<String, String> getHeadersFor(final StorageObject businessIn, final int context) {
    return getHeaders(businessIn, businessIn.size());
  }
}
