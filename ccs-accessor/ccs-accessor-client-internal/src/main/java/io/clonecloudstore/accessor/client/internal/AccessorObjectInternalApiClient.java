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

package io.clonecloudstore.accessor.client.internal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import io.clonecloudstore.accessor.client.internal.api.AccessorObjectInternalApi;
import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.driver.api.StorageType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.ClientWebApplicationException;

/**
 * Client for Accessor Object
 */
public class AccessorObjectInternalApiClient
    extends ClientAbstract<AccessorObject, AccessorObject, AccessorObjectInternalApi> {
  private AccessorFilter filter = null;

  /**
   * Constructor used by the Factory
   */
  protected AccessorObjectInternalApiClient(final AccessorObjectInternalApiFactory factory) {
    super(factory, factory.getUri());
  }

  /**
   * Check if object or directory exist
   *
   * @param pathDirectoryOrObject may contain only directory, not full path (as prefix)
   * @param fullCheck             if True, will check on Object Storage if Object only
   * @return the associated StorageType
   */
  public StorageType checkObjectOrDirectory(final String bucketName, final String pathDirectoryOrObject,
                                            final String clientId, final boolean fullCheck)
      throws CcsWithStatusException {
    final var uni =
        getService().checkObjectOrDirectory(bucketName, pathDirectoryOrObject, fullCheck, clientId, getOpId());
    final var accessorObject = new AccessorObject();
    accessorObject.setBucket(bucketName).setName(pathDirectoryOrObject);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return AccessorHeaderDtoConverter.getStorageTypeFromResponse(response);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        return StorageType.NONE;
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw CcsServerGenericExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(accessorObject, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }

  /**
   * @return the associated DTO
   */
  public AccessorObject getObjectInfo(final String bucketName, final String objectName, final String clientId)
      throws CcsWithStatusException {
    final var uni = getService().getObjectInfo(bucketName, objectName, clientId, getOpId());
    final var accessorObject = new AccessorObject();
    accessorObject.setBucket(bucketName).setName(objectName);
    try {
      return (AccessorObject) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw CcsServerGenericExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(accessorObject, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }

  public InputStreamBusinessOut<AccessorObject> getObject(final String bucketName, final String objectName,
                                                          final String clientId) throws CcsWithStatusException {
    return getObject(bucketName, objectName, clientId, true);
  }

  /**
   * Returns the InputStream and the AccessorObject
   */
  public InputStreamBusinessOut<AccessorObject> getObject(final String bucketName, final String objectName,
                                                          final String clientId, final boolean decompress)
      throws CcsWithStatusException {
    this.filter = null;
    final var accessorObject = new AccessorObject();
    accessorObject.setBucket(bucketName).setName(objectName);
    prepareInputStreamToReceive(AccessorProperties.isInternalCompression(), accessorObject);
    final var uni =
        getService().getObject(AccessorProperties.isInternalCompression(), bucketName, objectName, clientId, getOpId());
    return getInputStreamBusinessOutFromUni(decompress, uni);
  }

  /**
   * Returns an Iterator containing AccessorObjects
   */
  public Iterator<AccessorObject> listObjects(final String bucketName, final String clientId,
                                              final AccessorFilter filter) throws CcsWithStatusException {
    this.filter = filter == null ? new AccessorFilter() : filter;
    final var accessorObject = new AccessorObject();
    accessorObject.setBucket(bucketName);
    prepareInputStreamToReceive(AccessorProperties.isInternalCompression(), accessorObject);
    final var uni =
        getService().listObjects(AccessorProperties.isInternalCompression(), bucketName, clientId, getOpId());
    final var inputStream = getInputStreamBusinessOutFromUni(true, uni).inputStream();
    return StreamIteratorUtils.getIteratorFromInputStream(inputStream, AccessorObject.class);
  }

  @Override
  protected AccessorObject getApiBusinessOutFromResponseForCreate(final Response response) {
    // No PUSH
    return null;
  }

  @Override
  protected Map<String, String> getHeadersFor(final AccessorObject businessIn, final int context) {
    final var map = new HashMap<String, String>();
    if (context == CONTEXT_RECEIVE && filter != null) {
      AccessorHeaderDtoConverter.filterToMap(filter, map);
    }
    return map;
  }
}
