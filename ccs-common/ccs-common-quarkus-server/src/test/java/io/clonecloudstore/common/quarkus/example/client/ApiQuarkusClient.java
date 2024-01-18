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

package io.clonecloudstore.common.quarkus.example.client;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.ws.rs.core.Response;

import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_NAME;

public class ApiQuarkusClient extends ClientAbstract<ApiBusinessIn, ApiBusinessOut, ApiQuarkusServiceInterface> {
  /**
   * Constructor used by the Factory
   */
  protected ApiQuarkusClient(final ApiQuarkusClientFactory factory) {
    // In case of multiple targets, the constructor could expose the used URI and not implicitly the same as Factory
    // Or using newClient(uri) will do the trick
    super(factory, factory.getUri());
  }

  // Example of service out of any InputStream operations
  public boolean checkName(final String name) {
    final var uni = getService().checkName(name);
    // Business code should come here
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return name.equals(response.getHeaderString(X_NAME));
    } catch (final CcsClientGenericException | CcsServerGenericException | CcsWithStatusException e) {
      return false;
    }
  }

  // Example of service for Post InputStream
  public ApiBusinessOut postInputStream(final String name, final InputStream content, final long len,
                                        final boolean shallCompress, final boolean alreadyCompressed)
      throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    final var inputStream = prepareInputStreamToSend(content, shallCompress, alreadyCompressed, businessIn);
    final var uni = getService().createObject(name, len, inputStream);
    return getResultFromPostInputStreamUni(uni, inputStream);
  }

  public ApiBusinessOut putInputStream(final String name, final InputStream content, final long len,
                                       final boolean shallCompress, final boolean alreadyCompressed)
      throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    final var inputStream = prepareInputStreamToSend(content, shallCompress, alreadyCompressed, businessIn);
    final var uni = getService().createObjectUsingPut(name, len, inputStream);
    return getResultFromPostInputStreamUni(uni, inputStream);
  }

  public ApiBusinessOut postInputStreamThrough(final String name, final InputStream content, final long len,
                                               final boolean shallCompress, final boolean alreadyCompressed)
      throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    final var inputStream = prepareInputStreamToSend(content, shallCompress, alreadyCompressed, businessIn);
    final var uni = getService().createObjectThrough(name, len, inputStream);
    return getResultFromPostInputStreamUni(uni, inputStream);
  }

  // Example of service for Get InputStream
  public InputStreamBusinessOut<ApiBusinessOut> getInputStream(final String name, final long len,
                                                               final boolean acceptCompressed,
                                                               final boolean shallDecompress)
      throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    prepareInputStreamToReceive(acceptCompressed, businessIn);
    final var uni = getService().readObject(name);
    return getInputStreamBusinessOutFromUni(shallDecompress, uni);
  }

  public InputStreamBusinessOut<ApiBusinessOut> putAsGetInputStream(final String name, final long len,
                                                                    final boolean acceptCompressed,
                                                                    final boolean shallDecompress)
      throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    prepareInputStreamToReceive(acceptCompressed, businessIn);
    final var uni = getService().readObjectPut(name);
    return getInputStreamBusinessOutFromUni(shallDecompress, uni);
  }

  public InputStreamBusinessOut<ApiBusinessOut> getInputStreamThrough(final String name, final long len,
                                                                      final boolean acceptCompressed,
                                                                      final boolean shallDecompress)
      throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    prepareInputStreamToReceive(acceptCompressed, businessIn);
    final var uni = getService().readObjectThrough(name);
    return getInputStreamBusinessOutFromUni(shallDecompress, uni);
  }

  // Example of service out of any InputStream operations, including using the same URI but not same Accept header
  public ApiBusinessOut getObjectMetadata(final String name) throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    try {
      final var uni = getService().getObjectMetadata(name);
      return (ApiBusinessOut) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final CcsWithStatusException e) {
      throw e;
    } catch (final Exception e) {
      throw new CcsWithStatusException(businessIn, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }


  @Override
  protected ApiBusinessOut getApiBusinessOutFromResponseForCreate(final Response response) {
    try {
      final var businessOut = response.readEntity(ApiBusinessOut.class);
      if (businessOut != null) {
        return businessOut;
      }
    } catch (final RuntimeException ignore) {
      // Nothing
    }
    final var headers = response.getHeaders();
    ApiBusinessOut businessOut = new ApiBusinessOut();
    businessOut.name = (String) headers.getFirst(X_NAME);
    var xlen = headers.getFirst(X_LEN);
    if (!ParametersChecker.isEmpty(xlen)) {
      businessOut.len = Long.parseLong((String) xlen);
    }
    var xtime = headers.getFirst(X_CREATION_DATE);
    if (!ParametersChecker.isEmpty(xtime)) {
      businessOut.creationDate = Instant.parse((CharSequence) xtime);
    }
    return businessOut;
  }

  @Override
  protected Map<String, String> getHeadersFor(final ApiBusinessIn businessIn, final int context) {
    final Map<String, String> map = new HashMap<>();
    map.put(X_LEN, Long.toString(businessIn.len));
    map.put(X_NAME, businessIn.name);
    if (context == 1) {
      map.put(ApiConstants.X_CREATION_DATE, Instant.now().toString());
    }
    return map;
  }
}
