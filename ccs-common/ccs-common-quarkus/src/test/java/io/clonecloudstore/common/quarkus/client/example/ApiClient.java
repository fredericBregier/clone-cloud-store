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

package io.clonecloudstore.common.quarkus.client.example;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.ws.rs.core.Response;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.BIG_LONG;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_NAME;

public class ApiClient extends ClientAbstract<ApiBusinessIn, ApiBusinessOut, ApiServiceInterface> {
  /**
   * Constructor used by the Factory
   */
  protected ApiClient(final ApiClientFactory factory) {
    // In case of multiple targets, the constructor could expose the used URI and not implicitly the same as Factory
    // Or using newClient(uri) will do the trick
    super(factory, factory.getUri());
  }

  public boolean checkName(final String name) {
    final var uni = getService().checkName(name, BIG_LONG, BIG_LONG, Long.toString(BIG_LONG));
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return name.equals(response.getHeaderString(X_NAME));
    } catch (final CcsClientGenericException | CcsServerGenericException | CcsWithStatusException e) {
      return false;
    }
  }

  public ApiBusinessOut postInputStream(final String name, final InputStream content, final long len)
      throws CcsWithStatusException {
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    final var inputStream = prepareInputStreamToSend(content, false, false, businessIn);
    final var uni = getService().createObject(name, len, inputStream);
    return getResultFromPostInputStreamUni(uni, inputStream);
  }

  public InputStreamBusinessOut<ApiBusinessOut> getInputStreamBusinessOut(final String name, final long len)
      throws CcsWithStatusException {
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    prepareInputStreamToReceive(false, businessIn);
    final var uni = getService().readObject(name, len);
    return getInputStreamBusinessOutFromUni(true, uni);
  }

  public ApiBusinessOut getObjectMetadata(final String name) throws CcsWithStatusException {
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    try {
      final var uni = getService().getObjectMetadata(name);
      return (ApiBusinessOut) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      throw CcsServerGenericExceptionMapper.getBusinessException(e);
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
    map.put(ApiConstants.X_LEN, Long.toString(businessIn.len));
    map.put(ApiConstants.X_NAME, businessIn.name);
    if (context == 1) {
      map.put(ApiConstants.X_CREATION_DATE, Instant.now().toString());
    }
    return map;
  }
}
