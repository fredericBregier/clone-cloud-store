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

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import jakarta.ws.rs.core.Response;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.BIG_LONG;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_NAME;

public class SimpleApiClient extends SimpleClientAbstract<ApiServiceInterface> {

  /**
   * Constructor used by the Factory
   */
  protected SimpleApiClient(final SimpleApiClientFactory factory) {
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

  public ApiBusinessOut getObjectMetadata(final String name) throws CcsWithStatusException {
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    try {
      final var uni = getService().getObjectMetadata(name);
      return (ApiBusinessOut) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      throw CcsServerGenericExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(businessIn, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }
}
