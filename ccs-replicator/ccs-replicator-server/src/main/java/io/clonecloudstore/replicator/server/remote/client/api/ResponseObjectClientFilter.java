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

package io.clonecloudstore.replicator.server.remote.client.api;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.utils.AbstractResponseClientFilter;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.MultivaluedMap;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;

public class ResponseObjectClientFilter extends AbstractResponseClientFilter<AccessorObject> {

  @Override
  protected AccessorObject getOutFromHeader(final ResteasyReactiveClientRequestContext requestContext,
                                            final ClientResponseContext responseContext,
                                            final MultivaluedMap<String, String> headers) {
    if (ParametersChecker.isEmpty(headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_SITE))) {
      return null;
    }
    final var object = new AccessorObject();
    AccessorHeaderDtoConverter.objectFromMap(object, headers);
    return object;
  }
}
