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

package io.clonecloudstore.driver.google.example.client;

import java.time.Instant;

import io.clonecloudstore.common.quarkus.client.utils.AbstractResponseClientFilter;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.model.StorageObject;
import jakarta.ws.rs.client.ClientResponseContext;
import jakarta.ws.rs.core.MultivaluedMap;
import org.jboss.resteasy.reactive.client.spi.ResteasyReactiveClientRequestContext;

public class ResponseObjectClientFilter extends AbstractResponseClientFilter<StorageObject> {
  @Override
  protected StorageObject getOutFromHeader(final ResteasyReactiveClientRequestContext requestContext,
                                           final ClientResponseContext responseContext,
                                           final MultivaluedMap<String, String> headers) {
    if (ParametersChecker.isEmpty(headers.getFirst(ApiConstants.X_BUCKET))) {
      return null;
    }
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
}
