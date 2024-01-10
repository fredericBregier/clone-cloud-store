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

package io.clonecloudstore.test.server.service.example.server;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.test.server.service.FakeNativeStreamHandlerAbstract;
import io.clonecloudstore.test.server.service.example.model.ApiBusinessIn;
import io.clonecloudstore.test.server.service.example.model.ApiBusinessOut;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import org.jboss.logging.Logger;

import static io.clonecloudstore.test.server.service.example.client.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.test.server.service.example.client.ApiConstants.X_LEN;
import static io.clonecloudstore.test.server.service.example.client.ApiConstants.X_NAME;


@RequestScoped
public class ServerNativeStreamHandler extends FakeNativeStreamHandlerAbstract<ApiBusinessIn, ApiBusinessOut> {
  private static final Logger LOG = Logger.getLogger(ServerNativeStreamHandler.class);

  @Override
  protected ApiBusinessOut getBusinessOutForPushAnswer(final ApiBusinessIn apiBusinessIn, final String finalHash,
                                                       final long size) {
    final var businessOut = new ApiBusinessOut();
    businessOut.name = apiBusinessIn.name;
    businessOut.len = size;
    businessOut.creationDate = Instant.now();
    return businessOut;
  }

  @Override
  protected long getLengthFromBusinessIn(final ApiBusinessIn businessIn) {
    return businessIn.len;
  }

  @Override
  protected Map<String, String> getHeaderPushInputStream(final ApiBusinessIn apiBusinessIn, final String finalHash,
                                                         final long size, final ApiBusinessOut apiBusinessOut)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: headers for object name, object size, ...)
    return new HashMap<>();
  }

  @Override
  protected boolean checkPullAble(final ApiBusinessIn apiBusinessIn, final MultiMap headers)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here
    return true;
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final ApiBusinessIn apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: headers for object name, object size...)
    final Map<String, String> map = new HashMap<>();
    map.put(X_NAME, apiBusinessIn.name);
    map.put(X_CREATION_DATE, Instant.now().toString());
    var len = apiBusinessIn.len;
    if (len <= 0) {
      len = ApiService.LEN;
    }
    map.put(X_LEN, Long.toString(len));
    return map;
  }

  @Override
  protected Map<String, String> getHeaderError(final ApiBusinessIn apiBusinessIn, final int status) {
    // Business code should come here (example: get headers in case of error as Object name, Bucket name...)
    final Map<String, String> map = new HashMap<>();
    map.put(X_NAME, apiBusinessIn.name);
    return map;
  }
}
