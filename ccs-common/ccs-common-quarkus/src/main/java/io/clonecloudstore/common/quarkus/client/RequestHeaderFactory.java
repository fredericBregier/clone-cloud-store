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

package io.clonecloudstore.common.quarkus.client;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.eclipse.microprofile.rest.client.ext.ClientHeadersFactory;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_MODULE;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

@ApplicationScoped
public class RequestHeaderFactory implements ClientHeadersFactory {

  @Override
  public MultivaluedMap<String, String> update(final MultivaluedMap<String, String> incoming,
                                               final MultivaluedMap<String, String> outgoing) {
    MultivaluedMap<String, String> result = new MultivaluedHashMap<>();
    var xOpId = incoming.containsKey(X_OP_ID) ? incoming.getFirst(X_OP_ID) : null;
    if (ParametersChecker.isEmpty(xOpId)) {
      xOpId = outgoing.containsKey(X_OP_ID) ? outgoing.getFirst(X_OP_ID) : null;
    }
    if (ParametersChecker.isEmpty(xOpId)) {
      xOpId = SimpleClientAbstract.getMdcOpId();
    }
    result.putSingle(X_OP_ID, xOpId);
    var module = incoming.containsKey(X_MODULE) ? incoming.getFirst(X_MODULE) : "";
    if (ParametersChecker.isEmpty(module)) {
      module = outgoing.containsKey(X_MODULE) ? outgoing.getFirst(X_MODULE) : "";
    }
    if (ParametersChecker.isEmpty(module)) {
      module = QuarkusProperties.getCcsModule().name();
    }
    result.putSingle(X_MODULE, module);
    var isContentCompressed = SimpleClientAbstract.isBodyCompressed();
    if (isContentCompressed) {
      result.putSingle(HttpHeaderNames.CONTENT_ENCODING.toString(), HttpHeaderValues.ZSTD.toString());
    }
    var isResponseCompressed = SimpleClientAbstract.isAcceptCompression();
    if (isResponseCompressed) {
      result.putSingle(HttpHeaderNames.ACCEPT_ENCODING.toString(), HttpHeaderValues.ZSTD.toString());
    }
    var queryHeaders = SimpleClientAbstract.getHeadersMap();
    if (queryHeaders != null && !queryHeaders.isEmpty()) {
      for (final var item : queryHeaders.entrySet()) {
        result.putSingle(item.getKey(), item.getValue());
      }
    }
    return result;
  }
}
