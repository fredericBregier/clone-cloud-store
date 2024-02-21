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

package io.clonecloudstore.common.quarkus.server.service;

import jakarta.ws.rs.core.Response;

public class ServerStreamHandlerResponseException extends Exception {
  private final Response response;
  private final Exception exception;

  public ServerStreamHandlerResponseException(final Response response, final Exception e) {
    this.response = response;
    exception = e;
  }

  public Response getResponse() {
    return response;
  }

  public Exception getException() {
    return exception;
  }

  @Override
  public String getMessage() {
    return response.getStatus() + " : " + exception.getMessage();
  }
}
