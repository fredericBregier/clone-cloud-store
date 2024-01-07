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

package io.clonecloudstore.common.quarkus.exception;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.properties.Module;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.Response;

/***
 * Generic Abstract class exception for Client Error
 */
public class CcsClientGenericException extends ClientErrorException implements CcsExceptionInterface {
  private final Module module;

  /***
   * Constructor to CcsClientGenericException
   * @param message Exception Message
   * @param status HTTP Status Code
   */
  public CcsClientGenericException(final String message, final Response.Status status) {
    super(message, status);
    this.module = QuarkusProperties.getCcsModule();
  }

  /***
   * Constructor to CcsClientGenericException
   * @param message Exception Message
   * @param status HTTP Status Code
   * @param cause Initial Exception
   */
  public CcsClientGenericException(final String message, final Response.Status status, final Throwable cause) {
    super(message, status, cause);
    this.module = QuarkusProperties.getCcsModule();
  }

  @Override
  public String getMessage() {
    return module + " - " + super.getMessage();
  }

  @Override
  public Module getModule() {
    return module;
  }

  @Override
  public int getStatus() {
    return getResponse().getStatus();
  }
}
