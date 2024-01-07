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

import jakarta.ws.rs.core.Response;

/**
 * Not Acceptable exception
 */
public class CcsNotAcceptableException extends CcsClientGenericException {

  /**
   * Constructor to CcsNotAcceptableException
   *
   * @param message Exception Message
   */
  public CcsNotAcceptableException(final String message) {
    super(message, Response.Status.NOT_ACCEPTABLE);
  }

  /**
   * Constructor to CcsNotAcceptableException
   *
   * @param message Exception Message
   * @param cause   Initial Exception
   */
  public CcsNotAcceptableException(final String message, final Throwable cause) {
    super(message, Response.Status.NOT_ACCEPTABLE, cause);
  }

}
