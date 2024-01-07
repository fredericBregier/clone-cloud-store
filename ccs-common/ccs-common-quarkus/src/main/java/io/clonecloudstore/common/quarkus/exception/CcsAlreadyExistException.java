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
 * Already Exists exception
 */
public class CcsAlreadyExistException extends CcsClientGenericException {

  /**
   * Constructor to CcsAlreadyExistException
   *
   * @param message Exception Message
   */
  public CcsAlreadyExistException(final String message) {
    super(message, Response.Status.CONFLICT);
  }

  /**
   * Constructor to CcsAlreadyExistException
   *
   * @param message Exception Message
   * @param e       Throwable to associate
   */
  public CcsAlreadyExistException(final String message, final Throwable e) {
    super(message, Response.Status.CONFLICT, e);
  }
}
