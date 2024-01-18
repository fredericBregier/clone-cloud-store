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

package io.clonecloudstore.driver.api.exception;

/**
 * Exception raised by the driver: Generic error (meaning 400 or 500 in HTTP) for any not specialized errors
 */
public class DriverException extends Exception {
  public DriverException(final String message) {
    super(message);
  }

  public DriverException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public DriverException(final Throwable cause) {
    super(cleanMessage(cause), cause);
  }

  public static String cleanMessage(final Throwable e) {
    if (e == null || e.getMessage() == null) {
      return "";
    }
    return e.getMessage().replace('\n', ' ').replace('#', ' ');
  }

  public static DriverException getDriverExceptionFromStatus(final int status, final Exception e) {
    switch (status) {
      case 404:
        return new DriverNotFoundException(cleanMessage(e), e);
      case 406:
        return new DriverNotAcceptableException(cleanMessage(e), e);
      case 409:
        return new DriverAlreadyExistException(cleanMessage(e), e);
      default:
        return new DriverException(cleanMessage(e), e);
    }
  }
}
