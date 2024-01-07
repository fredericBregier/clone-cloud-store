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
 * Exception raised by the driver: the Bucket or Object was not found
 */
public class DriverNotFoundException extends DriverException {
  public DriverNotFoundException(final String message) {
    super(message);
  }

  public DriverNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public DriverNotFoundException(final Throwable cause) {
    super(cleanMessage(cause), cause);
  }
}
