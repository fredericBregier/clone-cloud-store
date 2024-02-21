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

package io.clonecloudstore.common.standard.exception;

/**
 * Ccs Exception used by extension to specify an error and the status according to one
 * BusinessIn object (optional)
 */
public class CcsWithStatusException extends Exception {
  public static final String STATUS_KEYWORD = "Status: ";
  private final transient Object businessIn;
  private final int status;

  public CcsWithStatusException(final Object businessIn, final int status) {
    super(STATUS_KEYWORD + status);
    this.businessIn = businessIn;
    this.status = status;
  }

  public CcsWithStatusException(final Object businessIn, final int status, final String message) {
    super(message);
    this.businessIn = businessIn;
    this.status = status;
  }

  public CcsWithStatusException(final Object businessIn, final int status, final String message,
                                final Throwable cause) {
    super(message, cause);
    this.businessIn = businessIn;
    this.status = status;
  }

  public CcsWithStatusException(final Object businessIn, final int status, final Throwable cause) {
    super(cause);
    this.businessIn = businessIn;
    this.status = status;
  }

  public Object getBusinessIn() {
    return businessIn;
  }

  public int getStatus() {
    return status;
  }

  @Override
  public String getMessage() {
    final var msg = super.getMessage();
    if (msg == null) {
      return STATUS_KEYWORD + status + (businessIn != null ? " as [" + businessIn + "] " : " ");
    }
    if (msg.contains(STATUS_KEYWORD)) {
      return (businessIn != null ? "As [" + businessIn + "] " : " ") + msg;
    }
    return STATUS_KEYWORD + status + (businessIn != null ? " as [" + businessIn + "] " : " ") + msg;
  }
}
