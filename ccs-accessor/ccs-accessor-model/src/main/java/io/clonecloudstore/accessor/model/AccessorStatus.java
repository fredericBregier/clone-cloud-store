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

package io.clonecloudstore.accessor.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Accessor Status DTO
 */
@RegisterForReflection
public enum AccessorStatus {
  UNKNOWN((short) 0),
  UPLOAD((short) 100),
  READY((short) 200),
  ERR_UPL((short) 500),
  DELETING((short) 102),
  DELETED((short) 204),
  ERR_DEL((short) 404);

  public static final int STATUS_LENGTH = 8;
  private final short status;

  AccessorStatus(final short status) {
    this.status = status;
  }

  public static String toString(final AccessorStatus status) {
    return switch (status) {
      case UPLOAD, READY, ERR_UPL, DELETING, DELETED, ERR_DEL -> status.name();
      default -> UNKNOWN.name();
    };
  }

  public static AccessorStatus fromStatusCode(final short code) {
    return switch (code) {
      case 100 -> UPLOAD;
      case 102 -> DELETING;
      case 200 -> READY;
      case 204 -> DELETED;
      case 404 -> ERR_DEL;
      case 500 -> ERR_UPL;
      default -> UNKNOWN;
    };
  }

  public short getStatus() {
    return status;
  }

}
