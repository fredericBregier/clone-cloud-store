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

package io.clonecloudstore.administration.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public enum ClientOwnership {
  READ((short) 1),
  WRITE((short) 2),
  READ_WRITE((short) 3),
  DELETE((short) 4),
  DELETE_READ((short) 5),
  DELETE_WRITE((short) 6),
  OWNER((short) 7),
  UNKNOWN((short) 0);

  private final short code;

  ClientOwnership(final short code) {
    this.code = code;
  }

  public static ClientOwnership fromStatusCode(final short code) {
    return switch (code) {
      case 1 -> READ;
      case 2 -> WRITE;
      case 3 -> READ_WRITE;
      case 4 -> DELETE;
      case 5 -> DELETE_READ;
      case 6 -> DELETE_WRITE;
      case 7 -> OWNER;
      default -> UNKNOWN;
    };
  }

  public boolean include(final ClientOwnership ownership) {
    return switch (ownership) {
      case READ -> code % 2 == 1;
      case WRITE -> this.equals(WRITE) || this.equals(READ_WRITE) || this.equals(DELETE_WRITE) || this.equals(OWNER);
      case READ_WRITE -> this.equals(READ_WRITE) || this.equals(OWNER);
      case DELETE -> code >= DELETE.code;
      case DELETE_READ -> this.equals(DELETE_READ) || this.equals(OWNER);
      case DELETE_WRITE -> this.code >= DELETE_WRITE.code;
      case OWNER -> this.equals(OWNER);
      case UNKNOWN -> this.equals(UNKNOWN);
    };
  }

  public ClientOwnership fusion(final ClientOwnership ownership) {
    // base this
    var base = this.getCode();
    if (ownership.include(READ) && !this.include(READ)) {
      base += READ.getCode();
    }
    if (ownership.include(WRITE) && !this.include(WRITE)) {
      base += WRITE.getCode();
    }
    if (ownership.include(DELETE) && !this.include(DELETE)) {
      base += DELETE.getCode();
    }
    return fromStatusCode(base);
  }

  public short getCode() {
    return code;
  }
}
