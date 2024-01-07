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

package io.clonecloudstore.topology.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public enum TopologyStatus {
  UP((short) 100),
  DOWN((short) 200),
  DELETED((short) 300),
  UNKNOWN((short) 0);

  private final short status;

  TopologyStatus(final short status) {
    this.status = status;
  }

  public static TopologyStatus fromStatusCode(final short code) {
    return switch (code) {
      case 100 -> UP;
      case 200 -> DOWN;
      case 300 -> DELETED;
      default -> UNKNOWN;
    };
  }

  public short getStatus() {
    return status;
  }
}
