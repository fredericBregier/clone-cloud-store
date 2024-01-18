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

package io.clonecloudstore.reconciliator.model;

public enum ReconciliationAction {
  UNKNOWN_ACTION((short) 0),
  /**
   * Driver and MD already deleted
   */
  DELETED_ACTION((short) 1),
  /**
   * To Delete if possible MD and Driver (one or both)
   */
  DELETE_ACTION((short) 2),
  /**
   * Object fully Ready
   */
  READY_ACTION((short) 10),
  /**
   * Driver Ready, partial MD
   */
  UPDATE_ACTION((short) 11),
  /**
   * MD almost Ready, Driver absent
   */
  UPLOAD_ACTION((short) 20),
  /**
   * For Disaster Recovery or New site initialization
   */
  UPGRADE_ACTION((short) 21),
  /**
   * When no Ready like available while UPLOAD action
   */
  ERROR_ACTION((short) 30);
  private final short status;

  ReconciliationAction(final short status) {
    this.status = status;
  }

  public static ReconciliationAction fromStatusCode(final short code) {
    return switch (code) {
      case 1 -> DELETE_ACTION;
      case 2 -> DELETED_ACTION;
      case 10 -> READY_ACTION;
      case 11 -> UPDATE_ACTION;
      case 20 -> UPLOAD_ACTION;
      case 21 -> UPGRADE_ACTION;
      case 30 -> ERROR_ACTION;
      default -> UNKNOWN_ACTION;
    };
  }

  public short getStatus() {
    return status;
  }

}
