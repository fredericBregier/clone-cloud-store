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

package io.clonecloudstore.replicator.config;

public class ReplicatorConstants {
  /**
   * /replicator/local : for local access requests for remote (ex: Accessor -> Replicator)
   * Can concern: Bucket/Object (Read/Check) or Reconciliation (Creation/Check)
   * /replicator/remote : for remote access requests from local (ex: Replicator -> Replicator)
   * Can concern: Bucket/Object (Read/Check) or Reconciliation (Creation/Check)
   */
  private ReplicatorConstants() {
  }

  public enum Action {
    CREATE,
    UPDATE,
    DELETE,
    UNKNOWN
  }

  /**
   * replicator-request-out replicator-request-in : topic for local replication requests
   * out from Accessor, in from Replicator (=> /replicator/remote/orders)
   * replicator-reconciliation-out replicator-reconciliation-in : topic for requests to remote requests
   * in from Reconciliator, out from Replicator (=> /replicator/remote => replicator-action-in)
   * replicator-action-out replicator-action-in : topic to apply remote requests locally
   * out from Replicator, in from Accessor (=> /replicator/local/buckets)
   */
  public static final class Topic {
    public static final String REPLICATOR_REQUEST_IN = "replicator-request-in";
    public static final String REPLICATOR_REQUEST_OUT = "replicator-request-out";
    public static final String REPLICATOR_ACTION_IN = "replicator-action-in";
    public static final String REPLICATOR_ACTION_OUT = "replicator-action-out";

    private Topic() {
      // Empty
    }
  }
}
