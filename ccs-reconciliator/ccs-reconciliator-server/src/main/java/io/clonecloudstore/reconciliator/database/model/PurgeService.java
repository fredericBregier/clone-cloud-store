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

package io.clonecloudstore.reconciliator.database.model;

import io.clonecloudstore.common.database.utils.exception.CcsDbException;

public interface PurgeService {
  /**
   * @param bucketForReadyExpired  if null, means delete, else move to this bucket. If the object is already in this
   *                               archive bucket, it will then be purged
   * @param futureExpireAddSeconds number of seconds > 0 to set a future expiration on archival process if any or 0 to
   *                               keep it forever
   */
  void purgeObjectsOnExpiredDate(final String clientId, final String bucketForReadyExpired,
                                 final long futureExpireAddSeconds) throws CcsDbException;

}
