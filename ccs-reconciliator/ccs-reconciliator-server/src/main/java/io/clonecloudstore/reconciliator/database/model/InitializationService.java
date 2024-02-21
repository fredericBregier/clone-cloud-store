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

import java.time.Instant;
import java.util.Map;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;

public interface InitializationService {
  /**
   * @param bucket                 Which bucket to look into
   * @param futureExpireAddSeconds number of seconds greather than 0 to set a future expiration, or none if less or
   *                               equals than 0
   * @param defaultMetadata        Default Metadata to apply to each found object
   */
  void importFromExistingBucket(final String clientId, final String bucket, final String prefix, final Instant from,
                                final Instant to, final long futureExpireAddSeconds,
                                final Map<String, String> defaultMetadata) throws CcsWithStatusException;

  /**
   * @param bucket     Which bucket to fully synchronized
   * @param remoteSite Which site to fully synchronized from
   * @param filter
   * @return
   */
  DaoRequest syncFromExistingSite(final String clientId, final String bucket, final String remoteSite,
                                  final AccessorFilter filter) throws CcsDbException;
}
