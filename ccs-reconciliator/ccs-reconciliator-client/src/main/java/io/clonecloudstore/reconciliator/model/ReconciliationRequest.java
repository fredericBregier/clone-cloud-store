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

package io.clonecloudstore.reconciliator.model;

import java.time.Instant;
import java.util.List;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record ReconciliationRequest(String id, String clientId, String bucket, AccessorFilter filter, String fromSite,
                                    String currentSite, List<String> contextSites, Instant start, long checked,
                                    long checkedDb, long checkedDriver, long checkedRemote, long actions,
                                    boolean dryRun, ReconciliationStep step, Instant stop) {
  public ReconciliationRequest(String id, String clientId, String bucket, AccessorFilter filter, String fromSite,
                               String currentSite, List<String> contextSites, boolean dryRun) {
    this(id, clientId, bucket, filter, fromSite, currentSite, contextSites, null, 0, 0, 0, 0, 0, dryRun,
        ReconciliationStep.CREATE, null);
  }
}
