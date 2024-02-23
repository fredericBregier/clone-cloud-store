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

import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;

public interface DaoRequestRepository extends RepositoryBaseInterface<DaoRequest> {
  String TABLE_NAME = "requests";
  String CLIENTID = "clientId";
  String BUCKET = DaoNativeListingRepository.BUCKET;
  String FILTER = "filter";
  String FROMSITE = "fromSite";
  String CURRENTSITE = "currentSite";
  String CONTEXTSITES = "contextSites";
  String CONTEXTSITESDONE = "contextSitesDone";
  String START = "start";
  String CHECKED = "checked";
  String CHECKED_DB = "checkedDb";
  String CHECKED_DRIVER = "checkedDriver";
  String CHECKED_REMOTE = "checkedRemote";
  String ACTIONS = "actions";
  String DRY_RUN = "dryRun";
  String STEP = "step";
  String STOP = "stop";
}
