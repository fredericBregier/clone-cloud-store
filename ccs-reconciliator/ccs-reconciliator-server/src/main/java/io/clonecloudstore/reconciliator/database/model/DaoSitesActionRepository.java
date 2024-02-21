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

public interface DaoSitesActionRepository extends RepositoryBaseInterface<DaoSitesAction> {
  String TABLE_NAME = "sitesactions";
  String REQUESTID = DaoNativeListingRepository.REQUESTID;
  String BUCKET = DaoNativeListingRepository.BUCKET;
  String NAME = DaoNativeListingRepository.NAME;
  String NEED_ACTION = "needAction";
  String NEED_ACTION_FROM = "needActionFrom";
  String SITES = DaoNativeListingRepository.SITE + "s";
}
