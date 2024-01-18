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

import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;

public interface DaoNativeListingRepository extends RepositoryBaseInterface<DaoNativeListing> {
  String TABLE_NAME = "nativelistings";
  String REQUESTID = "requestId";
  String BUCKET = DaoAccessorObjectRepository.BUCKET;
  String NAME = DaoAccessorObjectRepository.NAME;
  String DB = "db";
  String DRIVER = "driver";
  String SITE = DaoAccessorObjectRepository.SITE;
  String NSTATUS = "n" + DaoAccessorObjectRepository.STATUS;
  String EVENT = "event";

  /*
      String fieldsDest = DaoNativeListingRepository.ID + ", " + DaoNativeListingRepository.REQUESTID + ", " +
        DaoNativeListingRepository.SOURCE + ", " + DaoNativeListingRepository.BUCKET + ", " +
        DaoNativeListingRepository.OBJECT + ", " + DaoNativeListingRepository.CREATION + ", " +
        DaoNativeListingRepository.HASH + ", " + DaoNativeListingRepository.SIZE + ", " +
        DaoNativeListingRepository.SITE;
    String fieldsSrc = DaoAccessorObjectRepository.ID + ", " + "\"" + daoRequest.getId() + "\"" + ", " + "\"" +
        ReconciliationSource.DB.name() + "\"" + ", " + DaoAccessorObjectRepository.BUCKET + ", " +
        DaoAccessorObjectRepository.NAME + ", " + DaoAccessorObjectRepository.CREATION + ", " +
        DaoAccessorObjectRepository.HASH + ", " + DaoAccessorObjectRepository.SIZE + ", " +
        DaoAccessorObjectRepository.SITE;
    String insertFrom =
        "INSERT INTO " + DaoNativeListingRepository.TABLE_NAME + " (" + fieldsDest + ")" + " SELECT " +
            fieldsSrc+ " FROM "+DaoAccessorObjectRepository.TABLE_NAME;
    And adding Where condition
   */
}
