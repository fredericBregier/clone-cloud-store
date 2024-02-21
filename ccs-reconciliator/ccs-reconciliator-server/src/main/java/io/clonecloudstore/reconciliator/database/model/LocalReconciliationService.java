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

import java.util.List;

import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;

public interface LocalReconciliationService {
  List<String> STATUS_NAME_ORDERED =
      List.of(AccessorStatus.UNKNOWN.name(), AccessorStatus.UPLOAD.name(), AccessorStatus.READY.name(),
          AccessorStatus.ERR_UPL.name(), AccessorStatus.DELETING.name(), AccessorStatus.DELETED.name(),
          AccessorStatus.ERR_DEL.name());
  short UNKNOWN_RANK = 0;
  short UPLOAD_RANK = 1;
  short READY_RANK = 2;
  short ERR_UPL_RANK = 3;
  short DELETING_RANK = 4;
  short DELETED_RANK = 5;
  short ERR_DEL_RANK = 6;
  short TO_UPDATE_RANK = (short) STATUS_NAME_ORDERED.size();

  void step1CleanUpObjectsNativeListings(final DaoRequest daoPreviousRequest) throws CcsDbException;

  void step2ContinueFromPreviousRequest(String requestId, DaoRequest daoRequest, boolean replaceOldRequest)
      throws CcsDbException;

  void step3SaveNativeListingDb(DaoRequest daoRequest) throws CcsDbException;

  void step4SaveNativeListingDriver(DaoRequest daoRequest) throws CcsDbException;

  void step5CompareNativeListingDbDriver(DaoRequest daoRequest) throws CcsDbException;

  void cleanNativeListing(DaoRequest daoRequest) throws CcsDbException;

  ClosingIterator<DaoSitesListing> getSiteListing(DaoRequest daoRequest) throws CcsDbException;

  void cleanSitesListing(DaoRequest daoRequest) throws CcsDbException;
}
