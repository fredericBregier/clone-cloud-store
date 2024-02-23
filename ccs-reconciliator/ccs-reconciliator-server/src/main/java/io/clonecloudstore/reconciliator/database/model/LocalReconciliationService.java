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

import java.util.Iterator;
import java.util.List;

import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;

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

  /**
   * Clean Up Native Listing and Objects from status Object<br>
   * Index Objects: Bucket, Site, Status, creation
   */
  void step1CleanUpObjectsNativeListings(final DaoRequest daoPreviousRequest) throws CcsDbException;

  /**
   * Get Old listing to restart from (for each site): Optional step (if accepting eventual mistakes on old data)<br>
   * Index Native: requestId, bucket<br>
   * Step2: Copy NativeListing with new RequestId (or Replace requestId)
   */
  void step2ContinueFromPreviousRequest(String requestId, DaoRequest daoRequest, boolean replaceOldRequest)
      throws CcsDbException;

  /**
   * Listing according to filter such as now > dateFrom (DB), updating existing info<br>
   * Step3: From Db Objects into NativeListing local step<br>
   * Index Objects: site, bucket, event<br>
   * Index Native: requestId, bucket, name
   */
  void step3SaveNativeListingDb(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Listing according to filter such as now > dateFrom (DRIVER), updating existing info
   * Step4: From Driver to Native<br>
   * Index Native: requestId, bucket, name
   */
  void step4SaveNativeListingDriver(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Compare Native listing with DB and DRIVER (both or only one)<br>
   * Step5: Complete DB without Driver from NativeListing into SitesListing Local step<br>
   * Index Native: requestId, bucket, db, driver.event (optional)<br>
   * Index Objects: site, bucket, name<br>
   * Index Native: requestId, bucket, db.site, driver (optional)<br>
   * Index Native: requestId, bucket, db.site, driver.site<br>
   * Index Sites: requestId, bucket, name
   */
  void step5CompareNativeListingDbDriver(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Used only if NativeListing is not to be kept
   */
  void cleanNativeListing(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Get the local sites listing to send through network<br>
   * Step6: get all local sites listing<br>
   * Index Sites: requestId
   */
  ClosingIterator<DaoSitesListing> getSiteListing(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Used only if SitesListing is not to be kept
   */
  void cleanSitesListing(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Import Central Actions for this site
   */
  void importActions(DaoRequest daoRequest, Iterator<ReconciliationSitesAction> iterator) throws CcsDbException;

  /**
   * Apply imported or locally computed actions for this site
   */
  void applyActions(DaoRequest daoRequest) throws CcsDbException;
}
