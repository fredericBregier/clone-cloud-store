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

import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;

public interface CentralReconciliationService {
  /**
   * Add the remote sites listing to local aggregate one<br>
   * Step7: add all remote sites listing<br>
   * Index Sites: requestId, bucket, name
   */
  void saveRemoteNativeListing(DaoRequest daoRequest, Iterator<ReconciliationSitesListing> stream)
      throws CcsDbException;

  /**
   * Update Request once Remote listing done
   *
   * @return Once all done, returns True, allowing to go to step compute Actions
   */
  boolean updateRequestFromRemoteListing(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Compute actions from sites listing<br>
   * Step8: in 2 steps, all sites declared, not all sites declared
   * Index Sites: requestId, bucket, local.nstatus
   * Index Actions: requestId, bucket, name
   */
  void computeActions(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Compute count actions
   */
  void countFinalActions(DaoRequest daoRequest) throws CcsDbException;

  /**
   * Step9: return iterator of actions to populate topic<br>
   * Index Actions: requestId, bucket
   */
  ClosingIterator<DaoSitesAction> getSitesActon(DaoRequest daoRequest, final String target) throws CcsDbException;

  /**
   * Once all pushed into topic
   */
  void cleanSitesAction(DaoRequest daoRequest) throws CcsDbException;
}
