/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.reconciliator.server.application;

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;

import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.BUCKET;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.REQUESTID;
import static io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository.SITES;

public class ReconciliationConstants {
  public static final String REQUEST_LOCAL = "request-local";
  public static final String REQUEST_READY_FOR_CENTRAL = "request-ready-for-central";
  public static final String REQUEST_READY_FOR_LOCAL = "request-ready-for-local";
  public static final String REQUEST_RECONCILIATION = "request-reconciliation";
  public static final String WAY_OUT = "-out";
  public static final String WAY_IN = "-in";

  public static DbQuery subsetForOneSite(final DaoRequest daoRequest, final String remote) {
    return new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
        new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
        new DbQuery(RestQuery.QUERY.REVERSE_IN, SITES, remote));
  }

  private ReconciliationConstants() {
    // Empty
  }
}
