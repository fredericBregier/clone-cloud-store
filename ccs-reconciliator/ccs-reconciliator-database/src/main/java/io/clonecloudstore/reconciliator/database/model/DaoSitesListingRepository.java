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

import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;

public interface DaoSitesListingRepository extends RepositoryBaseInterface<DaoSitesListing> {
  String TABLE_NAME = "siteslisting";
  String REQUESTID = DaoNativeListingRepository.REQUESTID;
  String BUCKET = DaoNativeListingRepository.BUCKET;
  String NAME = DaoNativeListingRepository.NAME;
  String LOCAL = "local";

  ClosingIterator<DaoSitesListing> getSiteListing(final DaoRequest daoRequest) throws CcsDbException;

  void saveRemoteNativeListing(final DaoRequest daoRequest, final Iterator<DaoSitesListing> stream)
      throws CcsDbException;

  void updateRequestFromRemoteListing(final DaoRequest daoRequest) throws CcsDbException;

  void compareNativeToSitesListing(final DaoRequest daoRequest, final String site) throws CcsDbException;

  void updateRequestFromSitesListing(final DaoRequest daoRequest) throws CcsDbException;

  void cleanSitesListing(final DaoRequest daoRequest) throws CcsDbException;
}
