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

package io.clonecloudstore.reconciliator.database.mongodb;

import java.util.Iterator;

import com.mongodb.MongoException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.clonecloudstore.common.database.mongo.ExtendedPanacheMongoRepositoryBase;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;

import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;

/**
 * MongoDB DAO Bucket Repository
 */
@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgDaoSitesListingRepository extends ExtendedPanacheMongoRepositoryBase<DaoSitesListing, MgDaoSitesListing>
    implements DaoSitesListingRepository {
  private final MgDaoReconciliationService daoService;

  public MgDaoSitesListingRepository(MgDaoReconciliationService daoService) {
    this.daoService = daoService;
  }

  @Override
  public String getTable() {
    return TABLE_NAME;
  }

  public void createIndex() throws CcsDbException {
    //TODO: index à spécialiser
    try {
      mongoCollection().createIndex(Indexes.ascending(REQUESTID, BUCKET, NAME),
          new IndexOptions().name(getTable() + "_unique_filter_idx").unique(true));
      mongoCollection().createIndex(Indexes.ascending(REQUESTID, BUCKET, LOCAL + "." + DaoNativeListingRepository.SITE),
          new IndexOptions().name(getTable() + "_local_filter_idx"));
    } catch (final MongoException e) {
      throw new CcsDbException("Cannot Create Index", e);
    }
  }

  @Override
  public DaoSitesListing createEmptyItem() {
    return new MgDaoSitesListing();
  }

  @Override
  public void compareNativeToSitesListing(final DaoRequest daoRequest, final String site) throws CcsDbException {
    // FIXME
    throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
  }

  @Override
  public void updateRequestFromSitesListing(final DaoRequest daoRequest) throws CcsDbException {
    // FIXME
    throw new UnsupportedOperationException("NOT YET IMPLEMENTED");
  }

  @Override
  public void cleanSitesListing(final DaoRequest daoRequest) throws CcsDbException {
    daoService.cleanSitesListing(daoRequest);
  }

  @Override
  public ClosingIterator<DaoSitesListing> getSiteListing(final DaoRequest daoRequest) throws CcsDbException {
    return daoService.getSiteListing(daoRequest);
  }

  @Override
  public void saveRemoteNativeListing(final DaoRequest daoRequest, final Iterator<DaoSitesListing> stream)
      throws CcsDbException {
    daoService.saveRemoteNativeListing(daoRequest, stream);
  }

  @Override
  public void updateRequestFromRemoteListing(final DaoRequest daoRequest) {
    daoService.updateRequestFromRemoteListing(daoRequest);
  }

}
