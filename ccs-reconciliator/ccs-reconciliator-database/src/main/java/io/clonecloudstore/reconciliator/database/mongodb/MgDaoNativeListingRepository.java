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

import com.mongodb.MongoException;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.clonecloudstore.common.database.mongo.ExtendedPanacheMongoRepositoryBase;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListing;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;

import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;

/**
 * MongoDB DAO Bucket Repository
 */
@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgDaoNativeListingRepository
    extends ExtendedPanacheMongoRepositoryBase<DaoNativeListing, MgDaoNativeListing>
    implements DaoNativeListingRepository {
  private final MgDaoReconciliationService daoService;

  public MgDaoNativeListingRepository(MgDaoReconciliationService daoService) {
    this.daoService = daoService;
  }


  @Override
  public String getTable() {
    return TABLE_NAME;
  }

  public void createIndex() throws CcsDbException {
    //TODO: index to specialize
    try {
      mongoCollection().createIndex(Indexes.ascending(REQUESTID, BUCKET, NAME),
          new IndexOptions().name(getTable() + "_unique_filter_idx").unique(true));
      mongoCollection().createIndex(Indexes.ascending(BUCKET, NAME, DB + "." + SITE),
          new IndexOptions().name(getTable() + "_site_filter_idx"));
      mongoCollection().createIndex(Indexes.ascending(REQUESTID, BUCKET, DB + "." + SITE, DRIVER + "." + SITE),
          new IndexOptions().name(getTable() + "_db_driver_filter_idx"));
    } catch (final MongoException e) {
      throw new CcsDbException("Cannot Create Index", e);
    }
  }

  @Override
  public DaoNativeListing createEmptyItem() {
    return new MgDaoNativeListing();
  }

  @Override
  public void saveNativeListingDb(final DaoRequest daoRequest) throws CcsDbException {
    daoService.step3SaveNativeListingDb(daoRequest);
  }

  @Override
  public void saveNativeListingDriver(final DaoRequest daoRequest) throws CcsDbException {
    daoService.step4SaveNativeListingDriver(daoRequest);
  }

  @Override
  public void compareNativeListingDbDriver(final DaoRequest daoRequest) throws CcsDbException {
    daoService.step5CompareNativeListingDbDriver(daoRequest);
  }

  @Override
  public void cleanNativeListing(final DaoRequest daoRequest) throws CcsDbException {
    daoService.cleanNativeListing(daoRequest);
  }
}
