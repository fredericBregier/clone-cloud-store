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

import io.clonecloudstore.reconciliator.database.model.DaoNativeListing;
import io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import org.bson.codecs.pojo.annotations.BsonId;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;

/**
 * MongoDB DAO for Native (local) listing
 */
@MongoEntity(collection = DaoNativeListingRepository.TABLE_NAME)
public class MgDaoNativeListing extends DaoNativeListing {
  @BsonId
  @Column(length = UUID_B32_SIZE)
  private String id;
  @Column(name = DaoNativeListingRepository.DB)
  private SingleSiteObject db;
  @Column(name = DaoNativeListingRepository.DRIVER)
  private SingleSiteObject driver;

  public MgDaoNativeListing() {
    //Empty
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public MgDaoNativeListing setId(final String id) {
    this.id = id;
    return this;
  }

  @Override
  public SingleSiteObject getDb() {
    return db;
  }

  @Override
  public DaoNativeListing setDb(final SingleSiteObject db) {
    this.db = db;
    return this;
  }

  @Override
  public SingleSiteObject getDriver() {
    return driver;
  }

  @Override
  public DaoNativeListing setDriver(final SingleSiteObject driver) {
    this.driver = driver;
    return this;
  }
}
