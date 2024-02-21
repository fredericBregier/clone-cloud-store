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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;
import static io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository.LOCAL;
import static io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository.TABLE_NAME;

/**
 * MongoDB DAO for Bucket
 */
@MongoEntity(collection = TABLE_NAME)
public class MgDaoSitesListing extends DaoSitesListing {
  @BsonId
  @Column(length = UUID_B32_SIZE)
  private String id;

  @BsonProperty(LOCAL)
  @Column(name = LOCAL)
  List<SingleSiteObject> local;

  public MgDaoSitesListing() {
    //Empty
  }

  public MgDaoSitesListing(final ReconciliationSitesListing dto) {
    fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoSitesListing fromDto(final ReconciliationSitesListing dto) {
    return (MgDaoSitesListing) super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public ReconciliationSitesListing getDto() {
    return super.getDto();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public MgDaoSitesListing setId(final String id) {
    this.id = id;
    return this;
  }

  @Override
  public List<SingleSiteObject> getLocal() {
    return local;
  }

  @Override
  public MgDaoSitesListing setLocal(final List<SingleSiteObject> local) {
    this.local = local;
    return this;
  }
}
