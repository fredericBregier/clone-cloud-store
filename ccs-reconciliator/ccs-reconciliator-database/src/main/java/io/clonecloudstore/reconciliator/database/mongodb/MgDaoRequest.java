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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;

/**
 * MongoDB DAO for Bucket
 */
@MongoEntity(collection = DaoRequestRepository.TABLE_NAME)
public class MgDaoRequest extends DaoRequest {
  @BsonId
  @Column(length = UUID_B32_SIZE)
  private String id;

  public MgDaoRequest() {
    //Empty
  }

  public MgDaoRequest(final ReconciliationRequest dto) {
    fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoRequest fromDto(final ReconciliationRequest dto) {
    return (MgDaoRequest) super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public ReconciliationRequest getDto() {
    return super.getDto();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public MgDaoRequest setId(final String id) {
    this.id = id;
    return this;
  }
}
