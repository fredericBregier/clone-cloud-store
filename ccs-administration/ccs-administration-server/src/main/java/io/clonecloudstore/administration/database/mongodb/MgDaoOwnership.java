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

package io.clonecloudstore.administration.database.mongodb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.administration.database.model.DaoOwnership;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static io.clonecloudstore.administration.database.model.DaoOwnershipRepository.TABLE_NAME;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;


@MongoEntity(collection = TABLE_NAME)
public class MgDaoOwnership extends DaoOwnership {
  public MgDaoOwnership() {
    //Empty
  }

  public MgDaoOwnership(final ClientBucketAccess dto) {
    fromDto(dto);
  }

  @BsonId
  @Column(nullable = false, length = BUCKET_LENGTH)
  private String id;

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoOwnership fromDto(final ClientBucketAccess dto) {
    return (MgDaoOwnership) super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public ClientBucketAccess getDto() {
    return super.getDto();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public DaoOwnership setId(final String id) {
    ParametersChecker.checkParameter("Id cannot be null", id);
    ParametersChecker.checkSanityString(id);
    this.id = id;
    return this;
  }
}
