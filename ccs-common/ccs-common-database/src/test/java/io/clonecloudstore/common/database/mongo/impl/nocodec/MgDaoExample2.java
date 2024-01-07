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

package io.clonecloudstore.common.database.mongo.impl.nocodec;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.common.database.model.dao.DaoExample2;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static io.clonecloudstore.common.database.model.dao.DaoExample2Repository.TABLE_NAME;

@MongoEntity(collection = TABLE_NAME)
public class MgDaoExample2 extends DaoExample2 {
  @BsonId
  private String guid;

  public MgDaoExample2() {
    // Empty
  }

  public MgDaoExample2(final DtoExample dto) {
    fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public void fromDto(final DtoExample dto) {
    super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public DtoExample getDto() {
    return super.getDto();
  }

  public String getGuid() {
    return guid;
  }

  public MgDaoExample2 setGuid(final String guid) {
    ParametersChecker.checkSanityString(guid);
    this.guid = guid;
    return this;
  }
}
