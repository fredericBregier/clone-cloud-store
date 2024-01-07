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

package io.clonecloudstore.common.database.mongo.impl.codec;

import io.clonecloudstore.common.database.model.dao.DaoExample;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.mongodb.panache.common.MongoEntity;
import org.bson.codecs.pojo.annotations.BsonId;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;

/**
 * This is a fake example where some fields need a specific Codec
 * to enable MongoDB and Panache to handle them
 */
@MongoEntity(collection = TABLE_NAME)
public class MgDaoExample extends DaoExample {
  @BsonId
  private String guid;

  public MgDaoExample() {
    // Empty
  }

  public MgDaoExample(final DtoExample dto) {
    fromDto(dto);
  }

  @Override
  public String getGuid() {
    return guid;
  }

  @Override
  public MgDaoExample setGuid(final String guid) {
    ParametersChecker.checkSanityString(guid);
    this.guid = guid;
    return this;
  }
}
