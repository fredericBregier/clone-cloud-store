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

package io.clonecloudstore.accessor.server.database.mongodb;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.METADATA;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.TABLE_NAME;

/**
 * MongoDB DAO for Object
 */
@MongoEntity(collection = TABLE_NAME)
public class MgDaoAccessorObject extends DaoAccessorObject {
  @BsonId
  private String id;
  @Column(name = METADATA)
  private final UberMap metadata = new UberMap();

  public MgDaoAccessorObject() {
  }

  public MgDaoAccessorObject(final AccessorObject dto) {
    fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoAccessorObject fromDto(final AccessorObject dto) {
    return (MgDaoAccessorObject) super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public AccessorObject getDto() {
    return super.getDto();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public MgDaoAccessorObject setId(final String id) {
    ParametersChecker.checkSanityString(id);
    this.id = id;
    return this;
  }

  @Override
  public Map<String, String> getMetadata() {
    return metadata.getMap();
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public String getMetadata(final String key) {
    return metadata.getMap().get(key);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoAccessorObject addMetadata(final String key, final String value) {
    ParametersChecker.checkSanityString(key, value);
    metadata.getMap().put(key, value);
    return this;
  }

  @Override
  public MgDaoAccessorObject setMetadata(final Map<String, String> metadata) {
    for (final var entry : metadata.entrySet()) {
      ParametersChecker.checkSanityString(entry.getKey(), entry.getValue());
    }
    this.metadata.setMap(metadata);
    return this;
  }
}
