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

package io.clonecloudstore.topology.database.mongodb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.topology.database.model.DaoTopology;
import io.clonecloudstore.topology.model.Topology;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static io.clonecloudstore.common.standard.system.ParametersChecker.SITE_LENGTH;
import static io.clonecloudstore.topology.database.model.DaoTopologyRepository.TABLE_NAME;


@MongoEntity(collection = TABLE_NAME)
public class MgDaoTopology extends DaoTopology {
  public MgDaoTopology() {
    //Empty
  }

  public MgDaoTopology(final Topology dto) {
    fromDto(dto);
  }

  @BsonId
  @Column(nullable = false, length = SITE_LENGTH)
  private String id;

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoTopology fromDto(final Topology dto) {
    return (MgDaoTopology) super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public Topology getDto() {
    return super.getDto();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public DaoTopology setId(final String id) {
    ParametersChecker.checkParameter("Id cannot be null", id);
    ParametersChecker.checkSanityString(id);
    this.id = id;
    return this;
  }
}
