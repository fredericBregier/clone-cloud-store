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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucket;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.mongodb.panache.common.MongoEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Transient;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;

import static io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository.TABLE_NAME;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;

/**
 * MongoDB DAO for Bucket
 */
@MongoEntity(collection = TABLE_NAME)
public class MgDaoAccessorBucket extends DaoAccessorBucket {
  @BsonId
  @Column(length = BUCKET_LENGTH)
  private String id;

  public MgDaoAccessorBucket() {
    //Empty
  }

  public MgDaoAccessorBucket(final AccessorBucket dto) {
    fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public MgDaoAccessorBucket fromDto(final AccessorBucket dto) {
    return (MgDaoAccessorBucket) super.fromDto(dto);
  }

  @Transient
  @BsonIgnore
  @JsonIgnore
  @Override
  public AccessorBucket getDto() {
    return super.getDto();
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public MgDaoAccessorBucket setId(final String bucketTechnicalId) {
    ParametersChecker.checkSanityBucketName(bucketTechnicalId);
    this.id = bucketTechnicalId;
    return this;
  }
}
