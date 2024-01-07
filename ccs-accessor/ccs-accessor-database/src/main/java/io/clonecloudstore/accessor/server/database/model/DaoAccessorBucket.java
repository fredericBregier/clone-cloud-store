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

package io.clonecloudstore.accessor.server.database.model;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

import static io.clonecloudstore.accessor.model.AccessorStatus.STATUS_LENGTH;
import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_NOSITE_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.SITE_LENGTH;

/**
 * Bucket DAO
 */
@MappedSuperclass
public abstract class DaoAccessorBucket {
  @Column(name = DaoAccessorBucketRepository.SITE, nullable = false, length = SITE_LENGTH)
  private String site;
  @Column(name = DaoAccessorBucketRepository.NAME, nullable = false, length = BUCKET_NOSITE_LENGTH)
  private String name;
  @Column(name = DaoAccessorBucketRepository.CREATION, nullable = false, length = UUID_B32_SIZE)
  private Instant creation;

  @Column(name = DaoAccessorBucketRepository.EXPIRES, nullable = false, length = UUID_B32_SIZE)
  private Instant expires = null;

  @Column(name = DaoAccessorBucketRepository.BUCKET_STATUS, nullable = false, length = STATUS_LENGTH)
  private AccessorStatus status = null;
  /**
   * Reconciliation status is not used elsewhere than Reconciliation
   */
  @Column(name = DaoAccessorBucketRepository.BUCKET_RSTATUS)
  private short rstatus = 0;


  @Transient
  @JsonIgnore
  public DaoAccessorBucket fromDto(final AccessorBucket dto) {
    setId(dto.getId());
    setName(dto.getName());
    setSite(dto.getSite());
    setExpires(dto.getExpires());
    setCreation(dto.getCreation());
    setStatus(dto.getStatus());
    return this;
  }

  @Transient
  @JsonIgnore
  public AccessorBucket getDto() {
    return new AccessorBucket().setId(this.getId()).setName(this.getName()).setSite(this.getSite())
        .setCreation(this.getCreation()).setExpires(this.getExpires()).setStatus(this.getStatus());
  }

  public abstract String getId();

  public abstract DaoAccessorBucket setId(final String bucketTechnicalId);

  public String getSite() {
    return site;
  }

  public DaoAccessorBucket setSite(final String site) {
    this.site = site;
    return this;
  }

  public String getName() {
    return name;
  }

  public DaoAccessorBucket setName(final String name) {
    this.name = name;
    return this;
  }

  public Instant getCreation() {
    return creation;
  }

  public DaoAccessorBucket setCreation(final Instant creation) {
    this.creation = creation;
    return this;
  }

  public Instant getExpires() {
    return expires;
  }

  public DaoAccessorBucket setExpires(final Instant expires) {
    this.expires = expires;
    return this;
  }

  public AccessorStatus getStatus() {
    return status;
  }

  public DaoAccessorBucket setStatus(final AccessorStatus status) {
    this.status = status;
    return this;
  }

  public short getRstatus() {
    return rstatus;
  }

  public DaoAccessorBucket setRstatus(final short rstatus) {
    this.rstatus = rstatus;
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoAccessorBucket that) {
      return Objects.equals(getId(), that.getId()) && Objects.equals(site, that.site) &&
          Objects.equals(name, that.name) && Objects.equals(status, that.status) &&
          Objects.equals(creation, that.creation) && Objects.equals(expires, that.expires);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), site, name, creation, expires, status);
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return "{" + "\"id\":\"" + getId() + "\"" + ", \"site\":\"" + site + "\"" + ", \"name\":\"" + name + "\"" +
          ", \"status\":\"" + status.name() + "\"" + ", \"rstatus\":" + rstatus + ", \"creation\":" + creation +
          ", \"expires\":" + expires + "}";
    }
  }
}
