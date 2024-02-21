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

package io.clonecloudstore.reconciliator.database.model;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.OBJECT_LENGTH;

/**
 * SingleSiteObject uses ReconciliationAction rank
 */
@MappedSuperclass
public abstract class DaoSitesListing {
  @Column(name = DaoSitesListingRepository.REQUESTID, nullable = false, length = UUID_B32_SIZE)
  private String requestId;
  @Column(name = DaoSitesListingRepository.BUCKET, nullable = false, length = BUCKET_LENGTH)
  private String bucket;
  @Column(name = DaoSitesListingRepository.NAME, nullable = false, length = OBJECT_LENGTH)
  private String name;

  protected DaoSitesListing() {
  }

  public abstract String getId();

  public abstract DaoSitesListing setId(final String id);

  public String getRequestId() {
    return requestId;
  }

  public DaoSitesListing setRequestId(final String requestId) {
    this.requestId = requestId;
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public DaoSitesListing setBucket(final String bucket) {
    this.bucket = bucket;
    return this;
  }

  public String getName() {
    return name;
  }

  public DaoSitesListing setName(final String name) {
    this.name = name;
    return this;
  }

  public abstract List<SingleSiteObject> getLocal();

  public abstract DaoSitesListing setLocal(final List<SingleSiteObject> locals);

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      throw new CcsInvalidArgumentRuntimeException(e.getMessage());
    }
  }

  @Transient
  @JsonIgnore
  public DaoSitesListing fromDto(final ReconciliationSitesListing dto) {
    setId(dto.id()).setRequestId(dto.requestId()).setBucket(dto.bucket()).setName(dto.name()).setLocal(dto.local());
    return this;
  }

  @Transient
  @JsonIgnore
  public ReconciliationSitesListing getDto() {
    return new ReconciliationSitesListing(getId(), getRequestId(), getBucket(), getName(), getLocal());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoSitesListing that) {
      return Objects.equals(getRequestId(), that.getRequestId()) && Objects.equals(getBucket(), that.getBucket()) &&
          Objects.equals(getName(), that.getName()) && Objects.equals(getLocal(), that.getLocal());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRequestId(), getBucket(), getName(), getLocal());
  }

}
