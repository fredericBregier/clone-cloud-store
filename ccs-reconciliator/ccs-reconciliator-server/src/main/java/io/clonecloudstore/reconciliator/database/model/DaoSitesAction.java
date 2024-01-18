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
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.OBJECT_LENGTH;

/**
 * NeedAction uses ReconciliationAction rank
 */
@MappedSuperclass
public abstract class DaoSitesAction {
  @Column(name = DaoSitesActionRepository.REQUESTID, nullable = false, length = UUID_B32_SIZE)
  private String requestId;
  @Column(name = DaoSitesActionRepository.BUCKET, nullable = false, length = BUCKET_LENGTH)
  private String bucket;
  @Column(name = DaoSitesActionRepository.NAME, nullable = false, length = OBJECT_LENGTH)
  private String name;
  @Column(name = DaoSitesActionRepository.NEED_ACTION)
  private short needAction;
  @Column(name = DaoSitesActionRepository.NEED_ACTION_FROM)
  private List<String> needActionFrom;
  @Column(name = DaoSitesActionRepository.SITES)
  private List<String> sites;

  protected DaoSitesAction() {
  }

  public abstract String getId();

  public abstract DaoSitesAction setId(final String id);

  public String getRequestId() {
    return requestId;
  }

  public DaoSitesAction setRequestId(final String requestId) {
    this.requestId = requestId;
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public DaoSitesAction setBucket(final String bucket) {
    this.bucket = bucket;
    return this;
  }

  public String getName() {
    return name;
  }

  public DaoSitesAction setName(final String name) {
    this.name = name;
    return this;
  }

  public short getNeedAction() {
    return needAction;
  }

  public DaoSitesAction setNeedAction(final short needAction) {
    this.needAction = needAction;
    return this;
  }

  public List<String> getNeedActionFrom() {
    return needActionFrom;
  }

  public DaoSitesAction setNeedActionFrom(final List<String> needActionFrom) {
    this.needActionFrom = needActionFrom;
    return this;
  }

  public List<String> getSites() {
    return sites;
  }

  public DaoSitesAction setSites(final List<String> sites) {
    this.sites = sites;
    return this;
  }

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
  public DaoSitesAction fromDto(final ReconciliationSitesAction dto) {
    setId(dto.id()).setRequestId(dto.requestId()).setBucket(dto.bucket()).setName(dto.name())
        .setNeedAction(dto.needAction()).setNeedActionFrom(dto.needActionFrom()).setSites(dto.sites());
    return this;
  }

  @Transient
  @JsonIgnore
  public ReconciliationSitesAction getDto() {
    return new ReconciliationSitesAction(getId(), getRequestId(), getBucket(), getName(), getNeedAction(),
        getNeedActionFrom(), getSites());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoSitesAction that) {
      return Objects.equals(getRequestId(), that.getRequestId()) && Objects.equals(getBucket(), that.getBucket()) &&
          Objects.equals(getName(), that.getName()) && Objects.equals(getNeedAction(), that.getNeedAction()) &&
          Objects.equals(getNeedActionFrom(), that.getNeedActionFrom()) && Objects.equals(getSites(), that.getSites());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRequestId(), getBucket(), getName(), getNeedAction(), getNeedActionFrom(), getSites());
  }
}
