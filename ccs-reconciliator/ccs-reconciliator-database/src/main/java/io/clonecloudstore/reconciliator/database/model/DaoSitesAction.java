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

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.OBJECT_LENGTH;

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
  private String needActionFrom;

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

  public String getNeedActionFrom() {
    return needActionFrom;
  }

  public DaoSitesAction setNeedActionFrom(final String needActionFrom) {
    this.needActionFrom = needActionFrom;
    return this;
  }

  public abstract Set<String> getActions();

  public abstract DaoSitesAction setActions(final Set<String> actions);

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "{" + "\"id\":\"" + getId() + "\"" + ", \"requestId\":\"" + getRequestId() + "\"" + ", \"bucket\":\"" +
          getBucket() + "\"" + ", \"object\":\"" + getName() + "\"" + ", \"needAction\":" + getNeedAction() +
          ", \"actions\":\"" + getActions() + "\"" + ", \"needActionFrom\":\"" + getNeedActionFrom() + "\"" + "}";
    }
  }

  @Transient
  @JsonIgnore
  public DaoSitesAction fromDto(final ReconciliationSitesAction dto) {
    setId(dto.id()).setRequestId(dto.requestId()).setBucket(dto.bucket()).setName(dto.name())
        .setNeedAction(dto.needAction()).setNeedActionFrom(dto.needActionFrom()).setActions(dto.actions());
    return this;
  }

  @Transient
  @JsonIgnore
  public ReconciliationSitesAction getDto() {
    return new ReconciliationSitesAction(getId(), getRequestId(), getBucket(), getName(), getNeedAction(),
        getNeedActionFrom(), getActions());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoSitesAction that) {
      return Objects.equals(getRequestId(), that.getRequestId()) && Objects.equals(getBucket(), that.getBucket()) &&
          Objects.equals(getName(), that.getName()) && Objects.equals(getNeedAction(), that.getNeedAction()) &&
          Objects.equals(getNeedActionFrom(), that.getNeedActionFrom()) &&
          Objects.equals(getActions(), that.getActions());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRequestId(), getBucket(), getName(), getNeedAction(), getNeedActionFrom(), getActions());
  }
}
