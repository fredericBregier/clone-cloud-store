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

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.SITE_LENGTH;

@MappedSuperclass
public abstract class DaoRequest {
  @Column(name = DaoRequestRepository.CLIENTID, nullable = false, length = UUID_B32_SIZE)
  private String clientId;
  @Column(name = DaoRequestRepository.BUCKET, nullable = false, length = BUCKET_LENGTH)
  String bucket;
  @Column(name = DaoRequestRepository.FILTER)
  AccessorFilter filter;
  @Column(name = DaoRequestRepository.FROMSITE, nullable = false, length = SITE_LENGTH)
  String fromSite;
  @Column(name = DaoRequestRepository.CURRENTSITE, nullable = false, length = SITE_LENGTH)
  String currentSite;
  @Column(name = DaoRequestRepository.START)
  Instant start;
  @Column(name = DaoRequestRepository.CHECKED)
  long checked;
  @Column(name = DaoRequestRepository.CHECKED_DB)
  long checkedDb;
  @Column(name = DaoRequestRepository.CHECKED_DRIVER)
  long checkedDriver;
  @Column(name = DaoRequestRepository.CHECKED_REMOTE)
  long checkedRemote;
  @Column(name = DaoRequestRepository.ACTIONS)
  long actions;
  @Column(name = DaoRequestRepository.DRY_RUN)
  boolean dryRun;
  @Column(name = DaoRequestRepository.STOP)
  Instant stop;

  protected DaoRequest() {
  }

  public abstract String getId();

  public abstract DaoRequest setId(final String id);

  public String getClientId() {
    return clientId;
  }

  public DaoRequest setClientId(final String clientId) {
    this.clientId = clientId;
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public DaoRequest setBucket(final String bucket) {
    this.bucket = bucket;
    return this;
  }

  public AccessorFilter getFilter() {
    return filter;
  }

  public DaoRequest setFilter(final AccessorFilter filter) {
    this.filter = filter;
    return this;
  }

  public String getFromSite() {
    return fromSite;
  }

  public DaoRequest setFromSite(final String fromSite) {
    this.fromSite = fromSite;
    return this;
  }

  public String getCurrentSite() {
    return currentSite;
  }

  public DaoRequest setCurrentSite(final String currentSite) {
    this.currentSite = currentSite;
    return this;
  }

  public Instant getStart() {
    return start;
  }

  public DaoRequest setStart(final Instant start) {
    this.start = start;
    return this;
  }

  public long getChecked() {
    return checked;
  }

  public DaoRequest setChecked(final long checked) {
    this.checked = checked;
    return this;
  }

  public long getCheckedDb() {
    return checkedDb;
  }

  public DaoRequest setCheckedDb(final long checkedDb) {
    this.checkedDb = checkedDb;
    return this;
  }

  public long getCheckedDriver() {
    return checkedDriver;
  }

  public DaoRequest setCheckedDriver(final long checkedDriver) {
    this.checkedDriver = checkedDriver;
    return this;
  }

  public long getCheckedRemote() {
    return checkedRemote;
  }

  public DaoRequest setCheckedRemote(final long checkedRemote) {
    this.checkedRemote = checkedRemote;
    return this;
  }

  public long getActions() {
    return actions;
  }

  public DaoRequest setActions(final long actions) {
    this.actions = actions;
    return this;
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public DaoRequest setDryRun(final boolean dryRun) {
    this.dryRun = dryRun;
    return this;
  }

  public Instant getStop() {
    return stop;
  }

  public DaoRequest setStop(final Instant stop) {
    this.stop = stop;
    return this;
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "{" + "\"id\":\"" + getId() + "\"" + ", \"clientId\":\"" + getClientId() + "\"" + ", \"bucket\":\"" +
          getBucket() + "\"" + ", \"filter\":\"" + getFilter() + "\"" + ", \"fromSite\":\"" + getFromSite() + "\"" +
          ", \"currentSite\":\"" + getCurrentSite() + ", \"start\":\"" + getStart() + "\"" + "\"" + ", \"checked\":" +
          getChecked() + ", \"checkedDb\":" + getCheckedDb() + ", \"checkedDriver\":" + getCheckedDriver() + ", " +
          "\"checkedRemote\":" + getCheckedRemote() + ", \"actions\":" + getActions() + ", \"dryRun\":" + isDryRun() +
          ", \"stop\":\"" + getStop() + "\"" + "}";
    }
  }

  @Transient
  @JsonIgnore
  public DaoRequest fromDto(final ReconciliationRequest dto) {
    setId(dto.id()).setClientId(dto.clientId()).setBucket(dto.bucket()).setFilter(dto.filter())
        .setFromSite(dto.fromSite()).setCurrentSite(dto.currentSite()).setStart(dto.start()).setChecked(dto.checked())
        .setCheckedDb(dto.checkedDb()).setCheckedDriver(dto.checkedDriver()).setCheckedRemote(dto.checkedRemote())
        .setActions(dto.actions()).setDryRun(dto.dryRun()).setStop(dto.stop());
    return this;
  }

  @Transient
  @JsonIgnore
  public ReconciliationRequest getDto() {
    return new ReconciliationRequest(getId(), getClientId(), getBucket(), getFilter(), getFromSite(), getCurrentSite(),
        getStart(), getChecked(), getCheckedDb(), getCheckedDriver(), getCheckedRemote(), getActions(), isDryRun(),
        getStop());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoRequest that) {
      return Objects.equals(getId(), that.getId()) && Objects.equals(getClientId(), that.getClientId()) &&
          Objects.equals(getBucket(), that.getBucket()) && Objects.equals(getFilter(), that.getFilter()) &&
          Objects.equals(getFromSite(), that.getFromSite()) &&
          Objects.equals(getCurrentSite(), that.getCurrentSite()) && Objects.equals(getStart(), that.getStart()) &&
          Objects.equals(getChecked(), that.getChecked()) && Objects.equals(getCheckedDb(), that.getCheckedDb()) &&
          Objects.equals(getCheckedDriver(), that.getCheckedDriver()) &&
          Objects.equals(getCheckedRemote(), that.getCheckedRemote()) &&
          Objects.equals(getActions(), that.getActions()) && Objects.equals(isDryRun(), that.isDryRun()) &&
          Objects.equals(getStop(), that.getStop());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClientId(), getBucket(), getFilter(), getFromSite(), getCurrentSite(), getStart(),
        getChecked(), getCheckedDb(), getCheckedDriver(), getCheckedRemote(), getActions(), isDryRun(), getStop());
  }

}
