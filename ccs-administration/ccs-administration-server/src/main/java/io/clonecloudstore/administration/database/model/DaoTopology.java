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

package io.clonecloudstore.administration.database.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;
import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

import static io.clonecloudstore.common.standard.system.ParametersChecker.OBJECT_LENGTH;

@MappedSuperclass
public abstract class DaoTopology {
  @Column(name = DaoTopologyRepository.URI, nullable = false, length = 2000)
  @URL(message = "URI format error")
  private String uri;

  @Column(name = DaoTopologyRepository.NAME, nullable = false, length = OBJECT_LENGTH)
  @NotBlank(message = "Name should not be null")
  private String name;

  @Column(name = DaoTopologyRepository.STATUS, nullable = false)
  private TopologyStatus status = TopologyStatus.UNKNOWN;

  public abstract String getId();

  public abstract DaoTopology setId(final String id);

  public TopologyStatus getStatus() {
    return status;
  }

  public DaoTopology setStatus(TopologyStatus status) {
    this.status = status == null ? TopologyStatus.UNKNOWN : status;
    return this;
  }

  public String getUri() {
    return uri;
  }

  public DaoTopology setUri(String uri) {
    ParametersChecker.checkParameter("URI cannot be null", uri);
    ParametersChecker.checkSanityString(uri);
    this.uri = uri;
    return this;
  }

  public String getName() {
    return name;
  }

  public DaoTopology setName(String name) {
    ParametersChecker.checkParameter("Name cannot be null", name);
    ParametersChecker.checkSanityString(name);
    this.name = name;
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
  public DaoTopology fromDto(final Topology dto) {
    setUri(dto.uri()).setName(dto.name()).setStatus(dto.status()).setId(dto.id());
    return this;
  }

  @Transient
  @JsonIgnore
  public Topology getDto() {
    return new Topology(this.getId(), this.getName(), this.getUri(), this.getStatus());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoTopology that) {
      return Objects.equals(getId(), that.getId()) && Objects.equals(getUri(), that.getUri()) &&
          Objects.equals(getName(), that.getName()) && Objects.equals(getStatus(), that.getStatus());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), getName(), getUri(), getStatus());
  }

}
