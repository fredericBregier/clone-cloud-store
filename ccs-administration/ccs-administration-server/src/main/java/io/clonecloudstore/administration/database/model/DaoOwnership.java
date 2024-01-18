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
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;
import jakarta.validation.constraints.NotBlank;

import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;

@MappedSuperclass
public abstract class DaoOwnership {
  @Column(name = DaoOwnershipRepository.CLIENT_ID, nullable = false, length = BUCKET_LENGTH)
  private String clientId;

  @Column(name = DaoOwnershipRepository.BUCKET, nullable = false, length = BUCKET_LENGTH)
  @NotBlank(message = "Bucket should not be null")
  private String bucket;

  @Column(name = DaoOwnershipRepository.OWNERSHIP, nullable = false)
  private ClientOwnership ownership = ClientOwnership.UNKNOWN;

  public abstract String getId();

  public abstract DaoOwnership setId(final String id);

  public ClientOwnership getOwnership() {
    return ownership;
  }

  public DaoOwnership setOwnership(ClientOwnership ownership) {
    this.ownership = ownership == null ? ClientOwnership.UNKNOWN : ownership;
    return this;
  }

  public String getClientId() {
    return clientId;
  }

  public DaoOwnership setClientId(String clientId) {
    ParametersChecker.checkParameter("ClientId cannot be null", clientId);
    ParametersChecker.checkSanityString(clientId);
    this.clientId = clientId;
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public DaoOwnership setBucket(String bucket) {
    ParametersChecker.checkParameter("Bucket cannot be null", bucket);
    ParametersChecker.checkSanityBucketName(bucket);
    this.bucket = bucket;
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
  public DaoOwnership fromDto(final ClientBucketAccess dto) {
    setClientId(dto.client()).setBucket(dto.bucket()).setOwnership(dto.ownership())
        .setId(DaoOwnershipRepository.getInternalId(dto.client(), dto.bucket()));
    return this;
  }

  @Transient
  @JsonIgnore
  public ClientBucketAccess getDto() {
    return new ClientBucketAccess(this.getClientId(), this.getBucket(), this.getOwnership());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DaoOwnership that) {
      return Objects.equals(getId(), that.getId()) && Objects.equals(getClientId(), that.getClientId()) &&
          Objects.equals(getBucket(), that.getBucket()) && Objects.equals(getOwnership(), that.getOwnership());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), getClientId(), getBucket(), getOwnership());
  }

}
