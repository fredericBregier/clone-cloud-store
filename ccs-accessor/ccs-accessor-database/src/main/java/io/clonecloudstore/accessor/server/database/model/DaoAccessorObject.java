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
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

import static io.clonecloudstore.accessor.model.AccessorStatus.STATUS_LENGTH;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.BUCKET;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.CREATION;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.EXPIRES;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.HASH;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.NAME;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.RSTATUS;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.SITE;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.SIZE;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.STATUS;
import static io.clonecloudstore.common.standard.system.BaseXx.HASH_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.BUCKET_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.OBJECT_LENGTH;
import static io.clonecloudstore.common.standard.system.ParametersChecker.SITE_LENGTH;

/**
 * Object DAO
 */
@MappedSuperclass
public abstract class DaoAccessorObject {
  @Column(name = SITE, nullable = false, length = SITE_LENGTH)
  private String site;
  @Column(name = BUCKET, nullable = false, length = BUCKET_LENGTH)
  private String bucket;
  @Column(name = NAME, nullable = false, length = OBJECT_LENGTH)
  private String name;
  @Column(name = HASH, nullable = false, length = HASH_LENGTH)
  private String hash;
  @Column(name = STATUS, nullable = false, length = STATUS_LENGTH)
  private AccessorStatus status;
  @Column(name = CREATION, nullable = false)
  private Instant creation;
  @Column(name = EXPIRES)
  private Instant expires;
  @Column(name = SIZE)
  private long size;
  /**
   * Reconciliation status is not used elsewhere than Reconciliation,
   * based on AccessorStatus
   */
  @Column(name = RSTATUS)
  private short rstatus;

  public abstract String getId();

  public abstract DaoAccessorObject setId(String id);

  @Transient
  @JsonIgnore
  public DaoAccessorObject fromDto(final AccessorObject dto) {
    this.setId(dto.getId()).setSite(dto.getSite()).setBucket(dto.getBucket()).setName(dto.getName())
        .setHash(dto.getHash()).setStatus(dto.getStatus()).setCreation(dto.getCreation()).setExpires(dto.getExpires())
        .setSize(dto.getSize()).setMetadata(dto.getMetadata());
    return this;
  }

  /**
   * Ignore Id and Site
   *
   * @return True if updated locally
   */
  @Transient
  @JsonIgnore
  public boolean updateFromDtoExceptIdSite(final AccessorObject dto) {
    if (Objects.equals(dto.getBucket(), this.getBucket()) && Objects.equals(dto.getName(), this.getName()) &&
        Objects.equals(dto.getHash(), this.getHash()) && Objects.equals(dto.getStatus(), this.getStatus()) &&
        Objects.equals(dto.getCreation(), this.getCreation()) && Objects.equals(dto.getExpires(), this.getExpires()) &&
        Objects.equals(dto.getSize(), this.getSize()) && Objects.equals(dto.getMetadata(), this.getMetadata())) {
      return false;
    }
    this.setBucket(dto.getBucket()).setName(dto.getName()).setHash(dto.getHash()).setStatus(dto.getStatus())
        .setCreation(dto.getCreation()).setExpires(dto.getExpires()).setSize(dto.getSize())
        .setMetadata(dto.getMetadata());
    return true;
  }

  @Transient
  @JsonIgnore
  public AccessorObject getDto() {
    return new AccessorObject().setId(this.getId()).setSite(this.getSite()).setBucket(this.getBucket())
        .setName(this.getName()).setHash(this.getHash()).setStatus(this.getStatus()).setCreation(this.getCreation())
        .setExpires(this.getExpires()).setSize(this.getSize()).setMetadata(this.getMetadata());
  }

  public String getSite() {
    return site;
  }

  public DaoAccessorObject setSite(final String site) {
    ParametersChecker.checkSanityString(site);
    this.site = site;
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public DaoAccessorObject setBucket(final String bucket) {
    ParametersChecker.checkSanityBucketName(bucket);
    this.bucket = bucket;
    return this;
  }

  public String getName() {
    return name;
  }

  public DaoAccessorObject setName(final String name) {
    ParametersChecker.checkSanityObjectName(name);
    this.name = name;
    return this;
  }

  public String getHash() {
    return hash;
  }

  public DaoAccessorObject setHash(final String hash) {
    ParametersChecker.checkSanityString(hash);
    this.hash = hash;
    return this;
  }

  public AccessorStatus getStatus() {
    return status;
  }

  public DaoAccessorObject setStatus(final AccessorStatus status) {
    ParametersChecker.checkParameterNullOnly("Status cannot be null", status);
    this.status = status;
    return this;
  }

  public Instant getCreation() {
    return creation;
  }

  public DaoAccessorObject setCreation(final Instant creation) {
    this.creation = SystemTools.toMillis(creation);
    return this;
  }

  public Instant getExpires() {
    return expires;
  }

  public DaoAccessorObject setExpires(final Instant expires) {
    this.expires = SystemTools.toMillis(expires);
    return this;
  }

  public long getSize() {
    return size;
  }

  public DaoAccessorObject setSize(final long size) {
    this.size = size;
    return this;
  }

  public abstract Map<String, String> getMetadata();

  @Transient
  @JsonIgnore
  public abstract String getMetadata(String key);

  @Transient
  @JsonIgnore
  public abstract DaoAccessorObject addMetadata(String key, String value);

  public abstract DaoAccessorObject setMetadata(Map<String, String> metadata);

  public short getRstatus() {
    return rstatus;
  }

  public DaoAccessorObject setRstatus(final short rstatus) {
    this.rstatus = rstatus;
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof DaoAccessorObject that) {
      return size == that.size && Objects.equals(getId(), that.getId()) && Objects.equals(site, that.site) &&
          Objects.equals(bucket, that.bucket) && Objects.equals(name, that.name) && Objects.equals(hash, that.hash) &&
          Objects.equals(status, that.status) && Objects.equals(creation, that.creation) &&
          Objects.equals(expires, that.expires) && Objects.deepEquals(getMetadata(), that.getMetadata());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), site, bucket, name, hash, status, creation, expires, size, getMetadata());
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      throw new CcsInvalidArgumentRuntimeException(e.getMessage());
    }
  }
}
