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

package io.clonecloudstore.accessor.model;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Accessor Bucket DTO
 */
@RegisterForReflection
public class AccessorBucket {
  private String id;
  private String site;
  private String name;
  private Instant creation;
  private Instant expires;

  private AccessorStatus status = AccessorStatus.UNKNOWN;

  public AccessorBucket() {
    // Empty
  }

  public String getId() {
    return id;
  }

  public AccessorBucket setId(final String id) {
    ParametersChecker.checkSanityBucketName(id);
    this.id = id;
    return this;
  }

  public String getSite() {
    return site;
  }

  public AccessorBucket setSite(final String site) {
    this.site = site;
    return this;
  }

  public String getName() {
    return name;
  }

  public AccessorBucket setName(final String name) {
    ParametersChecker.checkSanityBucketName(name);
    this.name = name;
    return this;
  }

  public Instant getCreation() {
    return creation;
  }

  public AccessorBucket setCreation(final Instant creation) {
    this.creation = SystemTools.toMillis(creation);
    return this;
  }

  public Instant getExpires() {
    return expires;
  }

  public AccessorBucket setExpires(final Instant expires) {
    this.expires = SystemTools.toMillis(expires);
    return this;
  }

  public AccessorStatus getStatus() {
    return status;
  }

  public AccessorBucket setStatus(final AccessorStatus status) {
    this.status = status;
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AccessorBucket that) {
      return Objects.equals(id, that.id) && Objects.equals(site, that.site) && Objects.equals(name, that.name) &&
          Objects.equals(creation, that.creation) && Objects.equals(expires, that.expires) &&
          Objects.equals(status, that.status);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, site, name, creation, expires, status);
  }

  @Override
  public String toString() {
    try {
      return StandardProperties.getObjectMapper().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return "{" + "\"id\":\"" + id + "\"" + ", \"site\":\"" + site + "\"" + ", \"name\":\"" + name + "\"" +
          ", \"status\":\"" + status + "\"" + ", \"creation\":" + creation + ", \"expires\":" + expires + "}";
    }
  }

  public AccessorBucket cloneInstance() {
    final var accessorBucket = new AccessorBucket();
    final var finalStatus = getStatus() == null ? AccessorStatus.UNKNOWN : getStatus();
    accessorBucket.setId(getId()).setSite(getSite()).setName(getName()).setStatus(finalStatus)
        .setCreation(getCreation()).setExpires(getExpires());
    return accessorBucket;
  }
}
