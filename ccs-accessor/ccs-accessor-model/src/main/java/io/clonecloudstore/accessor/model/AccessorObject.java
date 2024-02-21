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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.Transient;

/**
 * Accessor Object DTO
 */
@RegisterForReflection
public class AccessorObject {
  /**
   * Internal Id
   */
  private String id;
  /**
   * Site for this Object
   */
  private String site;
  /**
   * Bucket name
   */
  private String bucket;
  /**
   * Object name
   */
  private String name;
  /**
   * Optional: SHA 256 hash
   */
  private String hash;
  /**
   * Status of this Object
   */
  private AccessorStatus status = AccessorStatus.UNKNOWN;
  /**
   * Creation or Modification datetime
   */
  private Instant creation;
  /**
   * Optional expiry datetime
   */
  private Instant expires;
  /**
   * Length of the content of this Object
   */
  private long size;
  /**
   * Metadata if any for this Object
   */
  private final Map<String, String> metadata = new HashMap<>();

  public AccessorObject() {
    // Empty
  }

  public String getId() {
    return id;
  }

  public AccessorObject setId(final String id) {
    this.id = id;
    return this;
  }

  public String getSite() {
    return site;
  }

  public AccessorObject setSite(final String site) {
    ParametersChecker.checkSanityString(site);
    this.site = site;
    return this;
  }

  public String getBucket() {
    return bucket;
  }

  public AccessorObject setBucket(final String bucket) {
    ParametersChecker.checkSanityBucketName(bucket);
    this.bucket = bucket;
    return this;
  }

  public String getName() {
    return name;
  }

  public AccessorObject setName(final String name) {
    final var decoded = ParametersChecker.getSanitizedName(name);
    ParametersChecker.checkSanityObjectName(decoded);
    this.name = decoded;
    return this;
  }

  public String getHash() {
    return hash;
  }

  public AccessorObject setHash(final String hash) {
    ParametersChecker.checkSanityString(hash);
    this.hash = hash;
    return this;
  }

  public AccessorStatus getStatus() {
    return status;
  }

  public AccessorObject setStatus(final AccessorStatus status) {
    ParametersChecker.checkParameterNullOnly("Status cannot be null", status);
    this.status = status;
    return this;
  }

  public Instant getCreation() {
    return creation;
  }

  public AccessorObject setCreation(final Instant creation) {
    this.creation = SystemTools.toMillis(creation);
    return this;
  }

  public Instant getExpires() {
    return expires;
  }

  public AccessorObject setExpires(final Instant expires) {
    this.expires = SystemTools.toMillis(expires);
    return this;
  }

  public long getSize() {
    return size;
  }

  public AccessorObject setSize(final long size) {
    this.size = size;
    return this;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Transient
  @JsonIgnore
  public AccessorObject addMetadata(final String key, final String value) {
    ParametersChecker.checkSanityString(key, value);
    ParametersChecker.checkSanityMapKey(key);
    metadata.put(key, value);
    return this;
  }

  @Transient
  @JsonIgnore
  public String getMetadata(final String key) {
    ParametersChecker.checkSanityString(key);
    return metadata.get(key);
  }

  public AccessorObject setMetadata(final Map<String, String> metadata) {
    ParametersChecker.checkSanityMap(metadata);
    this.metadata.putAll(metadata);
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof AccessorObject that) {
      return size == that.size && Objects.equals(id, that.id) && Objects.equals(site, that.site) &&
          Objects.equals(bucket, that.bucket) && Objects.equals(name, that.name) && Objects.equals(hash, that.hash) &&
          Objects.equals(status, that.status) && Objects.equals(creation, that.creation) &&
          Objects.equals(expires, that.expires) && Objects.deepEquals(metadata, that.metadata);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, site, bucket, name, hash, status, creation, expires, size, metadata);
  }

  @Override
  public String toString() {
    try {
      return StandardProperties.getObjectMapper().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      throw new CcsInvalidArgumentRuntimeException(e.getMessage());
    }
  }

  public AccessorObject cloneInstance() {
    final var accessorObject = new AccessorObject();
    final var finalStatus = getStatus() == null ? AccessorStatus.UNKNOWN : getStatus();
    accessorObject.setId(getId()).setSite(getSite()).setBucket(getBucket()).setName(getName()).setHash(getHash())
        .setStatus(finalStatus).setCreation(getCreation()).setExpires(getExpires()).setSize(getSize())
        .setMetadata(getMetadata());
    return accessorObject;
  }
}
