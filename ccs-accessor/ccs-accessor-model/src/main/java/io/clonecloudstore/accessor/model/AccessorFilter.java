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
import java.util.Arrays;
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
 * Accessor Filter DTO
 */
@RegisterForReflection
public class AccessorFilter {
  /**
   * Optional Prefix for the name, including path
   */
  private String namePrefix;
  /**
   * Optional list of status to filter on
   */
  private AccessorStatus[] statuses;
  /**
   * Optional datetime for creation before this date
   */
  private Instant creationBefore;
  /**
   * Optional datetime for creation after this date
   */
  private Instant creationAfter;
  /**
   * Optional datetime for expiry before this date
   */
  private Instant expiresBefore;
  /**
   * Optional datetime for expiry after this date
   */
  private Instant expiresAfter;
  /**
   * Optional length filter less than this length
   */
  private long sizeLessThan;
  /**
   * Optional length filter greater than this length
   */
  private long sizeGreaterThan;
  /**
   * Optional metadata filter based on equality
   */
  private final Map<String, String> metadataFilter = new HashMap<>();

  public AccessorFilter() {
    // Empty
  }

  public String getNamePrefix() {
    return namePrefix;
  }

  public AccessorFilter setNamePrefix(final String namePrefix) {
    ParametersChecker.checkSanityString(namePrefix);
    this.namePrefix = namePrefix;
    return this;
  }

  public AccessorStatus[] getStatuses() {
    return statuses;
  }

  public AccessorFilter setStatuses(final AccessorStatus[] statuses) {
    this.statuses = statuses;
    return this;
  }

  public Instant getCreationBefore() {
    return creationBefore;
  }

  public AccessorFilter setCreationBefore(final Instant creationBefore) {
    this.creationBefore = SystemTools.toMillis(creationBefore);
    return this;
  }

  public Instant getCreationAfter() {
    return creationAfter;
  }

  public AccessorFilter setCreationAfter(final Instant creationAfter) {
    this.creationAfter = SystemTools.toMillis(creationAfter);
    return this;
  }

  public Instant getExpiresBefore() {
    return expiresBefore;
  }

  public AccessorFilter setExpiresBefore(final Instant expiresBefore) {
    this.expiresBefore = SystemTools.toMillis(expiresBefore);
    return this;
  }

  public Instant getExpiresAfter() {
    return expiresAfter;
  }

  public AccessorFilter setExpiresAfter(final Instant expiresAfter) {
    this.expiresAfter = SystemTools.toMillis(expiresAfter);
    return this;
  }

  public long getSizeLessThan() {
    return sizeLessThan;
  }

  public AccessorFilter setSizeLessThan(final long sizeLessThan) {
    this.sizeLessThan = sizeLessThan;
    return this;
  }

  public long getSizeGreaterThan() {
    return sizeGreaterThan;
  }

  public AccessorFilter setSizeGreaterThan(final long sizeGreaterThan) {
    this.sizeGreaterThan = sizeGreaterThan;
    return this;
  }

  public Map<String, String> getMetadataFilter() {
    return metadataFilter;
  }

  public AccessorFilter setMetadataFilter(final Map<String, String> metadataFilter) {
    ParametersChecker.checkSanityMap(metadataFilter);
    this.metadataFilter.putAll(metadataFilter);
    return this;
  }

  @Transient
  @JsonIgnore
  public AccessorFilter addMetadata(final String key, final String value) {
    ParametersChecker.checkSanityString(key, value);
    ParametersChecker.checkSanityMapKey(key);
    metadataFilter.put(key, value);
    return this;
  }

  @Override
  public String toString() {
    try {
      return StandardProperties.getObjectMapper().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      throw new CcsInvalidArgumentRuntimeException(e.getMessage());
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof AccessorFilter that) {
      return sizeGreaterThan == that.sizeGreaterThan && sizeLessThan == that.sizeLessThan &&
          Objects.equals(namePrefix, that.namePrefix) && Objects.deepEquals(statuses, that.statuses) &&
          Objects.equals(creationAfter, that.creationAfter) && Objects.equals(creationBefore, that.creationBefore) &&
          Objects.equals(expiresAfter, that.expiresAfter) && Objects.equals(expiresBefore, that.expiresBefore) &&
          Objects.deepEquals(metadataFilter, that.metadataFilter);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sizeGreaterThan, sizeLessThan, namePrefix, Arrays.hashCode(statuses), creationAfter,
        creationBefore, expiresAfter, expiresBefore, metadataFilter);
  }
}
