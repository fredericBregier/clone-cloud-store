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

package io.clonecloudstore.driver.api.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Dto for Java and possibly Rest API: Object minimal information
 */
@RegisterForReflection
public record StorageObject(String bucket, String name, String hash, long size, Instant creationDate,
                            Instant expiresDate, Map<String, String> metadata) {
  public StorageObject(final String bucket, final String name, final String hash, final long size,
                       final Instant creationDate, final Instant expiresDate, final Map<String, String> metadata) {
    if (bucket != null) {
      ParametersChecker.checkSanityBucketName(bucket);
    }
    this.bucket = bucket;
    if (name != null) {
      ParametersChecker.checkSanityObjectName(name);
    }
    this.name = name;
    this.hash = hash;
    this.size = size;
    this.creationDate = SystemTools.toMillis(creationDate);
    this.expiresDate = SystemTools.toMillis(expiresDate);
    if (metadata != null) {
      this.metadata = new HashMap<>(metadata);
    } else {
      this.metadata = new HashMap<>();
    }
  }

  public StorageObject(final String bucket, final String name, final String hash, final long size,
                       final Instant creationDate) {
    this(bucket, name, hash, size, creationDate, null, null);
  }

  @Override
  public int hashCode() {
    if (name != null && bucket != null) {
      return name.hashCode() + bucket.hashCode();
    }
    // Fixed hash since both values are null
    return -1;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof final StorageObject object) {
      return Objects.equals(name, object.name) && Objects.equals(bucket, object.bucket);
    }
    return false;
  }

  @Override
  public String toString() {
    try {
      return StandardProperties.getObjectMapper().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return "{'bucket':'" + (bucket != null ? bucket : "") + "', 'name':'" + (name != null ? name : "") + "', " +
          "'creationDate':'" + (creationDate != null ? creationDate.toString() : "") + "'}";
    }
  }
}
