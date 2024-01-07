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
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Dto for Java and possibly Rest API: Bucket minimal information
 */
@RegisterForReflection
public record StorageBucket(String bucket, Instant creationDate) {
  public StorageBucket(final String bucket, final Instant creationDate) {
    if (bucket != null) {
      ParametersChecker.checkSanityBucketName(bucket);
    }
    this.bucket = bucket;
    this.creationDate = SystemTools.toMillis(creationDate);
  }

  @Override
  public int hashCode() {
    if (bucket != null) {
      return bucket.hashCode();
    }
    // Fixed hash since value is null
    return -1;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof final StorageBucket storageBucket) {
      return Objects.equals(bucket, storageBucket.bucket);
    }
    return false;
  }

  @Override
  public String toString() {
    try {
      return StandardProperties.getObjectMapper().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return "{'bucket':'" + (bucket != null ? bucket : "") + "', 'creationDate':'" +
          (creationDate != null ? creationDate.toString() : "") + "'}";
    }
  }
}
