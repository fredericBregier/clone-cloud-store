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

package io.clonecloudstore.administration.model;

import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record ClientBucketAccess(String client, String bucket, ClientOwnership ownership) {
  public ClientBucketAccess(final String client, final String bucket, final ClientOwnership ownership) {
    ParametersChecker.checkParameter("Client cannot be null", client);
    ParametersChecker.checkSanityString(client);
    this.client = client;
    ParametersChecker.checkParameter("Bucket cannot be null", bucket);
    ParametersChecker.checkSanityBucketName(bucket);
    this.bucket = bucket;
    this.ownership = Objects.requireNonNullElse(ownership, ClientOwnership.UNKNOWN);
  }

  public boolean include(final ClientOwnership ownership) {
    return ownership().include(ownership);
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      throw new CcsInvalidArgumentRuntimeException(e.getMessage());
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof final ClientBucketAccess that) {
      return Objects.equals(client, that.client) && Objects.equals(bucket, that.bucket) &&
          Objects.equals(ownership, that.ownership);
    }
    return false;
  }
}
