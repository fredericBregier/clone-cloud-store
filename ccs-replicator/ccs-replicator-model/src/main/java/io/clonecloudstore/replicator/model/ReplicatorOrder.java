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

package io.clonecloudstore.replicator.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.Transient;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;

@RegisterForReflection
public record ReplicatorOrder(String opId, String fromSite, String toSite, String clientId, String bucketName,
                              String objectName, long size, String hash, ReplicatorConstants.Action action) {
  public ReplicatorOrder(final String opId, final String fromSite, final String toSite, final String clientId,
                         final String bucketName, final ReplicatorConstants.Action action) {
    this(opId, fromSite, toSite, clientId, bucketName, null, 0, null, action);
  }

  public ReplicatorOrder(final ReplicatorOrder from, final String toSite) {
    this(from.opId, from.fromSite, toSite, from.clientId, from.bucketName, from.objectName, from.size, from.hash,
        from.action);
  }

  public ReplicatorOrder(final ReplicatorOrder from, final ReplicatorConstants.Action action) {
    this(from.opId, from.fromSite, from.toSite, from.clientId, from.bucketName, from.objectName, from.size, from.hash,
        action);
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return "{" + "\"opId\":\"" + opId + "\", \"toSite\":\"" + toSite + "\", \"clientId\":\"" + clientId +
          "\", \"action\":\"" + action + "\", \"fromSite\":\"" + fromSite + "\", \"bucketName\":\"" + bucketName +
          "\", \"hash\":\"" + hash + "\", \"size\":" + size + ", \"objectName\":\"" + objectName + "\"}";
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof final ReplicatorOrder order) {
      return order == this || (Objects.equals(order.action, action) && Objects.equals(order.objectName, objectName) &&
          Objects.equals(order.bucketName, bucketName) && Objects.equals(order.clientId, clientId) &&
          Objects.equals(order.fromSite, fromSite) && Objects.equals(order.toSite, toSite) &&
          Objects.equals(order.hash, hash) && Objects.equals(order.size, size));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromSite, toSite, clientId, bucketName, objectName, size, hash, action);
  }

  @Transient
  @JsonIgnore
  public Map<String, String> getHeaders() {
    final Map<String, String> map = new HashMap<>();
    if (clientId() != null) {
      map.put(AccessorConstants.Api.X_CLIENT_ID, clientId());
    }
    if (ParametersChecker.isNotEmpty(toSite())) {
      map.put(ReplicatorConstants.Api.X_TARGET_ID, toSite());
    }
    if (ParametersChecker.isNotEmpty(opId())) {
      map.put(X_OP_ID, opId());
    }
    return map;
  }
}

