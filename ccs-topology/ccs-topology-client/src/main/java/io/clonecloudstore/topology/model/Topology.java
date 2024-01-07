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

package io.clonecloudstore.topology.model;

import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record Topology(String id, String name, String uri, TopologyStatus status) {
  public Topology(final String id, final String name, final String uri, final TopologyStatus status) {
    ParametersChecker.checkParameter("Id cannot be null", id);
    ParametersChecker.checkSanityString(id);
    this.id = id;
    ParametersChecker.checkParameter("Name cannot be null", name);
    ParametersChecker.checkSanityString(name);
    this.name = name;
    ParametersChecker.checkParameter("Uri cannot be null", uri);
    ParametersChecker.checkSanityUri(uri);
    this.uri = uri;
    this.status = Objects.requireNonNullElse(status, TopologyStatus.UNKNOWN);
  }

  public Topology(final Topology topology, final TopologyStatus status) {
    this(topology.id, topology.name, topology.uri, status);
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (final JsonProcessingException e) {
      return "{" + "\"uri\":\"" + uri + "\"" + ", \"name\":\"" + name + "\"" + ", \"id\":\"" + id + "\"" +
          ", \"status\":" + status + "}";
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof final Topology that) {
      return Objects.equals(id, that.id) && Objects.equals(uri, that.uri) && Objects.equals(name, that.name) &&
          Objects.equals(status, that.status);
    }
    return false;
  }
}
