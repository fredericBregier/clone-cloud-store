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

package io.clonecloudstore.reconciliator.model;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record SingleSiteObject(String site, short nstatus, Instant event) {
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof SingleSiteObject sso) {
      return Objects.equals(site, sso.site) && Objects.equals(nstatus, sso.nstatus) && Objects.equals(event, sso.event);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(site, nstatus, event);
  }

  @Override
  public String toString() {
    try {
      return JsonUtil.getInstance().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return "{" + "\"site\":\"" + site + "\"" + ", \"nstatus\":" + nstatus + ", \"event\":\"" + event + "\"" + "}";
    }
  }
}
