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

package io.clonecloudstore.common.database.postgre.model.rich;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.Transient;

/**
 * Used in API: Rich version
 */
@RegisterForReflection
public class DtoRichExample {
  private String guid;
  private final Set<String> set1 = new HashSet<>();
  private final Map<String, String> map1 = new HashMap<>();
  private String field1;

  public String getField1() {
    return field1;
  }

  public DtoRichExample setField1(final String field1) {
    ParametersChecker.checkSanityString(field1);
    this.field1 = field1;
    return this;
  }

  @Transient
  @JsonIgnore
  public DtoRichExample addItem(final String item) {
    set1.add(item);
    return this;
  }

  @Transient
  @JsonIgnore
  public DtoRichExample removeItem(final String item) {
    set1.remove(item);
    return this;
  }

  @Transient
  @JsonIgnore
  public String getMapValue(final String key) {
    return map1.get(key);
  }

  @Transient
  @JsonIgnore
  public DtoRichExample addMapValue(final String key, final String value) {
    map1.put(key, value);
    return this;
  }

  public String getGuid() {
    return guid;
  }

  public DtoRichExample setGuid(final String guid) {
    this.guid = guid;
    return this;
  }

  public Set<String> getSet1() {
    return set1;
  }

  public DtoRichExample setSet1(final Collection<String> items) {
    set1.clear();
    set1.addAll(items);
    return this;
  }

  public Map<String, String> getMap1() {
    return map1;
  }

  public DtoRichExample setMap1(final Map<String, String> map) {
    map1.clear();
    map1.putAll(map);
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof DtoRichExample dto) {
      return Objects.equals(this.field1, dto.field1) && Objects.equals(this.guid, dto.guid) &&
          Objects.equals(this.map1, dto.map1) && Objects.equals(this.set1, dto.set1);
    }
    return false;
  }
}
