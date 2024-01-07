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
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

@MappedSuperclass
public abstract class DaoRichExample {
  // No Id nor BsonId
  @Column(name = DaoRichExampleRepository.FIELD1, nullable = false, length = 256)
  private String field1;

  @Transient
  @JsonIgnore
  public abstract DaoRichExample addItem(final String item);

  @Transient
  @JsonIgnore
  public abstract DaoRichExample removeItem(final String item);

  @Transient
  @JsonIgnore
  public abstract String getMapValue(final String key);

  @Transient
  @JsonIgnore
  public abstract DaoRichExample addMapValue(final String key, final String value);

  @Transient
  @JsonIgnore
  public void fromDto(final DtoRichExample dto) {
    setGuid(dto.getGuid()).setField1(dto.getField1()).setSet1(dto.getSet1()).setMap1(dto.getMap1());
  }

  @Transient
  @JsonIgnore
  public DtoRichExample getDto() {
    final var transferRequest = new DtoRichExample();
    transferRequest.setGuid(getGuid()).setField1(getField1()).setSet1(getSet1()).setMap1(getMap1());
    return transferRequest;
  }

  public String getField1() {
    return field1;
  }

  public DaoRichExample setField1(final String field1) {
    ParametersChecker.checkSanityString(field1);
    this.field1 = field1;
    return this;
  }

  public abstract String getGuid();

  public abstract DaoRichExample setGuid(String guid);

  public abstract Set<String> getSet1();

  public abstract DaoRichExample setSet1(final Collection<String> items);

  public abstract Map<String, String> getMap1();

  public abstract DaoRichExample setMap1(final Map<String, String> map);
}
