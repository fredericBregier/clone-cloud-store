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

package io.clonecloudstore.common.database.model.dao;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;

@MappedSuperclass
public abstract class DaoExample {
  // No Id nor BsonId
  @Column(name = DaoExampleRepository.FIELD1, nullable = false, length = 256)
  private String field1;
  @Column(name = DaoExampleRepository.FIELD2, nullable = false, length = 256)
  private String field2;
  @Column(name = DaoExampleRepository.TIME_FIELD, nullable = false)
  private Instant timeField;

  @Transient
  @JsonIgnore
  public void fromDto(final DtoExample dto) {
    setGuid(dto.getGuid()).setField1(dto.getField1()).setField2(dto.getField2()).setTimeField(dto.getTimeField());
  }

  @Transient
  @JsonIgnore
  public DtoExample getDto() {
    final var transferRequest = new DtoExample();
    transferRequest.setGuid(getGuid()).setField1(getField1()).setField2(getField2()).setTimeField(getTimeField());
    return transferRequest;
  }


  public String getField1() {
    return field1;
  }

  public DaoExample setField1(final String field1) {
    ParametersChecker.checkSanityString(field1);
    this.field1 = field1;
    return this;
  }

  public String getField2() {
    return field2;
  }

  public DaoExample setField2(final String field2) {
    ParametersChecker.checkSanityString(field2);
    this.field2 = field2;
    return this;
  }

  public Instant getTimeField() {
    return timeField;
  }

  public DaoExample setTimeField(final Instant timeField) {
    this.timeField = SystemTools.toMillis(timeField);
    return this;
  }

  public abstract String getGuid();

  public abstract DaoExample setGuid(String guid);
}
