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

package io.clonecloudstore.common.database.model.dto;

import java.time.Instant;
import java.util.Objects;

import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Used in API
 */
@RegisterForReflection
public class DtoExample {
  private String guid;
  private String field1;
  private String field2;
  private Instant timeField;

  public String getField1() {
    return field1;
  }

  public DtoExample setField1(final String field1) {
    ParametersChecker.checkSanityString(field1);
    this.field1 = field1;
    return this;
  }

  public String getField2() {
    return field2;
  }

  public DtoExample setField2(final String field2) {
    ParametersChecker.checkSanityString(field2);
    this.field2 = field2;
    return this;
  }

  public Instant getTimeField() {
    return timeField;
  }

  public DtoExample setTimeField(final Instant timeField) {
    this.timeField = SystemTools.toMillis(timeField);
    return this;
  }

  public String getGuid() {
    return guid;
  }

  public DtoExample setGuid(final String guid) {
    this.guid = guid;
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof DtoExample dto) {
      return Objects.equals(this.field1, dto.field1) && Objects.equals(this.field2, dto.field2) &&
          Objects.equals(this.guid, dto.guid) && Objects.equals(this.timeField, dto.timeField);
    }
    return false;
  }
}
