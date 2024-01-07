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

package io.clonecloudstore.common.database.postgre.impl.rich;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.clonecloudstore.common.database.postgre.PostgreStringArrayType;
import io.clonecloudstore.common.database.postgre.model.rich.DaoRichExample;
import io.clonecloudstore.common.database.postgre.model.rich.DtoRichExample;
import io.quarkus.runtime.annotations.IgnoreProperty;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Type;
import org.hibernate.type.SqlTypes;

import static io.clonecloudstore.common.database.postgre.PostgreSqlHelper.JSON_TYPE;
import static io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository.ARRAY1;
import static io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository.FIELD1;
import static io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository.ID_PG;
import static io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository.MAP1;
import static io.clonecloudstore.common.database.postgre.model.rich.DaoRichExampleRepository.TABLE_NAME;
import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;

@Entity
@Table(name = TABLE_NAME, indexes = {@Index(name = TABLE_NAME + "_filter_idx", columnList = FIELD1)})
public class PgDaoRichExample extends DaoRichExample {
  private static final String[] EMPTY = new String[0];
  /**
   * To get a Set internally instead of an array
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  private final Set<String> set1 = new HashSet<>();
  @Column(name = MAP1, columnDefinition = JSON_TYPE)
  @JdbcTypeCode(SqlTypes.JSON)
  private final Map<String, String> map1 = new HashMap<>();
  @Id
  @Column(name = ID_PG, nullable = false, length = UUID_B32_SIZE)
  private String guid;
  @Column(columnDefinition = "text[]", name = ARRAY1)
  @Type(value = PostgreStringArrayType.class)
  @JdbcTypeCode(SqlTypes.ARRAY)
  private String[] array1;
  /**
   * To ensure array ans set are correctly initialized
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  private boolean checked;


  public PgDaoRichExample() {
    // Empty
  }

  public PgDaoRichExample(final DtoRichExample dto) {
    fromDto(dto);
  }

  String[] getArray1() {
    check();
    return array1;
  }

  PgDaoRichExample setArray1(final String[] array1) {
    this.array1 = array1;
    setStringToSet();
    return this;
  }

  /**
   * To check if the array and set are correctly setup
   *
   * @return True if ok
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public PgDaoRichExample check() {
    if (checked) {
      return this;
    }
    if (array1 != null) {
      if (array1.length < set1.size()) {
        setSetToString();
      } else if (array1.length > set1.size()) {
        setStringToSet();
      }
    } else {
      setSetToString();
    }
    checked = true;
    return this;
  }

  private void setSetToString() {
    array1 = set1.toArray(EMPTY);
    checked = true;
  }

  private void setStringToSet() {
    set1.clear();
    Collections.addAll(set1, array1);
    checked = true;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  public PgDaoRichExample addItems(final Set<String> set1) {
    check();
    this.set1.addAll(set1);
    setSetToString();
    return this;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  @Override
  public PgDaoRichExample addItem(final String item) {
    if (this.set1 == null) {
      return this;
    }
    check();
    set1.add(item);
    setSetToString();
    return this;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  @Override
  public PgDaoRichExample removeItem(final String item) {
    check();
    set1.remove(item);
    setSetToString();
    return this;
  }

  @Override
  @IgnoreProperty
  @JsonIgnore
  @Transient
  public String getMapValue(final String key) {
    return map1.get(key);
  }

  @Override
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public PgDaoRichExample addMapValue(final String key, final String value) {
    map1.put(key, value);
    return this;
  }

  @Override
  public String getGuid() {
    return guid;
  }

  @Override
  public PgDaoRichExample setGuid(final String guid) {
    this.guid = guid;
    return this;
  }

  @Override
  @IgnoreProperty
  @Transient
  public Set<String> getSet1() {
    check();
    return set1;
  }

  @Override
  public PgDaoRichExample setSet1(final Collection<String> items) {
    if (this.set1 == null) {
      return this;
    }
    set1.clear();
    set1.addAll(items);
    setSetToString();
    return this;
  }

  @Override
  public Map<String, String> getMap1() {
    return map1;
  }

  @Override
  public PgDaoRichExample setMap1(final Map<String, String> map) {
    if (this.map1 == null) {
      return this;
    }
    this.map1.clear();
    this.map1.putAll(map);
    return this;
  }
}
