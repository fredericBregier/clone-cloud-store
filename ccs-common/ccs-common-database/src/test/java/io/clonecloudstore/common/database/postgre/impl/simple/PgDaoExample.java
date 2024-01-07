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

package io.clonecloudstore.common.database.postgre.impl.simple;

import io.clonecloudstore.common.database.model.dao.DaoExample;
import io.clonecloudstore.common.database.model.dto.DtoExample;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD1;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TABLE_NAME;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TIME_FIELD;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID_PG;
import static io.clonecloudstore.common.standard.guid.GuidLike.UUID_B32_SIZE;

@Entity
@Table(name = TABLE_NAME, indexes = {
    @Index(name = TABLE_NAME + "_filter_idx", columnList = FIELD1 + ", " + TIME_FIELD)})
public class PgDaoExample extends DaoExample {
  @Id
  @Column(name = ID_PG, nullable = false, length = UUID_B32_SIZE)
  private String guid;

  public PgDaoExample() {
    // Empty
  }

  public PgDaoExample(final DtoExample dto) {
    fromDto(dto);
  }

  @Override
  public String getGuid() {
    return guid;
  }

  @Override
  public PgDaoExample setGuid(final String guid) {
    this.guid = guid;
    return this;
  }
}
