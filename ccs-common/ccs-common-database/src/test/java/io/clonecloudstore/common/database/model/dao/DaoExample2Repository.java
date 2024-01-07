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

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;

public interface DaoExample2Repository extends RepositoryBaseInterface<DaoExample2> {
  String TABLE_NAME = "tbl_ex2";
  String FIELD1 = "field1";
  String FIELD2 = "field2";
  String TIME_FIELD = "timeField";

  DaoExample2 findUsingStream(final DbQuery dbQuery) throws CcsDbException;
}
