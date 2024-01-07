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

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;

public interface DaoRichExampleRepository extends RepositoryBaseInterface<DaoRichExample> {
  String TABLE_NAME = "dto_rich_example";
  String FIELD1 = "field1";
  String ARRAY1 = "array1";
  String MAP1 = "map1";

  DaoRichExample findUsingStream(final DbQuery dbQuery) throws CcsDbException;
}
