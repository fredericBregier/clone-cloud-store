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

package io.clonecloudstore.accessor.server.database.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.standard.system.ParametersChecker;

import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.CREATION;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.EXPIRES;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.NAME;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.SIZE;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.STATUS;

public class DbQueryAccessorHelper {

  public static DbQuery getDbQuery(final AccessorFilter filter) {
    if (filter != null) {
      final List<DbQuery> queryList = new ArrayList<>();
      if (ParametersChecker.isNotEmpty(filter.getNamePrefix())) {
        queryList.add(new DbQuery(RestQuery.QUERY.START_WITH, NAME, filter.getNamePrefix()));
      }
      if (filter.getSizeLessThan() > 0) {
        queryList.add(new DbQuery(RestQuery.QUERY.LTE, SIZE, filter.getSizeLessThan()));
      }
      if (filter.getSizeGreaterThan() > 0) {
        queryList.add(new DbQuery(RestQuery.QUERY.GTE, SIZE, filter.getSizeGreaterThan()));
      }
      if (ParametersChecker.isNotEmpty(filter.getCreationBefore())) {
        queryList.add(new DbQuery(RestQuery.QUERY.LTE, CREATION, filter.getCreationBefore()));
      }
      if (ParametersChecker.isNotEmpty(filter.getCreationAfter())) {
        queryList.add(new DbQuery(RestQuery.QUERY.GTE, CREATION, filter.getCreationAfter()));
      }
      if (ParametersChecker.isNotEmpty(filter.getExpiresBefore())) {
        queryList.add(new DbQuery(RestQuery.QUERY.LTE, EXPIRES, filter.getExpiresBefore()));
      }
      if (ParametersChecker.isNotEmpty(filter.getExpiresAfter())) {
        queryList.add(new DbQuery(RestQuery.QUERY.GTE, EXPIRES, filter.getExpiresAfter()));
      }
      if (filter.getStatuses() != null && filter.getStatuses().length > 0) {
        final var statusFilterArg = Arrays.stream(filter.getStatuses()).map(Enum::name).toList();
        queryList.add(new DbQuery(RestQuery.QUERY.IN, STATUS, statusFilterArg));
      }
      if (filter.getMetadataFilter() != null && !filter.getMetadataFilter().isEmpty()) {
        for (final var entry : filter.getMetadataFilter().entrySet()) {
          queryList.add(new DbQuery(RestQuery.QUERY.JSON_EQ, entry.getKey(), entry.getValue()));
        }
      }
      if (queryList.isEmpty()) {
        return null;
      }
      return new DbQuery(RestQuery.CONJUNCTION.AND, queryList);
    }
    return null;
  }

  private DbQueryAccessorHelper() {
    // Empty
  }
}
