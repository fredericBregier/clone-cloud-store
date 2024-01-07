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

package io.clonecloudstore.common.database.mongo;

import io.clonecloudstore.common.database.utils.DbQuery;

/**
 * Mongo Sql Helper
 */
public final class MongoSqlHelper {
  private MongoSqlHelper() {
    // Empty
  }

  /**
   * Usable in Panache find, delete... as condition with getSqlParamsAsArray()
   *
   * @param query the DbQuery
   * @return the native Query (Where part only) ready to use with extra parameters as "?"
   */
  public static String query(final DbQuery query) {
    return simpleQuery(query);
  }

  private static String simpleQuery(final DbQuery query) {
    final var builder = new StringBuilder(query.getMgQueryString());
    return replaceHashQueryParameter(builder, 1);
  }

  private static String replaceHashQueryParameter(final StringBuilder builder, final int rankStart) {
    var rank = rankStart;
    var pos = 0;
    while ((pos = builder.indexOf("#", pos)) >= 0) {
      builder.replace(pos, pos + 1, "?" + rank);
      rank++;
    }
    return builder.toString();
  }
}
