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

package io.clonecloudstore.common.database.postgre;

import java.util.ArrayList;
import java.util.List;

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;

/**
 * Postgre Sql Helper
 */
public final class PostgreSqlHelper {
  private static final String WHERE = " WHERE ";
  public static final String JSON_TYPE = "jsonb";

  private PostgreSqlHelper() {
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
    final var builder = new StringBuilder(query.getSqlQueryString());
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

  /**
   * For Native Query only with additional parameters using getSqlParamsAsArray()
   *
   * @param table the table name
   * @param query the DbQuery
   * @return the SELECT native Query  with extra parameters as "?"
   */
  public static String select(final String table, final DbQuery query) {
    if (query.isEmpty()) {
      return "SELECT * FROM " + table;
    }
    final var builder = new StringBuilder("SELECT * FROM ").append(table).append(WHERE).append(simpleQuery(query));
    return builder.toString();
  }

  /**
   * For Native Query only with additional parameters using getSqlParamsAsArray()
   *
   * @param table the table name
   * @param query the DbQuery
   * @return the COUNT native Query with extra parameters as "?"
   */
  public static String count(final String table, final DbQuery query) {
    if (query.isEmpty()) {
      return "SELECT COUNT(*) AS count FROM " + table;
    }
    final var builder =
        new StringBuilder("SELECT COUNT(*) AS count FROM ").append(table).append(WHERE).append(simpleQuery(query));
    return builder.toString();
  }

  /**
   * For Native Query only with additional parameters using getSqlParamsAsArray()
   *
   * @param table the table name
   * @param query the DbQuery
   * @return the DELETE native Query with extra parameters as "?"
   */
  public static String delete(final String table, final DbQuery query) {
    if (query.isEmpty()) {
      return "DELETE FROM " + table;
    }
    final var builder = new StringBuilder("DELETE FROM ").append(table).append(WHERE).append(simpleQuery(query));
    return builder.toString();
  }

  /**
   * Usable in Panache update with getUpdateParamsAsArray()
   *
   * @param update the DbUpdate
   * @param query  the DbQuery
   * @return the native Query (Update and Where parts only) with extra parameters as "?"
   */
  public static String update(final DbUpdate update, final DbQuery query) {
    final var builder = new StringBuilder(" SET ").append(update.getSqlQuery());
    if (!query.isEmpty()) {
      builder.append(WHERE).append(query.getSqlQueryString());
    }
    return replaceHashQueryParameter(builder, 1);
  }

  /**
   * @param update the DbUpdate
   * @param query  the DbQuery
   * @return the extra parameters to replace "?"
   */
  public static Object[] getUpdateParamsAsArray(final DbUpdate update, final DbQuery query) {
    final List<Object> list = new ArrayList<>(update.getSqlParams());
    list.addAll(query.getSqlParams());
    return list.toArray();
  }
}
