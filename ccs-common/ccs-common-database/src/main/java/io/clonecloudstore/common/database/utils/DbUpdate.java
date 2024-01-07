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

package io.clonecloudstore.common.database.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.mongodb.client.model.Updates;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import org.bson.conversions.Bson;

/**
 * DbUpdate (Update command) for both SQL and NoSQL.
 * A DbUpdate is not ready to use as is. It must be used through PostgreSqlHelper or
 * MongoSqlHelper or natively using Repository.
 */
public class DbUpdate {
  private static final String COMMA = ", ";
  private static final String EQUAL = " = ";
  private static final boolean IS_DB_TYPE_MONGODB = DbType.getInstance().isMongoDbType();
  private static final Object[] BSONS = IS_DB_TYPE_MONGODB ? new Bson[0] : null;
  private final StringBuilder builder = new StringBuilder();
  private final List<Object> params = new ArrayList<>();
  private final List<Object> bsonList = new ArrayList<>();

  /**
   * Add one value to a field Set
   *
   * @return this
   */
  public DbUpdate set(final String field, final Object value) {
    ParametersChecker.checkSanityString(field);
    if (!builder.isEmpty()) {
      builder.append(COMMA);
    }
    builder.append(field).append(" = # ");
    params.add(value);
    if (IS_DB_TYPE_MONGODB) {
      bsonList.add(Updates.set(field, value));
    }
    return this;
  }

  /**
   * Add one value to a set field (or array for Postgre)
   *
   * @return this
   */
  public DbUpdate addToSet(final String field, final String value) {
    ParametersChecker.checkSanityString(field, value);
    if (!builder.isEmpty()) {
      builder.append(COMMA);
    }
    builder.append(field).append(EQUAL).append(field).append(" || '{\"").append(value).append("\"}' ");
    if (IS_DB_TYPE_MONGODB) {
      bsonList.add(Updates.addToSet(field, value));
    }
    return this;
  }

  /**
   * Set a collection of values plus one to a field array
   *
   * @return this
   */
  public DbUpdate setArray(final String field, final String value, final Collection<String> oldValues) {
    ParametersChecker.checkSanityString(field, value);
    ParametersChecker.checkSanityString(oldValues.toArray(new String[0]));
    if (!builder.isEmpty()) {
      builder.append(COMMA);
    }
    builder.append(field).append(EQUAL).append("'{\"").append(value).append('"');
    for (final var s : oldValues) {
      builder.append(", \"").append(s).append('\"');
    }
    builder.append("}' ");
    if (IS_DB_TYPE_MONGODB) {
      final List<String> strings = new ArrayList<>(oldValues.size() + 1);
      strings.addAll(oldValues);
      strings.add(value);
      bsonList.add(Updates.set(field, strings));
    }
    return this;
  }

  /**
   * @return the Parameters (SQL) of this Update
   */
  public List<Object> getSqlParams() {
    return params;
  }

  /**
   * @return the StringBuilder of this Sql Update command
   */
  public StringBuilder getSqlQuery() {
    return builder;
  }

  /**
   * @return the Bson form of this Update for NoSQL
   */
  public Bson getBson() {
    return Updates.combine((Bson[]) bsonList.toArray(BSONS));
  }
}
