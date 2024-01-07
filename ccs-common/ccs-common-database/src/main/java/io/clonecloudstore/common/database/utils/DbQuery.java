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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.client.model.Filters;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.runtime.annotations.IgnoreProperty;
import jakarta.persistence.Transient;
import org.bson.conversions.Bson;

/**
 * DbQuery (Where Condition) for both SQL and NoSQL.
 * A DbQuery is not ready to use as is. It must be used through PostgreSqlHelper or
 * MongoSqlHelper or natively using Repository.
 */
public class DbQuery extends RestQuery {
  private static final DbQuery[] EMPTY_ARRAY_DB_QUERIES = new DbQuery[0];
  private static final String OR = " OR ";
  private static final String AND = " AND ";
  private static final String NOT = "NOT(";
  private static final char END_PARENTHESIS = ')';
  private static final String METADATA_FIELD = "metadata ->> '";
  private static final String COMMA = ", ";
  private static final char QUOTE_DOUBLE = '\"';
  private static final String PARAM = "#";
  private static final char BEGIN_PARENTHESIS = '(';
  private static final String METADATA_MG_FIELD = "metadata.";
  private static final boolean IS_DB_TYPE_MONGODB = DbType.getInstance().isMongoDbType();
  @IgnoreProperty
  @Transient
  @JsonIgnore
  private final StringBuilder builder = new StringBuilder();
  @IgnoreProperty
  @Transient
  @JsonIgnore
  private final StringBuilder builderMg = new StringBuilder();
  @IgnoreProperty
  @Transient
  @JsonIgnore
  private final List<Object> params = new ArrayList<>();
  @IgnoreProperty
  @Transient
  @JsonIgnore
  private Object bson;

  /**
   * Empty query
   */
  public DbQuery() {
    super();
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.empty();
    }
  }

  private DbQuery(final RestQuery restQuery) {
    super(restQuery);
    if (getOvalue() != null) {
      fieldObject(getQUERY(), getFieldName(), getOvalue());
    } else if (getSvalue() != null) {
      fieldString(getQUERY(), getFieldName(), getSvalue());
    } else if (getAvalues() != null) {
      fieldValues(getQUERY(), getFieldName(), getAvalues());
    } else if (getValueCollection() != null) {
      fieldCollection(getQUERY(), getFieldName(), getValueCollection());
    } else {
      throw new CcsInvalidArgumentRuntimeException("This constructor is not valid for empty parameter");
    }
  }

  private void setMgFromSql() {
    if (IS_DB_TYPE_MONGODB) {
      builderMg.setLength(0);
      builderMg.append(builder);
    }
  }

  private void fieldObject(final QUERY query, final String field, final Object value) {
    switch (query) {
      case EQ -> eq(field, value);
      case NEQ -> neq(field, value);
      case GTE -> gte(field, value);
      case LTE -> lte(field, value);
      default -> {
        // Nothing
      }
    }
    setMgFromSql();
  }

  private void fieldString(final QUERY query, final String field, final String value) {
    switch (query) {
      case EQ -> eq(field, value);
      case NEQ -> neq(field, value);
      case GTE -> gte(field, value);
      case LTE -> lte(field, value);
      case START_WITH -> startWith(field, value);
      case JSON_EQ -> jsonEq(field, value);
      case JSON_NEQ -> jsonNeq(field, value);
      default -> {
        // Nothing
      }
    }
    switch (query) {
      case EQ, NEQ, GTE, LTE, START_WITH -> setMgFromSql();
      default -> {
        // Nothing
      }
    }
  }

  private void fieldValues(final QUERY query, final String field, final Object[] values) {
    switch (query) {
      case IN -> in(field, values);
      case CONTAINS -> contains(field, values);
      case CONTAINED -> contained(field, values);
      case ANY -> any(field, values);
      default -> {
        // Nothing
      }
    }
    setMgFromSql();
  }

  private void fieldCollection(final QUERY query, final String field, final Collection<?> values) {
    switch (query) {
      case IN -> in(field, values);
      case CONTAINS -> contains(field, values);
      case CONTAINED -> contained(field, values);
      case ANY -> any(field, values);
      default -> {
        // Nothing
      }
    }
    setMgFromSql();
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void eq(final String field, final Object value) {
    builder.append(field).append(" = ").append(PARAM);
    params.add(value);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.eq(field, value);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void neq(final String field, final Object value) {
    builder.append(field).append(" <> ").append(PARAM);
    params.add(value);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.ne(field, value);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void gte(final String field, final Object value) {
    builder.append(field).append(" >= ").append(PARAM);
    params.add(value);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.gte(field, value);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void lte(final String field, final Object value) {
    builder.append(field).append(" <= ").append(PARAM);
    params.add(value);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.lte(field, value);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void startWith(final String field, final String prefix) {
    builder.append(field).append(" LIKE (").append(PARAM).append("||'%')");
    params.add(prefix);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.regex(field, '^' + prefix + ".*$");
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void jsonComp(final String field, final String value, final String comp) {
    builder.append(getPreMetadataField()).append(field).append(getPostMetadataField()).append(comp).append(PARAM);
    if (IS_DB_TYPE_MONGODB) {
      builderMg.append(getMgPreMetadataField()).append(field).append(getMgPostMetadataField()).append(comp)
          .append(PARAM);
    }
    params.add(value);
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void jsonEq(final String field, final String value) {
    jsonComp(field, value, " = ");
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.eq(getMgPreMetadataField() + field, value);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void jsonNeq(final String field, final String value) {
    jsonComp(field, value, " != ");
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.ne(getMgPreMetadataField() + field, value);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void addAllValues(final Object... values) {
    var next = false;
    for (final var value : values) {
      if (next) {
        builder.append(COMMA);
      }
      if (value instanceof String || value instanceof Instant) {
        builder.append(QUOTE_DOUBLE).append(value).append(QUOTE_DOUBLE);
      } else {
        builder.append(value);
      }
      next = true;
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void contains(final String field, final Object... values) {
    builder.append(field).append(" @> '{");
    addAllValues(values);
    builder.append("}'");
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.all(field, values);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void contained(final String field, final Object... values) {
    builder.append(field).append(" <@ '{");
    addAllValues(values);
    builder.append("}'");
    if (IS_DB_TYPE_MONGODB) {
      final List<Bson> filters = new ArrayList<>(values.length);
      for (final var value : values) {
        filters.add(Filters.eq(field, value));
      }
      bson = Filters.and(filters);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void addAllValues(final String toRepeat, final Object... values) {
    if (values.length > 1) {
      final var repeat = toRepeat.repeat(values.length - 1);
      builder.append(repeat);
    }
    params.addAll(List.of(values));
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void any(final String field, final Object... values) {
    builder.append(PARAM).append(" = ANY(").append(field).append(END_PARENTHESIS);
    addAllValues(AND + builder, values);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.all(field, values);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void in(final String field, final Object... values) {
    builder.append(field).append(" IN (").append(PARAM);
    addAllValues(COMMA + PARAM, values);
    builder.append(END_PARENTHESIS);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.in(field, values);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void addAllValues(final Collection<?> values) {
    var next = false;
    for (final var value : values) {
      if (next) {
        builder.append(COMMA);
      }
      if (value instanceof String || value instanceof Instant) {
        builder.append(QUOTE_DOUBLE).append(value).append(QUOTE_DOUBLE);
      } else {
        builder.append(value);
      }
      next = true;
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void contains(final String field, final Collection<?> values) {
    builder.append(field).append(" @> '{");
    addAllValues(values);
    builder.append("}'");
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.all(field, values);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void contained(final String field, final Collection<?> values) {
    builder.append(field).append(" <@ '{");
    addAllValues(values);
    builder.append("}'");
    if (IS_DB_TYPE_MONGODB) {
      final List<Bson> filters = new ArrayList<>(values.size());
      for (final var value : values) {
        filters.add(Filters.eq(field, value));
      }
      bson = Filters.and(filters);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void addAllValues(final String toRepeat, final Collection<?> values) {
    if (values.size() > 1) {
      final var repeat = toRepeat.repeat(values.size() - 1);
      builder.append(repeat);
    }
    params.addAll(values);
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void in(final String field, final Collection<?> values) {
    builder.append(field).append(" IN (").append(PARAM);
    addAllValues(COMMA + PARAM, values);
    builder.append(END_PARENTHESIS);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.in(field, values);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void any(final String field, final Collection<?> values) {
    builder.append(PARAM).append(" = ANY(").append(field).append(END_PARENTHESIS);
    addAllValues(AND + builder, values);
    if (IS_DB_TYPE_MONGODB) {
      bson = Filters.all(field, values);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  protected String getPreMetadataField() {
    return METADATA_FIELD;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  protected String getPostMetadataField() {
    return "'";
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  protected String getMgPreMetadataField() {
    return METADATA_MG_FIELD;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  protected String getMgPostMetadataField() {
    return "";
  }

  /**
   * Query using parameters
   */
  public DbQuery(final QUERY query, final String field, final Object... values) {
    super(query, field, values);
    fieldValues(query, field, values);
  }

  /**
   * Query using parameters
   */
  public DbQuery(final QUERY query, final String field, final Collection<?> values) {
    super(query, field, values);
    fieldCollection(query, field, values);
  }

  /**
   * Query using parameters
   */
  public DbQuery(final QUERY query, final String field, final String value) {
    super(query, field, value);
    fieldString(query, field, value);
  }

  /**
   * Query using parameters
   */
  public DbQuery(final QUERY query, final String field, final Object value) {
    super(query, field, value);
    if (value instanceof final Collection<?> values) {
      fieldCollection(query, field, values);
    } else if (value instanceof final Object[] values) {
      fieldValues(query, field, values);
    } else {
      fieldObject(query, field, value);
    }
  }

  /**
   * Query conjunction using parameters
   */
  public DbQuery(final CONJUNCTION conjunction, final DbQuery... queries) {
    super(conjunction, queries);
    conjunctionArray(conjunction, queries);
  }

  /**
   * Helper to request on ID
   *
   * @param id the ID unique value
   * @return the DbQuery
   */
  public static DbQuery idEquals(final String id) {
    return new DbQuery(QUERY.EQ, IS_DB_TYPE_MONGODB ? RepositoryBaseInterface.ID : RepositoryBaseInterface.ID_PG, id);
  }

  private void conjunctionArray(final CONJUNCTION conjunction, final DbQuery[] queries) {
    switch (conjunction) {
      case OR -> andOr(OR, queries);
      case AND -> andOr(AND, queries);
      case NOT -> {
        builder.append(NOT);
        if (IS_DB_TYPE_MONGODB) {
          builderMg.append(NOT);
        }
        andOr(AND, queries);
        builder.append(END_PARENTHESIS);
        if (IS_DB_TYPE_MONGODB) {
          builderMg.append(END_PARENTHESIS);
          bson = Filters.not((Bson) bson);
        }
      }
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private DbQuery andOr(final String op, final DbQuery... queries) {
    if (queries.length == 1) {
      final var single = queries[0];
      uniqueCond(single);
      return this;
    }
    var next = false;
    for (final var dbQuery : queries) {
      next = addNext(op, next, dbQuery);
    }
    if (IS_DB_TYPE_MONGODB) {
      next = false;
      final List<Bson> filters = new ArrayList<>(queries.length);
      for (final var dbQuery : queries) {
        next = addNextMg(op, next, filters, dbQuery);
      }
      bsonConjunction(op, filters);
    }
    return this;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void uniqueCond(final DbQuery single) {
    builder.append(single.builder);
    if (IS_DB_TYPE_MONGODB) {
      builderMg.append(single.builderMg);
      bson = single.bson;
    }
    params.addAll(single.params);
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void addNext(final String op, final boolean next, final StringBuilder stringBuilder,
                       final boolean isConjunction, final StringBuilder stringBuilderSubQuery) {
    if (next) {
      stringBuilder.append(op);
    }
    if (isConjunction) {
      stringBuilder.append(BEGIN_PARENTHESIS).append(stringBuilderSubQuery).append(END_PARENTHESIS);
    } else {
      stringBuilder.append(stringBuilderSubQuery);
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private boolean addNext(final String op, final boolean next, final DbQuery dbQuery) {
    addNext(op, next, builder, dbQuery.getConjunction() != null, dbQuery.builder);
    params.addAll(dbQuery.params);
    return true;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private boolean addNextMg(final String op, final boolean next, final List<Bson> filters, final DbQuery dbQuery) {
    addNext(op, next, builderMg, dbQuery.getConjunction() != null, dbQuery.builderMg);
    filters.add((Bson) dbQuery.bson);
    return true;
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private void bsonConjunction(final String op, final List<Bson> filters) {
    if (AND.equals(op)) {
      bson = Filters.and(filters);
    } else {
      bson = Filters.or(filters);
    }
  }

  /**
   * Query conjunction using parameters
   */
  public DbQuery(final CONJUNCTION conjunction, final Collection<DbQuery> queries) {
    super(conjunction, queries.toArray(EMPTY_ARRAY_DB_QUERIES));
    conjunctionList(conjunction, queries);
  }

  private void conjunctionList(final CONJUNCTION conjunction, final Collection<DbQuery> queries) {
    switch (conjunction) {
      case OR -> andOr(OR, queries);
      case AND -> andOr(AND, queries);
      case NOT -> {
        builder.append(NOT);
        if (IS_DB_TYPE_MONGODB) {
          builderMg.append(NOT);
        }
        andOr(AND, queries);
        builder.append(END_PARENTHESIS);
        if (IS_DB_TYPE_MONGODB) {
          builderMg.append(END_PARENTHESIS);
          bson = Filters.not((Bson) bson);
        }
      }
    }
  }

  @IgnoreProperty
  @Transient
  @JsonIgnore
  private DbQuery andOr(final String op, final Collection<DbQuery> queries) {
    final var length = queries.size();
    if (length == 1) {
      final var single = queries.iterator().next();
      uniqueCond(single);
      return this;
    }
    var next = false;
    for (final var dbQuery : queries) {
      next = addNext(op, next, dbQuery);
    }
    if (IS_DB_TYPE_MONGODB) {
      next = false;
      final List<Bson> filters = new ArrayList<>(length);
      for (final var dbQuery : queries) {
        next = addNextMg(op, next, filters, dbQuery);
      }
      bsonConjunction(op, filters);
    }
    return this;
  }

  /**
   * Build a DbQuery from RestQuery
   *
   * @param restQuery the RestQuery to transform
   * @return the new DbQuery
   */
  public static DbQuery fromRestQuery(final RestQuery restQuery) {
    if (restQuery.getQUERY() != null) {
      return new DbQuery(restQuery);
    } else if (restQuery.getConjunction() != null) {
      final List<DbQuery> dbQueries = new ArrayList<>();
      for (final var rq : restQuery.getRestQueries()) {
        dbQueries.add(new DbQuery(rq));
      }
      return new DbQuery(restQuery.getConjunction(), dbQueries);
    }
    return new DbQuery();
  }

  /**
   * Replace all parameters with this list.
   * Note that it works only for SQL query, not NoSQL one.
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public void replaceSqlParams(final List<Object> args) {
    if (RestQuery.isSingleArgQuery(getQUERY())) {
      throw new CcsInvalidArgumentRuntimeException("Query cannot use multiple arguments");
    }
    params.clear();
    params.addAll(args);
  }

  /**
   * Replace all parameters with one parameter.
   * Note that it works only for SQL query, not NoSQL one.
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public void replaceSqlParam(final Object args) {
    params.clear();
    params.add(args);
  }

  /**
   * @return the parameters as a List (for SQL)
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public List<Object> getSqlParams() {
    return params;
  }

  /**
   * @return the parameters as Array (for SQL)
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public Object[] getSqlParamsAsArray() {
    return params.toArray();
  }

  /**
   * @return the query as StringBuilder (for SQL)
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public StringBuilder getSqlQueryString() {
    return builder;
  }

  /**
   * @return the query as StringBuilder (for NoSQL)
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public StringBuilder getMgQueryString() {
    return builderMg;
  }

  /**
   * @return the query as Bson (for NoSQL)
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public Bson getBson() {
    if (bson == null) {
      return Filters.empty();
    }
    return (Bson) bson;
  }
}
