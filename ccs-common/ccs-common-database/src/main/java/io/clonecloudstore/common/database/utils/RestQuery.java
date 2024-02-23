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

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.runtime.annotations.IgnoreProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.persistence.Transient;

/**
 * RestQuery enabling Query to be serialized as Json
 */
@RegisterForReflection
public class RestQuery {
  private static final RestQuery[] EMPTY_ARRAY_REST_QUERIES = new RestQuery[0];
  private static final String ARGUMENT_SHALL_NOT_BE_EMPTY = "Argument shall not be empty";
  private static final String OPERATOR_NOT_ALLOWED_WITH_MULTIPLE_VALUES = "Operator not allowed with multiple values: ";
  private static final String OPERATOR_NOT_ALLOWED_WITH_STRING_VALUE = "Operator not allowed with String value: ";
  private static final String OPERATOR_NOT_ALLOWED_WITH_OBJECT_VALUE = "Operator not allowed with Object value: ";
  @JsonInclude(Include.NON_EMPTY)
  private final QUERY query;
  @JsonInclude(Include.NON_EMPTY)
  private final CONJUNCTION conjunction;
  @JsonInclude(Include.NON_EMPTY)
  private final String fieldName;
  @JsonInclude(Include.NON_EMPTY)
  private final String svalue;
  @JsonInclude(Include.NON_EMPTY)
  private final Object ovalue;
  @JsonInclude(Include.NON_EMPTY)
  private final Object[] avalues;
  @JsonInclude(Include.NON_EMPTY)
  private final Collection<Object> valueCollection;
  @JsonInclude(Include.NON_EMPTY)
  private final RestQuery[] restQueries;

  /**
   * Empty constructor
   */
  public RestQuery() {
    query = null;
    conjunction = null;
    fieldName = null;
    avalues = null;
    svalue = null;
    ovalue = null;
    valueCollection = null;
    restQueries = null;
    // Empty request
  }

  protected RestQuery(final RestQuery restQuery) {
    query = restQuery.query;
    conjunction = restQuery.conjunction;
    fieldName = restQuery.fieldName;
    avalues = restQuery.avalues;
    svalue = restQuery.svalue;
    ovalue = restQuery.ovalue;
    valueCollection = restQuery.valueCollection;
    restQueries = restQuery.restQueries;
  }

  /**
   * Constructor from Parameters
   */
  public RestQuery(final QUERY query, final String field, final Object... values) {
    this.query = query;
    conjunction = null;
    fieldName = field;
    avalues = values;
    svalue = null;
    ovalue = null;
    valueCollection = null;
    restQueries = null;
    if (ParametersChecker.isEmpty(query, field)) {
      throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
    }
    if (isSingleArgQuery(query)) {
      throw new CcsInvalidArgumentRuntimeException(OPERATOR_NOT_ALLOWED_WITH_MULTIPLE_VALUES + query.name());
    }
    ParametersChecker.checkSanityString(field);
    ParametersChecker.checkSanity(values);
  }

  static boolean isSingleArgQuery(final QUERY query) {
    return query.ordinal() < QUERY.CONTAINS.ordinal();
  }

  /**
   * Constructor from Parameters
   */
  public RestQuery(final QUERY query, final String field, final Collection<Object> values) {
    this.query = query;
    conjunction = null;
    fieldName = field;
    avalues = null;
    svalue = null;
    ovalue = null;
    valueCollection = values;
    restQueries = null;
    if (ParametersChecker.isEmpty(query, field, values)) {
      throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
    }
    if (isSingleArgQuery(query)) {
      throw new CcsInvalidArgumentRuntimeException(OPERATOR_NOT_ALLOWED_WITH_MULTIPLE_VALUES + query.name());
    }
    ParametersChecker.checkSanityString(field);
    ParametersChecker.checkSanity(values.toArray(new Object[0]));
  }

  /**
   * Constructor from Parameters
   */
  public RestQuery(final QUERY query, final String field, final String value) {
    this.query = query;
    conjunction = null;
    fieldName = field;
    avalues = null;
    svalue = value;
    ovalue = null;
    valueCollection = null;
    restQueries = null;
    if (ParametersChecker.isEmpty(query, field)) {
      throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
    }
    if (!isSingleArgQuery(query)) {
      throw new CcsInvalidArgumentRuntimeException(OPERATOR_NOT_ALLOWED_WITH_STRING_VALUE + query.name());
    }
    ParametersChecker.checkSanityString(field, value);
  }

  /**
   * Constructor from Parameters
   */
  public RestQuery(final QUERY query, final String field, final Object value) {
    this.query = query;
    conjunction = null;
    fieldName = field;
    svalue = null;
    restQueries = null;
    if (ParametersChecker.isEmpty(query, field)) {
      throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
    }
    ParametersChecker.checkSanityString(field);
    if (value instanceof final Collection<?> values) {
      avalues = null;
      ovalue = null;
      valueCollection = (Collection<Object>) values;
      if (ParametersChecker.isEmpty(values)) {
        throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
      }
      if (isSingleArgQuery(query)) {
        throw new CcsInvalidArgumentRuntimeException(OPERATOR_NOT_ALLOWED_WITH_MULTIPLE_VALUES + query.name());
      }
      ParametersChecker.checkSanity(values.toArray(new Object[0]));
    } else if (value instanceof final Object[] values) {
      avalues = values;
      ovalue = null;
      valueCollection = null;
      if (isSingleArgQuery(query)) {
        throw new CcsInvalidArgumentRuntimeException(OPERATOR_NOT_ALLOWED_WITH_MULTIPLE_VALUES + query.name());
      }
      ParametersChecker.checkSanity(values);
    } else {
      avalues = null;
      ovalue = value;
      valueCollection = null;
      if (!isObjectArgQuery(query)) {
        throw new CcsInvalidArgumentRuntimeException(OPERATOR_NOT_ALLOWED_WITH_OBJECT_VALUE + query.name());
      }
    }
  }

  private static boolean isObjectArgQuery(final QUERY query) {
    return query.ordinal() < QUERY.START_WITH.ordinal();
  }

  /**
   * Constructor from Parameters
   */
  public RestQuery(final CONJUNCTION conjunction, final RestQuery... queries) {
    query = null;
    this.conjunction = conjunction;
    fieldName = null;
    avalues = null;
    svalue = null;
    ovalue = null;
    valueCollection = null;
    restQueries = queries;
    if (ParametersChecker.isEmpty(conjunction, queries)) {
      throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
    }
  }

  /**
   * Constructor from Parameters
   */
  public RestQuery(final CONJUNCTION conjunction, final Collection<RestQuery> queries) {
    query = null;
    this.conjunction = conjunction;
    fieldName = null;
    avalues = null;
    svalue = null;
    ovalue = null;
    valueCollection = null;
    if (ParametersChecker.isEmpty(conjunction, queries)) {
      throw new CcsInvalidArgumentRuntimeException(ARGUMENT_SHALL_NOT_BE_EMPTY);
    }
    restQueries = queries.toArray(EMPTY_ARRAY_REST_QUERIES);
  }

  /**
   * @return True if this query has no criteria (equivalent to where "all")
   */
  @IgnoreProperty
  @Transient
  @JsonIgnore
  public boolean isEmpty() {
    return (query == null && conjunction == null);
  }

  /**
   * @return the QUERY
   */
  public QUERY getQUERY() {
    return query;
  }

  /**
   * @return the CONJUNCTION
   */
  public CONJUNCTION getConjunction() {
    return conjunction;
  }

  /**
   * @return the Field Name
   */

  public String getFieldName() {
    return fieldName;
  }

  /**
   * @return the value for a String
   */
  public String getSvalue() {
    return svalue;
  }

  /**
   * @return the value for an Object
   */
  public Object getOvalue() {
    return ovalue;
  }

  /**
   * @return the value for an array of Object
   */
  public Object[] getAvalues() {
    return avalues;
  }

  /**
   * @return the value for a collection of Object
   */
  public Collection<Object> getValueCollection() {
    return valueCollection;
  }

  /**
   * @return in case of Conjunction, the array of sub-queries
   */
  public RestQuery[] getRestQueries() {
    return restQueries;
  }

  /**
   * QUERY
   */
  public enum QUERY {
    EQ,
    NEQ,
    GTE,
    LTE,
    REVERSE_IN,
    START_WITH,
    JSON_EQ,
    JSON_NEQ,
    CONTAINS,
    CONTAINED,
    IN,
    ANY
  }

  /**
   * CONJUNCTION
   */
  public enum CONJUNCTION {
    AND,
    OR,
    NOT
  }
}
