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

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

/**
 * Array type for PostgreSQL through Hibernate
 */
public class PostgreStringArrayType implements UserType<String[]> {
  @Override
  public int getSqlType() {
    return Types.ARRAY;
  }

  @Override
  public Class returnedClass() {
    return String[].class;
  }

  @Override
  public boolean equals(final String[] strings, final String[] j1) {
    return Arrays.deepEquals(strings, j1);
  }

  @Override
  public int hashCode(final String[] strings) {
    return Arrays.hashCode(strings);
  }

  @Override
  public String[] nullSafeGet(final ResultSet resultSet, final int position,
                              final SharedSessionContractImplementor session, final Object owner) throws SQLException {
    final var array = resultSet.getArray(position);
    return array != null ? (String[]) array.getArray() : null;
  }

  @Override
  public void nullSafeSet(final PreparedStatement preparedStatement, final String[] value, final int index,
                          final SharedSessionContractImplementor session) throws SQLException {
    if (value != null && preparedStatement != null) {
      try (final var conn = session.getJdbcConnectionAccess().obtainConnection()) {
        final var array = conn.createArrayOf("text", value);
        preparedStatement.setArray(index, array);
      }
    } else if (preparedStatement != null) {
      preparedStatement.setNull(index, getSqlType());
    }
  }

  @Override
  public String[] deepCopy(final String[] strings) {
    return Arrays.copyOf(strings, strings.length);
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public Serializable disassemble(final String[] strings) {
    return strings;
  }

  @Override
  public String[] assemble(final Serializable cached, final Object owner) throws HibernateException {
    return (String[]) cached;
  }
}
