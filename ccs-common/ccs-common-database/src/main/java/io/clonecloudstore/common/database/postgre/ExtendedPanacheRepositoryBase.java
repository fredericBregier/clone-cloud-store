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

import java.util.stream.Stream;

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.transaction.Transactional;
import org.hibernate.jpa.HibernateHints;
import org.hibernate.query.Query;

import static io.clonecloudstore.common.database.postgre.PostgreStreamHelper.MAX_LIST;

/**
 * The base Postgre implementation of the RepositoryBaseInterface
 *
 * @param <E> the type for the DTO to use
 */
public abstract class ExtendedPanacheRepositoryBase<F, E extends F>
    implements PanacheRepositoryBase<E, String>, RepositoryBaseInterface<F> {
  protected final PostgreStreamHelper<F, E> streamHelper = new PostgreStreamHelper<>();
  protected final PostgreBulkHelper helper = new PostgreBulkHelper();
  private final E forClass;

  protected ExtendedPanacheRepositoryBase(final E forClass) {
    this.forClass = forClass;
  }

  @Override
  public boolean isSqlRepository() {
    return true;
  }

  protected void changeBulkSize(final int bulkSize) {
    helper.changeBulkSize(bulkSize);
  }

  protected int getBulkSize() {
    return helper.getBulkSize();
  }

  /**
   * SHall be in an enclosing Transactional to enforce bulk effects
   */
  @Override
  @Transactional
  public ExtendedPanacheRepositoryBase<F, E> addToInsertBulk(final F so) throws CcsDbException {
    try {
      // Implicit bulk through configuration: quarkus.hibernate-orm.jdbc.statement-batch-size
      insert(so);
      // Explicit one
      if (helper.addToBulk()) {
        flushAll();
      }
      return this;
    } catch (final RuntimeException e) {
      throw new CcsDbException("addToInsertBulk in error", e);
    }
  }

  /**
   * SHall be in an enclosing Transactional to enforce bulk effects
   */
  @Transactional
  public ExtendedPanacheRepositoryBase<F, E> addToUpdateBulk(final F so) throws CcsDbException {
    try {
      // Implicit bulk through configuration: quarkus.hibernate-orm.jdbc.statement-batch-size
      updateFull(so);
      // Explicit one
      if (helper.addToBulk()) {
        flushAll();
      }
      return this;
    } catch (final RuntimeException e) {
      throw new CcsDbException("addToInsertBulk in error", e);
    }
  }

  @Override
  @Transactional
  public void flushAll() throws CcsDbException {
    try {
      PanacheRepositoryBase.super.flush();
      helper.resetBulk();
    } catch (final RuntimeException e) {
      throw new CcsDbException("flushAll in error", e);
    }
  }

  @Override
  public F findOne(final DbQuery query) throws CcsDbException {
    try {
      if (query.isEmpty()) {
        return findAll().range(0, 1).firstResult();
      }
      return find(PostgreSqlHelper.query(query), query.getSqlParamsAsArray()).withHint(HibernateHints.HINT_READ_ONLY,
          true).withHint(HibernateHints.HINT_CACHEABLE, false).firstResult();
    } catch (final RuntimeException e) {
      throw new CcsDbException("findOne in error", e);
    }
  }

  @Override
  public F findWithPk(final String pk) throws CcsDbException {
    try {
      return findById(pk);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findWithPk in error", e);
    }
  }

  @Override
  @Transactional
  public void deleteWithPk(final String pk) throws CcsDbException {
    try {
      deleteById(pk);
    } catch (final RuntimeException e) {
      throw new CcsDbException("deleteWithPk in error", e);
    }
  }

  @Override
  public Stream<F> findStream(final DbQuery query) throws CcsDbException {
    try {
      return (Stream<F>) streamHelper.findStream((RepositoryBaseInterface<E>) this, query);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findStream in error", e);
    }
  }

  @Override
  public ClosingIterator<F> findIterator(final DbQuery query) throws CcsDbException {
    try {
      return (ClosingIterator<F>) streamHelper.findIterator((RepositoryBaseInterface<E>) this, query);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findIterator in error", e);
    }
  }

  @Override
  public long count(final DbQuery query) throws CcsDbException {
    try {
      return count(PostgreSqlHelper.query(query), query.getSqlParamsAsArray());
    } catch (final RuntimeException e) {
      throw new CcsDbException("count in error", e);
    }
  }

  @Override
  public long countAll() throws CcsDbException {
    try {
      return this.count();
    } catch (final RuntimeException e) {
      throw new CcsDbException("countAll in error", e);
    }
  }

  @Override
  @Transactional
  public long deleteAllDb() throws CcsDbException {
    try {
      return this.deleteAll();
    } catch (final RuntimeException e) {
      throw new CcsDbException("deleteAllDb in error", e);
    }
  }

  @Override
  @Transactional
  public long delete(final DbQuery query) throws CcsDbException {
    try {
      return delete(PostgreSqlHelper.query(query), query.getSqlParamsAsArray());
    } catch (final RuntimeException e) {
      throw new CcsDbException("delete in error", e);
    }
  }

  @Override
  @Transactional
  public long update(final DbQuery query, final DbUpdate update) throws CcsDbException {
    try {
      final var nativeQuery = this.getEntityManager()
          .createNativeQuery("UPDATE " + getTable() + " " + PostgreSqlHelper.update(update, query));
      final var params = PostgreSqlHelper.getUpdateParamsAsArray(update, query);
      int pos = 1;
      for (final var param : params) {
        nativeQuery.setParameter(pos, param);
        pos++;
      }
      return nativeQuery.executeUpdate();
    } catch (final RuntimeException e) {
      throw new CcsDbException("update in error", e);
    }
  }

  @Override
  @Transactional
  public boolean updateFull(final F item) throws CcsDbException {
    try {
      getEntityManager().merge(item);
      return true;
    } catch (final RuntimeException e) {
      throw new CcsDbException("updateFull in error", e);
    }
  }

  @Override
  @Transactional
  public void insert(final F item) throws CcsDbException {
    try {
      persist((E) item);
    } catch (final RuntimeException e) {
      throw new CcsDbException("insert in error", e);
    }
  }

  /**
   * Build the SELECT Hibernate Query from the DbQuery
   */
  public Query<E> findQuery(final DbQuery query) throws CcsDbException {
    try {
      return getSelectQuery(query);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findQuery in error", e);
    }
  }

  /**
   * Build the SELECT Hibernate Query from the DbQuery
   */
  public Query<E> getSelectQuery(final DbQuery dbQuery) throws CcsDbException {
    try {
      final var query = (Query<E>) getEntityManager().createNativeQuery(PostgreSqlHelper.select(getTable(), dbQuery),
              forClass.getClass()).setHint(HibernateHints.HINT_FETCH_SIZE, MAX_LIST)
          .setHint(HibernateHints.HINT_READ_ONLY, true).setHint(HibernateHints.HINT_CACHEABLE, false);
      setParameter(query, dbQuery);
      return query;
    } catch (final RuntimeException e) {
      throw new CcsDbException("getSelectQuery in error", e);
    }
  }

  private void setParameter(final Query<E> query, final DbQuery dbQuery) {
    var i = 1;
    for (final var param : dbQuery.getSqlParams()) {
      query.setParameter(i, param);
      i++;
    }
  }
}
