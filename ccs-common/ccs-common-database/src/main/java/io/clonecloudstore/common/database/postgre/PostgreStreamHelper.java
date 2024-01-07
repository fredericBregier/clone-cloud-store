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

import java.util.NoSuchElementException;
import java.util.stream.Stream;

import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.StreamHelperInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;

/**
 * Postgre implementation of the StreamHelper
 *
 * @param <F> the DAO interface type
 * @param <E> the real DAO type
 */
public class PostgreStreamHelper<F, E extends F> implements StreamHelperInterface<E> {
  public static final int MAX_LIST =
      QuarkusSystemPropertyUtil.getIntegerConfig("quarkus.hibernate-orm.jdbc.statement-fetch-size", 1000);

  /**
   * Constructor
   */
  public PostgreStreamHelper() {
    // Empty
  }

  @Override
  public Stream<E> findStream(final RepositoryBaseInterface<E> repositoryBase, final DbQuery query)
      throws CcsDbException {
    try {
      // Using native stream might be baddest for memory (stream seems to get full result first)
      final var iterator =
          new DbIteratorImpl<>(findScrollable((ExtendedPanacheRepositoryBase<F, E>) repositoryBase, query));
      return StreamIteratorUtils.getStreamFromIterator(iterator);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findStream in error", e);
    }
  }

  @Override
  public ClosingIterator<E> findIterator(final RepositoryBaseInterface<E> repositoryBase, final DbQuery query)
      throws CcsDbException {
    try {
      return new DbIteratorImpl<>(findScrollable((ExtendedPanacheRepositoryBase<F, E>) repositoryBase, query));
    } catch (final RuntimeException e) {
      throw new CcsDbException("findIterator in error", e);
    }
  }

  /**
   * Used by findStream (prefer findStream)
   *
   * @return the ScrollableResults for this DbQuery
   */
  private ScrollableResults<E> findScrollable(final ExtendedPanacheRepositoryBase<F, E> repositoryBase,
                                              final DbQuery query) throws CcsDbException {
    try {
      final var hibernateQuery = repositoryBase.getSelectQuery(query);
      final var scroll = hibernateQuery.scroll(ScrollMode.FORWARD_ONLY);
      scroll.setFetchSize(MAX_LIST);
      return scroll;
    } catch (final RuntimeException e) {
      throw new CcsDbException("findScrollable in error", e);
    }
  }

  private static class DbIteratorImpl<E> implements ClosingIterator<E> {
    private final ScrollableResults<E> results;
    private E element = null;

    private DbIteratorImpl(final ScrollableResults<E> results) {
      this.results = results;
    }

    @Override
    public boolean hasNext() {
      if (element == null && results.next()) {
        element = results.get();
      }
      final var hasNext = element != null;
      if (!hasNext) {
        results.close();
      }
      return hasNext;
    }

    @Override
    public E next() {
      if (element == null) {
        throw new NoSuchElementException("No next element");
      }
      final var result = element;
      element = null;
      return result;
    }

    @Override
    public void close() {
      results.close();
    }
  }
}
