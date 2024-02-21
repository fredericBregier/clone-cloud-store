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

import java.util.stream.Stream;

import com.mongodb.client.MongoCursor;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.StreamHelperInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import org.bson.Document;

/**
 * Mongo implementation of the StreamHelper
 *
 * @param <F> the DAO interface type
 * @param <E> the real DAO type
 */
public class MongoStreamHelper<F, E extends F> implements StreamHelperInterface<E> {
  public static final int MAX_LIST = 1000;

  /**
   * Constructor
   */
  public MongoStreamHelper() {
    // Empty
  }

  @Override
  public Stream<E> findStream(final RepositoryBaseInterface<E> repositoryBase, final DbQuery query)
      throws CcsDbException {
    try {
      return StreamIteratorUtils.getStreamFromIterator(findIterator(repositoryBase, query));
    } catch (final RuntimeException e) {
      throw new CcsDbException("findStream in error", e);
    }
  }

  public ClosingIterator<E> findIterator(final ExtendedPanacheMongoRepositoryBase<F, E> repositoryBase,
                                         final Document document) throws CcsDbException {
    try {
      return new DbIteratorImpl<>(repositoryBase.mongoCollection().find(document).batchSize(MAX_LIST).cursor());
    } catch (final RuntimeException e) {
      throw new CcsDbException("findIterator in error", e);
    }
  }

  @Override
  public ClosingIterator<E> findIterator(final RepositoryBaseInterface<E> repositoryBase, final DbQuery query)
      throws CcsDbException {
    try {
      if (query.isEmpty()) {
        return new DbIteratorImpl<>(
            ((ExtendedPanacheMongoRepositoryBase<F, E>) repositoryBase).mongoCollection().find().batchSize(MAX_LIST)
                .cursor());
      }
      return new DbIteratorImpl<>(
          ((ExtendedPanacheMongoRepositoryBase<F, E>) repositoryBase).mongoCollection().find(query.getBson())
              .batchSize(MAX_LIST).cursor());
    } catch (final RuntimeException e) {
      throw new CcsDbException("findIterator in error", e);
    }
  }

  private record DbIteratorImpl<E>(MongoCursor<E> mongoCursor) implements ClosingIterator<E> {

    @Override
    public void close() {
      mongoCursor.close();
    }

    @Override
    public boolean hasNext() {
      return mongoCursor.hasNext();
    }

    @Override
    public E next() {
      return mongoCursor.next();
    }
  }
}
