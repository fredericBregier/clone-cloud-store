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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import org.bson.Document;

/**
 * Mongo implementation of the BulkHelper
 *
 * @param <E> the DTO type to use
 */
public class MongoBulkInsertHelper<F, E extends F> {
  public static final int MAX_BATCH = 1000;
  public static final int MAX_IN = 10000;
  protected final List<E> listInsert = new ArrayList<>();
  protected final List<Document> listFindQuery = new ArrayList<>();
  protected final List<Document> listUpdate = new ArrayList<>();
  protected final List<E> listInsertForUpdate = new ArrayList<>();

  /**
   * Constructor
   */
  public MongoBulkInsertHelper() {
    // Empty
  }

  /**
   * @param object the element to persist within a bulk operation
   * @return True if bulk operation reaches the limit
   */
  public boolean addToInsertBulk(final E object) {
    listInsert.add(object);
    return listInsert.size() >= getMaxBatch();
  }

  /**
   * @return the current max batch value
   */
  protected int getMaxBatch() {
    return MAX_BATCH;
  }

  /**
   * Persist all elements using bulk operation
   *
   * @param repositoryBase the repository to use in bulk operation
   * @return this
   */
  public MongoBulkInsertHelper<F, E> bulkPersist(final RepositoryBaseInterface<E> repositoryBase)
      throws CcsDbException {
    try {
      if (!listInsert.isEmpty()) {
        ((ExtendedPanacheMongoRepositoryBase<F, E>) repositoryBase).mongoCollection().insertMany(listInsert);
        listInsert.clear();
      }
      return this;
    } catch (final RuntimeException e) {
      throw new CcsDbException("bulkPersist in error", e);
    }
  }

  /**
   * @param findQuery the find query associated in order with Upsert
   * @param update    the update part
   * @param original  the original one for insert (null means update only)
   * @return True if bulk operation reaches the limit
   */
  public boolean addToUpsertBulk(final Document findQuery, final Document update, final E original) {
    listFindQuery.add(findQuery);
    String id = null;
    if (findQuery.containsKey(RepositoryBaseInterface.ID)) {
      id = update.getString(RepositoryBaseInterface.ID);
      update.remove(RepositoryBaseInterface.ID);
    }
    if (id == null) {
      id = GuidLike.getGuid();
    }
    listUpdate.add(new Document("$set", update).append("$setOnInsert", new Document(RepositoryBaseInterface.ID, id)));
    listInsertForUpdate.add(original);
    return listUpdate.size() >= getMaxBatch();
  }

  /**
   * Upsert Persist all elements using bulk operation
   *
   * @param repositoryBase the repository to use in bulk operation
   * @return this
   */
  public MongoBulkInsertHelper<F, E> bulkUpsert(final RepositoryBaseInterface<E> repositoryBase) throws CcsDbException {
    try {
      if (!listUpdate.isEmpty()) {
        final var updateOptions = new UpdateOptions().upsert(true);
        final Iterator<Document> iterator = listUpdate.iterator();
        final Iterator<Document> iteratorFindQuery = listFindQuery.iterator();
        final Iterator<E> iteratorInsertForUpdate = listInsertForUpdate.iterator();
        final var operations = new ArrayList<WriteModel<E>>();
        while (iteratorFindQuery.hasNext() && iterator.hasNext() && iteratorInsertForUpdate.hasNext()) {
          final var find = iteratorFindQuery.next();
          final Document update = iterator.next();
          final E item = iteratorInsertForUpdate.next();
          if (item == null || ((ExtendedPanacheMongoRepositoryBase<?, ?>) repositoryBase).count(find) > 0) {
            final var upsert = new UpdateOneModel<E>(find, update, updateOptions);
            operations.add(upsert);
          } else {
            final var insert = new InsertOneModel<E>(item);
            operations.add(insert);
          }
        }
        ((ExtendedPanacheMongoRepositoryBase<F, E>) repositoryBase).mongoCollection().bulkWrite(operations);
        operations.clear();
        listFindQuery.clear();
        listUpdate.clear();
        listInsertForUpdate.clear();
      }
      return this;
    } catch (final RuntimeException e) {
      throw new CcsDbException("bulkUpsert in error", e);
    }
  }
}
