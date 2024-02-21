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
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import org.bson.Document;
import org.jboss.logging.Logger;

/**
 * Mongo implementation of the BulkHelper
 *
 * @param <E> the DTO type to use
 */
public class MongoBulkInsertHelper<F, E extends F> {
  private static final Logger LOGGER = Logger.getLogger(MongoBulkInsertHelper.class);
  public static final int MAX_BATCH = 1000;
  protected final ExtendedPanacheMongoRepositoryBase<F, E> repositoryBase;
  protected final List<E> listInsert = new ArrayList<>();
  protected final List<Document> listFindQuery = new ArrayList<>();
  protected final List<Document> listUpdate = new ArrayList<>();
  protected final List<E> listInsertForUpdate = new ArrayList<>();

  /**
   * Constructor
   */
  public MongoBulkInsertHelper(final ExtendedPanacheMongoRepositoryBase<F, E> repositoryBase) {
    this.repositoryBase = repositoryBase;
  }

  /**
   * @param object the element to persist within a bulk operation
   * @return True if bulk operation reaches the limit
   */
  public synchronized boolean addToInsertBulk(final E object) {
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
   * @return this
   */
  public synchronized MongoBulkInsertHelper<F, E> bulkPersist() throws CcsDbException {
    try {
      if (!listInsert.isEmpty()) {
        this.repositoryBase.mongoCollection().insertMany(listInsert);
        listInsert.clear();
      }
      return this;
    } catch (final RuntimeException e) {
      throw new CcsDbException("bulkPersist in error", e);
    }
  }

  /**
   * @param findQuery the find query associated in order with Upsert
   * @param original  the original one for insert (null means update only)
   * @return True if bulk operation reaches the limit
   */
  public synchronized boolean addToUpsertBulk(final Document findQuery, final E original) {
    listFindQuery.add(findQuery);
    listUpdate.add(null);
    listInsertForUpdate.add(original);
    return listUpdate.size() >= getMaxBatch();
  }

  /**
   * @param findQuery the find query associated in order with Upsert
   * @param update    the update part
   * @return True if bulk operation reaches the limit
   */
  public synchronized boolean addToUpdateBulk(final Document findQuery, final Document update) {
    listFindQuery.add(findQuery);
    listUpdate.add(update);
    listInsertForUpdate.add(null);
    return listUpdate.size() >= getMaxBatch();
  }

  private String getId(final Document findQuery, final Document update) {
    String id = null;
    if (update.containsKey(RepositoryBaseInterface.ID)) {
      id = update.getString(RepositoryBaseInterface.ID);
      update.remove(RepositoryBaseInterface.ID);
    }
    if (findQuery.containsKey(RepositoryBaseInterface.ID)) {
      var id2 = findQuery.getString(RepositoryBaseInterface.ID);
      if (id != null && !id2.equals(id)) {
        LOGGER.warnf("Upsert contains 2 different ID: %s vs %s", id, id2);
      }
      id = id2;
    }
    if (id == null) {
      id = GuidLike.getGuid();
    }
    return id;
  }

  /**
   * Upsert Persist all elements using bulk operation
   *
   * @return this
   */
  public synchronized MongoBulkInsertHelper<F, E> bulkUpsert() throws CcsDbException {
    try {
      if (!listUpdate.isEmpty()) {
        final var updateOptions = new UpdateOptions().upsert(true);
        final var finds = new Document("$or", listFindQuery);
        final var projection = Projections.fields(Projections.include(RepositoryBaseInterface.ID));
        final var founds = new ArrayList<String>(listFindQuery.size());
        repositoryBase.mongoCollection().find(finds).projection(projection).batchSize(listFindQuery.size())
            .forEach(e -> {
              var doc = repositoryBase.getDocumentFromObject(e);
              var id = doc.getString(RepositoryBaseInterface.ID);
              founds.add(id);
            });
        final Iterator<Document> iterator = listUpdate.iterator();
        final Iterator<Document> iteratorFindQuery = listFindQuery.iterator();
        final Iterator<E> iteratorInsertForUpdate = listInsertForUpdate.iterator();
        final var operations = new ArrayList<WriteModel<E>>();
        while (iteratorFindQuery.hasNext() && iterator.hasNext() && iteratorInsertForUpdate.hasNext()) {
          final var find = iteratorFindQuery.next();
          Document update = iterator.next();
          final E item = iteratorInsertForUpdate.next();
          if (update != null) {
            final var upsert = new UpdateOneModel<E>(find, update, updateOptions);
            operations.add(upsert);
          } else {
            final var document = repositoryBase.getDocumentFromObject(item);
            final String id = getId(find, document);
            if (founds.contains(id)) {
              final var upsertIem =
                  new Document("$set", document).append("$setOnInsert", new Document(RepositoryBaseInterface.ID, id));
              final var upsert = new UpdateOneModel<E>(find, upsertIem, updateOptions);
              operations.add(upsert);
            } else {
              final var insert = new InsertOneModel<E>(item);
              operations.add(insert);
            }
          }
        }
        repositoryBase.mongoCollection().bulkWrite(operations);
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
