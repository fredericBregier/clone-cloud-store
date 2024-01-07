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

import java.util.stream.Stream;

import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;

/**
 * Interface for Repository for a Database
 *
 * @param <E> The Type used by this Repository
 */
public interface RepositoryBaseInterface<E> {
  /**
   * Default Primary Key Name (MongoDB)
   */
  String ID = "_id";
  /**
   * Default Primary Key Name (PostgreSQL)
   */
  String ID_PG = "id";

  /**
   * @return True if SQL based repository, else False (NoSQL)
   */
  boolean isSqlRepository();

  /**
   * @return the table name
   */
  String getTable();

  /**
   * @return the Primary Key column name
   */

  default String getPkName() {
    return isSqlRepository() ? ID_PG : ID;
  }

  /***
   * Add one element to an insert bulk operation
   *
   * @param so the item to bulk (insert/update)
   * @return this
   */
  RepositoryBaseInterface<E> addToInsertBulk(E so) throws CcsDbException;

  /**
   * Force flush of all remaining bulk items, and if supported other items
   */
  void flushAll() throws CcsDbException;

  /**
   * @param query the where condition
   * @return the item according to the query
   */
  E findOne(DbQuery query) throws CcsDbException;

  /**
   * @param pk the Primary Key as String
   * @return the item according to the Primary Key
   */
  E findWithPk(String pk) throws CcsDbException;

  /**
   * Delete the item according to the Primary Key
   *
   * @param pk the Primary Key as String
   */
  void deleteWithPk(String pk) throws CcsDbException;

  /**
   * @param query the where condition
   * @return the Stream (real one) based on Query (where condition)
   */
  Stream<E> findStream(DbQuery query) throws CcsDbException;

  /**
   * @param query the where condition
   * @return the iterator based on Query (where condition)
   */
  ClosingIterator<E> findIterator(DbQuery query) throws CcsDbException;

  /**
   * @param query the where condition
   * @return the count based on Query (where condition)
   */
  long count(DbQuery query) throws CcsDbException;

  /**
   * @return the total number of rows
   */
  long countAll() throws CcsDbException;

  /**
   * @param query the where condition
   * @return the count of deleted items based on Query (where condition)
   */
  long delete(DbQuery query) throws CcsDbException;

  /**
   * @return the total number of deleted rows
   */
  long deleteAllDb() throws CcsDbException;

  /**
   * @param query  the where condition
   * @param update the update part
   * @return the count of updated items based on Query (where condition) and the Update
   */
  long update(DbQuery query, DbUpdate update) throws CcsDbException;

  /**
   * Update fully this E
   *
   * @return True if ok
   */
  boolean updateFull(E e) throws CcsDbException;

  /**
   * @param item the item to insert
   */
  void insert(E item) throws CcsDbException;

  /**
   * @return an empty item
   */
  E createEmptyItem();
}
