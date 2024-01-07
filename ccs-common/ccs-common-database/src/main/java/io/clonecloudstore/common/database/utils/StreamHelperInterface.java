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
 * Helper for stream (real one) and Iterator (faster) operation
 *
 * @param <E> the DTO type to use
 */
public interface StreamHelperInterface<E> {
  /**
   * @param repositoryBase the repository to use
   * @param query          the query
   * @return the stream of elements
   */
  Stream<E> findStream(RepositoryBaseInterface<E> repositoryBase, DbQuery query) throws CcsDbException;

  /**
   * @param repositoryBase the repository to use
   * @param query          the query
   * @return the iterator of elements
   */
  ClosingIterator<E> findIterator(RepositoryBaseInterface<E> repositoryBase, DbQuery query) throws CcsDbException;
}
