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

package io.clonecloudstore.common.database.postgre.impl.simple;

import java.util.Iterator;
import java.util.stream.Stream;

import io.clonecloudstore.common.database.model.dao.DaoExample;
import io.clonecloudstore.common.database.model.dao.DaoExampleRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PgTestStream {
  private static final Logger LOGGER = Logger.getLogger(PgTestStream.class);
  final DaoExampleRepository repository;

  public PgTestStream(final Instance<DaoExampleRepository> repositoryInstance) {
    repository = repositoryInstance.get();
  }

  public void createTransaction() {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    QuarkusTransaction.begin();
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
  }

  public Stream<DaoExample> getStream() throws CcsDbException {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    try {
      return repository.findStream(new DbQuery());
    } catch (CcsDbException e) {
      QuarkusTransaction.rollback();
      LOGGER.error(e, e);
      throw e;
    }
  }

  public ClosingIterator<DaoExample> getIterator() throws CcsDbException {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    try {
      return repository.findIterator(new DbQuery());
    } catch (CcsDbException e) {
      QuarkusTransaction.rollback();
      LOGGER.error(e, e);
      throw e;
    }
  }

  public void updateEach(final Stream<DaoExample> daoExampleStream) {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    daoExampleStream.forEach(daoExample -> updateOne(daoExample));
  }

  public void updateEach(final Iterator<DaoExample> daoExampleIterator) {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    while (daoExampleIterator.hasNext()) {
      final var item = daoExampleIterator.next();
      updateOne(item);
    }
  }

  public void suspendTransactionWithDbException() {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    QuarkusTransaction.suspendingExisting().run(() -> {
      try {
        LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
        LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
        simulateDbException();
        LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
        LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
      } catch (CcsDbException e) {
        LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
        LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
        LOGGER.error(e, e);
      }
    });
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
  }

  public boolean suspendTransaction(final Iterator<DaoExample> daoExampleIterator) {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    final var item = daoExampleIterator.hasNext() ? daoExampleIterator.next() : null;
    if (item != null) {
      QuarkusTransaction.suspendingExisting().run(() -> {
        LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
        LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
        updateOne(item);
        LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
        LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
      });
      return true;
    }
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    return false;
  }

  public boolean suspendTransactionWithDbException(final Iterator<DaoExample> daoExampleIterator) {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    final var item = daoExampleIterator.hasNext() ? daoExampleIterator.next() : null;
    if (item != null) {
      QuarkusTransaction.suspendingExisting().run(() -> {
        try {
          LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
          LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
          simulateDbException();
        } catch (CcsDbException e) {
          LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
          LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
          LOGGER.error(e, e);
        }
      });
      return true;
    }
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    return false;
  }

  @Transactional
  public void simulateDbException() throws CcsDbException {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    final var dao = repository.createEmptyItem();
    dao.setGuid(GuidLike.getGuid());
    repository.updateFull(dao);
  }

  public boolean finalizeTransaction() {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    boolean status = false;
    if (QuarkusTransaction.isActive()) {
      if (QuarkusTransaction.isRollbackOnly()) {
        QuarkusTransaction.rollback();
      } else {
        QuarkusTransaction.commit();
        status = true;
      }
    }
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    return status;
  }

  @Transactional
  protected void updateOne(final DaoExample daoExample) {
    LOGGER.infof("Currently active ? %b", QuarkusTransaction.isActive());
    LOGGER.infof("Currently rollbacking ? %b", QuarkusTransaction.isRollbackOnly());
    try {
      repository.updateFull(daoExample);
    } catch (CcsDbException e) {
      LOGGER.error(e, e);
      throw new RuntimeException(e);
    }
  }
}
