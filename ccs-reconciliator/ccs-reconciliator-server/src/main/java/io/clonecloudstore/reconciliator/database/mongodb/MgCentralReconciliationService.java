/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.reconciliator.database.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.clonecloudstore.common.database.mongo.MongoBulkInsertHelper;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.reconciliator.database.model.CentralReconciliationService;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesAction;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.BsonNull;
import org.bson.Document;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.BUCKET;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.EVENT;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NAME;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NSTATUS;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.REQUESTID;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.SITE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.CCS_FIRST;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.COND;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.DEFAULT_PK;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.IN;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.LOCAL_NSTATUS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.LOCAL_O_EVENT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.LOCAL_SITE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ADD_FIELDS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_COND;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_EACH;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_EQ;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_EXISTS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_FILTER;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_FROM;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_GTE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_IN;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INPUT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INSERT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INTO;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_LET;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_LTE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MATCH;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MERGE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_NE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_NOT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ON;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_OR;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_PULL;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_PUSH;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_REPLACE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_SET_UNION;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_SIZE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_SORT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_THIS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_UNSET;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_VARS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_WHEN_MATCHED;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_WHEN_NOT_MATCHED;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.runInThread;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.DELETED_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.DELETE_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.ERROR_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.READY_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UPDATE_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UPLOAD_ACTION;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgCentralReconciliationService implements CentralReconciliationService {
  private static final Logger LOGGER = Logger.getLogger(MgCentralReconciliationService.class);
  public static final String CENTRAL_RECONCILIATION = "central_reconciliation";
  private static final String CCS_FROM = "$$from.";
  private static final String CCS_TEMP_SOURCE = "tempSource";
  private final MgDaoSitesListingRepository sitesListingRepository;
  private final MgDaoSitesActionRepository sitesActionRepository;
  private final MgDaoRequestRepository requestRepository;
  private final BulkMetrics bulkMetrics;

  public MgCentralReconciliationService(final MgDaoSitesListingRepository sitesListingRepository,
                                        final MgDaoSitesActionRepository sitesActionRepository,
                                        final MgDaoRequestRepository requestRepository, final BulkMetrics bulkMetrics) {
    this.sitesListingRepository = sitesListingRepository;
    this.sitesActionRepository = sitesActionRepository;
    this.requestRepository = requestRepository;
    this.bulkMetrics = bulkMetrics;
  }


  /**
   * Add the remote sites listing to local aggregate one<br>
   * Step7: add all remote sites listing<br>
   * Index Sites: requestId, bucket, name
   */
  @Override
  public void saveRemoteNativeListing(final DaoRequest daoRequest, final Iterator<ReconciliationSitesListing> iterator)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>();
    final AtomicLong countEntries = new AtomicLong(0);
    final BlockingQueue<List<ReconciliationSitesListing>> blockingQueue = new ArrayBlockingQueue<>(10);
    final Semaphore semaphore = new Semaphore(0);
    SystemTools.STANDARD_EXECUTOR_SERVICE.execute(
        () -> applyBulkImportQueue(daoRequest, blockingQueue, semaphore, countEntries, possibleDbException));
    final int max = MongoBulkInsertHelper.MAX_BATCH;
    final List<ReconciliationSitesListing> toAdds = new ArrayList<>(max);
    try {
      try {
        while (iterator.hasNext()) {
          final var daoSitesListing = iterator.next();
          toAdds.add(daoSitesListing);
          if (toAdds.size() >= max) {
            final List<ReconciliationSitesListing> toAddsBis = new ArrayList<>(toAdds);
            blockingQueue.put(toAddsBis);
            toAdds.clear();
          }
          if (possibleDbException.get() != null) {
            throw possibleDbException.get();
          }
        }
        if (!toAdds.isEmpty()) {
          blockingQueue.put(toAdds);
        }
      } finally {
        blockingQueue.put(Collections.emptyList());
      }
      semaphore.acquire();
      sitesListingRepository.flushAll();
      if (possibleDbException.get() != null) {
        throw possibleDbException.get();
      }
      bulkMetrics.incrementCounter(1, CENTRAL_RECONCILIATION, BulkMetrics.KEY_SITE, BulkMetrics.TAG_FROM_REMOTE_SITE);
    } catch (final InterruptedException e) { // NOSONAR intentional
      throw new CcsDbException(e);
    } finally {
      SystemTools.consumeAll(iterator);
    }
    var checkedRemote = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId())).getCheckedRemote();
    daoRequest.setCheckedRemote(checkedRemote + countEntries.get());
    bulkMetrics.incrementCounter(countEntries.get(), CENTRAL_RECONCILIATION, BulkMetrics.KEY_OBJECT,
        BulkMetrics.TAG_FROM_REMOTE_SITES_LISTING);
    LOGGER.infof("Remote save listing %d items", countEntries.get());
    requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
        new DbUpdate().set(DaoRequestRepository.CHECKED_REMOTE, daoRequest.getCheckedRemote()));
  }

  private void applyBulkImportQueue(final DaoRequest daoRequest,
                                    final BlockingQueue<List<ReconciliationSitesListing>> blockingQueue,
                                    final Semaphore semaphore, final AtomicLong countEntries,
                                    final AtomicReference<CcsDbException> possibleDbException) {
    try {
      while (true) {
        var list = blockingQueue.take();
        if (list.isEmpty()) {
          // End
          return;
        }
        final var toAdds =
            list.stream().collect(Collectors.toMap(ReconciliationSitesListing::name, Function.identity()));
        list.clear();
        applyBulkImport(daoRequest, toAdds, countEntries, possibleDbException);
        if (possibleDbException.get() != null) {
          return;
        }
      }
    } catch (final RuntimeException | InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } finally {
      semaphore.release();
    }
  }

  private void applyBulkImport(final DaoRequest daoRequest, final Map<String, ReconciliationSitesListing> toAdds,
                               final AtomicLong countEntries, final AtomicReference<CcsDbException> possibleDbException)
      throws CcsDbException {
    final var listNames = new ArrayList<>(toAdds.keySet());
    try {
      // Existing items
      sitesListingRepository.find(new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
          .append(NAME, new Document(MG_IN, listNames))).stream().forEach(original -> {
        countEntries.incrementAndGet();
        var newItem = toAdds.remove(original.getName());
        final var find = new Document(ID, original.getId());
        final var originalList = original.getLocal();
        var dao = sitesListingRepository.createEmptyItem().fromDto(newItem).setId(null);
        if (originalList == null || originalList.isEmpty()) {
          try {
            sitesListingRepository.addToUpsertBulk(find, dao);
          } catch (final CcsDbException e) {
            LOGGER.warn(e);
            possibleDbException.compareAndSet(null, e);
            throw new RuntimeException(e); // NOSONAR intentional
          }
        } else {
          computeWithExistingNonEmptyLocal(possibleDbException, dao, originalList, find);
        }
      });
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      if (possibleDbException.get() != null) {
        throw possibleDbException.get();
      }
      throw new CcsDbException(e);
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
    // Non-existing items
    for (final var newItem : toAdds.values()) {
      countEntries.incrementAndGet();
      var dao = sitesListingRepository.createEmptyItem().fromDto(newItem);
      sitesListingRepository.addToInsertBulk(dao);
    }
    sitesListingRepository.flushAll();
  }

  private void computeWithExistingNonEmptyLocal(final AtomicReference<CcsDbException> possibleDbException,
                                                final DaoSitesListing newItem,
                                                final List<SingleSiteObject> originalList, final Document find) {
    // Compute to remove and then add all
    final var newItemNameList = newItem.getLocal().stream().map(SingleSiteObject::site).toList();
    var toRemoveOriginalList = new ArrayList<>();
    var originalNameList = originalList.stream().map(SingleSiteObject::site).toList();
    newItemNameList.forEach(item -> {
      if (originalNameList.contains(item)) {
        toRemoveOriginalList.add(item);
      }
    });
    if (!toRemoveOriginalList.isEmpty()) {
      var remove = new Document(MG_PULL,
          new Document(DaoSitesListingRepository.LOCAL, new Document(SITE, new Document(MG_IN, toRemoveOriginalList))));
      try {
        sitesListingRepository.addToUpdateBulk(find, remove);
      } catch (final CcsDbException e) {
        LOGGER.warn(e);
        possibleDbException.compareAndSet(null, e);
        throw new RuntimeException(e); // NOSONAR intentional
      }
    }
    var update = new Document(MG_PUSH, new Document(DaoSitesListingRepository.LOCAL,
        new Document(MG_EACH, newItem.getLocal()).append(MG_SORT, new Document(EVENT, -1).append(NSTATUS, 1))));
    try {
      sitesListingRepository.addToUpdateBulk(find, update);
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
      throw new RuntimeException(e); // NOSONAR intentional
    }
  }

  private static List<String> getAllSites(final DaoRequest request) {
    return request.getContextSites();
  }

  /**
   * Compute actions from sites listing<br>
   * Step8: in 2 steps, all sites declared, not all sites declared
   * Index Sites: requestId, bucket, local.nstatus
   * Index Actions: requestId, bucket, name
   */
  @Override
  public void computeActions(final DaoRequest daoRequest) throws CcsDbException {
    try {
      final AtomicReference<CcsDbException> ccsDbExceptionAtomicReference = new AtomicReference<>();
      runInThread(ccsDbExceptionAtomicReference,
          () -> computeActionsStepDelete(daoRequest, ccsDbExceptionAtomicReference),
          () -> computeActionsReadyLike(daoRequest, ccsDbExceptionAtomicReference),
          () -> computeActionsUpload(daoRequest, ccsDbExceptionAtomicReference),
          () -> computeActionsInvalidUpload(daoRequest, ccsDbExceptionAtomicReference));
      if (ccsDbExceptionAtomicReference.get() != null) {
        throw ccsDbExceptionAtomicReference.get();
      }
      countFinalActions(daoRequest);
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Index: REQUESTID, BUCKET, LOCAL.NSTATUS
   */
  public void computeActionsStepDelete(final DaoRequest daoRequest,
                                       final AtomicReference<CcsDbException> exceptionAtomicReference) {
    try {
      // Sub step A: Latest = Delete like and some are not Deleted
      final var matchStepDelete = new Document(MG_MATCH,
          getBaseMatchActions(daoRequest).append(DaoSitesListingRepository.LOCAL + ".0." + NSTATUS,
                  new Document(MG_IN, List.of(DELETE_ACTION.getStatus(), DELETED_ACTION.getStatus())))
              .append(MgDaoReconciliationUtils.LOCAL_NSTATUS, new Document(MG_IN,
                  List.of(DELETE_ACTION.getStatus(), READY_ACTION.getStatus(), UPDATE_ACTION.getStatus(),
                      UPLOAD_ACTION.getStatus()))));
      final var buildActions = new Document(MG_ADD_FIELDS,
          addPartialTargetSites(getBuildActionsDelete(), getValidTargetSitesForDelete(), daoRequest));
      final var buildActionsFinalTargetSites = getBuildActionsFinalTargetSites();
      final var filterActionsNotEmpty = new Document(MG_MATCH,
          new Document(DaoSitesActionRepository.SITES, new Document(MG_NOT, new Document(MG_SIZE, 0))));
      final var keepActionFieldsOnly = getUnsetLocalTempVars();
      final var mergeIntoActions = getMergeIntoActions();
      sitesListingRepository.mongoCollection().aggregate(
          List.of(matchStepDelete, buildActions, buildActionsFinalTargetSites, filterActionsNotEmpty,
              keepActionFieldsOnly, mergeIntoActions)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  private static Document getBuildActionsFinalTargetSites() {
    return new Document(MG_ADD_FIELDS, new Document(DaoSitesActionRepository.SITES,
        new Document(MG_SET_UNION, List.of("$unknownStatusSites", "$tempSites"))));
  }

  private static Document getBaseMatchActions(final DaoRequest daoRequest) {
    return new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
        .append(DaoSitesListingRepository.LOCAL, new Document(MG_EXISTS, true));
  }

  private static Document getValidTargetSitesForDelete() {
    return new Document(MG_LET, new Document(MG_VARS, new Document(CCS_FIRST, LOCAL_O_EVENT).append(MG_FROM,
        new Document(MG_FILTER, new Document(MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(COND,
            new Document(MG_NE, List.of(MG_THIS + NSTATUS, DELETED_ACTION.getStatus())))))).append(IN,
        CCS_FROM + SITE));
  }

  private static Document getBuildActionsDelete() {
    return new Document(DaoSitesActionRepository.NEED_ACTION_FROM, new BsonNull()).append(
        DaoSitesActionRepository.NEED_ACTION, DELETE_ACTION.getStatus());
  }

  private static Document getMergeIntoActions() {
    return new Document(MG_MERGE, new Document(MG_INTO, DaoSitesActionRepository.TABLE_NAME).append(MG_ON, DEFAULT_PK)
        .append(MG_WHEN_MATCHED, MG_REPLACE).append(MG_WHEN_NOT_MATCHED, MG_INSERT));
  }

  public void computeActionsReadyLike(final DaoRequest daoRequest,
                                      final AtomicReference<CcsDbException> exceptionAtomicReference) {
    try {
      // Sub step B: Latest = Ready like and some are not Ready
      // (note: at least 1 shall be Ready/Update)
      final var matchStepUpdate = new Document(MG_MATCH,
          getBaseMatchActions(daoRequest).append(DaoSitesListingRepository.LOCAL + ".0." + NSTATUS,
                  new Document(MG_IN, List.of(READY_ACTION.getStatus(), UPDATE_ACTION.getStatus())))
              .append(MgDaoReconciliationUtils.LOCAL_NSTATUS, new Document(MG_IN,
                  List.of(DELETED_ACTION.getStatus(), DELETE_ACTION.getStatus(), UPDATE_ACTION.getStatus(),
                      UPLOAD_ACTION.getStatus()))));
      final var buildActions = new Document(MG_ADD_FIELDS,
          addPartialTargetSites(getBuildActionsReadyLike(), getValidTargetSitesReadyLike(), daoRequest));
      final var buildActionsFinalTargetSites = getBuildActionsFinalTargetSites();
      final var filterActionsNotEmpty = new Document(MG_MATCH,
          new Document(DaoSitesActionRepository.SITES, new Document(MG_NOT, new Document(MG_SIZE, 0))).append(
              DaoSitesActionRepository.NEED_ACTION_FROM, new Document(MG_NOT, new Document(MG_SIZE, 0))));
      final var keepActionFieldsOnly = getUnsetLocalTempVars();
      final var mergeIntoActions = getMergeIntoActions();
      sitesListingRepository.mongoCollection().aggregate(
          List.of(matchStepUpdate, buildActions, buildActionsFinalTargetSites, filterActionsNotEmpty,
              keepActionFieldsOnly, mergeIntoActions)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  private static Document getBuildActionsReadyLike() {
    return new Document(DaoSitesActionRepository.NEED_ACTION_FROM, new Document(MG_COND,
        List.of(new Document(MG_IN, List.of(READY_ACTION.getStatus(), "$" + LOCAL_NSTATUS)), new Document(MG_LET,
            new Document(MG_VARS, new Document(CCS_FIRST, LOCAL_O_EVENT).append(MG_FROM, new Document(MG_FILTER,
                new Document(MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(COND,
                    new Document(MG_EQ, List.of(MG_THIS + NSTATUS, READY_ACTION.getStatus())))))).append(IN,
                CCS_FROM + SITE)), new Document(MG_LET, new Document(MG_VARS,
            new Document(CCS_FIRST, LOCAL_O_EVENT).append(MG_FROM, new Document(MG_FILTER,
                new Document(MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(COND,
                    new Document(MG_EQ, List.of(MG_THIS + NSTATUS, UPDATE_ACTION.getStatus())))))).append(IN,
            CCS_FROM + SITE))))).append(DaoSitesActionRepository.NEED_ACTION, new Document(MG_COND, List.of(
        new Document(MG_EQ, List.of(new Document(MG_SIZE, new Document(MG_FILTER,
                new Document(MG_INPUT, "$" + LOCAL_NSTATUS).append(COND, new Document(MG_OR,
                    List.of(new Document(MG_LTE, List.of(MgDaoReconciliationUtils.GG_THIS, DELETE_ACTION.getStatus())),
                        new Document(MG_GTE, List.of(MgDaoReconciliationUtils.GG_THIS, UPLOAD_ACTION.getStatus()))))))),
            0)), UPDATE_ACTION.getStatus(), UPLOAD_ACTION.getStatus()))).append(CCS_TEMP_SOURCE, new Document(MG_COND,
        List.of(new Document(MG_IN, List.of(READY_ACTION.getStatus(), "$" + LOCAL_NSTATUS)), READY_ACTION.getStatus(),
            UPDATE_ACTION.getStatus())));
  }

  private static Document getValidTargetSitesReadyLike() {
    return new Document(MG_LET, new Document(MG_VARS, new Document(CCS_FIRST, LOCAL_O_EVENT).append(MG_FROM,
        new Document(MG_FILTER, new Document(MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(COND,
            new Document(MG_NE, List.of(MG_THIS + NSTATUS, READY_ACTION.getStatus())))))).append(IN, CCS_FROM + SITE));
  }

  public void computeActionsUpload(final DaoRequest daoRequest,
                                   final AtomicReference<CcsDbException> exceptionAtomicReference) {
    try {
      // Sub step C: Latest = Upload and some are Ready or Update
      final var matchStepUpload = new Document(MG_MATCH,
          getBaseMatchActions(daoRequest).append(DaoSitesListingRepository.LOCAL + ".0." + NSTATUS,
              new Document(MG_IN, List.of(UPLOAD_ACTION.getStatus()))).append(MgDaoReconciliationUtils.LOCAL_NSTATUS,
              new Document(MG_IN, List.of(READY_ACTION.getStatus(), UPDATE_ACTION.getStatus()))));
      final var buildActions = new Document(MG_ADD_FIELDS,
          addPartialTargetSites(getBuildActionsUpload(), getValidTargetSitesReadyLike(), daoRequest));
      final var buildActionsFinalTargetSites = getBuildActionsFinalTargetSites();
      final var filterActionsNotEmpty = getFilterActionsNotEmpty();
      final var keepActionFieldsOnly = getUnsetLocalTempVars();
      final var mergeIntoActions = getMergeIntoActions();
      sitesListingRepository.mongoCollection().aggregate(
          List.of(matchStepUpload, buildActions, buildActionsFinalTargetSites, filterActionsNotEmpty,
              keepActionFieldsOnly, mergeIntoActions)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  private static Document getBuildActionsUpload() {
    return new Document(DaoSitesActionRepository.NEED_ACTION_FROM, new Document(MG_COND,
        List.of(new Document(MG_IN, List.of(READY_ACTION.getStatus(), "$" + LOCAL_NSTATUS)), new Document(MG_LET,
            new Document(MG_VARS, new Document(CCS_FIRST, LOCAL_O_EVENT).append(MG_FROM, new Document(MG_FILTER,
                new Document(MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(COND,
                    new Document(MG_EQ, List.of(MG_THIS + NSTATUS, READY_ACTION.getStatus())))))).append(IN,
                CCS_FROM + SITE)), new Document(MG_LET, new Document(MG_VARS,
            new Document(CCS_FIRST, LOCAL_O_EVENT).append(MG_FROM, new Document(MG_FILTER,
                new Document(MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(COND,
                    new Document(MG_EQ, List.of(MG_THIS + NSTATUS, UPDATE_ACTION.getStatus())))))).append(IN,
            CCS_FROM + SITE))))).append(DaoSitesActionRepository.NEED_ACTION, UPLOAD_ACTION.getStatus())
        .append(CCS_TEMP_SOURCE, new Document(MG_COND,
            List.of(new Document(MG_IN, List.of(READY_ACTION.getStatus(), "$" + LOCAL_NSTATUS)),
                READY_ACTION.getStatus(), new Document(MG_COND,
                    List.of(new Document(MG_IN, List.of(UPDATE_ACTION.getStatus(), "$" + LOCAL_NSTATUS)),
                        UPDATE_ACTION.getStatus(), 0)))));
  }

  private static Document getFilterActionsNotEmpty() {
    return new Document(MG_MATCH,
        new Document(DaoSitesActionRepository.SITES, new Document(MG_NOT, new Document(MG_SIZE, 0))).append(
                DaoSitesActionRepository.NEED_ACTION_FROM, new Document(MG_NOT, new Document(MG_SIZE, 0)))
            .append(DaoSitesActionRepository.NEED_ACTION, new Document(MG_NE, 0)));
  }

  public void computeActionsInvalidUpload(final DaoRequest daoRequest,
                                          final AtomicReference<CcsDbException> exceptionAtomicReference) {
    try {
      // Sub step D: Latest = Upload and no Ready or Update => Delete Action
      final var matchStepInvalidUpload = new Document(MG_MATCH,
          getBaseMatchActions(daoRequest).append(DaoSitesListingRepository.LOCAL + ".0." + NSTATUS,
              new Document(MG_IN, List.of(UPLOAD_ACTION.getStatus()))).append(MgDaoReconciliationUtils.LOCAL_NSTATUS,
              new Document(MG_NOT, new Document(MG_IN, List.of(READY_ACTION.getStatus(), UPDATE_ACTION.getStatus())))));
      final var buildActions = new Document(MG_ADD_FIELDS,
          addPartialTargetSites(getBuildActionsInvalidUpload(), getValidTargetSitesForDelete(), daoRequest));
      final var buildActionsFinalTargetSites = getBuildActionsFinalTargetSites();
      final var filterActionsNotEmpty = new Document(MG_MATCH,
          new Document(DaoSitesActionRepository.SITES, new Document(MG_NOT, new Document(MG_SIZE, 0))));
      final var keepActionFieldsOnly = getUnsetLocalTempVars();
      final var mergeIntoActions = getMergeIntoActions();
      sitesListingRepository.mongoCollection().aggregate(
          List.of(matchStepInvalidUpload, buildActions, buildActionsFinalTargetSites, filterActionsNotEmpty,
              keepActionFieldsOnly, mergeIntoActions)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  private static Document getBuildActionsInvalidUpload() {
    return new Document(DaoSitesActionRepository.NEED_ACTION_FROM, new BsonNull()).append(
            DaoSitesActionRepository.NEED_ACTION, ERROR_ACTION.getStatus())
        .append(DaoSitesActionRepository.SITES, getValidTargetSitesForDelete());
  }

  private static Document addPartialTargetSites(final Document addVarStep, final Document validTarget,
                                                final DaoRequest daoRequest) {
    return addVarStep.append("unknownStatusSites", new Document(MG_LET,
            new Document(MG_VARS, new Document("all", getAllSites(daoRequest))).append(IN, new Document(MG_FILTER,
                new Document(MG_INPUT, "$$all").append(COND, new Document(MG_NOT,
                    new Document(MG_IN, List.of(MgDaoReconciliationUtils.GG_THIS, "$" + LOCAL_SITE))))))))
        .append("tempSites", validTarget);
  }

  private static Document getUnsetLocalTempVars() {
    return new Document(MG_UNSET,
        List.of(DaoSitesListingRepository.LOCAL, CCS_TEMP_SOURCE, "tempSites", "unknownStatusSites"));
  }

  @Override
  public void countFinalActions(final DaoRequest daoRequest) throws CcsDbException {
    // Sub Step 3 Count
    try {
      long actions = sitesActionRepository.count(new Document(REQUESTID, daoRequest.getId()));
      daoRequest.setActions(actions);
      bulkMetrics.incrementCounter(actions, CENTRAL_RECONCILIATION, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_TO_ACTIONS);
      LOGGER.infof("Final actions %d items", actions);
      requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
          new DbUpdate().set(DaoRequestRepository.ACTIONS, actions));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void updateRequestFromRemoteListing(final DaoRequest daoRequest) throws CcsDbException {
    // Update Checked, Site ? or done previously
    final var original = requestRepository.findOne(DbQuery.idEquals(daoRequest.getId()));
    var originalList = original.getContextSitesDone();
    if (originalList == null || !originalList.contains(daoRequest.getCurrentSite())) {
      requestRepository.mongoCollection().updateOne(new Document(ID, daoRequest.getId()),
          new Document(MG_PUSH, new Document(DaoRequestRepository.CONTEXTSITESDONE, daoRequest.getCurrentSite())));
    }
  }

  /**
   * Step9: return iterator of actions to populate topic<br>
   * Index Actions: requestId, bucket
   */
  @Override
  public ClosingIterator<DaoSitesAction> getSitesActon(final DaoRequest daoRequest) throws CcsDbException {
    return sitesActionRepository.findIterator(
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
            new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket())));
  }

  /**
   * Once all pushed into topic
   */
  @Override
  public void cleanSitesAction(final DaoRequest daoRequest) throws CcsDbException {
    sitesActionRepository.delete(
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
            new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket())));
  }
}
