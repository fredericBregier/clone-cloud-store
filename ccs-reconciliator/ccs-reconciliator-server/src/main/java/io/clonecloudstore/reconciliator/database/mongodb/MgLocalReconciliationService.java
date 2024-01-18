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

package io.clonecloudstore.reconciliator.database.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.model.DbQueryAccessorHelper;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.mongo.MongoBulkInsertHelper;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.ReconciliatorProperties;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.database.model.LocalReconciliationService;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.model.AccessorStatus.DELETED;
import static io.clonecloudstore.accessor.model.AccessorStatus.ERR_DEL;
import static io.clonecloudstore.accessor.model.AccessorStatus.ERR_UPL;
import static io.clonecloudstore.accessor.model.AccessorStatus.READY;
import static io.clonecloudstore.accessor.model.AccessorStatus.UNKNOWN;
import static io.clonecloudstore.accessor.model.AccessorStatus.UPLOAD;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.CREATION;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.RSTATUS;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.STATUS;
import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.BUCKET;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.DB;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.DRIVER;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.EVENT;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NAME;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NSTATUS;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.REQUESTID;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.SITE;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.TABLE_NAME;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.DB_EVENT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.DB_NSTATUS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.DB_SITE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.DEFAULT_PK;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ADD_FIELDS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_AND;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ARRAY_ELEM_AT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_CONCAT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_COND;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_EXISTS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_IN;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INDEX_OF_ARRAY;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INSERT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INTO;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_LTE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MATCH;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MERGE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MULTIPLY;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ON;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_PROJECT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_RAND;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_REPLACE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_SET;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_TO_LONG;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_TO_STRING;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_UNSET;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_WHEN_MATCHED;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_WHEN_NOT_MATCHED;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.DELETED_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.DELETE_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.READY_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UNKNOWN_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UPDATE_ACTION;
import static io.clonecloudstore.reconciliator.model.ReconciliationAction.UPLOAD_ACTION;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgLocalReconciliationService implements LocalReconciliationService {
  private static final Logger LOGGER = Logger.getLogger(MgLocalReconciliationService.class);
  public static final String LOCAL_RECONCILIATOR = "local_reconciliator";
  private final MgDaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;
  private final MgDaoNativeListingRepository nativeListingRepository;
  private final MgDaoSitesListingRepository sitesListingRepository;
  private final MgDaoRequestRepository requestRepository;
  private final BulkMetrics bulkMetrics;

  public MgLocalReconciliationService(final MgDaoAccessorObjectRepository objectRepository,
                                      final MgDaoNativeListingRepository nativeListingRepository,
                                      final MgDaoSitesListingRepository sitesListingRepository,
                                      final MgDaoRequestRepository requestRepository, final BulkMetrics bulkMetrics) {
    this.objectRepository = objectRepository;
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
    this.nativeListingRepository = nativeListingRepository;
    this.sitesListingRepository = sitesListingRepository;
    this.requestRepository = requestRepository;
    this.bulkMetrics = bulkMetrics;
  }

  /**
   * Clean Up Native Listing and Objects from status Object<br>
   * Index Objects: Bucket, Site, Status, creation
   */
  @Override
  public void step1CleanUpObjectsNativeListings(final DaoRequest daoPreviousRequest) throws CcsDbException {
    step1SubStep1CleanUpStatusUnknownObjectsNativeListings(daoPreviousRequest);
    step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(daoPreviousRequest);
    step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(daoPreviousRequest);
  }

  private void internalCleanUpObjects(final List<String> listId,
                                      final AtomicReference<CcsDbException> possibleDbException) {
    try {
      var del = objectRepository.delete(new DbQuery(RestQuery.QUERY.IN, ID, listId));
      LOGGER.debugf("Deleted: %d", del);
    } catch (CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } finally {
      listId.clear();
    }
  }

  /**
   * Index Native: bucket, site, name (requestId)
   */
  private void internalCleanUpNativeListing(final DaoRequest daoPreviousRequest, final List<String> listName,
                                            final AtomicReference<CcsDbException> possibleDbException) {
    final var deleteFilter = daoPreviousRequest.getId() != null ?
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoPreviousRequest.getBucket()),
            new DbQuery(RestQuery.QUERY.IN, NAME, listName),
            new DbQuery(RestQuery.QUERY.EQ, DB_SITE, ServiceProperties.getAccessorSite()),
            new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoPreviousRequest.getId())) :
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoPreviousRequest.getBucket()),
            new DbQuery(RestQuery.QUERY.IN, NAME, listName),
            new DbQuery(RestQuery.QUERY.EQ, DB_SITE, ServiceProperties.getAccessorSite()));
    try {
      var del = nativeListingRepository.delete(deleteFilter);
      LOGGER.debugf("Deleted: %d", del);
    } catch (CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } finally {
      listName.clear();
    }
  }

  /**
   * Clean Up Native Listing and Objects from status Object<br>
   * Remove UNKNOWN status<br>
   * Index Objects: Bucket, Site, Status
   */
  public void step1SubStep1CleanUpStatusUnknownObjectsNativeListings(final DaoRequest daoPreviousRequest)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    final var listName = new ArrayList<String>(MongoBulkInsertHelper.MAX_BATCH);
    final var listId = new ArrayList<String>(MongoBulkInsertHelper.MAX_BATCH);
    objectRepository.mongoCollection().aggregate(List.of(new Document(MG_MATCH,
        new Document(BUCKET, daoPreviousRequest.getBucket()).append(SITE, ServiceProperties.getAccessorSite())
            .append(STATUS, UNKNOWN.name())))).forEach(object -> {
      listName.add(object.getName());
      listId.add(object.getId());
      if (listId.size() >= MongoBulkInsertHelper.MAX_BATCH) {
        internalCleanUpObjects(listId, possibleDbException);
      }
      if (listName.size() >= MongoBulkInsertHelper.MAX_BATCH) {
        internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
      }
    });
    if (!listId.isEmpty()) {
      internalCleanUpObjects(listId, possibleDbException);
    }
    if (!listName.isEmpty()) {
      internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
    // Update all RSTATUS using current STATUS
    objectRepository.mongoCollection().updateMany(new Document(), List.of(new Document(MG_SET, new Document(RSTATUS,
        new Document(MgDaoReconciliationUtils.MG_INDEX_OF_ARRAY, List.of(STATUS_NAME_ORDERED, "$" + STATUS))))));
  }

  /**
   * Clean Up Objects ReconciliationStatus from status Object<br>
   * Index Objects: Bucket, Site, Status, creation
   */
  public void step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(final DaoRequest daoPreviousRequest)
      throws CcsDbException {
    try {
      Document filter = getFilterPreviousRequest(daoPreviousRequest);
      var update = List.of(new Document(MG_SET, new Document(RSTATUS, new Document(MG_ARRAY_ELEM_AT, List.of(
          List.of(UNKNOWN_RANK, TO_UPDATE_RANK, READY_RANK, TO_UPDATE_RANK, DELETING_RANK, DELETED_RANK, DELETING_RANK),
          new Document(MG_INDEX_OF_ARRAY, List.of(STATUS_NAME_ORDERED, "$" + STATUS)))))));
      objectRepository.mongoCollection().updateMany(filter, update);
    } catch (final RuntimeException e) {
      LOGGER.error(e, e);
      throw new CcsDbException(e);
    }
  }

  private static Document getFilterPreviousRequest(final DaoRequest daoPreviousRequest) {
    if (daoPreviousRequest.getStart() != null) {
      return new Document(MG_AND, List.of(new Document(BUCKET, daoPreviousRequest.getBucket()),
          new Document(SITE, ServiceProperties.getAccessorSite()),
          new Document(CREATION, new Document(MG_LTE, daoPreviousRequest.getStart()))));
    }
    return new Document(MG_AND, List.of(new Document(BUCKET, daoPreviousRequest.getBucket()),
        new Document(SITE, ServiceProperties.getAccessorSite())));
  }

  /**
   * Clean Up Native Listing from status Object<br>
   * Remove UNKNOWN, UPLOAD, ERR_UPL, DELETED, ERR_DEL items<br>
   * Index Objects: Bucket, Site, Status
   */
  public void step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(final DaoRequest daoPreviousRequest)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    final var listName = new ArrayList<String>(MongoBulkInsertHelper.MAX_BATCH);
    // UNKNOWN, ERR_UPL, DELETED, ERR_DEL status on NATIVE only
    try {
      objectRepository.mongoCollection().aggregate(List.of(new Document(MG_MATCH,
              new Document(BUCKET, daoPreviousRequest.getBucket()).append(SITE, ServiceProperties.getAccessorSite())
                  .append(STATUS, new Document(MG_IN,
                      List.of(UNKNOWN.name(), UPLOAD.name(), ERR_UPL.name(), DELETED.name(), ERR_DEL.name()))))))
          .forEach(object -> {
            listName.add(object.getName());
            if (listName.size() >= MongoBulkInsertHelper.MAX_BATCH) {
              internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
            }
          });
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    }
    if (!listName.isEmpty()) {
      internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
  }

  private List<Document> createIdAggregate() {
    // Use 2 longs to  create a random UUID
    final var addFieldStep1 = new Document(MG_ADD_FIELDS, new Document("val1", new Document(MG_TO_LONG,
        new Document(MG_MULTIPLY, List.of(new Document(MG_RAND, new Document()), 10000000000000L)))).append("val2",
        new Document(MG_TO_LONG,
            new Document(MG_MULTIPLY, List.of(new Document(MG_RAND, new Document()), 10000000000000L)))));
    final var addFieldStep2 = new Document(MG_ADD_FIELDS, new Document(ID,
        new Document(MG_CONCAT, List.of(new Document(MG_TO_STRING, "$val1"), new Document(MG_TO_STRING, "$val2")))));
    final var unsetStep3 = new Document(MG_UNSET, List.of("val1", "val2"));
    return List.of(addFieldStep1, addFieldStep2, unsetStep3);
  }

  /**
   * Get Old listing to restart from (for each site): Optional step (if accepting eventual mistakes on old data)<br>
   * Index Native: requestId, bucket<br>
   * Step2: Copy NativeListing with new RequestId (or Replace requestId)
   */
  @Override
  public void step2ContinueFromPreviousRequest(final String requestId, final DaoRequest daoRequest,
                                               final boolean replaceOldRequest) throws CcsDbException {
    final List<Document> aggregate = new ArrayList<>(6);
    final var matchStep1 =
        new Document(MG_MATCH, new Document(REQUESTID, requestId).append(BUCKET, daoRequest.getBucket()));
    final var addFieldStep2 = new Document(MG_ADD_FIELDS, new Document(REQUESTID, daoRequest.getId()));
    aggregate.add(matchStep1);
    aggregate.add(addFieldStep2);
    if (replaceOldRequest) {
      final var mergeStep3 = new Document(MG_MERGE,
          new Document(MG_INTO, TABLE_NAME).append(MG_ON, ID).append(MG_WHEN_MATCHED, MG_REPLACE)
              .append(MG_WHEN_NOT_MATCHED, MG_INSERT));
      aggregate.add(mergeStep3);
    } else {
      aggregate.addAll(createIdAggregate());
      final var mergeStep4 = new Document(MG_MERGE,
          new Document(MG_INTO, TABLE_NAME).append(MG_ON, DEFAULT_PK).append(MG_WHEN_MATCHED, MG_REPLACE)
              .append(MG_WHEN_NOT_MATCHED, MG_INSERT));
      aggregate.add(mergeStep4);
    }
    try {
      nativeListingRepository.mongoCollection().aggregate(aggregate).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Listing according to filter such as now > dateFrom (DB), updating existing info<br>
   * Step3: From Db Objects into NativeListing local step<br>
   * Index Objects: site, bucket, event<br>
   * Index Native: requestId, bucket, name
   */
  @Override
  public void step3SaveNativeListingDb(final DaoRequest daoRequest) throws CcsDbException {
    // Insert into NativeListing select from AccessorObject according to filter from DB
    final DbQuery filterQuery = DbQueryAccessorHelper.getDbQuery(daoRequest.getFilter());
    final var filter = new Document(DaoAccessorObjectRepository.SITE, ServiceProperties.getAccessorSite()).append(
        DaoAccessorObjectRepository.BUCKET, daoRequest.getBucket());
    if (filterQuery != null && !filter.isEmpty()) {
      filter.putAll(filterQuery.getBson().toBsonDocument());
    }

    final var matchStep1 = new Document(MG_MATCH, filter);
    final var addFieldsStep2 = new Document(MG_ADD_FIELDS, new Document(REQUESTID, daoRequest.getId()).append(DB,
        new Document(SITE, "$" + SITE).append(NSTATUS, "$" + RSTATUS).append(EVENT, "$" + CREATION)));
    final var projectStep3 = new Document(MG_PROJECT,
        new Document(ID, 1L).append(BUCKET, 1L).append(NAME, 1L).append(DB, 1L).append(REQUESTID, 1L));
    // Special attention on adding to existing Driver if deleted => no driver
    final var mergeStep4 = new Document(MG_MERGE, new Document(MG_INTO, TABLE_NAME).append(MG_ON, DEFAULT_PK)
        .append(MG_WHEN_MATCHED, List.of(new Document(MG_ADD_FIELDS,
            new Document(DB, MgDaoReconciliationUtils.MG_NEW + DB).append(DRIVER, new Document(MG_COND, List.of(
                new Document(MG_IN, List.of(MgDaoReconciliationUtils.MG_NEW + MgDaoReconciliationUtils.DB_NSTATUS,
                    List.of(DELETING_RANK, DELETED_RANK, ERR_DEL_RANK))), new BsonUndefined(), "$" + DRIVER))))))
        .append(MG_WHEN_NOT_MATCHED, MG_INSERT));

    try {
      objectRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4))
          .allowDiskUse(true).first();
      final long countDb = nativeListingRepository.count(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, DB_SITE, ServiceProperties.getAccessorSite())));
      daoRequest.setCheckedDb(countDb);
      bulkMetrics.incrementCounter(countDb, LOCAL_RECONCILIATOR, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_FROM_DB);
      requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
          new DbUpdate().set(DaoRequestRepository.CHECKED_DB, countDb));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Listing according to filter such as now > dateFrom (DRIVER), updating existing info
   * Step4: From Driver to Native<br>
   * Index Native: requestId, bucket, name
   */
  @Override
  public void step4SaveNativeListingDriver(final DaoRequest daoRequest) throws CcsDbException {
    // Insert into NativeListing select from AccessorObject according to filter from Driver
    Iterator<StorageObject> iteratorDriver;
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    final AtomicLong countEntries = new AtomicLong();
    if (daoRequest.getFilter() != null) {
      try (final var driver = storageDriverFactory.getInstance()) {
        iteratorDriver = driver.objectsIteratorInBucket(daoRequest.getBucket(), daoRequest.getFilter().getNamePrefix(),
            daoRequest.getFilter().getCreationAfter(), daoRequest.getFilter().getCreationBefore());
        step4WithBulkOperation(iteratorDriver, daoRequest, possibleDbException, countEntries);
      } catch (final DriverException e) {
        throw new CcsDbException(e);
      }
    } else {
      try (final var driver = storageDriverFactory.getInstance()) {
        iteratorDriver = driver.objectsIteratorInBucket(daoRequest.getBucket());
        step4WithBulkOperation(iteratorDriver, daoRequest, possibleDbException, countEntries);
      } catch (final DriverException e) {
        throw new CcsDbException(e);
      }
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
    daoRequest.setCheckedDriver(countEntries.get());
    bulkMetrics.incrementCounter(countEntries.get(), LOCAL_RECONCILIATOR, BulkMetrics.KEY_OBJECT,
        BulkMetrics.TAG_FROM_DRIVER);
    requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
        new DbUpdate().set(DaoRequestRepository.CHECKED_DRIVER, countEntries));
  }

  private void step4WithBulkOperation(final Iterator<StorageObject> iteratorDriver, final DaoRequest daoRequest,
                                      final AtomicReference<CcsDbException> possibleDbException,
                                      final AtomicLong countEntries) {
    try {
      final BlockingQueue<List<MgDaoNativeListing>> blockingQueue = new ArrayBlockingQueue<>(10);
      var semaphore = new Semaphore(0);
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(
          () -> addToBulkUpdateOrUpsertQueue(daoRequest, countEntries, blockingQueue, semaphore, possibleDbException));
      List<MgDaoNativeListing> listings = new ArrayList<>(MongoBulkInsertHelper.MAX_BATCH);
      try {
        while (iteratorDriver.hasNext()) {
          final var storageObject = iteratorDriver.next();
          final var dao = (MgDaoNativeListing) new MgDaoNativeListing().setRequestId(daoRequest.getId())
              .setBucket(storageObject.bucket()).setName(storageObject.name()).setDriver(
                  new SingleSiteObject(ServiceProperties.getAccessorSite(), READY_RANK, storageObject.creationDate()));
          listings.add(dao);
          if (listings.size() >= MongoBulkInsertHelper.MAX_BATCH) {
            final List<MgDaoNativeListing> finalListings = listings;
            blockingQueue.put(finalListings);
            listings = new ArrayList<>(MongoBulkInsertHelper.MAX_BATCH);
          }
          if (possibleDbException.get() != null) {
            return;
          }
        }
        if (!listings.isEmpty()) {
          blockingQueue.put(listings);
        }
      } finally {
        blockingQueue.put(Collections.emptyList());
      }
      semaphore.acquire();
      nativeListingRepository.flushAll();
    } catch (final RuntimeException | InterruptedException e) {// NOSONAR intentional
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } finally {
      SystemTools.consumeAll(iteratorDriver);
    }
  }

  private void addToBulkUpdateOrUpsertQueue(final DaoRequest daoRequest, final AtomicLong countEntries,
                                            final BlockingQueue<List<MgDaoNativeListing>> blockingQueue,
                                            final Semaphore semaphore,
                                            final AtomicReference<CcsDbException> possibleDbException) {
    try {
      final var nbThreads = ReconciliatorProperties.getReconciliatorThreads();
      final Runnable[] runnables = new Runnable[nbThreads];
      Arrays.fill(runnables,
          (Runnable) () -> runnableBulkUpdateOrUpsert(daoRequest, countEntries, blockingQueue, possibleDbException));
      MgDaoReconciliationUtils.runInThread(possibleDbException, runnables);
    } finally {
      semaphore.release();
    }
  }

  private void runnableBulkUpdateOrUpsert(final DaoRequest daoRequest, final AtomicLong countEntries,
                                          final BlockingQueue<List<MgDaoNativeListing>> blockingQueue,
                                          final AtomicReference<CcsDbException> possibleDbException) {
    try {
      while (true) {
        var list = blockingQueue.take();
        if (list.isEmpty()) {
          // End and inform others
          blockingQueue.put(list);
          return;
        }
        addToBulkUpdateOrUpsert(list, daoRequest, countEntries, possibleDbException);
        if (possibleDbException.get() != null) {
          return;
        }
      }
    } catch (final RuntimeException | InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    }
  }

  private void addToBulkUpdateOrUpsert(final List<MgDaoNativeListing> listings, final DaoRequest daoRequest,
                                       final AtomicLong countEntries,
                                       final AtomicReference<CcsDbException> possibleDbException) {
    try {
      final Map<String, String> mapFound = HashMap.newHashMap(listings.size());
      final var names = listings.stream().map(MgDaoNativeListing::getName).toList();
      nativeListingRepository.findStream(
              new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
                  new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
                  new DbQuery(RestQuery.QUERY.IN, NAME, names)))
          .forEach(daoNativeListing -> mapFound.put(daoNativeListing.getName(), daoNativeListing.getId()));
      for (var dao : listings) {
        var foundId = mapFound.remove(dao.getName());
        if (foundId != null) {
          dao.setId(foundId);
          final var find = new Document(ID, foundId);
          nativeListingRepository.addToUpsertBulk(find, dao);
        } else {
          dao.setId(GuidLike.getGuid());
          nativeListingRepository.addToInsertBulk(dao);
        }
        countEntries.getAndIncrement();
      }
      nativeListingRepository.flushAll();
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } finally {
      listings.clear();
    }
  }

  /**
   * Compare Native listing with DB and DRIVER (both or only one)<br>
   * Step5: Complete DB without Driver from NativeListing into SitesListing Local step<br>
   * Index Native: requestId, bucket, db, driver.event (optional)<br>
   * Index Objects: site, bucket, name<br>
   * Index Native: requestId, bucket, db.site, driver (optional)<br>
   * Index Native: requestId, bucket, db.site, driver.site<br>
   * Index Sites: requestId, bucket, name
   */
  @Override
  public void step5CompareNativeListingDbDriver(final DaoRequest daoRequest) throws CcsDbException {
    final AtomicReference<CcsDbException> ccsDbExceptionAtomicReference = new AtomicReference<>();
    final BlockingQueue<List<DaoSitesListing>> blockingQueue = new ArrayBlockingQueue<>(10);
    final Semaphore semaphore = new Semaphore(0);
    try {
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(
          () -> updateObjectFromDriverQueue(blockingQueue, semaphore, ccsDbExceptionAtomicReference));
      MgDaoReconciliationUtils.runInThread(ccsDbExceptionAtomicReference,
          () -> step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest, ccsDbExceptionAtomicReference),
          () -> step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest, ccsDbExceptionAtomicReference,
              blockingQueue), () -> step53UpdateWhereNoDriverIntoObjects(daoRequest, ccsDbExceptionAtomicReference),
          () -> step54UpsertWhereNoDriverIntoSiteListing(daoRequest, ccsDbExceptionAtomicReference),
          () -> step55UpdateBothDbDriverIntoObjects(daoRequest, ccsDbExceptionAtomicReference),
          () -> step56UpdateBothDbDriverIntoSiteListing(daoRequest, ccsDbExceptionAtomicReference, blockingQueue));
    } finally {
      try {
        blockingQueue.put(Collections.emptyList());
      } catch (final InterruptedException e) { // NOSONAR intentional
        LOGGER.warn(e);
        ccsDbExceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    }
    try {
      semaphore.acquire();
      objectRepository.flushAll();
      sitesListingRepository.flushAll();
      nativeListingRepository.flushAll();
    } catch (final InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      ccsDbExceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
    if (ccsDbExceptionAtomicReference.get() != null) {
      throw ccsDbExceptionAtomicReference.get();
    }
    step58CountFinalSiteListing(daoRequest);
  }

  public void step51InsertMissingObjectsFromExistingDriverIntoObjects(final DaoRequest daoRequest,
                                                                      final AtomicReference<CcsDbException> exceptionAtomicReference) {
    try {
      // Insert into SitesListing from NativeListing with DB absent but DRIVER present
      // Sub Step 1: Fix Object missing (DB missing), Update later on (asynchronous step to read from Driver Metadata)
      // Update SiteListing in subStep 2
      final var matchStep1 = getMatchDbMissingDriverExistForCurrentSite(daoRequest);
      final var addFieldsStep2 = new Document(MG_ADD_FIELDS,
          new Document(CREATION, "$" + MgDaoReconciliationUtils.DRIVER_EVENT).append(SITE,
                  "$" + MgDaoReconciliationUtils.DRIVER_SITE).append(STATUS, STATUS_NAME_ORDERED.get(READY_RANK))
              .append(RSTATUS, TO_UPDATE_RANK));
      final var unsetStep3 = getUnsetDriverDbRequestId();
      // Only if not exists in Objects table
      final var mergeStep4 = new Document(MG_MERGE,
          new Document(MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MG_ON, List.of(SITE, BUCKET, NAME))
              .append(MG_WHEN_MATCHED, MgDaoReconciliationUtils.MG_KEEP_EXISTING)
              .append(MG_WHEN_NOT_MATCHED, MG_INSERT));
      nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, unsetStep3, mergeStep4))
          .allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  private static Document getUnsetDriverDbRequestId() {
    return new Document(MG_UNSET, List.of(DRIVER, DB, REQUESTID));
  }

  private static Document getDbMissingDriverExistForCurrentSite(final DaoRequest daoRequest) {
    return new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
        .append(DB, new Document(MG_EXISTS, false))
        .append(MgDaoReconciliationUtils.DRIVER_EVENT, new Document(MG_EXISTS, true))
        .append(MgDaoReconciliationUtils.DRIVER_SITE,
            new Document(MG_IN, List.of(ServiceProperties.getAccessorSite())));
  }

  private static Document getMatchDbMissingDriverExistForCurrentSite(final DaoRequest daoRequest) {
    return new Document(MG_MATCH, getDbMissingDriverExistForCurrentSite(daoRequest));
  }

  public void step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(final DaoRequest daoRequest,
                                                                          final AtomicReference<CcsDbException> exceptionAtomicReference,
                                                                          final BlockingQueue<List<DaoSitesListing>> blockingQueue) {
    try {
      // Sub Step 2: Merge into SitesListing
      final var query = getDbMissingDriverExistForCurrentSite(daoRequest);
      final var matchStep1 = new Document(MG_MATCH, query);
      // Special status for Action: TO_UPDATE but check will see that Object is there
      final var addFieldsStep2 = new Document(MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL, List.of(
          new Document(SITE, "$" + MgDaoReconciliationUtils.DRIVER_SITE).append(NSTATUS, UPDATE_ACTION.getStatus())
              .append(EVENT, "$" + MgDaoReconciliationUtils.DRIVER_EVENT))));
      final var projectStep3 = getUnsetDbDriver();
      // Special attention on adding to existing array if any
      final var mergeStep4 = getMergeIntoSitesListingTakingExistingArray();
      nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4))
          .allowDiskUse(true).first();
      // If any try to reload Object from Driver metadata
      reloadObjectFromDriver(daoRequest, exceptionAtomicReference, blockingQueue, query);
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, e);
    } catch (final RuntimeException | InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  private void reloadObjectFromDriver(final DaoRequest daoRequest,
                                      final AtomicReference<CcsDbException> exceptionAtomicReference,
                                      final BlockingQueue<List<DaoSitesListing>> blockingQueue, final Document query)
      throws InterruptedException, CcsDbException {
    if (blockingQueue == null) {
      return;
    }
    try (final var iterator = nativeListingRepository.findIterator(query)) {
      final int max = MongoBulkInsertHelper.MAX_BATCH;
      final List<String> toAdds = new ArrayList<>(max);
      while (iterator.hasNext()) {
        final var dao = iterator.next();
        toAdds.add(dao.getName());
        if (toAdds.size() >= max) {
          final List<DaoSitesListing> toAddsBis = sitesListingRepository.findStream(
              new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
                  new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
                  new DbQuery(RestQuery.QUERY.IN, NAME, toAdds))).toList();
          blockingQueue.put(toAddsBis);
          toAdds.clear();
        }
        if (exceptionAtomicReference.get() != null) {
          throw exceptionAtomicReference.get();
        }
      }
      if (!toAdds.isEmpty()) {
        final List<DaoSitesListing> toAddsBis = sitesListingRepository.findStream(
            new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
                new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
                new DbQuery(RestQuery.QUERY.IN, NAME, toAdds))).toList();
        blockingQueue.put(toAddsBis);
      }
    }
  }

  private static Document getMergeIntoSitesListingTakingExistingArray() {
    return new Document(MG_MERGE, new Document(MG_INTO, DaoSitesListingRepository.TABLE_NAME).append(MG_ON, DEFAULT_PK)
        .append(MG_WHEN_MATCHED, List.of(new Document(MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL,
            new Document(MgDaoReconciliationUtils.MG_IF_NULL, List.of(
                new Document(MgDaoReconciliationUtils.MG_CONCAT_ARRAYS, List.of("$" + DaoSitesListingRepository.LOCAL,
                    MgDaoReconciliationUtils.MG_NEW + DaoSitesListingRepository.LOCAL)),
                MgDaoReconciliationUtils.MG_NEW + DaoSitesListingRepository.LOCAL))))))
        .append(MG_WHEN_NOT_MATCHED, MG_INSERT));
  }

  private static Document getUnsetDbDriver() {
    return new Document(MgDaoReconciliationUtils.MG_UNSET, List.of(DB, DRIVER));
  }

  public void step53UpdateWhereNoDriverIntoObjects(final DaoRequest daoRequest,
                                                   final AtomicReference<CcsDbException> exceptionAtomicReference) {
    // Only if exists in Objects table
    final var mergeStep4 = getMergeIntoObjectsMergeOrDiscard();
    MgDaoReconciliationUtils.runInThread(exceptionAtomicReference, () -> {
      try {
        // Sub Step 1: Fix Object UPLOAD, Update SiteListing in subStep 2
        final var matchStep1 = new Document(MG_MATCH,
            new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
                .append(MgDaoReconciliationUtils.MG_AND,
                    List.of(new Document(DB_SITE, new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                        new Document(MgDaoReconciliationUtils.DB_NSTATUS,
                            new Document(MG_IN, List.of(UPLOAD_RANK, READY_RANK, ERR_UPL_RANK, TO_UPDATE_RANK))),
                        new Document(DRIVER, new Document(MG_EXISTS, false)))));
        final var addFieldsStep2 = new Document(MG_ADD_FIELDS,
            new Document(STATUS, STATUS_NAME_ORDERED.get(UPLOAD_RANK)).append(RSTATUS, TO_UPDATE_RANK)
                .append(SITE, "$" + DB_SITE));
        final var unsetStep3 = getUnsetDriverDbRequestId();
        nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, unsetStep3, mergeStep4))
            .allowDiskUse(true).first();
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    }, () -> {
      try {
        // Sub Step 2: Fix Object DELETED, Update SiteListing in subStep 2
        final var matchStep1 = new Document(MG_MATCH,
            new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
                .append(MgDaoReconciliationUtils.MG_AND,
                    List.of(new Document(DB_SITE, new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                        new Document(MgDaoReconciliationUtils.DB_NSTATUS,
                            new Document(MG_IN, List.of(DELETING_RANK, DELETED_RANK, ERR_DEL_RANK))),
                        new Document(DRIVER, new Document(MG_EXISTS, false)))));
        final var addFieldsStep2 = new Document(MG_ADD_FIELDS,
            new Document(STATUS, STATUS_NAME_ORDERED.get(DELETED_RANK)).append(RSTATUS, DELETED_RANK)
                .append(SITE, "$" + DB_SITE));
        final var unsetStep3 = getUnsetDriverDbRequestId();
        nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, unsetStep3, mergeStep4))
            .allowDiskUse(true).first();
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    });
  }

  private static Document getMergeIntoObjectsMergeOrDiscard() {
    return new Document(MG_MERGE,
        new Document(MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MG_ON, List.of(SITE, BUCKET, NAME))
            .append(MG_WHEN_MATCHED, MgDaoReconciliationUtils.MG_MERGE_MATCHED)
            .append(MG_WHEN_NOT_MATCHED, MgDaoReconciliationUtils.MG_DISCARD));
  }

  public void step54UpsertWhereNoDriverIntoSiteListing(final DaoRequest daoRequest,
                                                       final AtomicReference<CcsDbException> exceptionAtomicReference) {
    try {
      // Sub Step2: Insert into SitesListing from NativeListing with DRIVER absent but DB present
      final var matchStep1 = new Document(MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoReconciliationUtils.MG_AND,
                  List.of(new Document(DB_SITE, new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                      new Document(DRIVER, new Document(MG_EXISTS, false)))));
      final var addFieldsStep2 = new Document(MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL, List.of(
          new Document(MG_COND,
              List.of(new Document(MG_IN, List.of("$" + MgDaoReconciliationUtils.DB_NSTATUS, List.of(UNKNOWN_RANK))),
                  new Document(SITE, "$" + DB_SITE).append(NSTATUS, UNKNOWN_ACTION.getStatus())
                      .append(EVENT, "$" + MgDaoReconciliationUtils.DB_EVENT), new Document(MG_COND, List.of(
                      new Document(MG_IN, List.of("$" + MgDaoReconciliationUtils.DB_NSTATUS,
                          List.of(UPLOAD_RANK, READY_RANK, ERR_UPL_RANK, TO_UPDATE_RANK))),
                      new Document(SITE, "$" + DB_SITE).append(NSTATUS, UPLOAD_ACTION.getStatus())
                          .append(EVENT, "$" + MgDaoReconciliationUtils.DB_EVENT),
                      new Document(SITE, "$" + DB_SITE).append(NSTATUS, DELETED_ACTION.getStatus())
                          .append(EVENT, "$" + MgDaoReconciliationUtils.DB_EVENT))))))));
      final var projectStep3 = getUnsetDbDriver();
      // Special attention on adding to existing array if any
      final var mergeStep4 = getMergeIntoSitesListingTakingExistingArray();
      nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4))
          .allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }

  public void step55UpdateBothDbDriverIntoObjects(final DaoRequest daoRequest,
                                                  final AtomicReference<CcsDbException> exceptionAtomicReference) {
    MgDaoReconciliationUtils.runInThread(exceptionAtomicReference, () -> {
      try {
        // Sub Step 1: Fix Object READY, Update SiteListing in subStep 2
        final var matchStep1 = new Document(MG_MATCH, getFilterBothDbDriverToUpdate(daoRequest));
        final var addFieldsStep2 = new Document(MG_ADD_FIELDS,
            new Document(STATUS, STATUS_NAME_ORDERED.get(READY_RANK)).append(RSTATUS, TO_UPDATE_RANK)
                .append(SITE, "$" + DB_SITE));
        final var unsetStep3 = getUnsetDriverDbRequestId();
        // Only if exists in Objects table
        final var mergeStep4 = getMergeIntoObjectsMergeOrDiscard();
        nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, unsetStep3, mergeStep4))
            .allowDiskUse(true).first();
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    }, () -> {
      try {
        // Sub Step 2: Fix Object DELETING, Update SiteListing in subStep 2
        final var matchStep1 = new Document(MG_MATCH, getFilterBothDbDriverToDelete(daoRequest));
        final var addFieldsStep2 = new Document(MG_ADD_FIELDS,
            new Document(STATUS, STATUS_NAME_ORDERED.get(DELETING_RANK)).append(RSTATUS, DELETING_RANK)
                .append(SITE, "$" + DB_SITE));
        final var unsetStep3 = getUnsetDriverDbRequestId();
        // Only if exists in Objects table
        final var mergeStep4 = getMergeIntoObjectsMergeOrDiscard();
        nativeListingRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, unsetStep3, mergeStep4))
            .allowDiskUse(true).first();
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    });
  }

  private static Document getFilterBothDbDriverToDelete(final DaoRequest daoRequest) {
    return new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
        .append(MgDaoReconciliationUtils.MG_AND,
            List.of(new Document(DB_SITE, new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                new Document(MgDaoReconciliationUtils.DB_NSTATUS,
                    new Document(MG_IN, List.of(DELETING_RANK, DELETED_RANK, ERR_DEL_RANK))),
                new Document(MgDaoReconciliationUtils.DRIVER_SITE,
                    new Document(MG_IN, List.of(ServiceProperties.getAccessorSite())))));
  }

  private static Document getFilterBothDbDriverToUpdate(final DaoRequest daoRequest) {
    return new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
        .append(MgDaoReconciliationUtils.MG_AND,
            List.of(new Document(DB_SITE, new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                new Document(MgDaoReconciliationUtils.DB_NSTATUS,
                    new Document(MG_IN, List.of(UPLOAD_RANK, ERR_UPL_RANK, TO_UPDATE_RANK))),
                new Document(MgDaoReconciliationUtils.DRIVER_SITE,
                    new Document(MG_IN, List.of(ServiceProperties.getAccessorSite())))));
  }

  public void step56UpdateBothDbDriverIntoSiteListing(final DaoRequest daoRequest,
                                                      final AtomicReference<CcsDbException> exceptionAtomicReference,
                                                      final BlockingQueue<List<DaoSitesListing>> blockingQueue) {
    MgDaoReconciliationUtils.runInThread(exceptionAtomicReference, () -> {
      try {
        // Sub Step4: Insert into SitesListing from NativeListing with both existing DB and Driver but not Object READY
        final var query = getFilterBothDbDriverToUpdate(daoRequest);
        final var matchStep1 = new Document(MG_MATCH, query);
        var action = UPDATE_ACTION.getStatus();
        final var addFieldsStep2 = getAddFieldsUsingAction(action);
        final var projectStep3 = getUnsetDbDriver();
        // Special attention on adding to existing array if any
        final var mergeStep4 = getMergeIntoSitesListingTakingExistingArray();

        nativeListingRepository.mongoCollection()
            .aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4)).allowDiskUse(true).first();

        // If any try to reload Object from Driver metadata
        reloadObjectFromDriver(daoRequest, exceptionAtomicReference, blockingQueue, query);
      } catch (final CcsDbException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, e);
      } catch (final RuntimeException | InterruptedException e) { // NOSONAR intentional
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    }, () -> {
      try {
        // Sub Step4bis: Insert into SitesListing from NativeListing with both existing DB and Driver and Object real
        // READY
        final var matchStep1 = new Document(MG_MATCH,
            new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
                .append(MgDaoReconciliationUtils.MG_AND,
                    List.of(new Document(DB_SITE, new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                        new Document(MgDaoReconciliationUtils.DB_NSTATUS, new Document(MG_IN, List.of(READY_RANK))),
                        new Document(MgDaoReconciliationUtils.DRIVER_SITE,
                            new Document(MG_IN, List.of(ServiceProperties.getAccessorSite()))))));
        var action = READY_ACTION.getStatus();
        final var addFieldsStep2 = getAddFieldsUsingAction(action);
        final var projectStep3 = getUnsetDbDriver();
        // Special attention on adding to existing array if any
        final var mergeStep4 = getMergeIntoSitesListingTakingExistingArray();

        nativeListingRepository.mongoCollection()
            .aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4)).allowDiskUse(true).first();
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    }, () -> {
      try {
        // Sub Step5: Insert into SitesListing from NativeListing with both existing DB and Driver and Object DELETING
        final var matchStep1 = new Document(MG_MATCH, getFilterBothDbDriverToDelete(daoRequest));
        var action = DELETE_ACTION.getStatus();
        final var addFieldsStep2 = getAddFieldsUsingAction(action);
        final var projectStep3 = getUnsetDbDriver();
        // Special attention on adding to existing array if any
        final var mergeStep4 = getMergeIntoSitesListingTakingExistingArray();

        nativeListingRepository.mongoCollection()
            .aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4)).allowDiskUse(true).first();
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
      }
    });
  }

  private static Document getAddFieldsUsingAction(final short action) {
    return new Document(MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL, List.of(
        new Document(SITE, "$" + DB_SITE).append(NSTATUS, action).append(EVENT, new Document(MG_COND, List.of(
            new Document(MgDaoReconciliationUtils.MG_GTE,
                List.of("$" + MgDaoReconciliationUtils.DB_EVENT, "$" + MgDaoReconciliationUtils.DRIVER_EVENT)),
            "$" + MgDaoReconciliationUtils.DB_EVENT, "$" + MgDaoReconciliationUtils.DRIVER_EVENT))))));
  }

  private void updateObjectFromDriverQueue(final BlockingQueue<List<DaoSitesListing>> blockingQueue,
                                           final Semaphore semaphore,
                                           final AtomicReference<CcsDbException> possibleDbException) {
    AtomicLong cptUpdateToReady = new AtomicLong();
    try {
      final var nbThreads = ReconciliatorProperties.getReconciliatorThreads();
      final Runnable[] runnables = new Runnable[nbThreads];
      Arrays.fill(runnables,
          (Runnable) () -> runnableMethodUpdateObjectFromDriverQueue(blockingQueue, possibleDbException,
              cptUpdateToReady));
      MgDaoReconciliationUtils.runInThread(possibleDbException, runnables);
    } finally {
      bulkMetrics.incrementCounter(cptUpdateToReady.get(), LOCAL_RECONCILIATOR, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_UPDATE_FROM_DRIVER);
      semaphore.release();
    }
  }

  private void runnableMethodUpdateObjectFromDriverQueue(final BlockingQueue<List<DaoSitesListing>> blockingQueue,
                                                         final AtomicReference<CcsDbException> possibleDbException,
                                                         final AtomicLong cptUpdateToReady) {
    try (final var driver = storageDriverFactory.getInstance()) {
      while (true) {
        var list = blockingQueue.take();
        if (list.isEmpty()) {
          // End and inform others
          blockingQueue.put(list);
          return;
        }
        for (final var item : list) {
          if (updateObjectFromDriver(possibleDbException, item, driver)) {
            cptUpdateToReady.incrementAndGet();
          }
        }
        objectRepository.flushAll();
        sitesListingRepository.flushAll();
        nativeListingRepository.flushAll();
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
    }
  }

  private boolean updateObjectFromDriver(final AtomicReference<CcsDbException> possibleDbException,
                                         final DaoSitesListing item, final DriverApi driver) {
    try {
      DaoAccessorObject object = objectRepository.findOne(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, BUCKET, item.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, NAME, item.getName())));
      final var storageObject = driver.objectGetMetadataInBucket(item.getBucket(), item.getName());
      object.setStatus(READY).setCreation(storageObject.creationDate()).setHash(storageObject.hash())
          .setSize(storageObject.size()).setExpires(storageObject.creationDate()).setMetadata(storageObject.metadata())
          .setRstatus(READY_ACTION.getStatus());
      objectRepository.addToUpsertBulk(new Document(ID, object.getId()), object);
      var local = item.getLocal().getFirst();
      var newOne = new SingleSiteObject(local.site(), READY_ACTION.getStatus(), local.event());
      item.getLocal().removeFirst();
      item.getLocal().add(newOne);
      sitesListingRepository.addToUpsertBulk(new Document(ID, item.getId()), item);
      var update = new Document(MG_SET,
          new Document(DB_NSTATUS, READY_RANK).append(DB_SITE, local.site()).append(DB_EVENT, local.event()));
      nativeListingRepository.addToUpdateBulk(
          new Document(REQUESTID, item.getRequestId()).append(BUCKET, item.getBucket()).append(NAME, item.getName()),
          update);
      return true;
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } catch (final DriverException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    }
    return false;
  }

  public void step58CountFinalSiteListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step 5 finalize count
      final long countDb = sitesListingRepository.count(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, MgDaoReconciliationUtils.LOCAL_SITE,
                  ServiceProperties.getAccessorSite())));
      daoRequest.setChecked(countDb);
      bulkMetrics.incrementCounter(countDb, LOCAL_RECONCILIATOR, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_TO_SITES_LISTING);
      requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
          new DbUpdate().set(DaoRequestRepository.CHECKED, countDb));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Used only if NativeListing is not to be kept
   */
  @Override
  public void cleanNativeListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      nativeListingRepository.delete(new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Get the local sites listing to send through network<br>
   * Step6: get all local sites listing<br>
   * Index Sites: requestId
   */
  @Override
  public ClosingIterator<DaoSitesListing> getSiteListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      bulkMetrics.incrementCounter(1, LOCAL_RECONCILIATOR, BulkMetrics.KEY_SITE, BulkMetrics.TAG_TO_REMOTE_SITE);
      return sitesListingRepository.findIterator(new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Used only if SitesListing is not to be kept
   */
  @Override
  public void cleanSitesListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      sitesListingRepository.delete(new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }
}
