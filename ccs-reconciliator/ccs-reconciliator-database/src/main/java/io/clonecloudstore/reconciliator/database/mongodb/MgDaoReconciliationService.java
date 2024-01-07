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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.model.DbQueryAccessorHelper;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObject;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoService;
import io.clonecloudstore.reconciliator.database.model.DaoSitesAction;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListing;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import io.clonecloudstore.reconciliator.model.SingleSiteObject;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.BsonUndefined;
import org.bson.Document;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.model.AccessorStatus.DELETED;
import static io.clonecloudstore.accessor.model.AccessorStatus.DELETING;
import static io.clonecloudstore.accessor.model.AccessorStatus.ERR_DEL;
import static io.clonecloudstore.accessor.model.AccessorStatus.ERR_UPL;
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
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.DB_SITE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.DEFAULT_PK;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MAXUPLOAD_SITE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_ADD_FIELDS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_CONCAT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_IN;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_INSERT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_INTO;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_LTE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_MATCH;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_MERGE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_MULTIPLY;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_ON;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_RAND;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_REPLACE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_SET;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_TO_LONG;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_TO_STRING;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_UNSET;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_WHEN_MATCHED;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoConstants.MG_WHEN_NOT_MATCHED;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgDaoReconciliationService implements DaoService {
  private static final Logger LOGGER = Logger.getLogger(MgDaoReconciliationService.class);
  private final MgDaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;
  private final MgDaoNativeListingRepository nativeListingRepository;
  private final MgDaoSitesListingRepository sitesListingRepository;
  private final MgDaoSitesActionRepository sitesActionRepository;
  private final MgDaoRequestRepository requestRepository;

  public MgDaoReconciliationService(final MgDaoAccessorObjectRepository objectRepository,
                                    final MgDaoNativeListingRepository nativeListingRepository,
                                    final MgDaoSitesListingRepository sitesListingRepository,
                                    final MgDaoSitesActionRepository sitesActionRepository,
                                    final MgDaoRequestRepository requestRepository) {
    this.objectRepository = objectRepository;
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
    this.nativeListingRepository = nativeListingRepository;
    this.sitesListingRepository = sitesListingRepository;
    this.sitesActionRepository = sitesActionRepository;
    this.requestRepository = requestRepository;
  }

  private void internalCleanUpObjects(final List<String> listId,
                                      final AtomicReference<CcsDbException> possibleDbException) {
    try {
      var del = objectRepository.delete(new DbQuery(RestQuery.QUERY.IN, ID, listId));
      LOGGER.debugf("Deleted: %d", del);
    } catch (CcsDbException e) {
      LOGGER.error(e);
      possibleDbException.set(e);
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
            new DbQuery(RestQuery.QUERY.EQ, MgDaoConstants.DB_SITE, ServiceProperties.getAccessorSite()),
            new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoPreviousRequest.getId())) :
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoPreviousRequest.getBucket()),
            new DbQuery(RestQuery.QUERY.IN, NAME, listName),
            new DbQuery(RestQuery.QUERY.EQ, MgDaoConstants.DB_SITE, ServiceProperties.getAccessorSite()));
    try {
      var del = nativeListingRepository.delete(deleteFilter);
      LOGGER.debugf("Deleted: %d", del);
    } catch (CcsDbException e) {
      LOGGER.error(e);
      possibleDbException.set(e);
    } finally {
      listName.clear();
    }
  }

  /**
   * Clean Up Native Listing and Objects from status Object<br>
   * Remove UNKNOWN status<br>
   * Index Objects: Bucket, Site, Status
   */
  @Override
  public void step1SubStep1CleanUpStatusUnknownObjectsNativeListings(final DaoRequest daoPreviousRequest)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    final var listName = new ArrayList<String>(1000);
    final var listId = new ArrayList<String>(1000);
    objectRepository.mongoCollection().aggregate(List.of(new Document(MG_MATCH,
        new Document(BUCKET, daoPreviousRequest.getBucket()).append(SITE, ServiceProperties.getAccessorSite())
            .append(STATUS, AccessorStatus.UNKNOWN.name())))).forEach(object -> {
      listName.add(object.getName());
      listId.add(object.getId());
      if (listId.size() >= 1000) {
        internalCleanUpObjects(listId, possibleDbException);
      }
      if (listName.size() >= 1000) {
        internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
      }
    });
    if (!listId.isEmpty()) {
      internalCleanUpObjects(listId, possibleDbException);
    }
    if (!listName.isEmpty()) {
      internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
    }
    // Update all RSTATUS using current STATUS
    objectRepository.mongoCollection().updateMany(new Document(), List.of(new Document(MG_SET, new Document(RSTATUS,
        new Document(MgDaoConstants.MG_INDEX_OF_ARRAY, List.of(STATUS_NAME_ORDERED, "$" + STATUS))))));
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
  }

  private void checkUploadedObject(final MgDaoAccessorObject object) throws CcsDbException {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.objectGetMetadataInBucket(object.getBucket(), object.getName());
      // Object exists in Driver: fix its status
      LOGGER.debugf("Find object in driver: %s %s", object.getBucket(), object.getName());
      object.setRstatus(READY_RANK);
      objectRepository.addToUpdateBulk(new Document(ID, object.getId()), object);
    } catch (DriverNotFoundException e) {
      // Not found so in error Upload
      LOGGER.debugf("Cannot find object in driver: %s %s", object.getBucket(), object.getName());
      object.setRstatus(ERR_UPL_RANK);
      objectRepository.addToUpdateBulk(new Document(ID, object.getId()), object);
    } catch (DriverException e) {
      // Cannot decide: log but ignore
      LOGGER.error(e.getMessage(), e);
    }
  }

  private void checkDeletedObject(final MgDaoAccessorObject object) throws CcsDbException {
    try (final var driver = storageDriverFactory.getInstance()) {
      driver.objectGetMetadataInBucket(object.getBucket(), object.getName());
      // Object exists in Driver: but shouldn't
      LOGGER.debugf("Find object in driver: %s %s", object.getBucket(), object.getName());
      object.setRstatus(DELETING_RANK);
      objectRepository.addToUpdateBulk(new Document(ID, object.getId()), object);
    } catch (DriverNotFoundException e) {
      // Not found so Deleted
      LOGGER.debugf("Cannot find object in driver: %s %s", object.getBucket(), object.getName());
      object.setRstatus(DELETED_RANK);
      objectRepository.addToUpdateBulk(new Document(ID, object.getId()), object);
    } catch (DriverException e) {
      // Cannot decide: log but ignore
      LOGGER.error(e.getMessage(), e);
    }
  }

  private static void nop() {
    // Empty
  }

  /**
   * Clean Up Objects from status Object<br>
   * Remove UPLOAD, ERR_UPL, DELETING, ERR_DEL items<br>
   * Index Objects: Bucket, Site, Status, creation
   */
  @Override
  public void step1SubStep2CleanUpStatusOlderUploadDeleteCheckObjectsNativeListings(final DaoRequest daoPreviousRequest)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    try {
      List<Document> aggregate;
      if (daoPreviousRequest.getStart() != null) {
        aggregate = List.of(new Document(MG_MATCH,
            new Document(BUCKET, daoPreviousRequest.getBucket()).append(SITE, ServiceProperties.getAccessorSite())
                .append(STATUS,
                    new Document(MG_IN, List.of(UPLOAD.name(), ERR_UPL.name(), DELETING.name(), ERR_DEL.name())))
                .append(CREATION, new Document(MG_LTE, daoPreviousRequest.getStart()))));
      } else {
        aggregate = List.of(new Document(MG_MATCH,
            new Document(BUCKET, daoPreviousRequest.getBucket()).append(SITE, ServiceProperties.getAccessorSite())
                .append(STATUS,
                    new Document(MG_IN, List.of(UPLOAD.name(), ERR_UPL.name(), DELETING.name(), ERR_DEL.name())))));
      }
      objectRepository.mongoCollection().aggregate(aggregate).forEach(object -> {
        try {
          switch (object.getStatus()) {
            case UPLOAD, ERR_UPL -> checkUploadedObject(object);
            case DELETING, ERR_DEL -> checkDeletedObject(object);
            default -> nop();
          }
        } catch (CcsDbException e) {
          // Cannot decide: log but ignore
          LOGGER.error(e);
          possibleDbException.set(e);
        }
      });
    } catch (final RuntimeException e) {
      LOGGER.error(e);
      possibleDbException.set(new CcsDbException(e));
    }
    try {
      objectRepository.flushAll();
    } catch (CcsDbException e) {
      // Cannot decide: log but ignore
      LOGGER.error(e);
      possibleDbException.set(e);
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
  }

  /**
   * Clean Up Native Listing from status Object<br>
   * Remove UNKNOWN, ERR_UPL, DELETED, ERR_DEL items<br>
   * Index Objects: Bucket, Site, Status
   */
  @Override
  public void step1SubStep3CleanUpPreviousErrorUploadAndDeletedNativeListing(final DaoRequest daoPreviousRequest)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    final var listName = new ArrayList<String>(1000);
    // UNKNOWN, ERR_UPL, DELETED, ERR_DEL status on NATIVE only
    try {
      objectRepository.mongoCollection().aggregate(List.of(new Document(MG_MATCH,
              new Document(BUCKET, daoPreviousRequest.getBucket()).append(SITE, ServiceProperties.getAccessorSite())
                  .append(STATUS,
                      new Document(MG_IN, List.of(UNKNOWN.name(), ERR_UPL.name(), DELETED.name(), ERR_DEL.name()))))))
          .forEach(object -> {
            listName.add(object.getName());
            if (listName.size() >= 1000) {
              internalCleanUpNativeListing(daoPreviousRequest, listName, possibleDbException);
            }
          });
    } catch (final RuntimeException e) {
      LOGGER.error(e);
      possibleDbException.set(new CcsDbException(e));
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
    final List<Document> aggregate;
    if (replaceOldRequest) {
      final var matchStep1 =
          new Document(MG_MATCH, new Document(REQUESTID, requestId).append(BUCKET, daoRequest.getBucket()));
      final var addFieldStep2 = new Document(MG_ADD_FIELDS, new Document(REQUESTID, daoRequest.getId()));
      final var mergeStep3 = new Document(MG_MERGE,
          new Document(MG_INTO, TABLE_NAME).append(MG_ON, ID).append(MG_WHEN_MATCHED, MG_REPLACE)
              .append(MG_WHEN_NOT_MATCHED, MG_INSERT));
      aggregate = List.of(matchStep1, addFieldStep2, mergeStep3);
    } else {
      final var matchStep1 =
          new Document(MG_MATCH, new Document(REQUESTID, requestId).append(BUCKET, daoRequest.getBucket()));
      final var addFieldStep2 = new Document(MG_ADD_FIELDS, new Document(REQUESTID, daoRequest.getId()));
      final var mergeStep5 = new Document(MG_MERGE,
          new Document(MG_INTO, TABLE_NAME).append(MG_ON, DEFAULT_PK).append(MG_WHEN_MATCHED, MG_REPLACE)
              .append(MG_WHEN_NOT_MATCHED, MG_INSERT));
      aggregate = new ArrayList<>();
      aggregate.add(matchStep1);
      aggregate.add(addFieldStep2);
      aggregate.addAll(createIdAggregate());
      aggregate.add(mergeStep5);
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

    final var matchStep1 = new Document(MgDaoConstants.MG_MATCH, filter);
    final var addFieldsStep2 = new Document(MgDaoConstants.MG_ADD_FIELDS,
        new Document(REQUESTID, daoRequest.getId()).append(DB,
            new Document(SITE, "$" + SITE).append(NSTATUS, "$" + RSTATUS)
                .append(EVENT, "$" + DaoAccessorObjectRepository.CREATION)));
    final var projectStep3 = new Document(MgDaoConstants.MG_PROJECT,
        new Document(ID, 1L).append(BUCKET, 1L).append(NAME, 1L).append(DB, 1L).append(REQUESTID, 1L));
    // Special attention on adding to existing Driver if deleted => no driver
    final var mergeStep4 = new Document(MgDaoConstants.MG_MERGE,
        new Document(MgDaoConstants.MG_INTO, TABLE_NAME).append(MgDaoConstants.MG_ON, MgDaoConstants.DEFAULT_PK)
            .append(MgDaoConstants.MG_WHEN_MATCHED, List.of(new Document(MgDaoConstants.MG_ADD_FIELDS,
                new Document(DB, MgDaoConstants.MG_NEW + DB).append(DRIVER, new Document(MgDaoConstants.MG_COND,
                    List.of(new Document(MgDaoConstants.MG_IN,
                            List.of(MgDaoConstants.MG_NEW + MgDaoConstants.DB_NSTATUS,
                                List.of(DELETING_RANK, DELETED_RANK, ERR_DEL_RANK))), new BsonUndefined(),
                        "$" + DRIVER)))))).append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));

    try {
      objectRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4))
          .allowDiskUse(true).first();
      final long countDb = nativeListingRepository.count(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, MgDaoConstants.DB_SITE, ServiceProperties.getAccessorSite())));
      daoRequest.setCheckedDb(countDb);
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
    Iterator<StorageObject> iteratorDriver = null;
    try (final var driver = storageDriverFactory.getInstance()) {
      if (daoRequest.getFilter() != null) {
        iteratorDriver = driver.objectsIteratorInBucket(daoRequest.getBucket(), daoRequest.getFilter().getNamePrefix(),
            daoRequest.getFilter().getCreationAfter(), daoRequest.getFilter().getCreationBefore());
      } else {
        iteratorDriver = driver.objectsIteratorInBucket(daoRequest.getBucket());
      }
      final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
      final AtomicLong countEntries = new AtomicLong();
      while (iteratorDriver.hasNext()) {
        final var storageObject = iteratorDriver.next();
        final var find = new Document(REQUESTID, daoRequest.getId()).append(BUCKET, storageObject.bucket())
            .append(NAME, storageObject.name());
        final var dao = (MgDaoNativeListing) new MgDaoNativeListing().setRequestId(daoRequest.getId())
            .setBucket(storageObject.bucket()).setName(storageObject.name()).setDriver(
                new SingleSiteObject(ServiceProperties.getAccessorSite(), READY_RANK, storageObject.creationDate()));
        addToBulkUpdateOrUpsert(find, dao, countEntries, possibleDbException);
      }
      nativeListingRepository.flushAll();
      daoRequest.setCheckedDriver(countEntries.get());
      requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
          new DbUpdate().set(DaoRequestRepository.CHECKED_DRIVER, countEntries));
      if (possibleDbException.get() != null) {
        throw possibleDbException.get();
      }
    } catch (final DriverException e) {
      throw new CcsDbException(e);
    } finally {
      if (iteratorDriver != null) {
        SystemTools.consumeAll(iteratorDriver);
      }
    }
  }

  private void addToBulkUpdateOrUpsert(final Document find, final MgDaoNativeListing dao, final AtomicLong countEntries,
                                       final AtomicReference<CcsDbException> possibleDbException) {
    try {
      var found = nativeListingRepository.count(find) > 0;
      if (found) {
        nativeListingRepository.addToUpdateBulk(find, dao);
      } else {
        dao.setId(GuidLike.getGuid());
        nativeListingRepository.addToUpsertBulk(find, dao);
      }
      countEntries.getAndIncrement();
    } catch (final CcsDbException e) {
      possibleDbException.set(e);
    }
  }

  /**
   * Compare Native listing with DB and DRIVER (both or only one)<br>
   * Step5: Complete DB without Driver from NativeListing into SitesListing Local step<br>
   * Index Native: db, driver.event (optional)<br>
   * Index Objects: site, bucket, name<br>
   * Index Native: requestId, bucket, db.site, driver (optional)<br>
   * Index Native: requestId, bucket, db.site, driver.site<br>
   * Index Sites: requestId, bucket, name
   */
  @Override
  public void step5CompareNativeListingDbDriver(final DaoRequest daoRequest) throws CcsDbException {
    step51InsertMissingObjectsFromExistingDriverIntoObjects(daoRequest);
    step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(daoRequest);
    step53UpdateWhereNoDriverIntoObjects(daoRequest);
    step54UpsertWhereNoDriverIntoSiteListing(daoRequest);
    step55UpdateBothDbDriverIntoObjects(daoRequest);
    step56UpdateBothDbDriverIntoSiteListing(daoRequest);
    step57CleanSiteListingForUnusedStatus(daoRequest);
    step58CountFinalSiteListing(daoRequest);
  }

  @Override
  public void step51InsertMissingObjectsFromExistingDriverIntoObjects(final DaoRequest daoRequest)
      throws CcsDbException {
    try {
      // Insert into SitesListing from NativeListing with DB absent but DRIVER present
      // Sub Step 1: Fix Object missing (DB missing), Update later on (subStep 2)
      final var matchStep11 = new Document(MgDaoConstants.MG_MATCH,
          new Document(DB, new Document(MgDaoConstants.MG_EXISTS, false)).append(MgDaoConstants.DRIVER_EVENT,
              new Document(MgDaoConstants.MG_EXISTS, true)).append(MgDaoConstants.DRIVER_SITE,
              new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))));
      final var addFieldsStep12 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(CREATION, "$" + MgDaoConstants.DRIVER_EVENT).append(SITE, "$" + MgDaoConstants.DRIVER_SITE)
              .append(STATUS, STATUS_NAME_ORDERED.get(READY_RANK)).append(RSTATUS, TO_UPDATE_RANK));
      final var unsetStep13 = new Document(MgDaoConstants.MG_UNSET, List.of(DRIVER, DB, REQUESTID));
      // Only if not exists in Objects table
      final var mergeStep14 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  List.of(SITE, BUCKET, NAME)).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_KEEP_EXISTING)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep11, addFieldsStep12, unsetStep13, mergeStep14)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void step52UpsertMissingObjectsFromExistingDriverIntoSiteListing(final DaoRequest daoRequest)
      throws CcsDbException {
    try {
      // Sub Step 2: Merge into SitesListing (missing hash and size could help later on to update the metadata)
      final var matchStep21 = new Document(MgDaoConstants.MG_MATCH,
          new Document(DB, new Document(MgDaoConstants.MG_EXISTS, false)).append(MgDaoConstants.DRIVER_EVENT,
              new Document(MgDaoConstants.MG_EXISTS, true)).append(MgDaoConstants.DRIVER_SITE,
              new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))));
      // Special status for Action: TO_UPDATE but check will see that Object is there
      final var addFieldsStep22 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesListingRepository.LOCAL, List.of(
              new Document(SITE, "$" + MgDaoConstants.DRIVER_SITE).append(NSTATUS, TO_UPDATE_RANK)
                  .append(EVENT, "$" + MgDaoConstants.DRIVER_EVENT))));
      final var projectStep23 = new Document(MgDaoConstants.MG_PROJECT, new Document(DB, 0L).append(DRIVER, 0L));
      // Special attention on adding to existing array if any
      final var mergeStep24 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoSitesListingRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  MgDaoConstants.DEFAULT_PK).append(MgDaoConstants.MG_WHEN_MATCHED, List.of(
                  new Document(MgDaoConstants.MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL,
                      new Document(MgDaoConstants.MG_IF_NULL, List.of(new Document(MgDaoConstants.MG_CONCAT_ARRAYS,
                              List.of("$" + DaoSitesListingRepository.LOCAL,
                                  MgDaoConstants.MG_NEW + DaoSitesListingRepository.LOCAL)),
                          MgDaoConstants.MG_NEW + DaoSitesListingRepository.LOCAL))))))
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep21, addFieldsStep22, projectStep23, mergeStep24)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void step53UpdateWhereNoDriverIntoObjects(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step 1: Fix Object UPLOAD, Update later on (subStep 2)
      final var matchStep31 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoConstants.MG_AND, List.of(new Document(MgDaoConstants.DB_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                  new Document(MgDaoConstants.DB_NSTATUS,
                      new Document(MgDaoConstants.MG_IN, List.of(READY_RANK, ERR_UPL_RANK, TO_UPDATE_RANK))),
                  new Document(DRIVER, new Document(MgDaoConstants.MG_EXISTS, false)))));
      final var addFieldsStep12 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(STATUS, STATUS_NAME_ORDERED.get(UPLOAD_RANK)).append(RSTATUS, TO_UPDATE_RANK)
              .append(SITE, "$" + DB_SITE));
      final var unsetStep13 = new Document(MgDaoConstants.MG_UNSET, List.of(DRIVER, DB, REQUESTID));
      // Only if exists in Objects table
      final var mergeStep14 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  List.of(SITE, BUCKET, NAME)).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_MERGE_MATCHED)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_DISCARD));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep31, addFieldsStep12, unsetStep13, mergeStep14)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
    try {
      // Sub Step 2: Fix Object DELETED, Update later on (subStep 2)
      final var matchStep31 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoConstants.MG_AND, List.of(new Document(MgDaoConstants.DB_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                  new Document(MgDaoConstants.DB_NSTATUS,
                      new Document(MgDaoConstants.MG_IN, List.of(DELETING_RANK, ERR_DEL_RANK))),
                  new Document(DRIVER, new Document(MgDaoConstants.MG_EXISTS, false)))));
      final var addFieldsStep12 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(STATUS, STATUS_NAME_ORDERED.get(DELETED_RANK)).append(RSTATUS, DELETED_RANK)
              .append(SITE, "$" + DB_SITE));
      final var unsetStep13 = new Document(MgDaoConstants.MG_UNSET, List.of(DRIVER, DB, REQUESTID));
      // Only if exists in Objects table
      final var mergeStep14 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  List.of(SITE, BUCKET, NAME)).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_MERGE_MATCHED)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_DISCARD));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep31, addFieldsStep12, unsetStep13, mergeStep14)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void step54UpsertWhereNoDriverIntoSiteListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step2: Insert into SitesListing from NativeListing with DRIVER absent but DB present
      final var matchStep31 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoConstants.MG_AND, List.of(new Document(MgDaoConstants.DB_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                  new Document(DRIVER, new Document(MgDaoConstants.MG_EXISTS, false)))));
      final var addFieldsStep32 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesListingRepository.LOCAL, List.of(new Document(MgDaoConstants.MG_COND, List.of(
              new Document(MgDaoConstants.MG_IN, List.of("$" + MgDaoConstants.DB_NSTATUS, List.of(UNKNOWN_RANK))),
              new Document(SITE, "$" + MgDaoConstants.DB_SITE).append(NSTATUS, UNKNOWN_RANK)
                  .append(EVENT, "$" + MgDaoConstants.DB_EVENT), new Document(MgDaoConstants.MG_COND, List.of(
                  new Document(MgDaoConstants.MG_IN, List.of("$" + MgDaoConstants.DB_NSTATUS,
                      List.of(UPLOAD_RANK, READY_RANK, ERR_UPL_RANK, TO_UPDATE_RANK))),
                  new Document(SITE, "$" + MgDaoConstants.DB_SITE).append(NSTATUS, UPLOAD_RANK)
                      .append(EVENT, "$" + MgDaoConstants.DB_EVENT),
                  new Document(SITE, "$" + MgDaoConstants.DB_SITE).append(NSTATUS, DELETED_RANK)
                      .append(EVENT, "$" + MgDaoConstants.DB_EVENT))))))));
      final var projectStep33 = new Document(MgDaoConstants.MG_PROJECT, new Document(DB, 0L).append(DRIVER, 0L));
      // Special attention on adding to existing array if any
      final var mergeStep34 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoSitesListingRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  MgDaoConstants.DEFAULT_PK).append(MgDaoConstants.MG_WHEN_MATCHED, List.of(
                  new Document(MgDaoConstants.MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL,
                      new Document(MgDaoConstants.MG_IF_NULL, List.of(new Document(MgDaoConstants.MG_CONCAT_ARRAYS,
                              List.of("$" + DaoSitesListingRepository.LOCAL,
                                  MgDaoConstants.MG_NEW + DaoSitesListingRepository.LOCAL)),
                          MgDaoConstants.MG_NEW + DaoSitesListingRepository.LOCAL))))))
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep31, addFieldsStep32, projectStep33, mergeStep34)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void step55UpdateBothDbDriverIntoObjects(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step 1: Fix Object READY, Update later on (subStep 2)
      final var matchStep41 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoConstants.MG_AND, List.of(new Document(MgDaoConstants.DB_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                  new Document(MgDaoConstants.DB_NSTATUS,
                      new Document(MgDaoConstants.MG_IN, List.of(UPLOAD_RANK, ERR_UPL_RANK, TO_UPDATE_RANK))),
                  new Document(MgDaoConstants.DRIVER_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))))));
      final var addFieldsStep12 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(STATUS, STATUS_NAME_ORDERED.get(READY_RANK)).append(RSTATUS, READY_RANK)
              .append(SITE, "$" + DB_SITE));
      final var unsetStep13 = new Document(MgDaoConstants.MG_UNSET, List.of(DRIVER, DB, REQUESTID));
      // Only if exists in Objects table
      final var mergeStep14 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  List.of(SITE, BUCKET, NAME)).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_MERGE_MATCHED)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_DISCARD));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep41, addFieldsStep12, unsetStep13, mergeStep14)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
    try {
      // Sub Step 2: Fix Object DELETING, Update later on (subStep 2)
      final var matchStep41 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoConstants.MG_AND, List.of(new Document(MgDaoConstants.DB_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                  new Document(MgDaoConstants.DB_NSTATUS,
                      new Document(MgDaoConstants.MG_IN, List.of(DELETED_RANK, ERR_DEL_RANK))),
                  new Document(MgDaoConstants.DRIVER_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))))));
      final var addFieldsStep12 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(STATUS, STATUS_NAME_ORDERED.get(DELETING_RANK)).append(RSTATUS, DELETING_RANK)
              .append(SITE, "$" + DB_SITE));
      final var unsetStep13 = new Document(MgDaoConstants.MG_UNSET, List.of(DRIVER, DB, REQUESTID));
      // Only if exists in Objects table
      final var mergeStep14 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoAccessorObjectRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  List.of(SITE, BUCKET, NAME)).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_MERGE_MATCHED)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_DISCARD));
      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep41, addFieldsStep12, unsetStep13, mergeStep14)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void step56UpdateBothDbDriverIntoSiteListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step4: Insert into SitesListing from NativeListing with both existing DB and Driver
      final var matchStep41 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(MgDaoConstants.MG_AND, List.of(new Document(MgDaoConstants.DB_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))),
                  new Document(MgDaoConstants.DRIVER_SITE,
                      new Document(MgDaoConstants.MG_IN, List.of(ServiceProperties.getAccessorSite()))))));
      final var addFieldsStep42 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesListingRepository.LOCAL, List.of(
              new Document(SITE, "$" + MgDaoConstants.DB_SITE).append(NSTATUS, new Document(MgDaoConstants.MG_COND,
                  List.of(new Document(MgDaoConstants.MG_GTE,
                          List.of("$" + MgDaoConstants.DB_EVENT, "$" + MgDaoConstants.DRIVER_EVENT)),
                      "$" + MgDaoConstants.DB_NSTATUS, "$" + MgDaoConstants.DRIVER_NSTATUS))).append(EVENT,
                  new Document(MgDaoConstants.MG_COND, List.of(new Document(MgDaoConstants.MG_GTE,
                          List.of("$" + MgDaoConstants.DB_EVENT, "$" + MgDaoConstants.DRIVER_EVENT)),
                      "$" + MgDaoConstants.DB_EVENT, "$" + MgDaoConstants.DRIVER_EVENT))))));
      final var projectStep43 = new Document(MgDaoConstants.MG_PROJECT, new Document(DB, 0L).append(DRIVER, 0L));
      // Special attention on adding to existing array if any
      final var mergeStep44 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoSitesListingRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  MgDaoConstants.DEFAULT_PK).append(MgDaoConstants.MG_WHEN_MATCHED, List.of(
                  new Document(MgDaoConstants.MG_ADD_FIELDS, new Document(DaoSitesListingRepository.LOCAL,
                      new Document(MgDaoConstants.MG_IF_NULL, List.of(new Document(MgDaoConstants.MG_CONCAT_ARRAYS,
                              List.of("$" + DaoSitesListingRepository.LOCAL,
                                  MgDaoConstants.MG_NEW + DaoSitesListingRepository.LOCAL)),
                          MgDaoConstants.MG_NEW + DaoSitesListingRepository.LOCAL))))))
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));

      nativeListingRepository.mongoCollection()
          .aggregate(List.of(matchStep41, addFieldsStep42, projectStep43, mergeStep44)).allowDiskUse(true).first();

    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  /**
   * Index Site Listing: requestId, bucket, local.site, local.nstatus
   */
  @Override
  public void step57CleanSiteListingForUnusedStatus(final DaoRequest daoRequest) throws CcsDbException {
    // Cleanup SiteListing changing Deleting, Err_Del to Delete
    try {
      sitesListingRepository.update(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, MgDaoConstants.LOCAL_SITE, ServiceProperties.getAccessorSite()),
              new DbQuery(RestQuery.QUERY.IN, MgDaoConstants.LOCAL_NSTATUS, List.of(DELETING_RANK, ERR_DEL_RANK))),
          new DbUpdate().set(MgDaoConstants.LOCAL_NSTATUS, DELETED_RANK));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
    // Cleanup SiteListing except READY, DELETED, TO_UPDATE
    try {
      sitesListingRepository.delete(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, MgDaoConstants.LOCAL_SITE, ServiceProperties.getAccessorSite()),
              new DbQuery(RestQuery.QUERY.IN, MgDaoConstants.LOCAL_NSTATUS,
                  List.of(UNKNOWN_RANK, UPLOAD_RANK, ERR_UPL, DELETING_RANK, ERR_DEL_RANK))));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void step58CountFinalSiteListing(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step 5 finalize count
      final long countDb = sitesListingRepository.count(
          new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()),
              new DbQuery(RestQuery.QUERY.EQ, BUCKET, daoRequest.getBucket()),
              new DbQuery(RestQuery.QUERY.EQ, MgDaoConstants.LOCAL_SITE, ServiceProperties.getAccessorSite())));
      daoRequest.setChecked(countDb);
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
    ClosingIterator<DaoSitesListing> iterator = null;
    try {
      iterator = sitesListingRepository.findIterator(new DbQuery(RestQuery.QUERY.EQ, REQUESTID, daoRequest.getId()));
      return iterator;
    } catch (final RuntimeException e) {
      if (iterator != null) { // NOSONAR can be null
        iterator.close();
      }
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

  /**
   * Add the remote sites listing to local aggregate one<br>
   * Step7: add all remote sites listing<br>
   * Index Sites: requestId, bucket, name
   */
  @Override
  public void saveRemoteNativeListing(final DaoRequest daoRequest, final Iterator<DaoSitesListing> iterator)
      throws CcsDbException {
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>();
    final AtomicLong countEntries = new AtomicLong(0);
    while (iterator.hasNext()) {
      final var daoSitesListing = iterator.next();
      try {
        final var find =
            new Document(REQUESTID, daoSitesListing.getRequestId()).append(BUCKET, daoSitesListing.getBucket())
                .append(NAME, daoSitesListing.getName());
        var found = sitesListingRepository.count(find) > 0;
        if (found) {
          daoSitesListing.setId(null);
          sitesListingRepository.addToUpdateBulk(find, daoSitesListing);
        } else {
          sitesListingRepository.addToUpsertBulk(find, daoSitesListing);
        }
        countEntries.incrementAndGet();
      } catch (final CcsDbException e) {
        possibleDbException.compareAndSet(null, e);
      }
    }
    sitesListingRepository.flushAll();
    daoRequest.setCheckedRemote(countEntries.get());
    // FIXME missing count on remote+local to know the end
    requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
        new DbUpdate().set(DaoRequestRepository.CHECKED_REMOTE, daoRequest.getCheckedRemote()));
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
  }

  private List<String> getAllSites() {
    // FIXME Should depend on Request
    return List.of();
  }

  /**
   * Compute actions from sites listing<br>
   * Step8: in 2 steps, all sites declared, not all sites declared
   * Index Sites: requestId, bucket, local.site
   * Index Actions: requestId, bucket, name
   */
  @Override
  public void computeActions(final DaoRequest daoRequest) throws CcsDbException {
    try {
      computeActionsWithAllSites(daoRequest);
      computeActionsWithPartialSites(daoRequest);
      countFinalActions(daoRequest);
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void computeActionsWithAllSites(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Sub Step 1 : for objects will all sites having a status
      final var matchStep11 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(DaoSitesListingRepository.LOCAL, new Document(MgDaoConstants.MG_EXISTS, true))
              .append(MgDaoConstants.LOCAL_SITE, new Document(MgDaoConstants.MG_ALL, getAllSites())));
      final var addFieldsStep12 = new Document(MgDaoConstants.MG_ADD_FIELDS, new Document(MgDaoConstants.MAXUPLOAD,
          new Document(MgDaoConstants.MG_LET, new Document(MgDaoConstants.MG_VARS,
              new Document(MgDaoConstants.VALID_FIELD, new Document(MgDaoConstants.MG_FILTER,
                  new Document(MgDaoConstants.MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(
                      MgDaoConstants.COND, new Document(MgDaoConstants.MG_IN,
                          List.of(MgDaoConstants.MG_THIS + NSTATUS, List.of(READY_RANK, TO_UPDATE_RANK))))))).append(
              MgDaoConstants.IN, new Document(MgDaoConstants.MG_MAX, "$$" + MgDaoConstants.VALID_FIELD)))).append(
          MgDaoConstants.MAXDELETE, new Document(MgDaoConstants.MG_LET, new Document(MgDaoConstants.MG_VARS,
              new Document(MgDaoConstants.VALID_FIELD, new Document(MgDaoConstants.MG_FILTER,
                  new Document(MgDaoConstants.MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(
                      MgDaoConstants.COND, new Document(MgDaoConstants.MG_EQ,
                          List.of(MgDaoConstants.MG_THIS + NSTATUS, DELETED_RANK)))))).append(MgDaoConstants.IN,
              new Document(MgDaoConstants.MG_MAX, "$$" + MgDaoConstants.VALID_FIELD)))));
      final var addFieldsStep13 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesActionRepository.NEED_ACTION, new Document(MgDaoConstants.MG_COND,
              new Document(MgDaoConstants.MG_IF, new Document(MgDaoConstants.MG_GTE,
                  List.of(MgDaoConstants.MAXUPLOAD_EVENT, MgDaoConstants.MAXDELETE_EVENT))).append(
                      MgDaoConstants.MG_THEN, MgDaoConstants.MAXUPLOAD_NSTATUS)
                  .append(MgDaoConstants.MG_ELSE, DELETED_RANK))).append("needActionFrom",
              new Document(MgDaoConstants.MG_COND, new Document(MgDaoConstants.MG_IF,
                  new Document(MgDaoConstants.MG_GTE,
                      List.of(MgDaoConstants.MAXUPLOAD_EVENT, MgDaoConstants.MAXDELETE_EVENT))).append(
                  MgDaoConstants.MG_THEN, MAXUPLOAD_SITE).append(MgDaoConstants.MG_ELSE, ""))));
      final var matchStep14 = new Document(MgDaoConstants.MG_MATCH,
          new Document(DaoSitesActionRepository.NEED_ACTION, new Document(MgDaoConstants.MG_EXISTS, true)));
      final var addFieldsStep15 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesActionRepository.ACTIONS, new Document(MgDaoConstants.MG_LET,
              new Document(MgDaoConstants.MG_VARS, new Document("from", new Document(MgDaoConstants.MG_FILTER,
                  new Document(MgDaoConstants.MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(
                      MgDaoConstants.COND, new Document(MgDaoConstants.MG_NE, List.of(MgDaoConstants.MG_THIS + NSTATUS,
                          "$" + DaoSitesActionRepository.NEED_ACTION)))))).append(MgDaoConstants.IN,
                  "$$from." + SITE))));
      final var matchStep16 = new Document(MgDaoConstants.MG_MATCH, new Document(DaoSitesActionRepository.ACTIONS,
          new Document(MgDaoConstants.MG_NOT, new Document(MgDaoConstants.MG_SIZE, 0L))));
      final var projectStep17 = new Document(MgDaoConstants.MG_PROJECT,
          new Document(DaoSitesListingRepository.LOCAL, 0L).append(MgDaoConstants.MAXUPLOAD, 0L)
              .append(MgDaoConstants.MAXDELETE, 0L));
      final var mergeStep18 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoSitesActionRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  MgDaoConstants.DEFAULT_PK).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_REPLACE)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));
      sitesListingRepository.mongoCollection().aggregate(
          List.of(matchStep11, addFieldsStep12, addFieldsStep13, matchStep14, addFieldsStep15, matchStep16,
              projectStep17, mergeStep18)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void computeActionsWithPartialSites(final DaoRequest daoRequest) throws CcsDbException {
    try {
      // Step 2 : for objects will not all sites having a status
      final var matchStep21 = new Document(MgDaoConstants.MG_MATCH,
          new Document(REQUESTID, daoRequest.getId()).append(BUCKET, daoRequest.getBucket())
              .append(DaoSitesListingRepository.LOCAL, new Document(MgDaoConstants.MG_EXISTS, true))
              .append(MgDaoConstants.LOCAL_SITE,
                  new Document(MgDaoConstants.MG_NOT, new Document(MgDaoConstants.MG_ALL, getAllSites()))));
      final var addFieldsStep22 = new Document(MgDaoConstants.MG_ADD_FIELDS, new Document(MgDaoConstants.MAXUPLOAD,
          new Document(MgDaoConstants.MG_LET, new Document(MgDaoConstants.MG_VARS,
              new Document(MgDaoConstants.VALID_FIELD, new Document(MgDaoConstants.MG_FILTER,
                  new Document(MgDaoConstants.MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(
                      MgDaoConstants.COND, new Document(MgDaoConstants.MG_IN,
                          List.of(MgDaoConstants.MG_THIS + NSTATUS, List.of(READY_RANK, TO_UPDATE_RANK))))))).append(
              MgDaoConstants.IN, new Document(MgDaoConstants.MG_MAX, "$$" + MgDaoConstants.VALID_FIELD)))).append(
          MgDaoConstants.MAXDELETE, new Document(MgDaoConstants.MG_LET, new Document(MgDaoConstants.MG_VARS,
              new Document(MgDaoConstants.VALID_FIELD, new Document(MgDaoConstants.MG_FILTER,
                  new Document(MgDaoConstants.MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(
                      MgDaoConstants.COND, new Document(MgDaoConstants.MG_EQ,
                          List.of(MgDaoConstants.MG_THIS + NSTATUS, DELETED_RANK)))))).append(MgDaoConstants.IN,
              new Document(MgDaoConstants.MG_MAX, "$$" + MgDaoConstants.VALID_FIELD)))).append("unknownStatusSites",
          new Document(MgDaoConstants.MG_LET,
              new Document(MgDaoConstants.MG_VARS, new Document("allsite", getAllSites())).append(MgDaoConstants.IN,
                  new Document(MgDaoConstants.MG_FILTER,
                      new Document(MgDaoConstants.MG_INPUT, "$$allsite").append(MgDaoConstants.COND,
                          new Document(MgDaoConstants.MG_NOT, new Document(MgDaoConstants.MG_IN,
                              List.of("$$this", "$" + MgDaoConstants.LOCAL_SITE)))))))));
      final var addFieldsStep23 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesActionRepository.NEED_ACTION, new Document(MgDaoConstants.MG_COND,
              new Document(MgDaoConstants.MG_IF, new Document(MgDaoConstants.MG_GTE,
                  List.of(MgDaoConstants.MAXUPLOAD_EVENT, MgDaoConstants.MAXDELETE_EVENT))).append(
                      MgDaoConstants.MG_THEN, MgDaoConstants.MAXUPLOAD_NSTATUS)
                  .append(MgDaoConstants.MG_ELSE, DELETED_RANK))).append(DaoSitesActionRepository.NEED_ACTION_FROM,
              new Document(MgDaoConstants.MG_COND, new Document(MgDaoConstants.MG_IF,
                  new Document(MgDaoConstants.MG_GTE,
                      List.of(MgDaoConstants.MAXUPLOAD_EVENT, MgDaoConstants.MAXDELETE_EVENT))).append(
                  MgDaoConstants.MG_THEN, MgDaoConstants.MAXUPLOAD_SITE).append(MgDaoConstants.MG_ELSE, ""))));
      final var matchStep24 = new Document(MgDaoConstants.MG_MATCH,
          new Document(DaoSitesActionRepository.NEED_ACTION, new Document(MgDaoConstants.MG_EXISTS, true)));
      final var addFieldsStep25 = new Document(MgDaoConstants.MG_ADD_FIELDS,
          new Document(DaoSitesActionRepository.ACTIONS, new Document(MgDaoConstants.MG_SET_UNION,
              List.of("$unknownStatusSites", new Document(MgDaoConstants.MG_LET, new Document(MgDaoConstants.MG_VARS,
                  new Document("from", new Document(MgDaoConstants.MG_FILTER,
                      new Document(MgDaoConstants.MG_INPUT, "$" + DaoSitesListingRepository.LOCAL).append(
                          MgDaoConstants.COND, new Document(MgDaoConstants.MG_NE,
                              List.of(MgDaoConstants.MG_THIS + NSTATUS,
                                  "$" + DaoSitesActionRepository.NEED_ACTION)))))).append(MgDaoConstants.IN,
                  "$$from." + SITE))))));
      final var projectStep26 = new Document(MgDaoConstants.MG_PROJECT,
          new Document(DaoSitesListingRepository.LOCAL, 0L).append(MgDaoConstants.MAXUPLOAD, 0L)
              .append(MgDaoConstants.MAXDELETE, 0L).append("unknownStatusSites", 0L));
      final var mergeStep27 = new Document(MgDaoConstants.MG_MERGE,
          new Document(MgDaoConstants.MG_INTO, DaoSitesActionRepository.TABLE_NAME).append(MgDaoConstants.MG_ON,
                  MgDaoConstants.DEFAULT_PK).append(MgDaoConstants.MG_WHEN_MATCHED, MgDaoConstants.MG_REPLACE)
              .append(MgDaoConstants.MG_WHEN_NOT_MATCHED, MgDaoConstants.MG_INSERT));
      sitesListingRepository.mongoCollection().aggregate(
          List.of(matchStep21, addFieldsStep22, addFieldsStep23, matchStep24, addFieldsStep25, projectStep26,
              mergeStep27)).allowDiskUse(true).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void countFinalActions(final DaoRequest daoRequest) throws CcsDbException {
    // Sub Step 3 Count
    try {
      long actions = sitesActionRepository.count(new Document(REQUESTID, daoRequest.getId()));
      // FIXME change name
      daoRequest.setActions(actions);
      requestRepository.update(DbQuery.idEquals(daoRequest.getId()),
          new DbUpdate().set(DaoRequestRepository.ACTIONS, actions));
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  @Override
  public void updateRequestFromRemoteListing(final DaoRequest daoRequest) {
    // Update Checked, Site ? or done previously
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
