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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.AbstractPublicObjectHelper;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.model.DbQueryAccessorHelper;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.DaoRequest;
import io.clonecloudstore.reconciliator.database.model.DaoRequestRepository;
import io.clonecloudstore.reconciliator.database.model.DaoSitesActionRepository;
import io.clonecloudstore.reconciliator.database.model.InitializationService;
import io.clonecloudstore.reconciliator.model.ReconciliationAction;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.Document;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.STATUS;
import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;
import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.BUCKET;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NAME;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.REQUESTID;
import static io.clonecloudstore.reconciliator.database.mongodb.MgCentralReconciliationService.CENTRAL_RECONCILIATION;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.DEFAULT_PK;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ADD_FIELDS;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INSERT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_INTO;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MATCH;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_MERGE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_ON;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_PROJECT;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_REPLACE;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_WHEN_MATCHED;
import static io.clonecloudstore.reconciliator.database.mongodb.MgDaoReconciliationUtils.MG_WHEN_NOT_MATCHED;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgInitializationService implements InitializationService {
  private static final Logger LOGGER = Logger.getLogger(MgInitializationService.class);
  public static final String INITIALIZATION_SERVICE = "initialization-service";
  private final MgDaoAccessorBucketRepository bucketRepository;
  private final MgDaoAccessorObjectRepository objectRepository;
  private final MgDaoRequestRepository requestRepository;
  private final MgDaoSitesActionRepository sitesActionRepository;
  private final DriverApiFactory storageDriverFactory;
  private final BulkMetrics bulkMetrics;

  public MgInitializationService(final MgDaoAccessorBucketRepository bucketRepository,
                                 final MgDaoAccessorObjectRepository objectRepository,
                                 final MgDaoRequestRepository requestRepository,
                                 final MgDaoSitesActionRepository sitesActionRepository,
                                 final BulkMetrics bulkMetrics) {
    this.bucketRepository = bucketRepository;
    this.objectRepository = objectRepository;
    this.requestRepository = requestRepository;
    this.sitesActionRepository = sitesActionRepository;
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
    this.bulkMetrics = bulkMetrics;
  }

  @Override
  public void importFromExistingBucket(final String clientId, final String bucket, final String prefix,
                                       final Instant from, final Instant to, final long futureExpireAddSeconds,
                                       final Map<String, String> defaultMetadata) throws CcsWithStatusException {
    try (final var driver = storageDriverFactory.getInstance()) {
      final var storageBucket = driver.bucketGet(bucket);
      if (storageBucket != null) {
        final var newStorageBucket = new StorageBucket(storageBucket.bucket(), clientId, storageBucket.creationDate());
        driver.bucketImport(newStorageBucket);
        tryToInsertBucket(newStorageBucket);
        final var countImported = new AtomicLong();
        final var possibleException = new AtomicReference<CcsDbException>(null);
        final var expired = futureExpireAddSeconds > 0 ? Instant.now().plusSeconds(futureExpireAddSeconds) : null;
        final var streamObject = driver.objectsStreamInBucket(storageBucket, prefix, from, to);
        streamObject.forEach(
            storageObject -> importExistingObject(storageObject, defaultMetadata, expired, countImported,
                possibleException));
        objectRepository.flushAll();
        if (countImported.get() > 0) {
          bulkMetrics.incrementCounter(countImported.get(), INITIALIZATION_SERVICE, BulkMetrics.KEY_OBJECT,
              BulkMetrics.TAG_CREATE);
        }
        if (possibleException.get() != null) {
          throw possibleException.get();
        }
      }
    } catch (final DriverNotFoundException e) {
      LOGGER.warnf("Bucket not found: %s (%s)", bucket, e);
      throw new CcsWithStatusException(bucket, 404, e);
    } catch (final DriverException | CcsDbException e) {
      LOGGER.warnf("Issue while importing: %s (%s)", bucket, e);
      throw new CcsWithStatusException(bucket, 500, e);
    }
  }

  private void tryToInsertBucket(final StorageBucket storageBucket) {
    final var daoBucket = bucketRepository.createEmptyItem().setClientId(storageBucket.clientId())
        .setCreation(storageBucket.creationDate()).setStatus(AccessorStatus.READY).setId(storageBucket.bucket())
        .setSite(ServiceProperties.getAccessorSite());
    try {
      bucketRepository.insert(daoBucket);
    } catch (final CcsDbException e) {
      LOGGER.warnf("Bucket might have been already created: %s", e);
    }
  }

  private void importExistingObject(final StorageObject storageObject, final Map<String, String> defaultMetadata,
                                    final Instant expired, final AtomicLong count,
                                    final AtomicReference<CcsDbException> possibleException) {
    final var dto = AbstractPublicObjectHelper.getFromStorageObject(storageObject);
    dto.setSite(ServiceProperties.getAccessorSite()).setExpires(expired).setStatus(AccessorStatus.READY)
        .setId(GuidLike.getGuid());
    try {
      if (objectRepository.count(new DbQuery(RestQuery.CONJUNCTION.AND,
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.BUCKET, storageObject.bucket()),
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.NAME, storageObject.name()),
          new DbQuery(RestQuery.QUERY.EQ, DaoAccessorObjectRepository.SITE, ServiceProperties.getAccessorSite()))) >
          0) {
        // Already imported
        return;
      }
      final var newMap = new HashMap<String, String>();
      if (dto.getMetadata() != null) {
        newMap.putAll(dto.getMetadata());
      }
      if (defaultMetadata != null) {
        newMap.putAll(defaultMetadata);
      }
      dto.setMetadata(newMap);
      objectRepository.addToInsertBulk(objectRepository.createEmptyItem().fromDto(dto));
      count.incrementAndGet();
    } catch (final CcsDbException e) {
      possibleException.compareAndSet(null, e);
    }
  }

  @Override
  public DaoRequest syncFromExistingSite(final String clientId, final String bucket, final String remoteSite,
                                         final AccessorFilter filter) throws CcsDbException {
    final var request =
        requestRepository.createEmptyItem().setBucket(bucket).setFromSite(ServiceProperties.getAccessorSite())
            .setId(GuidLike.getGuid()).setClientId(clientId).setStart(Instant.now())
            .setContextSites(List.of(remoteSite)).setFilter(filter);
    if (bucketRepository.count(
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, DaoAccessorBucketRepository.ID, bucket),
            new DbQuery(RestQuery.QUERY.EQ, DaoAccessorBucketRepository.SITE, ServiceProperties.getAccessorSite()))) >
        0) {
      requestRepository.insert(request);
      try {
        final DbQuery filterQuery = DbQueryAccessorHelper.getDbQuery(filter);
        final var filterFinal =
            new Document(DaoAccessorObjectRepository.SITE, ServiceProperties.getAccessorSite()).append(
                DaoAccessorObjectRepository.BUCKET, bucket).append(STATUS, AccessorStatus.READY.name());
        if (filterQuery != null && !filterFinal.isEmpty()) {
          filterFinal.putAll(filterQuery.getBson().toBsonDocument());
        }
        final var matchStep1 = new Document(MG_MATCH, filterFinal);
        final var addFieldsStep2 = new Document(MG_ADD_FIELDS,
            new Document(REQUESTID, request.getId()).append(DaoSitesActionRepository.NEED_ACTION,
                    ReconciliationAction.UPLOAD_ACTION.getStatus())
                .append(DaoSitesActionRepository.NEED_ACTION_FROM, List.of(ServiceProperties.getAccessorSite()))
                .append(DaoSitesActionRepository.SITES, List.of(remoteSite)));
        final var projectStep3 = new Document(MG_PROJECT,
            new Document(ID, 1L).append(BUCKET, 1L).append(NAME, 1L).append(DaoSitesActionRepository.REQUESTID, 1L)
                .append(DaoSitesActionRepository.NEED_ACTION_FROM, 1L).append(DaoSitesActionRepository.NEED_ACTION, 1L)
                .append(DaoSitesActionRepository.SITES, 1L));
        final var mergeStep4 = new Document(MG_MERGE,
            new Document(MG_INTO, DaoSitesActionRepository.TABLE_NAME).append(MG_ON, DEFAULT_PK)
                .append(MG_WHEN_MATCHED, MG_REPLACE).append(MG_WHEN_NOT_MATCHED, MG_INSERT));
        objectRepository.mongoCollection().aggregate(List.of(matchStep1, addFieldsStep2, projectStep3, mergeStep4))
            .allowDiskUse(true).first();
        sitesActionRepository.flushAll();
        long actions = sitesActionRepository.count(new Document(REQUESTID, request.getId()));
        request.setActions(actions);
        bulkMetrics.incrementCounter(actions, CENTRAL_RECONCILIATION, BulkMetrics.KEY_OBJECT,
            BulkMetrics.TAG_TO_ACTIONS);
        LOGGER.infof("SyncFromExistingSite %d items", actions);
        requestRepository.update(DbQuery.idEquals(request.getId()),
            new DbUpdate().set(DaoRequestRepository.ACTIONS, actions));
        return request;
      } catch (final RuntimeException e) {
        LOGGER.warn(e);
        throw new CcsDbException(e);
      }
    }
    return null;
  }
}
