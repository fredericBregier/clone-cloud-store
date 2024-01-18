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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucket;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.mongodb.MgDaoAccessorObjectRepository;
import io.clonecloudstore.common.database.mongo.MongoBulkInsertHelper;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.ReconciliatorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.reconciliator.database.model.PurgeService;
import io.clonecloudstore.replicator.topic.LocalBrokerService;
import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.ARCHIVED_FROM_BUCKET;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.ARCHIVED_FROM_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.ARCHIVED_FROM_NAME;
import static io.clonecloudstore.accessor.model.AccessorStatus.DELETED;
import static io.clonecloudstore.accessor.model.AccessorStatus.READY;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository.CLIENT_ID;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository.EXPIRES;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.CREATION;
import static io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository.STATUS;
import static io.clonecloudstore.common.database.utils.DbType.CCS_DB_TYPE;
import static io.clonecloudstore.common.database.utils.DbType.MONGO;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.BUCKET;

@LookupIfProperty(name = CCS_DB_TYPE, stringValue = MONGO)
@ApplicationScoped
public class MgPurgeService implements PurgeService {
  private static final Logger LOGGER = Logger.getLogger(MgPurgeService.class);
  public static final String PURGE_SERVICE = "purge_service";
  private final MgDaoAccessorBucketRepository bucketRepository;
  private final MgDaoAccessorObjectRepository objectRepository;
  private final DriverApiFactory storageDriverFactory;
  private final LocalBrokerService localBrokerService;
  private final BulkMetrics bulkMetrics;

  public MgPurgeService(final MgDaoAccessorBucketRepository bucketRepository,
                        final MgDaoAccessorObjectRepository objectRepository,
                        final LocalBrokerService localBrokerService, final BulkMetrics bulkMetrics) {
    this.bucketRepository = bucketRepository;
    this.objectRepository = objectRepository;
    this.storageDriverFactory = DriverApiRegistry.getDriverApiFactory();
    this.localBrokerService = localBrokerService;
    this.bulkMetrics = bulkMetrics;
  }

  /**
   * INDEX : bucket, status, expired
   *
   * @param bucketForReadyExpired  if null, means delete, else move to this bucket. If the object is already in this
   *                               archive bucket, it will then be purged
   * @param futureExpireAddSeconds number of seconds > 0 to set a future expiration on archival process if any or 0 to
   *                               keep it forever
   */
  @Override
  public void purgeObjectsOnExpiredDate(final String clientId, final String bucketForReadyExpired,
                                        final long futureExpireAddSeconds) throws CcsDbException {
    List<DaoAccessorBucket> validBuckets;
    try (final var stream = bucketRepository.findStream(new DbQuery(RestQuery.QUERY.EQ, CLIENT_ID, clientId))) {
      validBuckets = stream.toList();
    }
    if (validBuckets.isEmpty()) {
      return;
    }
    final var validBucketNames = validBuckets.stream().map(DaoAccessorBucket::getId).toList();
    final AtomicReference<CcsDbException> possibleDbException = new AtomicReference<>(null);
    // Purge Deleted entries once expiry is over
    try {
      final BlockingQueue<List<DaoAccessorObject>> blockingQueue = new ArrayBlockingQueue<>(10);
      var semaphore = new Semaphore(0);
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(
          () -> internalDeleteOverExpire(blockingQueue, semaphore, possibleDbException));
      findAllExpiredDeletedObject(blockingQueue, possibleDbException, validBucketNames);
      semaphore.acquire();
      objectRepository.flushAll();
    } catch (final InterruptedException e) {// NOSONAR intentional
      throw new CcsDbException(e);
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
    // Purge Ready to Delete once expiry is over, setting a new Expiry date for later purge of Delete
    try {
      final BlockingQueue<List<DaoAccessorObject>> blockingQueue = new ArrayBlockingQueue<>(10);
      final var semaphore = new Semaphore(0);
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(
          () -> internalReadyOverExpire(blockingQueue, semaphore, possibleDbException, bucketForReadyExpired,
              futureExpireAddSeconds, clientId));
      findAllExpiredReadyObject(blockingQueue, possibleDbException, validBucketNames);
      semaphore.acquire();
      objectRepository.flushAll();
    } catch (final InterruptedException e) {// NOSONAR intentional
      throw new CcsDbException(e);
    }
    if (possibleDbException.get() != null) {
      throw possibleDbException.get();
    }
  }

  private void findAllExpiredDeletedObject(final BlockingQueue<List<DaoAccessorObject>> blockingQueue,
                                           final AtomicReference<CcsDbException> possibleDbException,
                                           final List<String> validBuckets)
      throws CcsDbException, InterruptedException {
    try (final var iterator = objectRepository.findIterator(
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, STATUS, DELETED.name()),
            new DbQuery(RestQuery.QUERY.LTE, DaoAccessorObjectRepository.EXPIRES, Instant.now()),
            new DbQuery(RestQuery.QUERY.IN, BUCKET, validBuckets)))) {
      final var listObject = new ArrayList<DaoAccessorObject>(MongoBulkInsertHelper.MAX_BATCH);
      long cpt = 0;
      while (iterator.hasNext()) {
        final var object = iterator.next();
        listObject.add(object);
        if (listObject.size() >= MongoBulkInsertHelper.MAX_BATCH) {
          blockingQueue.put(new ArrayList<>(listObject));
          listObject.clear();
        }
        cpt++;
      }
      if (!listObject.isEmpty()) {
        blockingQueue.put(new ArrayList<>(listObject));
        listObject.clear();
      }
      if (possibleDbException.get() != null) {
        throw possibleDbException.get();
      }
      LOGGER.infof("Deleted Expired items: %d", cpt);
    } finally {
      blockingQueue.put(Collections.emptyList());
    }
  }

  private void internalDeleteOverExpire(final BlockingQueue<List<DaoAccessorObject>> blockingQueue,
                                        final Semaphore semaphore,
                                        final AtomicReference<CcsDbException> possibleDbException) {
    try {
      final var nbThreads = ReconciliatorProperties.getReconciliatorThreads();
      final Runnable[] runnables = new Runnable[nbThreads];
      Arrays.fill(runnables, (Runnable) () -> runnableInternalDeleteOverExpire(blockingQueue, possibleDbException));
      MgDaoReconciliationUtils.runInThread(possibleDbException, runnables);
    } finally {
      LOGGER.debugf("End of Deletes: %d",
          (long) bulkMetrics.getCounter(PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_PURGE).count());
      semaphore.release();
    }
  }

  private void runnableInternalDeleteOverExpire(final BlockingQueue<List<DaoAccessorObject>> blockingQueue,
                                                final AtomicReference<CcsDbException> possibleDbException) {
    try {
      while (true) {
        var list = blockingQueue.take();
        if (list.isEmpty()) {
          // End and inform others
          blockingQueue.put(list);
          return;
        }
        purgeObjectAndDriver(possibleDbException, list);
        if (possibleDbException.get() != null) {
          return;
        }
      }
    } catch (final InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    } finally {
      LOGGER.debugf("End of Deletes on thread: %d",
          (long) bulkMetrics.getCounter(PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_PURGE).count());
    }
  }

  private void purgeObjectAndDriver(final AtomicReference<CcsDbException> possibleDbException,
                                    final List<DaoAccessorObject> list) {
    int cpt = 0;
    int cptDriverDelete = 0;
    try (final var driver = storageDriverFactory.getInstance()) {
      for (var expire : list) {
        var status = driverPurgeIfAny(driver, expire, possibleDbException);
        if (status == 1) {
          cptDriverDelete++;
        }
        if (status < 0) {
          continue;
        }
        objectRepository.delete(DbQuery.idEquals(expire.getId()));
        cpt++;
        if (ReconciliatorProperties.isReconciliatorPurgeLog()) {
          LOGGER.infof("Purge Deleted item: %s %s", expire.getBucket(), expire.getName());
        }
      }
    } catch (final RuntimeException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    } catch (final CcsDbException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, e);
    } finally {
      LOGGER.debugf("Purge Driver %d Purged %d", cptDriverDelete, cpt);
      bulkMetrics.incrementCounter(cpt, PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_PURGE);
    }
  }

  private int driverPurgeIfAny(final DriverApi driver, final DaoAccessorObject expire,
                               final AtomicReference<CcsDbException> possibleDbException) {
    try {
      if (driver.directoryOrObjectExistsInBucket(expire.getBucket(), expire.getName()).equals(StorageType.OBJECT)) {
        driver.objectDeleteInBucket(expire.getBucket(), expire.getName());
        if (ReconciliatorProperties.isReconciliatorPurgeLog()) {
          LOGGER.infof("Purge Deleted DriverObject item: %s %s", expire.getBucket(), expire.getName());
        }
        return 1;
      }
      return 0;
    } catch (final DriverNotFoundException ignore) {
      // Ignore
      return 0;
    } catch (final DriverException e) {
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
      return -1;
    }
  }

  private void findAllExpiredReadyObject(final BlockingQueue<List<DaoAccessorObject>> blockingQueue,
                                         final AtomicReference<CcsDbException> possibleDbException,
                                         final List<String> validBuckets) throws CcsDbException, InterruptedException {
    try (final var iterator = objectRepository.findIterator(
        new DbQuery(RestQuery.CONJUNCTION.AND, new DbQuery(RestQuery.QUERY.EQ, STATUS, READY.name()),
            new DbQuery(RestQuery.QUERY.LTE, DaoAccessorObjectRepository.EXPIRES, Instant.now()),
            new DbQuery(RestQuery.QUERY.IN, BUCKET, validBuckets)))) {
      final var listObject = new ArrayList<DaoAccessorObject>(MongoBulkInsertHelper.MAX_BATCH);
      long cpt = 0;
      while (iterator.hasNext()) {
        final var object = iterator.next();
        listObject.add(object);
        if (listObject.size() >= MongoBulkInsertHelper.MAX_BATCH) {
          blockingQueue.put(new ArrayList<>(listObject));
          listObject.clear();
        }
        if (possibleDbException.get() != null) {
          throw possibleDbException.get();
        }
        cpt++;
      }
      if (!listObject.isEmpty()) {
        blockingQueue.put(new ArrayList<>(listObject));
        listObject.clear();
      }
      LOGGER.infof("Ready Expired items: %d", cpt);
    } finally {
      blockingQueue.put(Collections.emptyList());
    }
  }

  private void internalReadyOverExpire(final BlockingQueue<List<DaoAccessorObject>> blockingQueue,
                                       final Semaphore semaphore,
                                       final AtomicReference<CcsDbException> possibleDbException,
                                       final String bucketForReadyExpired, final long futureExpireAddSeconds,
                                       final String clientId) {
    try {
      final var nbThreads = ReconciliatorProperties.getReconciliatorThreads();
      final Runnable[] runnables = new Runnable[nbThreads];
      Arrays.fill(runnables,
          (Runnable) () -> runnableInternalReadyOverExpire(blockingQueue, possibleDbException, bucketForReadyExpired,
              futureExpireAddSeconds, clientId));
      MgDaoReconciliationUtils.runInThread(possibleDbException, runnables);
    } finally {
      LOGGER.infof("End of Ready, Archived %d Deleted %d",
          (long) bulkMetrics.getCounter(PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ARCHIVE).count(),
          (long) bulkMetrics.getCounter(PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_DELETE).count());
      semaphore.release();
    }
  }

  private void runnableInternalReadyOverExpire(final BlockingQueue<List<DaoAccessorObject>> blockingQueue,
                                               final AtomicReference<CcsDbException> possibleDbException,
                                               final String bucketForReadyExpired, final long futureExpireAddSeconds,
                                               final String clientId) {
    try {
      while (true) {
        var list = blockingQueue.take();
        if (list.isEmpty()) {
          // End and inform others
          blockingQueue.put(list);
          return;
        }
        deleteDriverAndObjectExpired(possibleDbException, list, bucketForReadyExpired, futureExpireAddSeconds,
            clientId);
        if (possibleDbException.get() != null) {
          return;
        }
      }
    } catch (final InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      possibleDbException.compareAndSet(null, new CcsDbException(e));
    } finally {
      LOGGER.debugf("End of Ready on thread, Archives: %d Deleted: %d",
          (long) bulkMetrics.getCounter(PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ARCHIVE).count(),
          (long) bulkMetrics.getCounter(PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_DELETE).count());
    }
  }

  private void deleteDriverAndObjectExpired(final AtomicReference<CcsDbException> possibleDbException,
                                            final List<DaoAccessorObject> list, final String bucketForReadyExpired,
                                            final long futureExpireAddSeconds, final String clientId) {
    var cpt = 0;
    var cptArchive = 0;
    LOGGER.debugf("READY to purge %d", list.size());
    try (final var driver = storageDriverFactory.getInstance()) {
      for (var expire : list) {
        try {
          if (ParametersChecker.isNotEmpty(bucketForReadyExpired) &&
              !bucketForReadyExpired.equals(expire.getBucket())) {
            copyToArchiveOrPurgeIfAlreadyArchived(expire, bucketForReadyExpired, futureExpireAddSeconds, driver);
            cptArchive++;
          }
          deleteAsPublicClient(expire, futureExpireAddSeconds, driver, clientId);
          cpt++;
          if (ReconciliatorProperties.isReconciliatorPurgeLog()) {
            LOGGER.infof("Delete Expired item: %s %s", expire.getBucket(), expire.getName());
          }
        } catch (final DriverException | RuntimeException e) {
          LOGGER.warn(e, e);
          possibleDbException.compareAndSet(null, new CcsDbException(e));
        } catch (final CcsDbException e) {
          LOGGER.warn(e);
          possibleDbException.compareAndSet(null, e);
        }
      }
    } finally {
      LOGGER.debugf("Archived %d Delete %d", cptArchive, cpt);
      bulkMetrics.incrementCounter(cptArchive, PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_ARCHIVE);
      bulkMetrics.incrementCounter(cpt, PURGE_SERVICE, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_DELETE);
    }
  }

  private void deleteAsPublicClient(final DaoAccessorObject expire, final long futureExpireAddSeconds,
                                    final DriverApi driver, final String clientId) throws CcsDbException {
    // Delete as Public Accessor does
    try {
      if (StorageType.OBJECT.equals(driver.directoryOrObjectExistsInBucket(expire.getBucket(), expire.getName()))) {
        driver.objectDeleteInBucket(expire.getBucket(), expire.getName());
        if (ReconciliatorProperties.isReconciliatorPurgeLog()) {
          LOGGER.infof("Delete Expired DriverObject item: %s %s", expire.getBucket(), expire.getName());
        }
      }
    } catch (final DriverNotFoundException ignore) {
      // Ignore
    } catch (final DriverException e) {
      throw new CcsDbException(e);
    }
    updateAsDeleted(expire, futureExpireAddSeconds, clientId);
  }

  private void updateAsDeleted(final DaoAccessorObject expire, final long futureExpireAddSeconds, final String clientId)
      throws CcsDbException {
    var update = new DbUpdate().set(STATUS, DELETED.name()).set(CREATION, Instant.now());
    if (futureExpireAddSeconds > 0) {
      update.set(EXPIRES, Instant.now().plusSeconds(futureExpireAddSeconds));
    }
    objectRepository.update(DbQuery.idEquals(expire.getId()), update);
    // Signal Proactive delete order
    try {
      //Send use replicator service to send delete order.
      localBrokerService.deleteObject(expire.getBucket(), expire.getName(), clientId);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Relicator Object Deletion error", e);
    }
  }

  private void copyToArchiveOrPurgeIfAlreadyArchived(final DaoAccessorObject expire, final String bucketForReadyExpired,
                                                     final long futureExpireAddSeconds, final DriverApi driver)
      throws DriverException, CcsDbException {
    // Move to another bucket using Public Accessor for Delete only
    try {
      if (StorageType.OBJECT.equals(driver.directoryOrObjectExistsInBucket(expire.getBucket(), expire.getName()))) {
        Map<String, String> newMetadata = new HashMap<>();
        if (expire.getMetadata() != null) {
          newMetadata.putAll(expire.getMetadata());
        }
        newMetadata.put(ARCHIVED_FROM_BUCKET, expire.getBucket());
        newMetadata.put(ARCHIVED_FROM_ID, expire.getId());
        newMetadata.put(ARCHIVED_FROM_NAME, expire.getName());
        Instant futureExpiry = null;
        if (futureExpireAddSeconds > 0) {
          futureExpiry = Instant.now().plusSeconds(futureExpireAddSeconds);
        }
        StorageObject storageObjectSource =
            new StorageObject(expire.getBucket(), expire.getName(), expire.getHash(), expire.getSize(),
                expire.getCreation(), null, newMetadata);
        StorageObject storageObjectTarget =
            new StorageObject(bucketForReadyExpired, expire.getName(), expire.getHash(), expire.getSize(),
                expire.getCreation(), futureExpiry, newMetadata);
        driver.objectCopy(storageObjectSource, storageObjectTarget);
        var newObject = objectRepository.createEmptyItem().fromDto(expire.getDto());
        newObject.setExpires(storageObjectTarget.expiresDate()).setBucket(bucketForReadyExpired)
            .setId(GuidLike.getGuid()).setMetadata(newMetadata).setStatus(READY);
        objectRepository.insert(newObject);
        if (ReconciliatorProperties.isReconciliatorPurgeLog()) {
          LOGGER.infof("Archive Expired item: %s %s to %s", expire.getBucket(), expire.getName(),
              storageObjectTarget.bucket());
        }
      }
    } catch (final DriverNotFoundException ignore) {
      // Ignore
    }
  }
}
