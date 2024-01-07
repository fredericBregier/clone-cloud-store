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

package io.clonecloudstore.accessor.server.resource;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.MongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(MongoKafkaProfile.class)
class AccessorObjectResourceNoS3Test {
  private static final Logger LOG = Logger.getLogger(AccessorObjectResourceNoS3Test.class);
  public static final String BUCKET_NAME = "testbucketnos3";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @Inject
  AccessorBucketApiFactory factoryBucket;
  @Inject
  AccessorObjectApiFactory factory;
  @Inject
  Instance<DaoAccessorObjectRepository> objectRepositoryInstance;
  DaoAccessorObjectRepository objectRepository;
  @Inject
  Instance<DaoAccessorBucketRepository> bucketRepositoryInstance;
  DaoAccessorBucketRepository bucketRepository;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();
    DriverS3Properties.setDynamicS3Parameters("http://127.0.0.1:9999", "AccessKey", "SecretKey", "Region");
  }

  @BeforeEach
  void beforeEach() {
    bucketRepository = bucketRepositoryInstance.get();
    objectRepository = objectRepositoryInstance.get();
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException, CcsDbException {
    final var finalBucketName = DaoAccessorBucketRepository.getBucketTechnicalName(clientId, BUCKET_NAME);
    createBucketAndObject(BUCKET_NAME, finalBucketName);
  }

  void createBucketAndObject(final String bucketName, final String finalBucketName)
      throws CcsWithStatusException, CcsDbException {
    try (final var client = factoryBucket.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName, clientId)).getStatus());
      // delete item in DB to ensure correctness later on
      bucketRepository.deleteAllDb();
    }
    try (final var client = factory.newClient()) {
      // 404 since Bucket does not exists
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, clientId, new FakeInputStream(100))).getStatus());
    }
    // Without creating object
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.NONE, objectType);
    }
    // With created object and bucket
    final var daoBucket = bucketRepository.createEmptyItem();
    daoBucket.setSite(AccessorProperties.getAccessorSite()).setId(finalBucketName)
        .setName(DaoAccessorBucketRepository.getBucketName(clientId, bucketName)).setCreation(Instant.now())
        .setStatus(AccessorStatus.READY);
    bucketRepository.insert(daoBucket);
    final var dao = objectRepository.createEmptyItem();
    dao.setId(GuidLike.getGuid()).setBucket(finalBucketName).setName(ParametersChecker.getSanitizedName(OBJECT))
        .setSite(AccessorProperties.getAccessorSite()).setCreation(Instant.now()).setStatus(AccessorStatus.READY);
    objectRepository.insert(dao);
    LOG.infof("DAO %s", dao);
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, OBJECT, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.OBJECT, objectType);
    }
    try (final var client = factory.newClient()) {
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      // No S3 usage
      assertEquals(StorageType.OBJECT,
          assertDoesNotThrow(() -> client.checkObjectOrDirectory(bucketName, OBJECT, clientId)));
    }
    try (final var client = factory.newClient()) {
      // Does not check Object but only in DB: shall we check real bucket?
      final var objectType = client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId);
      LOG.infof("ObjectType: %s", objectType);
      assertEquals(StorageType.DIRECTORY, objectType);
    }
    try (final var client = factory.newClient()) {
      final var object = client.getObjectInfo(bucketName, OBJECT, clientId);
      LOG.infof("Object: %s", object);
      Assertions.assertEquals(AccessorStatus.READY, object.getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      final var iterator = client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME));
      final var cpt = new AtomicLong(0);
      while (iterator.hasNext()) {
        final var accessorObject = iterator.next();
        cpt.incrementAndGet();
        LOG.infof("List %d: %s", cpt.get(), accessorObject);
      }
      ;
      assertEquals(1, cpt.get());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.deleteObject(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factoryBucket.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName, clientId)).getStatus());
    }
  }
}
