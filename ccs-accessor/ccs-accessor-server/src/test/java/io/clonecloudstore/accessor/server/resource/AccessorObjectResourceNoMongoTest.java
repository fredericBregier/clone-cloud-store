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

import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.MinioKafkaProfile;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(MinioKafkaProfile.class)
class AccessorObjectResourceNoMongoTest {
  private static final Logger LOG = Logger.getLogger(AccessorObjectResourceNoMongoTest.class);
  public static final String BUCKET_NAME = "testbucket";
  public static final String DIR_NAME = "dir/";
  public static final String OBJECT = DIR_NAME + "testObject";

  @Inject
  AccessorBucketApiFactory factoryBucket;
  @Inject
  AccessorObjectApiFactory factory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();

    // Bug fix on "localhost"
    var url = MinIoResource.getUrlString();
    if (url.contains("localhost")) {
      url = url.replace("localhost", "127.0.0.1");
    }
    DriverS3Properties.setDynamicS3Parameters(url, MinIoResource.getAccessKey(), MinIoResource.getSecretKey(),
        MinIoResource.getRegion());
  }

  @Test
  void createBucketAndObject() throws CcsWithStatusException {
    final var finalBucketName = DaoAccessorBucketRepository.getBucketTechnicalName(clientId, BUCKET_NAME);
    createBucketAndObject(BUCKET_NAME);
  }

  void createBucketAndObject(final String bucketName) {
    try (final var client = factoryBucket.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName, clientId)).getStatus());
      LOG.info("CreateBucket");
    }
    try (final var client = factory.newClient()) {
      // 404 since Bucket does not exists
      final var accessorObject = new AccessorObject().setBucket(bucketName).setName(OBJECT).setSize(100);
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.createObject(accessorObject, clientId, new FakeInputStream(100))).getStatus());
      LOG.info("CreateObject");
    }
    // Without creating object
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectory(bucketName, OBJECT, clientId)).getStatus());
      LOG.info("CheckObject");
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectory(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectory(bucketName, DIR_NAME, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.getObjectInfo(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, OBJECT, clientId)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.listObjects(bucketName, clientId, new AccessorFilter().setNamePrefix(DIR_NAME))).getStatus());
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
