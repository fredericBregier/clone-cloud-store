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

import java.io.IOException;
import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.server.FakeActionTopicConsumer;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.CleanupTestUtil;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.resource.AzureMongoKafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(AzureMongoKafkaProfile.class)
class AccessorBucketObjectWithRemoteResourceTest {
  private static final Logger LOG = Logger.getLogger(AccessorBucketObjectWithRemoteResourceTest.class);
  @Inject
  AccessorBucketApiFactory factoryBucket;
  @Inject
  AccessorObjectApiFactory factoryObject;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();

    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
  }

  @BeforeEach
  void beforeEach() {
    FakeActionTopicConsumer.reset();
    // Clean all
    CleanupTestUtil.cleanUp();
  }

  @Test
  void checkBucket() {
    final var bucketName = "testcheckbucket13";
    assertTrue(AccessorProperties.isRemoteRead());
    assertTrue(AccessorProperties.isFixOnAbsent());
    try (final var client = factoryBucket.newClient()) {
      assertEquals(0, FakeActionTopicConsumer.getBucketCreate());
      // check non-existing bucket
      FakeCommonBucketResourceHelper.errorCode = 404;
      assertEquals(StorageType.NONE, client.checkBucket(bucketName, clientId));
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName, clientId));
      assertEquals(0, FakeActionTopicConsumer.getBucketCreateFromTopic(0));

      // simple check existing bucket
      FakeCommonBucketResourceHelper.errorCode = 204;
      assertEquals(StorageType.BUCKET, client.checkBucket(bucketName, clientId));
      var res = client.getBucket(bucketName, clientId);
      assertEquals(bucketName, res.getId());
      assertEquals(1, FakeActionTopicConsumer.getBucketCreateFromTopic(1));
    } catch (final CcsWithStatusException e) {
      fail(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private long getObjectCreateFromTopic(final long desired) throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      final var value = FakeActionTopicConsumer.getObjectCreate();
      if (value >= desired) {
        Log.infof("Found %d", value);
        return value;
      }
      Thread.sleep(10);
    }
    return FakeActionTopicConsumer.getObjectCreate();
  }

  @Test
  void checkObject() {
    final var bucketName = "testcheckbucket14";
    final var objectName = "dir/testcheckobject1";
    assertTrue(AccessorProperties.isRemoteRead());
    assertTrue(AccessorProperties.isFixOnAbsent());
    try (final var client = factoryObject.newClient()) {
      assertEquals(0, getObjectCreateFromTopic(0));
      // check non-existing object
      FakeCommonBucketResourceHelper.errorCode = 204;
      FakeCommonObjectResourceHelper.errorCode = 404;
      var res = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      assertEquals(StorageType.NONE, res);
      Thread.sleep(100);
      assertEquals(0, getObjectCreateFromTopic(0));
      assertThrows(CcsWithStatusException.class, () -> client.getObject(bucketName, objectName, clientId));
      Thread.sleep(100);
      assertEquals(0, getObjectCreateFromTopic(0));

      // simple check existing object
      FakeCommonBucketResourceHelper.errorCode = 204;
      FakeCommonObjectResourceHelper.errorCode = 204;
      res = client.checkObjectOrDirectory(bucketName, objectName, clientId);
      assertEquals(StorageType.OBJECT, res);
      assertEquals(1, getObjectCreateFromTopic(1));
      // Same using get
      final var res2 = client.getObject(bucketName, objectName, clientId);
      FakeInputStream.consumeAll(res2.inputStream());
      assertEquals(objectName, res2.dtoOut().getName());
      assertEquals(2, getObjectCreateFromTopic(2));
    } catch (final CcsWithStatusException e) {
      fail(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
