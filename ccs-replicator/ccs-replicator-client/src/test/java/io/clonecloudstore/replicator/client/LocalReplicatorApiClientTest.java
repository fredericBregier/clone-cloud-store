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

package io.clonecloudstore.replicator.client;

import java.time.Instant;

import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.replicator.model.ReplicatorResponse;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.replicator.test.conf.Constants.BUCKET_NAME;
import static io.clonecloudstore.replicator.test.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.replicator.test.conf.Constants.FULL_CHECK;
import static io.clonecloudstore.replicator.test.conf.Constants.OBJECT_PATH;
import static io.clonecloudstore.replicator.test.conf.Constants.OP_ID;
import static io.clonecloudstore.replicator.test.conf.Constants.REMOTE_READ_STREAM_LEN;
import static io.clonecloudstore.replicator.test.conf.Constants.SITE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class LocalReplicatorApiClientTest {
  private static final Logger logger = Logger.getLogger(LocalReplicatorApiClientTest.class);
  @Inject
  LocalReplicatorApiClientFactory factory;

  @BeforeAll
  static void beforeAll()
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException, DriverNotFoundException {
    final var driver = DriverApiRegistry.getDriverApiFactory().getInstance();
    driver.bucketCreate(new StorageBucket(BUCKET_NAME, null));
    final var objectName = ParametersChecker.getSanitizedName(OBJECT_PATH);
    driver.objectPrepareCreateInBucket(
        new StorageObject(BUCKET_NAME, objectName, "hash", REMOTE_READ_STREAM_LEN, Instant.now()),
        new FakeInputStream(REMOTE_READ_STREAM_LEN));
    driver.objectFinalizeCreateInBucket(BUCKET_NAME, objectName, REMOTE_READ_STREAM_LEN, "hash");
  }

  @Test
  void checkBucketApiResponses() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting check bucket found responses");
        FakeCommonBucketResourceHelper.errorCode = 0;
        final var storageType =
            Assertions.assertDoesNotThrow(() -> client.checkBucket(BUCKET_NAME, FULL_CHECK, CLIENT_ID, OP_ID));
        assertEquals(new ReplicatorResponse<>(StorageType.BUCKET, null), storageType);
      }
      {
        logger.debugf("\n\nTesting check bucket not found responses");
        FakeCommonBucketResourceHelper.errorCode = 404;
        final var storageType =
            Assertions.assertDoesNotThrow(() -> client.checkBucket(BUCKET_NAME, FULL_CHECK, CLIENT_ID, OP_ID));
        assertEquals(new ReplicatorResponse<>(StorageType.NONE, null), storageType);
      }
      {
        logger.debugf("\n\nTesting check bucket error responses");
        FakeCommonBucketResourceHelper.errorCode = 400;
        assertThrows(CcsWithStatusException.class, () -> {
          client.checkBucket(BUCKET_NAME, FULL_CHECK, CLIENT_ID, OP_ID);
        });
      }
    }
  }

  @Test
  void checkObjectApiResponses() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting check object found responses");
        FakeCommonObjectResourceHelper.errorCode = 0;
        final var storageType = Assertions.assertDoesNotThrow(
            () -> client.checkObjectOrDirectory(BUCKET_NAME, OBJECT_PATH, FULL_CHECK, CLIENT_ID, OP_ID));
        assertEquals(new ReplicatorResponse<>(StorageType.OBJECT, null), storageType);
      }
      {
        logger.debugf("\n\nTesting check object not found responses");
        FakeCommonObjectResourceHelper.errorCode = 404;
        final var storageType = Assertions.assertDoesNotThrow(
            () -> client.checkObjectOrDirectory(BUCKET_NAME, OBJECT_PATH, FULL_CHECK, CLIENT_ID, OP_ID));
        assertEquals(new ReplicatorResponse<>(StorageType.NONE, null), storageType);
      }
      {
        logger.debugf("\n\nTesting check object error responses");
        FakeCommonObjectResourceHelper.errorCode = 400;
        assertThrows(CcsWithStatusException.class, () -> {
          client.checkObjectOrDirectory(BUCKET_NAME, OBJECT_PATH, FULL_CHECK, CLIENT_ID, OP_ID);
        });
      }
    }
  }

  @Test
  void checkRemoteReadApiResponses() {
    try (final var client = factory.newClient()) {
      {
        logger.debugf("\n\nTesting read remote object");
        FakeCommonObjectResourceHelper.errorCode = 0;
        FakeCommonObjectResourceHelper.length = REMOTE_READ_STREAM_LEN;
        final var stream = Assertions.assertDoesNotThrow(
            () -> client.readRemoteObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID, "", OP_ID).inputStream());
        final var len = assertDoesNotThrow(() -> FakeInputStream.consumeAll(stream));
        assertEquals(REMOTE_READ_STREAM_LEN, len);
      }
      {
        logger.debugf("\n\nTesting read remote object");
        FakeCommonObjectResourceHelper.errorCode = 0;
        FakeCommonObjectResourceHelper.length = REMOTE_READ_STREAM_LEN;
        final var stream = Assertions.assertDoesNotThrow(
            () -> client.readRemoteObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID, SITE, OP_ID).inputStream());
        final var len = assertDoesNotThrow(() -> FakeInputStream.consumeAll(stream));
        assertEquals(REMOTE_READ_STREAM_LEN, len);
      }

      logger.debugf("\n\nTesting read remote object not found");
      FakeCommonObjectResourceHelper.errorCode = 404;
      FakeCommonObjectResourceHelper.length = REMOTE_READ_STREAM_LEN;
      final CcsWithStatusException e = assertThrows(CcsWithStatusException.class, () -> {
        client.readRemoteObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID, "", OP_ID).inputStream();
      });
      assertEquals(e.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }
  }
}
