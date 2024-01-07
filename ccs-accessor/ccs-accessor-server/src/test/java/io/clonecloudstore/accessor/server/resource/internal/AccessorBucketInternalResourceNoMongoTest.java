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

package io.clonecloudstore.accessor.server.resource.internal;

import java.util.UUID;

import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.s3.DriverS3Properties;
import io.clonecloudstore.test.resource.s3.MinIoResource;
import io.clonecloudstore.test.resource.s3.MinioProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(MinioProfile.class)
class AccessorBucketInternalResourceNoMongoTest {

  @Inject
  AccessorBucketInternalApiFactory factory;
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
  void getBucketReplicator() {
    final var bucketName = "testgetbucket2";
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.getBucket(DaoAccessorBucketRepository.getPrefix(clientId) + bucketName, clientId)).getStatus());

      // Check bucket not exist
      final var bucketUnknownName = "unknown";
      final var unknownException =
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketUnknownName, clientId));
      assertEquals(500, unknownException.getStatus());
    }
  }

  @Test
  void getBuckets() {
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class, () -> client.getBuckets(clientId)).getStatus());
    }
  }

  @Test
  void checkBucketReplicator() {
    final var bucketName = "testcheckbucket2";
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkBucket(DaoAccessorBucketRepository.getPrefix(clientId) + bucketName, clientId,
              true)).getStatus());
    }
    try (final var client = factory.newClient()) {
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkBucket(DaoAccessorBucketRepository.getPrefix(clientId) + bucketName, clientId,
              false)).getStatus());
    }
  }
}
