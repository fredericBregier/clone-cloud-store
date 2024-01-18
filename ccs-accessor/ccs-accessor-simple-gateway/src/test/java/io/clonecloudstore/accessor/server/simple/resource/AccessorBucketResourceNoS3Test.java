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

package io.clonecloudstore.accessor.server.simple.resource;

import java.util.UUID;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.test.resource.NoResourceProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(NoResourceProfile.class)
class AccessorBucketResourceNoS3Test {
  private static final Logger LOG = Logger.getLogger(AccessorBucketResourceNoS3Test.class);
  @Inject
  AccessorBucketApiFactory factory;
  private static String clientId = null;

  @BeforeAll
  static void setup() {
    clientId = UUID.randomUUID().toString();
  }

  @Test
  void testInternalExceptions() {
    final var bucketName1 = "testinternalexception1";
    final var bucketName2 = "testinternalexception2";
    // save S3 parameters for later
    try (final var client = factory.newClient()) {
      // THEN
      // Only case where No S3 leads to issue
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.createBucket(bucketName1, clientId)).getStatus());
      // No S3 usage from here
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.checkBucket(bucketName1, clientId)).getStatus());
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(bucketName1, clientId)).getStatus());
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.deleteBucket(bucketName1, clientId)).getStatus());
    }
  }
}
