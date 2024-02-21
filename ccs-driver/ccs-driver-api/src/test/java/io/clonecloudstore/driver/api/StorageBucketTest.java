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

package io.clonecloudstore.driver.api;

import java.time.Instant;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class StorageBucketTest {

  @Test
  void checkBean() {
    final var bucket = new StorageBucket("name", "client", Instant.now());
    final var bucket1 = new StorageBucket(bucket.bucket(), bucket.clientId(), bucket.creationDate());
    assertEquals(bucket1, bucket);
    assertEquals(bucket1, bucket);
    assertEquals(bucket1.hashCode(), bucket.hashCode());
    assertEquals(bucket1, bucket1);
    assertTrue(bucket1.toString().contains(bucket1.bucket()));
    assertTrue(bucket1.toString().contains(bucket1.creationDate().toString()));
    Log.info(bucket);
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> new StorageBucket("test@test", "client", null));
    final var bucket2 = new StorageBucket(null, null, null);
    assertNotEquals(bucket, bucket2);
    assertNotEquals(-1, bucket2.hashCode());
    assertFalse(bucket.equals(new Object()));
  }
}
