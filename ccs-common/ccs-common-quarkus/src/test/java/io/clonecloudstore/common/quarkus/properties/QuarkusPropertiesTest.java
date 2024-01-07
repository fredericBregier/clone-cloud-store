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

package io.clonecloudstore.common.quarkus.properties;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class QuarkusPropertiesTest {
  @Inject
  Vertx vertx;

  @Test
  void testProperties() {
    final var isSha256 = QuarkusProperties.serverComputeSha256();
    QuarkusProperties.setServerComputeSha256(false);
    assertFalse(QuarkusProperties.serverComputeSha256());
    QuarkusProperties.setServerComputeSha256(true);
    assertTrue(QuarkusProperties.serverComputeSha256());
    QuarkusProperties.setServerComputeSha256(isSha256);

    final var maxTransferMs = QuarkusProperties.getDurationResponseTimeout().toMillis();
    QuarkusProperties.setClientResponseTimeOut(maxTransferMs + 1000);
    assertEquals(maxTransferMs + 1000, QuarkusProperties.getDurationResponseTimeout().toMillis());
    QuarkusProperties.setClientResponseTimeOut(maxTransferMs);
    assertEquals(maxTransferMs, QuarkusProperties.getDurationResponseTimeout().toMillis());

    assertEquals(vertx.getDelegate(), QuarkusProperties.getVertx());
  }

}
