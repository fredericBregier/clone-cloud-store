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

package io.clonecloudstore.administration.model;

import io.clonecloudstore.administration.model.conf.Constants;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.model.ClientOwnership.READ;
import static io.clonecloudstore.administration.model.ClientOwnership.READ_WRITE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ClientBucketAccessTest {
  private static final Logger logger = Logger.getLogger(ClientBucketAccessTest.class);

  @Test
  void checkDtoInit() {
    logger.debugf("Testing bean dto initialisation...");
    final var relation = assertDoesNotThrow(() -> new ClientBucketAccess(Constants.CLIENT_ID, Constants.BUCKET, READ));
    final var relation2 =
        assertDoesNotThrow(() -> new ClientBucketAccess(Constants.CLIENT_ID, Constants.BUCKET, READ_WRITE));
    assertTrue(relation.equals(relation));
    assertFalse(relation.equals(relation2));
    assertTrue(relation.include(READ));
    assertTrue(relation2.include(READ));
    assertFalse(relation.include(READ_WRITE));
    assertTrue(relation2.include(READ_WRITE));

    assertFalse(relation.equals(logger));

    logger.debugf("Testing dto attributes format");
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new ClientBucketAccess(Constants.CLIENT_ID, "aa__bb", READ));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> new ClientBucketAccess("<script>", Constants.BUCKET, READ));
    assertTrue(relation.toString().contains(Constants.CLIENT_ID));
  }
}
