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

package io.clonecloudstore.administration.database.mongodb;

import io.clonecloudstore.administration.database.model.DaoOwnershipRepository;
import io.clonecloudstore.administration.database.mongodb.conf.Constants;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.database.utils.DbType;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.administration.database.mongodb.conf.Constants.OWNERSHIP_BUCKET;
import static io.clonecloudstore.administration.database.mongodb.conf.Constants.OWNERSHIP_CLIENT_ID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
class MgDaoOwnershipRepositoryDataTest {
  private static final Logger logger = Logger.getLogger(MgDaoOwnershipRepositoryDataTest.class);

  @Inject
  Instance<DaoOwnershipRepository> repositoryInstance;
  DaoOwnershipRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void constraintViolationEmptyData() throws CcsDbException {
    logger.debugf("\n\nTesting ownership creation failure with empty attributes");
    final var count = repository.findAllOwnerships(OWNERSHIP_CLIENT_ID).size();
    // insert empty ownership should not be possible
    assertThrows(CcsDbException.class, () -> repository.insertOwnership(null, null, null));

    // insert ownership with empty site should not be possible
    assertThrows(CcsDbException.class,
        () -> repository.insertOwnership(null, Constants.OWNERSHIP_BUCKET, ClientOwnership.READ));

    // insert ownership with empty name should not be possible
    assertThrows(CcsDbException.class,
        () -> repository.insertOwnership(OWNERSHIP_CLIENT_ID, null, ClientOwnership.READ));

    // insert ownership with empty status leads to a UNKNOWN STATUS
    assertEquals(ClientOwnership.UNKNOWN, repository.insertOwnership(OWNERSHIP_CLIENT_ID, OWNERSHIP_BUCKET, null));

    // double check no ownership was inserted except 1
    final var bucketAccesses = assertDoesNotThrow(() -> repository.findAllOwnerships(OWNERSHIP_CLIENT_ID));
    assertEquals(count + 1, bucketAccesses.size());
    assertDoesNotThrow(() -> repository.deleteOwnerships(OWNERSHIP_BUCKET));
  }

  @Test
  void constraintViolationWrongData() throws CcsDbException {
    logger.debugf("\n\nTesting ownership creation with invalid content");
    final var count = repository.findAllOwnerships(OWNERSHIP_CLIENT_ID).size();
    {
      // insert name too long should not be possible
      assertThrows(CcsDbException.class,
          () -> repository.insertOwnership(OWNERSHIP_CLIENT_ID, Constants.INVALID_NAME, ClientOwnership.READ));
    }
    {
      // double check no ownership was inserted
      final var bucketAccesses = assertDoesNotThrow(() -> repository.findAllOwnerships(OWNERSHIP_CLIENT_ID));
      assertEquals(bucketAccesses.size(), count);
    }
    {
      // double check no ownership was inserted
      final var bucketAccesses = assertDoesNotThrow(() -> repository.findAllOwnerships(OWNERSHIP_CLIENT_ID));
      assertEquals(bucketAccesses.size(), count);
    }
  }

  @Test
  void constraintViolationDuplicateKey() {
    logger.debugf("\n\nTesting ownership duplicate creation failure with same attributes");

    final var bucketAccesses = assertDoesNotThrow(() -> repository.findAllOwnerships(OWNERSHIP_CLIENT_ID + "new"));
    // insert first ownership
    final var ownership = assertDoesNotThrow(
        () -> repository.insertOwnership(OWNERSHIP_CLIENT_ID + "new", OWNERSHIP_BUCKET, ClientOwnership.READ));
    assertNotNull(ownership);

    // insert same ownership should not be possible
    assertThrows(CcsDbException.class,
        () -> repository.insertOwnership(OWNERSHIP_CLIENT_ID + "new", OWNERSHIP_BUCKET, ClientOwnership.READ));

    // double check only one ownership is stored in database
    final var bucketAccesses1 = assertDoesNotThrow(() -> repository.findAllOwnerships(OWNERSHIP_CLIENT_ID + "new"));
    assertEquals(bucketAccesses.size() + 1, bucketAccesses1.size());
    assertDoesNotThrow(() -> repository.deleteOwnerships(OWNERSHIP_BUCKET));
  }
}
