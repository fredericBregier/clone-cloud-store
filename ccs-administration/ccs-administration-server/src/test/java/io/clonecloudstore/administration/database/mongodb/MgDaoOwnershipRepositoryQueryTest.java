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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(MongoDbProfile.class)
class MgDaoOwnershipRepositoryQueryTest {
  private static final Logger logger = Logger.getLogger(MgDaoOwnershipRepositoryQueryTest.class);

  @Inject
  Instance<DaoOwnershipRepository> repositoryInstance;
  MgDaoOwnershipRepository repository;

  @BeforeEach
  void beforeEach() throws CcsDbException {
    repository = (MgDaoOwnershipRepository) repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
    repository.deleteAllDb();
  }

  @Test
  void checkConf() {
    logger.debugf("\n\nTesting dao ownership repository conf");

    assertEquals(DaoOwnershipRepository.TABLE_NAME, repository.getTable());
    assertDoesNotThrow(() -> repository.createIndex());
  }

  @Test
  void repositoryQueryValidation() {
    {
      logger.debugf("\n\nTesting ownership creation with status READ");
      final var createdTopology = assertDoesNotThrow(
          () -> repository.insertOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET,
              ClientOwnership.READ));
      assertEquals(ClientOwnership.READ, createdTopology);
    }
    {
      logger.debugf("\n\nTesting cannot create duplicate ownership");
      assertThrows(CcsDbException.class,
          () -> repository.insertOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET,
              ClientOwnership.READ));
    }
    {
      logger.debugf("\n\nTesting ownership creation with status WRITE");
      final var ownership = assertDoesNotThrow(
          () -> repository.insertOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET + 2,
              ClientOwnership.WRITE));
      assertEquals(ClientOwnership.WRITE, ownership);
    }
    {
      logger.debugf("\n\nTesting ownership retrieval by bucket");
      final var ownership =
          assertDoesNotThrow(() -> repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET));
      assertEquals(ClientOwnership.READ, ownership);
      assertDoesNotThrow(() -> assertNull(
          repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID + "wrong", Constants.OWNERSHIP_BUCKET)));
      assertDoesNotThrow(() -> assertNull(
          repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET + "wrong")));
    }
    {
      logger.debugf("\n\nTesting ownership list retrieval");
      final var bucketAccesses = assertDoesNotThrow(() -> repository.findAllOwnerships(Constants.OWNERSHIP_CLIENT_ID));
      assertNotNull(bucketAccesses);
      assertEquals(2, bucketAccesses.size());
      assertEquals(Constants.OWNERSHIP_BUCKET, bucketAccesses.get(0).bucket());
      assertEquals(Constants.OWNERSHIP_BUCKET + 2, bucketAccesses.get(1).bucket());
    }
    {
      logger.debugf("\n\nTesting available ownership list retrieval");
      final var bucketAccesses =
          assertDoesNotThrow(() -> repository.findOwnerships(Constants.OWNERSHIP_CLIENT_ID, ClientOwnership.READ));
      assertNotNull(bucketAccesses);
      assertEquals(1, bucketAccesses.size());
      assertEquals(Constants.OWNERSHIP_BUCKET, bucketAccesses.getFirst().bucket());
      final var bucketAccesses1 =
          assertDoesNotThrow(() -> repository.findOwnerships(Constants.OWNERSHIP_CLIENT_ID, ClientOwnership.UNKNOWN));
      assertNotNull(bucketAccesses1);
      assertEquals(0, bucketAccesses1.size());
    }
    {
      logger.debugf("\n\nTesting find ownership by bucket");
      final var ownership =
          assertDoesNotThrow(() -> repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET));
      assertEquals(ClientOwnership.READ, ownership);

      logger.debugf("\n\nTesting update ownership status");
      final var ownership1 = assertDoesNotThrow(
          () -> repository.updateOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET,
              ClientOwnership.WRITE));
      assertEquals(ClientOwnership.READ_WRITE, ownership1);
      assertThrows(CcsDbException.class,
          () -> repository.updateOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET + "notexists",
              ClientOwnership.READ));
    }
    {
      logger.debugf("\n\nTesting available topologies before ownership deletion");
      final var bucketAccesses =
          assertDoesNotThrow(() -> repository.findOwnerships(Constants.OWNERSHIP_CLIENT_ID, ClientOwnership.WRITE));
      assertNotNull(bucketAccesses);
      assertEquals(2, bucketAccesses.size());
      assertEquals(1, assertDoesNotThrow(
          () -> repository.findOwnerships(Constants.OWNERSHIP_CLIENT_ID, ClientOwnership.READ)).size());
    }
    {
      logger.debugf("\n\nTesting delete ownership");
      assertDoesNotThrow(() -> repository.deleteOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET));
      final var all = assertDoesNotThrow(() -> repository.findAllOwnerships(Constants.OWNERSHIP_CLIENT_ID));
      assertNotNull(all);
      assertEquals(1, all.size());

      final var available =
          assertDoesNotThrow(() -> repository.findOwnerships(Constants.OWNERSHIP_CLIENT_ID, ClientOwnership.READ));
      assertNotNull(available);
      assertEquals(0, available.size());
    }
    {
      logger.debugf("\n\nTesting delete ownership that do not exists");
      assertThrows(CcsDbException.class, () -> repository.deleteOwnership(Constants.OWNERSHIP_CLIENT_ID, "xx"));
    }
    {
      logger.debugf("\n\nTesting find ownership by site after deletion");
      final var ownership =
          assertDoesNotThrow(() -> repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET));
      assertNull(ownership);
      var ownership1 = assertDoesNotThrow(
          () -> repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET + 2));
      assertDoesNotThrow(() -> repository.deleteOwnerships(Constants.OWNERSHIP_BUCKET + 2));
    }
  }
}
