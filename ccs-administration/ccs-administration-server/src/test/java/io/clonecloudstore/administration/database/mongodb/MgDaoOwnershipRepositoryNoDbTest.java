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
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(NoMongoDbProfile.class)
class MgDaoOwnershipRepositoryNoDbTest {
  @Inject
  Instance<DaoOwnershipRepository> repositoryInstance;
  DaoOwnershipRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void checkNoDbException() {
    assertThrows(CcsDbException.class, () -> ((MgDaoOwnershipRepository) repository).createIndex());
    assertThrows(CcsDbException.class, () -> repository.findAllOwnerships(Constants.OWNERSHIP_CLIENT_ID));
    assertThrows(CcsDbException.class,
        () -> repository.findOwnerships(Constants.OWNERSHIP_CLIENT_ID, ClientOwnership.READ));
    assertThrows(CcsDbException.class,
        () -> repository.insertOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET,
            ClientOwnership.READ));
    assertThrows(CcsDbException.class,
        () -> repository.deleteOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET));
    assertThrows(CcsDbException.class,
        () -> repository.updateOwnership(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET,
            ClientOwnership.READ));
    assertThrows(CcsDbException.class,
        () -> repository.findByBucket(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET));
  }
}
