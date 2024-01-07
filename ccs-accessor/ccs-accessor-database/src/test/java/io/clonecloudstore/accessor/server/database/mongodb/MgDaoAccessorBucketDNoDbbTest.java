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

package io.clonecloudstore.accessor.server.database.mongodb;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbType;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.test.resource.mongodb.NoMongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@TestProfile(NoMongoDbProfile.class)
class MgDaoAccessorBucketDNoDbbTest {

  @Inject
  Instance<DaoAccessorBucketRepository> repositoryInstance;
  MgDaoAccessorBucketRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = (MgDaoAccessorBucketRepository) repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void validDbModel() {
    assertThrows(CcsDbException.class, () -> repository.createIndex());
    // GIVEN
    final var creationDate = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final var bucket =
        new MgDaoAccessorBucket().setName("bucket").setId("client-bucket").setSite(AccessorProperties.getAccessorSite())
            .setStatus(AccessorStatus.UPLOAD);
    assertThrows(CcsDbException.class, () -> repository.insert((MgDaoAccessorBucket) bucket));

    assertThrows(CcsDbException.class, () -> repository.findOne(DbQuery.idEquals(bucket.getId())));

    assertThrows(CcsDbException.class,
        () -> repository.updateBucketStatus(bucket.getDto(), AccessorStatus.READY, creationDate));

    assertThrows(CcsDbException.class, () -> repository.findBucketById(bucket.getId()));

    assertThrows(CcsDbException.class, () -> repository.listBuckets("client"));
  }
}
