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

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbType;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.test.resource.mongodb.MongoDbProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@TestProfile(MongoDbProfile.class)
class MgDaoAccessorBucketDbTest {

  @Inject
  Instance<DaoAccessorBucketRepository> repositoryInstance;
  MgDaoAccessorBucketRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = (MgDaoAccessorBucketRepository) repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void validDbModel() throws CcsDbException {
    repository.createIndex();
    // GIVEN
    final var creationDate = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final var bucket = new MgDaoAccessorBucket().setId("bucket").setSite(AccessorProperties.getAccessorSite())
        .setStatus(AccessorStatus.UPLOAD).setClientId("client");
    repository.insert((MgDaoAccessorBucket) bucket);

    // WHEN
    var bucket2 = repository.findOne(DbQuery.idEquals(bucket.getId()));
    // THEN
    assertEquals(bucket, bucket2);
    assertTrue(bucket2.toString().contains("bucket"));

    // WHEN
    repository.updateBucketStatus(bucket.getDto(), AccessorStatus.READY, creationDate);
    bucket2 = repository.findOne(DbQuery.idEquals(bucket.getId()));
    // THEN
    assertNotEquals(bucket, bucket2);
    Assertions.assertEquals(AccessorStatus.READY, bucket2.getStatus());
    Assertions.assertEquals(creationDate, bucket2.getCreation());

    // WHEN
    final var bucket3 = repository.findBucketById(bucket.getId());
    // THEN
    Assertions.assertEquals(bucket2.getDto(), bucket3);

    // WHEN inserting twice, get DbException
    final var twice = repository.createEmptyItem().fromDto(bucket3);
    assertThrows(CcsDbException.class, () -> repository.insert(twice));

    // WHEN (adding a bucket using insertBucket from DTO)
    final var dtoBucket5 = new AccessorBucket().setId("bucket3").setSite(AccessorProperties.getAccessorSite())
        .setStatus(AccessorStatus.UPLOAD).setCreation(creationDate).setClientId("client");
    repository.insertBucket(dtoBucket5);
    var bucketList = repository.listBuckets("client");
    // THEN (it's listed)
    Assertions.assertEquals(2, bucketList.size());
  }
}
