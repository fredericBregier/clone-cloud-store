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
import java.util.HashMap;

import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbType;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
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
class MgDaoAccessorObjectDbNoDbTest {

  @Inject
  Instance<DaoAccessorObjectRepository> repositoryInstance;
  MgDaoAccessorObjectRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = (MgDaoAccessorObjectRepository) repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void validDbModel() {
    assertThrows(CcsDbException.class, () -> repository.createIndex());
    // GIVEN
    final var creationDate = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final var metadata = new HashMap<String, String>();
    metadata.put("HEAD1", "Value 1");
    metadata.put("HEAD2", "Value 2");
    final var object = new MgDaoAccessorObject().setId(GuidLike.getGuid()).setSite(AccessorProperties.getAccessorSite())
        .setBucket("test-bucket").setName("dir/TestObject").setHash("aaaqqqzzzwww").setStatus(AccessorStatus.UPLOAD)
        .setCreation(creationDate).setSize(42).setMetadata(metadata);
    assertThrows(CcsDbException.class, () -> repository.insert((MgDaoAccessorObject) object));
    assertThrows(CcsDbException.class, () -> repository.findOne(DbQuery.idEquals(object.getId())));
    assertThrows(CcsDbException.class,
        () -> repository.updateObjectStatus(object.getBucket(), object.getName(), AccessorStatus.READY, null));
    assertThrows(CcsDbException.class, () -> repository.getObject(object.getBucket(), object.getName()));
    assertThrows(CcsDbException.class,
        () -> repository.getObject(object.getBucket(), object.getName(), AccessorStatus.READY));
    assertThrows(CcsDbException.class,
        () -> repository.getObjectPrefix(object.getBucket(), object.getName(), AccessorStatus.READY));
    assertThrows(CcsDbException.class,
        () -> repository.updateObjectStatusHashLen(object.getBucket(), object.getName(), AccessorStatus.UPLOAD, "hash2",
            110));
    assertThrows(CcsDbException.class, () -> repository.getObjectPrefix(object.getBucket(), object.getName(), null));
    assertThrows(CcsDbException.class, () -> repository.updateFull((MgDaoAccessorObject) object));
  }

}
