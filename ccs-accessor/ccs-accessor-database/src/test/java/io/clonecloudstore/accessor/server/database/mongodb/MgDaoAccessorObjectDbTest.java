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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@TestProfile(MongoDbProfile.class)
class MgDaoAccessorObjectDbTest {

  @Inject
  Instance<DaoAccessorObjectRepository> repositoryInstance;
  MgDaoAccessorObjectRepository repository;

  @BeforeEach
  void beforeEach() {
    repository = (MgDaoAccessorObjectRepository) repositoryInstance.get();
    assertTrue(DbType.getInstance().isMongoDbType());
  }

  @Test
  void validDbModel() throws CcsDbException {
    repository.createIndex();
    // GIVEN
    final var creationDate = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final var metadata = new HashMap<String, String>();
    metadata.put("HEAD1", "Value 1");
    metadata.put("HEAD2", "Value 2");
    final var object = new MgDaoAccessorObject().setId(GuidLike.getGuid()).setSite(AccessorProperties.getAccessorSite())
        .setBucket("test-bucket").setName("dir/TestObject").setHash("aaaqqqzzzwww").setStatus(AccessorStatus.UPLOAD)
        .setCreation(creationDate).setSize(42).setMetadata(metadata);
    repository.insert((MgDaoAccessorObject) object);
    var object2 = repository.findOne(DbQuery.idEquals(object.getId()));
    assertEquals(object, object2);
    assertEquals("Value 1", object2.getMetadata("HEAD1"));
    try {
      repository.updateObjectStatus(object.getBucket(), object.getName(), AccessorStatus.READY, null);
    } catch (final CcsDbException e) {
      Assertions.fail(e);
    }
    object2 = repository.findOne(DbQuery.idEquals(object.getId()));
    assertNotEquals(object, object2);
    assertEquals(AccessorStatus.READY, object2.getStatus());
    var object3 = repository.getObject(object.getBucket(), object.getName());
    assertEquals(object2, object3);
    object3 = repository.getObject(object.getBucket(), object.getName(), AccessorStatus.READY);
    assertEquals(object2, object3);
    var iterator = repository.getObjectPrefix(object.getBucket(), object.getName(), AccessorStatus.READY);
    iterator.hasNext();
    object3 = iterator.next();
    assertEquals(object2, object3);
    iterator = repository.getObjectPrefix(object.getBucket(), "dir/", AccessorStatus.READY);
    iterator.hasNext();
    object3 = iterator.next();
    assertEquals(object2, object3);
    try {
      repository.updateObjectStatusHashLen(object.getBucket(), object.getName(), AccessorStatus.UPLOAD, "hash2", 110);
    } catch (final CcsDbException e) {
      Assertions.fail(e);
    }
    object3 = repository.getObject(object.getBucket(), object.getName());
    assertNotEquals(object2, object3);
    assertEquals(110, object3.getSize());
    assertEquals("hash2", object3.getHash());
    assertEquals(AccessorStatus.UPLOAD, object3.getStatus());
    iterator = repository.getObjectPrefix(object.getBucket(), object.getName(), AccessorStatus.READY);
    assertFalse(iterator.hasNext());
    iterator = repository.getObjectPrefix(object.getBucket(), object.getName(), AccessorStatus.UPLOAD);
    assertTrue(iterator.hasNext());
    iterator = repository.getObjectPrefix(object.getBucket(), object.getName(), null);
    assertTrue(iterator.hasNext());
    assertEquals(object3, object3);
    assertNotEquals(object3, new Object());
    object3.addMetadata("HEAD3", "Value3");
    repository.updateFull((MgDaoAccessorObject) object3);
    final var object4 = repository.getObject(object.getBucket(), object.getName());
    assertEquals(object3, object4);
    assertEquals("Value3", object4.getMetadata("HEAD3"));
    object4.addMetadata("HEAD3", "Value4");
    repository.updateFull((MgDaoAccessorObject) object4);
    final var object5 = repository.getObject(object.getBucket(), object.getName());
    assertNotEquals(object3, object5);
    assertEquals(object4, object5);
    assertEquals("Value4", object5.getMetadata("HEAD3"));
    final var object6 = new MgDaoAccessorObject(object5.getDto());
    assertEquals(object5, object6);
    object6.setMetadata(metadata);
    repository.updateFull(object6);
    final var object7 = repository.getObject(object.getBucket(), object.getName());
    assertEquals(object6, object7);
    assertNotEquals(object5, object7);

    // WHEN inserting twice, get DbException
    final var twice = repository.createEmptyItem().fromDto(object7.getDto());
    assertThrows(CcsDbException.class, () -> repository.insert(twice));
    repository.deleteAllDb();
  }

}
