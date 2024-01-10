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

package io.clonecloudstore.accessor.server.database.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class DaoTest {
  private static final String clientId = "clientid";
  private static final String bucket = "bucket";
  private static final String object = "object";

  static class TestDaoAccessorBucket extends DaoAccessorBucket {
    private String id;

    @Override
    public String getId() {
      return id;
    }

    @Override
    public DaoAccessorBucket setId(final String guid) {
      this.id = guid;
      return this;
    }
  }

  @Test
  void testBucket() {
    final var accessorBucket =
        new AccessorBucket().setSite(AccessorProperties.getAccessorSite()).setCreation(Instant.now())
            .setName(DaoAccessorBucketRepository.getBucketName(clientId, bucket))
            .setId(DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucket))
            .setStatus(AccessorStatus.READY);
    final DaoAccessorBucket daoAccessorBucket = new TestDaoAccessorBucket();
    daoAccessorBucket.fromDto(accessorBucket);
    final var second = daoAccessorBucket.getDto();
    Assertions.assertEquals(accessorBucket, second);
    assertEquals(accessorBucket, accessorBucket);
    final var secondDao = new TestDaoAccessorBucket();
    secondDao.fromDto(second);
    assertEquals(daoAccessorBucket, secondDao);
    assertEquals(daoAccessorBucket.hashCode(), secondDao.hashCode());
    assertEquals(DaoAccessorBucketRepository.getBucketName(clientId, secondDao.getId()), secondDao.getName());
    assertEquals(DaoAccessorBucketRepository.getBucketName(clientId, secondDao.getName()), secondDao.getName());
    assertEquals(DaoAccessorBucketRepository.getBucketTechnicalName(clientId, secondDao.getName()), secondDao.getId());
    assertEquals(DaoAccessorBucketRepository.getPrefix(clientId) + secondDao.getName(), secondDao.getId());
    assertEquals(DaoAccessorBucketRepository.getFinalBucketName(clientId, secondDao.getId(), false), secondDao.getId());
    assertEquals(DaoAccessorBucketRepository.getFinalBucketName(clientId, secondDao.getName(), true),
        secondDao.getId());
    assertTrue(daoAccessorBucket.toString().contains(AccessorProperties.getAccessorSite()));
  }

  @Test
  void checkEqualsBucket() {
    final var accessorBucket = new TestDaoAccessorBucket();
    final var accessorBucket2 = new TestDaoAccessorBucket();
    final var accessorObject = new AccessorObject();
    assertFalse(accessorBucket.equals(accessorObject));
    assertNotEquals(accessorBucket.hashCode(), accessorObject.hashCode());
    assertTrue(accessorBucket.equals(accessorBucket));
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setId("idbucket");
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setId("idbucket");
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setSite("id");
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setSite("id");
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setName("idbucket");
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setName("idbucket");
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setStatus(AccessorStatus.READY);
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setStatus(AccessorStatus.READY);
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setCreation(Instant.now());
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setCreation(accessorBucket.getCreation());
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket.setExpires(Instant.now());
    assertFalse(accessorBucket.equals(accessorBucket2));
    assertNotEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
    accessorBucket2.setExpires(accessorBucket.getExpires());
    assertTrue(accessorBucket.equals(accessorBucket2));
    assertEquals(accessorBucket.hashCode(), accessorBucket2.hashCode());
  }

  static class TestDaoAccessorObject extends DaoAccessorObject {
    private String id;
    private final Map<String, String> map = new HashMap<>();

    @Override
    public String getId() {
      return id;
    }

    @Override
    public DaoAccessorObject setId(final String id) {
      this.id = id;
      return this;
    }

    @Override
    public Map<String, String> getMetadata() {
      return map;
    }

    @Override
    public String getMetadata(final String key) {
      return map.get(key);
    }

    @Override
    public DaoAccessorObject addMetadata(final String key, final String value) {
      map.put(key, value);
      return this;
    }

    @Override
    public DaoAccessorObject setMetadata(final Map<String, String> metadata) {
      map.clear();
      map.putAll(metadata);
      return this;
    }
  }

  @Test
  void testObject() {
    final var accessorObject =
        new AccessorObject().setSite(AccessorProperties.getAccessorSite()).setCreation(Instant.now())
            .setBucket(DaoAccessorBucketRepository.getBucketTechnicalName(clientId, bucket)).setName(object)
            .setId(GuidLike.getGuid()).setStatus(AccessorStatus.READY);
    final DaoAccessorObject daoAccessorObject = new TestDaoAccessorObject();
    daoAccessorObject.fromDto(accessorObject);
    final var second = daoAccessorObject.getDto();
    Assertions.assertEquals(accessorObject, second);
    final var secondDao = new TestDaoAccessorObject();
    secondDao.fromDto(second);
    assertEquals(daoAccessorObject, secondDao);
    assertEquals(daoAccessorObject.hashCode(), secondDao.hashCode());
    assertEquals(object, ParametersChecker.getSanitizedName(object));
    assertEquals(object, ParametersChecker.getSanitizedName("/" + object));
    assertEquals(object, ParametersChecker.getSanitizedName("//" + object));
    assertEquals(object, ParametersChecker.getSanitizedName("\\\\" + object));
    assertTrue(daoAccessorObject.toString().contains(AccessorProperties.getAccessorSite()));
  }

  @Test
  void checkEqualsObjects() {
    final var accessorObject = new TestDaoAccessorObject();
    final var accessorObject1 = new TestDaoAccessorObject();
    final var accessorBucket = new AccessorBucket();
    assertFalse(accessorObject.equals(accessorBucket));
    assertNotEquals(accessorObject.hashCode(), accessorBucket.hashCode());
    assertTrue(accessorObject.equals(accessorObject));
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setId("idbucket");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setId("idbucket");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setSite("id");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setSite("id");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setName("idbucket");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setName("idbucket");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setStatus(AccessorStatus.READY);
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setStatus(AccessorStatus.READY);
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setCreation(Instant.now());
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setCreation(accessorObject.getCreation());
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setExpires(Instant.now());
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setExpires(accessorObject.getExpires());
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setSize(1);
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setSize(1);
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setBucket("idbucket");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setBucket("idbucket");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject.setHash("hash");
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setHash("hash");
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    final var map = new HashMap<String, String>();
    map.put("key", "val");
    accessorObject.setMetadata(map);
    assertFalse(accessorObject.equals(accessorObject1));
    assertNotEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    accessorObject1.setMetadata(map);
    assertTrue(accessorObject.equals(accessorObject1));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    final var dto = accessorObject1.getDto();
    assertFalse(accessorObject.updateFromDtoExceptIdSite(dto));
    dto.setSize(100);
    assertTrue(accessorObject.updateFromDtoExceptIdSite(dto));
    assertEquals(100, accessorObject.getSize());
  }

  @Test
  void testProperties() {
    assertEquals("Pékin", AccessorProperties.getAccessorSite());
    assertTrue(AccessorProperties.isRemoteRead());
  }
}
