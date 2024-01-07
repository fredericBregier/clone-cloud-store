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

package io.clonecloudstore.accessor.client.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.driver.api.StorageType;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.MultiMap;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.jaxrs.ResponseBuilderImpl;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class AccessorHeaderDtoConverterTest {
  @Test
  void objectConverter() {
    final var accessorObject0 = new AccessorObject();
    final var accessorObject01 = new AccessorObject();
    final Map<String, String> headersEmpty = new HashMap<>();
    final MultivaluedMap<String, String> multivaluedMapEmpty = new MultivaluedHashMap<>();
    AccessorHeaderDtoConverter.objectFromMap(accessorObject01, multivaluedMapEmpty);
    assertEquals(accessorObject0.hashCode(), accessorObject01.hashCode());
    AccessorHeaderDtoConverter.objectToMap(accessorObject01, headersEmpty);
    assertFalse(headersEmpty.isEmpty());
    assertEquals(AccessorStatus.UNKNOWN.toString(), headersEmpty.get(AccessorConstants.HeaderObject.X_OBJECT_STATUS));
    assertEquals("0", headersEmpty.get(AccessorConstants.HeaderObject.X_OBJECT_SIZE));
    headersEmpty.remove(AccessorConstants.HeaderObject.X_OBJECT_STATUS);
    headersEmpty.remove(AccessorConstants.HeaderObject.X_OBJECT_SIZE);
    assertTrue(headersEmpty.isEmpty());

    final var accessorObject = new AccessorObject();
    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    accessorObject.setId(GuidLike.getGuid()).setBucket("bucket").setName("name").setStatus(AccessorStatus.READY)
        .setSize(100).setHash("hash").setCreation(Instant.now()).setExpires(Instant.now().plusSeconds(60))
        .setSite("site").setMetadata(map);
    final Map<String, String> headers = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(accessorObject, headers);
    final var accessorObject1 = new AccessorObject();
    final MultivaluedMap<String, String> multivaluedMap = new MultivaluedHashMap<>();
    for (final var entry : headers.entrySet()) {
      multivaluedMap.putSingle(entry.getKey(), entry.getValue());
    }
    AccessorHeaderDtoConverter.objectFromMap(accessorObject1, multivaluedMap);
    assertEquals(accessorObject, accessorObject1);
    assertEquals(accessorObject.getMetadata("key1"), accessorObject1.getMetadata("key1"));
    assertEquals(accessorObject.hashCode(), accessorObject1.hashCode());
    final var multiMap = MultiMap.caseInsensitiveMultiMap();
    for (final var entry : headers.entrySet()) {
      multiMap.add(entry.getKey(), entry.getValue());
    }
    final var accessorObject2 = new AccessorObject();
    AccessorHeaderDtoConverter.objectFromMap(accessorObject2, multiMap);
    assertEquals(accessorObject, accessorObject2);
    final var accessorObjectClone = accessorObject.cloneInstance();
    assertEquals(accessorObject, accessorObjectClone);
    assertEquals(accessorObject, accessorObject);
    assertTrue(accessorObject.toString().contains("value2"));
    accessorObjectClone.addMetadata("key2", "value3");
    assertNotEquals(accessorObject, accessorObjectClone);
  }

  @Test
  void bucketConverter() {
    final var accessorBucket0 = new AccessorBucket();
    final var accessorBucket01 = new AccessorBucket();
    final MultivaluedMap<String, String> multivaluedMapEmpty = new MultivaluedHashMap<>();
    AccessorHeaderDtoConverter.bucketFromMap(accessorBucket01, multivaluedMapEmpty);
    assertEquals(accessorBucket0.hashCode(), accessorBucket01.hashCode());
    final Map<String, String> headersEmpty = new HashMap<>();
    AccessorHeaderDtoConverter.bucketToMap(accessorBucket01, headersEmpty);
    assertFalse(headersEmpty.isEmpty());
    assertEquals(AccessorStatus.UNKNOWN.toString(), headersEmpty.get(AccessorConstants.HeaderBucket.X_BUCKET_STATUS));
    headersEmpty.remove(AccessorConstants.HeaderBucket.X_BUCKET_STATUS);
    assertTrue(headersEmpty.isEmpty());

    final var accessorBucket = new AccessorBucket();
    accessorBucket.setId("id-name").setName("name").setStatus(AccessorStatus.READY).setCreation(Instant.now())
        .setExpires(Instant.now().plusSeconds(60)).setSite("site");
    final Map<String, String> headers = new HashMap<>();
    AccessorHeaderDtoConverter.bucketToMap(accessorBucket, headers);
    final var accessorBucket1 = new AccessorBucket();
    final MultivaluedMap<String, String> multivaluedMap = new MultivaluedHashMap<>();
    for (final var entry : headers.entrySet()) {
      multivaluedMap.putSingle(entry.getKey(), entry.getValue());
    }
    AccessorHeaderDtoConverter.bucketFromMap(accessorBucket1, multivaluedMap);
    assertEquals(accessorBucket, accessorBucket1);
    assertEquals(accessorBucket.hashCode(), accessorBucket1.hashCode());
    final var accessorObjectClone = accessorBucket.cloneInstance();
    assertEquals(accessorBucket, accessorObjectClone);
    assertEquals(accessorBucket, accessorBucket);
    assertTrue(accessorBucket.toString().contains("id-name"));
    accessorObjectClone.setName("name2");
    assertNotEquals(accessorBucket, accessorObjectClone);
  }

  @Test
  void filterConverter() {
    final var accessorFilter0 = new AccessorFilter();
    final var accessorFilter01 = new AccessorFilter();
    final var multiMapEmpty = MultiMap.caseInsensitiveMultiMap();
    AccessorHeaderDtoConverter.filterFromMap(accessorFilter01, multiMapEmpty);
    assertEquals(accessorFilter0.hashCode(), accessorFilter01.hashCode());
    final Map<String, String> headersEmpty = new HashMap<>();
    AccessorHeaderDtoConverter.filterToMap(accessorFilter01, headersEmpty);
    assertTrue(headersEmpty.isEmpty());

    final Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    final var accessorFilter = new AccessorFilter();
    accessorFilter.setNamePrefix("name").setStatuses(new AccessorStatus[]{AccessorStatus.READY, AccessorStatus.DELETED})
        .setCreationBefore(Instant.now()).setCreationAfter(Instant.now().plusSeconds(1))
        .setExpiresBefore(Instant.now().plusSeconds(60)).setExpiresAfter(Instant.now().plusSeconds(10))
        .setSizeLessThan(100).setSizeGreaterThan(10).setMetadataFilter(map);
    final Map<String, String> headers = new HashMap<>();
    AccessorHeaderDtoConverter.filterToMap(accessorFilter, headers);
    final var accessorFilter1 = new AccessorFilter();
    final var multiMap = MultiMap.caseInsensitiveMultiMap();
    for (final var entry : headers.entrySet()) {
      multiMap.add(entry.getKey(), entry.getValue());
    }
    AccessorHeaderDtoConverter.filterFromMap(accessorFilter1, multiMap);
    assertEquals(accessorFilter, accessorFilter1);
    assertEquals(accessorFilter.hashCode(), accessorFilter1.hashCode());
    assertEquals(accessorFilter, accessorFilter);
    assertTrue(accessorFilter1.toString().contains("value2"));
    accessorFilter1.addMetadata("key2", "value3");
    assertNotEquals(accessorFilter, accessorFilter1);
  }

  @Test
  void statusTest() {
    for (var status : AccessorStatus.values()) {
      assertEquals(status.name(), status.toString());
      assertEquals(status.name(), AccessorStatus.toString(status));
      assertEquals(status, AccessorStatus.fromStatusCode(status.getStatus()));
    }
  }

  @Test
  void getStorageTypeTest() {
    Response responseHeaderNoValue = new ResponseBuilderImpl().header(X_TYPE, "").build();
    assertEquals(StorageType.NONE, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseHeaderNoValue));
    Response responseNoHeader = new ResponseBuilderImpl().build();
    assertEquals(StorageType.NONE, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseNoHeader));
    Response responseHeaderWrongValue = new ResponseBuilderImpl().header(X_TYPE, "notvalid").build();
    assertEquals(StorageType.NONE, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseHeaderWrongValue));
    Response responseHeaderValueNone = new ResponseBuilderImpl().header(X_TYPE, StorageType.NONE.toString()).build();
    assertEquals(StorageType.NONE, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseHeaderValueNone));
    Response responseHeaderValueDir =
        new ResponseBuilderImpl().header(X_TYPE, StorageType.DIRECTORY.toString()).build();
    assertEquals(StorageType.DIRECTORY, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseHeaderValueDir));
    Response responseHeaderValueBucket =
        new ResponseBuilderImpl().header(X_TYPE, StorageType.BUCKET.toString()).build();
    assertEquals(StorageType.BUCKET, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseHeaderValueBucket));
    Response responseHeaderValueObject =
        new ResponseBuilderImpl().header(X_TYPE, StorageType.OBJECT.toString()).build();
    assertEquals(StorageType.OBJECT, AccessorHeaderDtoConverter.getStorageTypeFromResponse(responseHeaderValueObject));
  }
}
