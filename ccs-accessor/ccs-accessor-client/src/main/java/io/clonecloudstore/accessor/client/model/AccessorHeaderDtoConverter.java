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
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.vertx.core.MultiMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

/**
 * Utility class for Headers
 */
public class AccessorHeaderDtoConverter {
  private static final TypeReference<Map<String, String>> typeReferenceMapStringString = new TypeReference<>() {
  };
  private static final TypeReference<AccessorStatus[]> typeReferenceAccessorStatusArray = new TypeReference<>() {
  };
  private static final String INVALID_ARGUMENT = "Invalid Argument";

  private static Instant getInstant(final MultivaluedMap<String, String> headers, final String headerName) {
    final var instantAsString = getString(headers, headerName);
    if (ParametersChecker.isNotEmpty(instantAsString)) {
      assert instantAsString != null;
      return Instant.parse(instantAsString);
    }
    return null;
  }

  private static String getString(final MultivaluedMap<String, String> headers, final String headerName) {
    if (headers.containsKey(headerName)) {
      return headers.getFirst(headerName);
    }
    return null;
  }

  /**
   * Headers to AccessorObject
   */
  public static void objectFromMap(final AccessorObject accessorObject, final MultivaluedMap<String, String> headers) {
    try {
      var value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_ID);
      if (value != null) {
        accessorObject.setId(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_SITE);
      if (value != null) {
        accessorObject.setSite(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_BUCKET);
      if (value != null) {
        accessorObject.setBucket(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_NAME);
      if (value != null) {
        accessorObject.setName(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_HASH);
      if (value != null) {
        accessorObject.setHash(value);
      }
      final var status = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_STATUS);
      if (ParametersChecker.isNotEmpty(status)) {
        accessorObject.setStatus(AccessorStatus.valueOf(status));
      }
      accessorObject.setCreation(getInstant(headers, AccessorConstants.HeaderObject.X_OBJECT_CREATION));
      accessorObject.setExpires(getInstant(headers, AccessorConstants.HeaderObject.X_OBJECT_EXPIRES));
      final var size = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_SIZE);
      if (ParametersChecker.isNotEmpty(size)) {
        assert size != null;
        accessorObject.setSize(Long.parseLong(size));
      }
      final var metadata = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_METADATA);
      if (ParametersChecker.isNotEmpty(metadata)) {
        accessorObject.setMetadata(JsonUtil.getInstance().readValue(metadata, typeReferenceMapStringString));
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  private static Instant getInstant(final MultiMap headers, final String headerName) {
    final var instantAsString = getString(headers, headerName);
    if (ParametersChecker.isNotEmpty(instantAsString)) {
      assert instantAsString != null;
      return Instant.parse(instantAsString);
    }
    return null;
  }

  private static long getLong(final MultiMap headers, final String headerName) {
    final var value = getString(headers, headerName);
    if (ParametersChecker.isNotEmpty(value)) {
      assert value != null;
      return Long.parseLong(value);
    }
    return 0;
  }

  private static Map<String, String> getMap(final MultiMap headers, final String headerName)
      throws JsonProcessingException {
    final var metadata = getString(headers, headerName);
    if (ParametersChecker.isNotEmpty(metadata)) {
      return JsonUtil.getInstance().readValue(metadata, typeReferenceMapStringString);
    }
    return Map.of();
  }

  private static String getString(final MultiMap headers, final String headerName) {
    if (headers.contains(headerName)) {
      return headers.get(headerName);
    }
    return null;
  }

  /**
   * Headers to AccessorObject
   */
  public static void objectFromMap(final AccessorObject accessorObject, final MultiMap headers) {
    try {
      var value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_ID);
      if (value != null) {
        accessorObject.setId(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_SITE);
      if (value != null) {
        accessorObject.setSite(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_BUCKET);
      if (value != null) {
        accessorObject.setBucket(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_NAME);
      if (value != null) {
        accessorObject.setName(value);
      }
      value = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_HASH);
      if (value != null) {
        accessorObject.setHash(value);
      }
      final var status = getString(headers, AccessorConstants.HeaderObject.X_OBJECT_STATUS);
      if (ParametersChecker.isNotEmpty(status)) {
        accessorObject.setStatus(AccessorStatus.valueOf(status));
      }
      accessorObject.setCreation(getInstant(headers, AccessorConstants.HeaderObject.X_OBJECT_CREATION));
      accessorObject.setExpires(getInstant(headers, AccessorConstants.HeaderObject.X_OBJECT_EXPIRES));
      accessorObject.setSize(getLong(headers, AccessorConstants.HeaderObject.X_OBJECT_SIZE));
      accessorObject.setMetadata(getMap(headers, AccessorConstants.HeaderObject.X_OBJECT_METADATA));
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  /**
   * AccessorObject to Headers
   */
  public static void objectToMap(final AccessorObject accessorObject, final Map<String, String> map) {
    try {
      var value = accessorObject.getId();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_ID, value);
      }
      value = accessorObject.getSite();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_SITE, value);
      }
      value = accessorObject.getBucket();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_BUCKET, value);
      }
      value = accessorObject.getName();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_NAME, value);
      }
      value = accessorObject.getHash();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_HASH, value);
      }
      var instant = accessorObject.getCreation();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_CREATION, instant.toString());
      }
      instant = accessorObject.getExpires();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES, instant.toString());
      }
      final var status = accessorObject.getStatus();
      if (ParametersChecker.isNotEmpty(status)) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_STATUS, status.name());
      }
      map.put(AccessorConstants.HeaderObject.X_OBJECT_SIZE, Long.toString(accessorObject.getSize()));
      if (!accessorObject.getMetadata().isEmpty()) {
        map.put(AccessorConstants.HeaderObject.X_OBJECT_METADATA,
            JsonUtil.getInstance().writeValueAsString(accessorObject.getMetadata()));
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  /**
   * Headers to AccessorFilter
   */
  public static boolean filterFromMap(final AccessorFilter accessorFilter, final MultiMap headers) {
    try {
      var found = false;
      accessorFilter.setNamePrefix(getString(headers, AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX));
      if (ParametersChecker.isNotEmpty(accessorFilter.getNamePrefix())) {
        accessorFilter.setNamePrefix(ParametersChecker.getSanitizedObjectName(accessorFilter.getNamePrefix()));
        found = true;
      }
      final var full = getString(headers, AccessorConstants.HeaderFilterObject.FILTER_STATUSES);
      if (ParametersChecker.isNotEmpty(full)) {
        final var result = JsonUtil.getInstance().readValue(full, typeReferenceAccessorStatusArray);
        accessorFilter.setStatuses(result);
        found = true;
      }
      var instant = getInstant(headers, AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER);
      if (instant != null) {
        accessorFilter.setCreationAfter(instant);
        found = true;
      }
      instant = getInstant(headers, AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE);
      if (instant != null) {
        accessorFilter.setCreationBefore(instant);
        found = true;
      }
      instant = getInstant(headers, AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER);
      if (instant != null) {
        accessorFilter.setExpiresAfter(instant);
        found = true;
      }
      instant = getInstant(headers, AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE);
      if (instant != null) {
        accessorFilter.setExpiresBefore(instant);
        found = true;
      }
      accessorFilter.setSizeGreaterThan(getLong(headers, AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT));
      if (accessorFilter.getSizeGreaterThan() > 0) {
        found = true;
      }
      accessorFilter.setSizeLessThan(getLong(headers, AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT));
      if (accessorFilter.getSizeLessThan() > 0) {
        found = true;
      }
      final var map = getMap(headers, AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ);
      if (map != null) {
        accessorFilter.setMetadataFilter(map);
        found = true;
      }
      return found;
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  /**
   * AccessorFilter to Headers
   */
  public static void filterToMap(final AccessorFilter accessorFilter, final Map<String, String> map) {
    try {
      final var value = accessorFilter.getNamePrefix();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX, value);
      }
      var instant = accessorFilter.getCreationAfter();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER, instant.toString());
      }
      instant = accessorFilter.getCreationBefore();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE, instant.toString());
      }
      instant = accessorFilter.getExpiresAfter();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER, instant.toString());
      }
      instant = accessorFilter.getExpiresBefore();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE, instant.toString());
      }
      final var status = accessorFilter.getStatuses();
      if (status != null && status.length > 0) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_STATUSES,
            JsonUtil.getInstance().writeValueAsString(status));
      }
      if (accessorFilter.getSizeGreaterThan() > 0) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT,
            Long.toString(accessorFilter.getSizeGreaterThan()));
      }
      if (accessorFilter.getSizeLessThan() > 0) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT, Long.toString(accessorFilter.getSizeLessThan()));
      }
      if (accessorFilter.getMetadataFilter() != null && !accessorFilter.getMetadataFilter().isEmpty()) {
        map.put(AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ,
            JsonUtil.getInstance().writeValueAsString(accessorFilter.getMetadataFilter()));
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  public static StorageType getStorageTypeFromResponse(final Response response) {
    final var xType = response.getHeaders().get(AccessorConstants.Api.X_TYPE);
    if (xType == null) {
      return StorageType.NONE;
    }
    final var type = (String) xType.getFirst();
    if (ParametersChecker.isEmpty(type)) {
      return StorageType.NONE;
    }
    try {
      return StorageType.valueOf(type);
    } catch (final IllegalArgumentException e) {
      return StorageType.NONE;
    }
  }

  protected AccessorHeaderDtoConverter() {
    // Empty
  }

}
