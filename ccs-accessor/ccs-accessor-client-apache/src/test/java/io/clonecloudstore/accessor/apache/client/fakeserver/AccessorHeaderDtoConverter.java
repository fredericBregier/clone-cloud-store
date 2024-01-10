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

package io.clonecloudstore.accessor.apache.client.fakeserver;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
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

  /**
   * Headers to AccessorBucket
   */
  public static void bucketFromMap(final AccessorBucket accessorBucket, final MultivaluedMap<String, String> headers) {
    try {
      if (headers.containsKey(AccessorConstants.HeaderBucket.X_BUCKET_ID)) {
        accessorBucket.setId(headers.getFirst(AccessorConstants.HeaderBucket.X_BUCKET_ID));
      }
      if (headers.containsKey(AccessorConstants.HeaderBucket.X_BUCKET_SITE)) {
        accessorBucket.setSite(headers.getFirst(AccessorConstants.HeaderBucket.X_BUCKET_SITE));
      }
      if (headers.containsKey(AccessorConstants.HeaderBucket.X_BUCKET_NAME)) {
        accessorBucket.setName(headers.getFirst(AccessorConstants.HeaderBucket.X_BUCKET_NAME));
      }
      if (headers.containsKey(AccessorConstants.HeaderBucket.X_BUCKET_CREATION)) {
        final var instantAsString = headers.getFirst(AccessorConstants.HeaderBucket.X_BUCKET_CREATION);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorBucket.setCreation(Instant.parse(instantAsString));
        }
      }
      if (headers.containsKey(AccessorConstants.HeaderBucket.X_BUCKET_EXPIRES)) {
        final var instantAsString = headers.getFirst(AccessorConstants.HeaderBucket.X_BUCKET_EXPIRES);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorBucket.setExpires(Instant.parse(instantAsString));
        }
      }
      if (headers.containsKey(AccessorConstants.HeaderBucket.X_BUCKET_STATUS)) {
        final var status = headers.getFirst(AccessorConstants.HeaderBucket.X_BUCKET_STATUS);
        if (ParametersChecker.isNotEmpty(status)) {
          accessorBucket.setStatus(AccessorStatus.valueOf(status));
        }
      }
    } catch (final RuntimeException e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  /**
   * AccessorBucket to Headers
   */
  public static void bucketToMap(final AccessorBucket accessorBucket, final Map<String, String> map) {
    try {
      var value = accessorBucket.getId();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderBucket.X_BUCKET_ID, value);
      }
      value = accessorBucket.getSite();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderBucket.X_BUCKET_SITE, value);
      }
      value = accessorBucket.getName();
      if (ParametersChecker.isNotEmpty(value)) {
        map.put(AccessorConstants.HeaderBucket.X_BUCKET_NAME, value);
      }
      var instant = accessorBucket.getCreation();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderBucket.X_BUCKET_CREATION, instant.toString());
      }
      instant = accessorBucket.getExpires();
      if (ParametersChecker.isNotEmpty(instant)) {
        map.put(AccessorConstants.HeaderBucket.X_BUCKET_EXPIRES, instant.toString());
      }
      final var status = accessorBucket.getStatus();
      if (ParametersChecker.isNotEmpty(status)) {
        map.put(AccessorConstants.HeaderBucket.X_BUCKET_STATUS, status.name());
      }
    } catch (final RuntimeException e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  /**
   * Headers to AccessorObject
   */
  public static void objectFromMap(final AccessorObject accessorObject, final MultivaluedMap<String, String> headers) {
    try {
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_ID)) {
        accessorObject.setId(headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_ID));
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_SITE)) {
        accessorObject.setSite(headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_SITE));
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_BUCKET)) {
        accessorObject.setBucket(headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_BUCKET));
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_NAME)) {
        accessorObject.setName(headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_NAME));
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_HASH)) {
        accessorObject.setHash(headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_HASH));
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_STATUS)) {
        final var status = headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_STATUS);
        if (ParametersChecker.isNotEmpty(status)) {
          accessorObject.setStatus(AccessorStatus.valueOf(status));
        }
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_CREATION)) {
        final var instantAsString = headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_CREATION);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorObject.setCreation(Instant.parse(instantAsString));
        }
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES)) {
        final var instantAsString = headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorObject.setExpires(Instant.parse(instantAsString));
        }
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_SIZE)) {
        final var size = headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_SIZE);
        if (ParametersChecker.isNotEmpty(size)) {
          accessorObject.setSize(Long.parseLong(size));
        }
      }
      if (headers.containsKey(AccessorConstants.HeaderObject.X_OBJECT_METADATA)) {
        final var metadata = headers.getFirst(AccessorConstants.HeaderObject.X_OBJECT_METADATA);
        if (ParametersChecker.isNotEmpty(metadata)) {
          accessorObject.setMetadata(JsonUtil.getInstance().readValue(metadata, typeReferenceMapStringString));
        }
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  /**
   * Headers to AccessorObject
   */
  public static void objectFromMap(final AccessorObject accessorObject, final MultiMap headers) {
    try {
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_ID)) {
        accessorObject.setId(headers.get(AccessorConstants.HeaderObject.X_OBJECT_ID));
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_SITE)) {
        accessorObject.setSite(headers.get(AccessorConstants.HeaderObject.X_OBJECT_SITE));
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_BUCKET)) {
        accessorObject.setBucket(headers.get(AccessorConstants.HeaderObject.X_OBJECT_BUCKET));
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_NAME)) {
        accessorObject.setName(headers.get(AccessorConstants.HeaderObject.X_OBJECT_NAME));
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_HASH)) {
        accessorObject.setHash(headers.get(AccessorConstants.HeaderObject.X_OBJECT_HASH));
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_STATUS)) {
        final var status = headers.get(AccessorConstants.HeaderObject.X_OBJECT_STATUS);
        if (ParametersChecker.isNotEmpty(status)) {
          accessorObject.setStatus(AccessorStatus.valueOf(status));
        }
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_CREATION)) {
        final var instantAsString = headers.get(AccessorConstants.HeaderObject.X_OBJECT_CREATION);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorObject.setCreation(Instant.parse(instantAsString));
        }
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES)) {
        final var instantAsString = headers.get(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorObject.setExpires(Instant.parse(instantAsString));
        }
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_SIZE)) {
        final var size = headers.get(AccessorConstants.HeaderObject.X_OBJECT_SIZE);
        if (ParametersChecker.isNotEmpty(size)) {
          accessorObject.setSize(Long.parseLong(size));
        }
      }
      if (headers.contains(AccessorConstants.HeaderObject.X_OBJECT_METADATA)) {
        final var metadata = headers.get(AccessorConstants.HeaderObject.X_OBJECT_METADATA);
        if (ParametersChecker.isNotEmpty(metadata)) {
          accessorObject.setMetadata(JsonUtil.getInstance().readValue(metadata, typeReferenceMapStringString));
        }
      }
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
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX)) {
        accessorFilter.setNamePrefix(headers.get(AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX));
        found = true;
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_STATUSES)) {
        final var full = headers.get(AccessorConstants.HeaderFilterObject.FILTER_STATUSES);
        final var result = JsonUtil.getInstance().readValue(full, typeReferenceAccessorStatusArray);
        accessorFilter.setStatuses(result);
        found = true;
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER)) {
        final var instantAsString = headers.get(AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorFilter.setCreationAfter(Instant.parse(instantAsString));
          found = true;
        }
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE)) {
        final var instantAsString = headers.get(AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorFilter.setCreationBefore(Instant.parse(instantAsString));
          found = true;
        }
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER)) {
        final var instantAsString = headers.get(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorFilter.setExpiresAfter(Instant.parse(instantAsString));
          found = true;
        }
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE)) {
        final var instantAsString = headers.get(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE);
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorFilter.setExpiresBefore(Instant.parse(instantAsString));
          found = true;
        }
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT)) {
        final var size = headers.get(AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT);
        if (ParametersChecker.isNotEmpty(size)) {
          accessorFilter.setSizeGreaterThan(Long.parseLong(size));
          found = true;
        }
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT)) {
        final var size = headers.get(AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT);
        if (ParametersChecker.isNotEmpty(size)) {
          accessorFilter.setSizeLessThan(Long.parseLong(size));
          found = true;
        }
      }
      if (headers.contains(AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ)) {
        final var metadata = headers.get(AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ);
        if (ParametersChecker.isNotEmpty(metadata)) {
          accessorFilter.setMetadataFilter(JsonUtil.getInstance().readValue(metadata, typeReferenceMapStringString));
          found = true;
        }
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
