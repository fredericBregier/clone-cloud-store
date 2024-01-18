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
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.vertx.core.MultiMap;

/**
 * Utility class for Headers
 */
public class AccessorHeaderDtoConverter {
  private static final TypeReference<Map<String, String>> typeReferenceMapStringString = new TypeReference<>() {
  };
  private static final TypeReference<AccessorStatus[]> typeReferenceAccessorStatusArray = new TypeReference<>() {
  };
  private static final String INVALID_ARGUMENT = "Invalid Argument";

  private static Instant getInstant(final MultiMap headers, final String headerName) {
    final var instantAsString = getString(headers, headerName);
    if (ParametersChecker.isNotEmpty(instantAsString)) {
      assert instantAsString != null;
      return Instant.parse(instantAsString);
    }
    return null;
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

  protected AccessorHeaderDtoConverter() {
    // Empty
  }

}
