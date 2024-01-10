/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.accessor.apache.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.type.TypeReference;
import io.clonecloudstore.accessor.apache.client.internal.ApiErrorResponse;
import io.clonecloudstore.accessor.apache.client.internal.InputStreamClosingContext;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.ZstdCompressInputStream;
import io.clonecloudstore.common.standard.inputstream.ZstdDecompressInputStream;
import io.clonecloudstore.common.standard.properties.ApiConstants;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;

import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;

public class AccessorClient implements Closeable {
  private static final String API_ROOT = AccessorConstants.Api.API_ROOT;
  private static final TypeReference<List<AccessorBucket>> bucketList = new TypeReference<>() {
  };
  private static final TypeReference<Map<String, String>> typeReferenceMapStringString = new TypeReference<>() {
  };
  private static final String INVALID_ARGUMENT = "Invalid argument";
  private final AccessorApiFactory factory;
  private final CloseableHttpClient client;

  protected AccessorClient(final AccessorApiFactory factory) {
    this.factory = factory;
    client = this.factory.getCloseableHttpClient();
  }

  /**
   * @return the collection of Buckets
   */
  public Collection<AccessorBucket> getBuckets() throws CcsWithStatusException {
    final var request = new HttpGet(factory.getBaseUri() + API_ROOT);
    setCommonHeaders(request);
    acceptProvideHeaders(request, true, false);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      List<AccessorBucket> list = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_OK) {
            return StandardProperties.getObjectMapper().readValue(response.getEntity().getContent(), bucketList);
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (list != null) {
        return list;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return the StorageType for this Bucket
   */
  public StorageType checkBucket(final String bucketName) throws CcsWithStatusException {
    final var request = new HttpHead(factory.getBaseUri() + API_ROOT + "/" + bucketName);
    setCommonHeaders(request);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      StorageType storageType = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_OK || response.getCode() == HttpStatus.SC_NO_CONTENT) {
            return getStorageTypeFromResponse(response);
          } else if (response.getCode() == HttpStatus.SC_NOT_FOUND) {
            return StorageType.NONE;
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (storageType != null) {
        return storageType;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return the Bucket Metadata
   */
  public AccessorBucket getBucket(final String bucketName) throws CcsWithStatusException {
    final var request = new HttpGet(factory.getBaseUri() + API_ROOT + "/" + bucketName);
    setCommonHeaders(request);
    acceptProvideHeaders(request, true, false);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      var result = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_OK) {
            return StandardProperties.getObjectMapper()
                .readValue(response.getEntity().getContent(), AccessorBucket.class);
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (result != null) {
        return result;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return the DTO if the Bucket is created
   */
  public AccessorBucket createBucket(final String bucketName) throws CcsWithStatusException {
    final var request = new HttpPost(factory.getBaseUri() + API_ROOT + "/" + bucketName);
    setCommonHeaders(request);
    acceptProvideHeaders(request, true, false);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      var result = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_CREATED || response.getCode() == HttpStatus.SC_OK) {
            return StandardProperties.getObjectMapper()
                .readValue(response.getEntity().getContent(), AccessorBucket.class);
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (result != null) {
        return result;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return True if deleted (or already deleted), else False or an exception
   */
  public boolean deleteBucket(final String bucketName) throws CcsWithStatusException {
    final var request = new HttpDelete(factory.getBaseUri() + API_ROOT + "/" + bucketName);
    setCommonHeaders(request);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      var result = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_NO_CONTENT || response.getCode() == HttpStatus.SC_GONE) {
            return true;
          } else {
            errorReference.set(handleError(response));
            return false;
          }
        }
      });
      if (errorReference.get() != null) {
        throw getCcsError(errorReference.get());
      }
      return result;
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }


  /**
   * Check if object or directory exist
   *
   * @param pathDirectoryOrObject may contain only directory, not full path (as prefix)
   * @return the associated StorageType
   */
  public StorageType checkObjectOrDirectory(final String bucketName, final String pathDirectoryOrObject)
      throws CcsWithStatusException {
    final var request = new HttpHead(factory.getBaseUri() + API_ROOT + "/" + bucketName + "/" + pathDirectoryOrObject);
    setCommonHeaders(request);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      StorageType storageType = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_OK || response.getCode() == HttpStatus.SC_NO_CONTENT) {
            return getStorageTypeFromResponse(response);
          } else if (response.getCode() == HttpStatus.SC_NOT_FOUND) {
            return StorageType.NONE;
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (storageType != null) {
        return storageType;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return the associated DTO
   */
  public AccessorObject getObjectInfo(final String bucketName, final String objectName) throws CcsWithStatusException {
    final var request = new HttpGet(factory.getBaseUri() + API_ROOT + "/" + bucketName + "/" + objectName);
    setCommonHeaders(request);
    acceptProvideHeaders(request, true, false);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      var result = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_OK) {
            return StandardProperties.getObjectMapper()
                .readValue(response.getEntity().getContent(), AccessorObject.class);
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (result != null) {
        return result;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return both InputStream and Object DTO
   */
  public InputStreamBusinessOut<AccessorObject> getObject(final String bucketName, final String objectName)
      throws CcsWithStatusException {
    return getObject(bucketName, objectName, false);
  }

  /**
   * @return both InputStream and Object DTO
   */
  public InputStreamBusinessOut<AccessorObject> getObject(final String bucketName, final String objectName,
                                                          final boolean compressed) throws CcsWithStatusException {
    final var request = new HttpGet(factory.getBaseUri() + API_ROOT + "/" + bucketName + "/" + objectName);
    setCommonHeaders(request);
    acceptProvideHeaders(request, false, false);
    try {
      if (compressed) {
        setCompressionAcceptHeaders(request);
      }
      final var closeableHttpClient = this.factory.getCloseableHttpClient();
      var response = closeableHttpClient.executeOpen(null, request, null);
      if (response.getCode() == HttpStatus.SC_OK) {
        final var object = getAccessorObjectFromResponse(response, true);
        var inputStream = response.getEntity().getContent();
        var encoding = response.getFirstHeader(HttpHeaders.CONTENT_ENCODING);
        if (encoding != null && encoding.getValue().equalsIgnoreCase(COMPRESSION_ZSTD)) {
          inputStream = new ZstdDecompressInputStream(inputStream);
        }
        inputStream = new InputStreamClosingContext(inputStream, response, closeableHttpClient);
        return new InputStreamBusinessOut<>(object, inputStream);
      }
      throw getCcsError(handleError(response));
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Returns an Iterator containing AccessorObjects
   */
  public Iterator<AccessorObject> listObjects(final String bucketName, final AccessorFilter filter)
      throws CcsWithStatusException {
    final var request = new HttpPut(factory.getBaseUri() + API_ROOT + "/" + bucketName);
    setCommonHeaders(request);
    acceptProvideHeaders(request, false, false);
    try {
      setCompressionAcceptHeaders(request);
      final var closeableHttpClient = this.factory.getCloseableHttpClient();
      setHeadersForFilter(request, filter == null ? new AccessorFilter() : filter);
      var response = closeableHttpClient.executeOpen(null, request, null);
      if (response.getCode() == HttpStatus.SC_OK) {
        var inputStream = response.getEntity().getContent();
        var encoding = response.getFirstHeader(HttpHeaders.CONTENT_ENCODING);
        if (encoding != null && encoding.getValue().equalsIgnoreCase(COMPRESSION_ZSTD)) {
          inputStream = new ZstdDecompressInputStream(inputStream);
        }
        inputStream = new InputStreamClosingContext(inputStream, response, closeableHttpClient);
        return StreamIteratorUtils.getIteratorFromInputStream(inputStream, AccessorObject.class);
      }
      throw getCcsError(handleError(response));
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return the associated DTO
   */
  public AccessorObject createObject(final AccessorObject accessorObject, final InputStream body)
      throws CcsWithStatusException {
    return createObject(accessorObject, body, false);
  }

  /**
   * @return the associated DTO
   */
  public AccessorObject createObject(final AccessorObject accessorObject, final InputStream body,
                                     final boolean useCompression) throws CcsWithStatusException {
    final var request = new HttpPost(
        factory.getBaseUri() + API_ROOT + "/" + accessorObject.getBucket() + "/" + accessorObject.getName());
    setCommonHeaders(request);
    acceptProvideHeaders(request, true, true);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      setHeadersForObject(request, accessorObject);
      InputStream inputStream = body;
      if (useCompression) {
        inputStream = new ZstdCompressInputStream(inputStream);
        setCompressionEncodingHeaders(request);
      }
      final var inputStreamFinal = inputStream;
      final var entity = EntityBuilder.create().chunked().setStream(inputStreamFinal).build();
      request.setEntity(entity);
      var result = client.execute(request, response -> {
        try (inputStreamFinal; response) {
          if (response.getCode() == HttpStatus.SC_CREATED) {
            return getAccessorObjectFromResponse(response, false);
          } else {
            errorReference.set(handleError(response));
            return null;
          }
        }
      });
      if (result != null) {
        return result;
      }
      throw getCcsError(errorReference.get());
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  /**
   * @return True if deleted (or already deleted), else False or an exception
   */
  public boolean deleteObject(final String bucketName, final String objectName) throws CcsWithStatusException {
    final var request = new HttpDelete(factory.getBaseUri() + API_ROOT + "/" + bucketName + "/" + objectName);
    setCommonHeaders(request);
    final AtomicReference<ApiErrorResponse> errorReference = new AtomicReference<>();
    try {
      var result = client.execute(request, response -> {
        try (response) {
          if (response.getCode() == HttpStatus.SC_NO_CONTENT) {
            return true;
          } else if (response.getCode() == HttpStatus.SC_GONE) {
            return false;
          } else {
            errorReference.set(handleError(response));
            return false;
          }
        }
      });
      if (errorReference.get() != null) {
        throw getCcsError(errorReference.get());
      }
      return result;
    } catch (final IOException | RuntimeException e) {
      throw new CcsWithStatusException(null, HttpStatus.SC_SERVER_ERROR, e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private AccessorObject getAccessorObjectFromResponse(final ClassicHttpResponse response,
                                                       final boolean fromHeaderOnly) {
    if (!fromHeaderOnly) {
      try {
        final var accessorObject =
            StandardProperties.getObjectMapper().readValue(response.getEntity().getContent(), AccessorObject.class);
        if (accessorObject != null) {
          return accessorObject;
        }
      } catch (final RuntimeException | IOException ignore) {
        // Nothing
      }
    }
    final var accessorObject = new AccessorObject();
    try {
      var header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_ID);
      if (header != null) {
        accessorObject.setId(header.getValue());
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_SITE);
      if (header != null) {
        accessorObject.setSite(header.getValue());
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_BUCKET);
      if (header != null) {
        accessorObject.setBucket(header.getValue());
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_NAME);
      if (header != null) {
        accessorObject.setName(header.getValue());
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_HASH);
      if (header != null) {
        accessorObject.setHash(header.getValue());
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_STATUS);
      if (header != null) {
        var status = header.getValue();
        if (ParametersChecker.isNotEmpty(status)) {
          accessorObject.setStatus(AccessorStatus.valueOf(status));
        }
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_CREATION);
      if (header != null) {
        final var instantAsString = header.getValue();
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorObject.setCreation(Instant.parse(instantAsString));
        }
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES);
      if (header != null) {
        final var instantAsString = header.getValue();
        if (ParametersChecker.isNotEmpty(instantAsString)) {
          accessorObject.setExpires(Instant.parse(instantAsString));
        }
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_SIZE);
      if (header != null) {
        final var size = header.getValue();
        if (ParametersChecker.isNotEmpty(size)) {
          accessorObject.setSize(Long.parseLong(size));
        }
      }
      header = response.getFirstHeader(AccessorConstants.HeaderObject.X_OBJECT_METADATA);
      if (header != null) {
        final var metadata = header.getValue();
        if (ParametersChecker.isNotEmpty(metadata)) {
          accessorObject.setMetadata(
              StandardProperties.getObjectMapper().readValue(metadata, typeReferenceMapStringString));
        }
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
    return accessorObject;
  }

  private void setHeadersForObject(final HttpUriRequestBase request, final AccessorObject accessorObject) {
    try {
      var value = accessorObject.getId();
      if (ParametersChecker.isNotEmpty(value)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_ID, value);
      }
      value = accessorObject.getSite();
      if (ParametersChecker.isNotEmpty(value)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_SITE, value);
      }
      value = accessorObject.getBucket();
      if (ParametersChecker.isNotEmpty(value)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_BUCKET, value);
      }
      value = accessorObject.getName();
      if (ParametersChecker.isNotEmpty(value)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_NAME, value);
      }
      value = accessorObject.getHash();
      if (ParametersChecker.isNotEmpty(value)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_HASH, value);
      }
      var instant = accessorObject.getCreation();
      if (ParametersChecker.isNotEmpty(instant)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_CREATION, instant.toString());
      }
      instant = accessorObject.getExpires();
      if (ParametersChecker.isNotEmpty(instant)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES, instant.toString());
      }
      final var status = accessorObject.getStatus();
      if (ParametersChecker.isNotEmpty(status)) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_STATUS, status.name());
      }
      request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_SIZE, Long.toString(accessorObject.getSize()));
      if (!accessorObject.getMetadata().isEmpty()) {
        request.addHeader(AccessorConstants.HeaderObject.X_OBJECT_METADATA,
            StandardProperties.getObjectMapper().writeValueAsString(accessorObject.getMetadata()));
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  private void setHeadersForFilter(final HttpUriRequestBase request, final AccessorFilter accessorFilter) {
    try {
      final var value = accessorFilter.getNamePrefix();
      if (ParametersChecker.isNotEmpty(value)) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX, value);
      }
      var instant = accessorFilter.getCreationAfter();
      if (ParametersChecker.isNotEmpty(instant)) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER, instant.toString());
      }
      instant = accessorFilter.getCreationBefore();
      if (ParametersChecker.isNotEmpty(instant)) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE, instant.toString());
      }
      instant = accessorFilter.getExpiresAfter();
      if (ParametersChecker.isNotEmpty(instant)) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER, instant.toString());
      }
      instant = accessorFilter.getExpiresBefore();
      if (ParametersChecker.isNotEmpty(instant)) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE, instant.toString());
      }
      final var status = accessorFilter.getStatuses();
      if (status != null && status.length > 0) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_STATUSES,
            StandardProperties.getObjectMapper().writeValueAsString(status));
      }
      if (accessorFilter.getSizeGreaterThan() > 0) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT,
            Long.toString(accessorFilter.getSizeGreaterThan()));
      }
      if (accessorFilter.getSizeLessThan() > 0) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT,
            Long.toString(accessorFilter.getSizeLessThan()));
      }
      if (accessorFilter.getMetadataFilter() != null && !accessorFilter.getMetadataFilter().isEmpty()) {
        request.addHeader(AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ,
            StandardProperties.getObjectMapper().writeValueAsString(accessorFilter.getMetadataFilter()));
      }
    } catch (final Exception e) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_ARGUMENT, e);
    }
  }

  private void setCommonHeaders(final HttpUriRequestBase request) {
    request.addHeader(AccessorConstants.Api.X_CLIENT_ID, factory.getClientId());
    request.addHeader(ApiConstants.X_OP_ID, GuidLike.getGuid());
  }

  private void setCompressionEncodingHeaders(final HttpUriRequestBase request) {
    request.addHeader(HttpHeaders.CONTENT_ENCODING, COMPRESSION_ZSTD);
  }

  private void setCompressionAcceptHeaders(final HttpUriRequestBase request) {
    request.addHeader(HttpHeaders.ACCEPT_ENCODING, COMPRESSION_ZSTD);
  }

  private void acceptProvideHeaders(final HttpUriRequestBase request, final boolean acceptJsonOrElseOctetStream,
                                    final boolean provideOctetStream) {
    if (acceptJsonOrElseOctetStream) {
      request.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON);
    } else {
      request.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_OCTET_STREAM);
    }
    if (provideOctetStream) {
      request.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM);
    }
  }

  private CcsWithStatusException getCcsError(final ApiErrorResponse error) {
    return new CcsWithStatusException(error.opId(), error.status(), error.message());
  }

  private ApiErrorResponse handleError(final ClassicHttpResponse response) {
    var xmessage = response.getFirstHeader(ApiConstants.X_ERROR);
    String message = null;
    if (xmessage != null) {
      message = xmessage.getValue();
    }
    var xopId = response.getFirstHeader(ApiConstants.X_OP_ID);
    String opId = null;
    if (xopId != null) {
      opId = response.getFirstHeader(ApiConstants.X_OP_ID).getValue();
    }
    return new ApiErrorResponse(response.getCode(), message, opId);
  }

  private StorageType getStorageTypeFromResponse(final ClassicHttpResponse response) {
    var xType = response.getFirstHeader(AccessorConstants.Api.X_TYPE);
    if (xType == null || ParametersChecker.isEmpty(xType.getValue())) {
      return StorageType.NONE;
    }
    var type = xType.getValue();
    try {
      return StorageType.valueOf(type);
    } catch (final IllegalArgumentException e) {
      return StorageType.NONE;
    }
  }
}
