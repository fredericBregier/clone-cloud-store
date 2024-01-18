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

package io.clonecloudstore.driver.azure;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlockBlobOutputStreamOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import static io.clonecloudstore.driver.azure.DriverAzureProperties.CLIENT_ID;
import static io.clonecloudstore.driver.azure.DriverAzureProperties.EXPIRY;
import static io.clonecloudstore.driver.azure.DriverAzureProperties.SHA_256;

@ApplicationScoped
@Unremovable
public class DriverAzureHelper {
  private static final Logger LOGGER = Logger.getLogger(DriverAzureHelper.class);
  private static final String BUCKET_CANNOT_BE_NULL = "Bucket cannot be null";
  private static final String BUCKET_OR_OBJECT_CANNOT_BE_NULL = "Bucket or Object cannot be null";
  private final BlobServiceClient blobServiceClient;

  DriverAzureHelper(final BlobServiceClient blobServiceClient) {
    this.blobServiceClient = blobServiceClient;
  }

  BlobServiceClient getBlobServiceClient() {
    return blobServiceClient;
  }

  PagedIterable<BlobContainerItem> getBuckets() throws DriverException {
    try {
      return blobServiceClient.listBlobContainers();
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    }
  }

  StorageBucket fromBlobContainerItem(final BlobContainerItem bucket) {
    final var clientId = bucket.getMetadata().get(CLIENT_ID);
    return new StorageBucket(bucket.getName(), clientId, bucket.getProperties().getLastModified().toInstant());
  }

  StorageBucket createBucket(final StorageBucket bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var map = Map.of(CLIENT_ID, bucket.clientId());
      blobServiceClient.createBlobContainer(bucket.bucket()).setMetadata(map);
      return getBucket(bucket.bucket());
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageBucket importBucket(final StorageBucket bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      if (existBucket(bucket.bucket())) {
        final var map = Map.of(CLIENT_ID, bucket.clientId());
        blobServiceClient.getBlobContainerClient(bucket.bucket()).setMetadata(map);
        return getBucket(bucket.bucket());
      }
      throw new DriverNotFoundException("Bucket Not Found");
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  void deleteBucket(final String bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      if (countObjectsInBucket(bucket) > 0) {
        throw new DriverNotAcceptableException("Bucket not empty");
      }
      blobServiceClient.deleteBlobContainer(bucket);
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageBucket getBucket(final String bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var blobClient = blobServiceClient.getBlobContainerClient(bucket);
      if (blobClient.exists()) {
        final var clientId = blobClient.getProperties().getMetadata().get(CLIENT_ID);
        return new StorageBucket(bucket, clientId, blobClient.getProperties().getLastModified().toInstant());
      }
      throw new DriverNotFoundException("Bucket Not Found");
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final IllegalStateException e) {
      LOGGER.warn(e);
      throw new DriverException(e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  boolean existBucket(final String bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      return blobServiceClient.getBlobContainerClient(bucket)
          .existsWithResponse(Duration.ofMillis(StandardProperties.getMaxWaitMs()), Context.NONE).getValue();
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final IllegalStateException e) {
      LOGGER.warn(e, e);
      return false;
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  long countObjectsInBucket(final String bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var client = blobServiceClient.getBlobContainerClient(bucket);
      return client.listBlobs().stream().count();
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  Iterator<BlobItem> getObjectsIteratorFilteredInBucket(final String bucket, final String prefix, final Instant from,
                                                        final Instant to) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var client = blobServiceClient.getBlobContainerClient(bucket);
      ListBlobsOptions options = new ListBlobsOptions().setDetails(new BlobListDetails().setRetrieveMetadata(true));
      if (ParametersChecker.isNotEmpty(prefix)) {
        options.setPrefix(prefix);
      }
      final var iterator = client.listBlobs(options, null).iterator();
      if (from != null || to != null) {
        return new BlobItemIterator(iterator, from, to);
      }
      return iterator;
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  Stream<BlobItem> getObjectsStreamFilteredInBucket(final String bucket, final String prefix, final Instant from,
                                                    final Instant to) throws DriverException {
    return getObjectsStreamFilteredInBucket(bucket, prefix, from, to, false);
  }

  private Stream<BlobItem> getObjectsStreamFilteredInBucket(final String bucket, final String prefix,
                                                            final Instant from, final Instant to,
                                                            final boolean existOnly) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var client = blobServiceClient.getBlobContainerClient(bucket);
      ListBlobsOptions options = new ListBlobsOptions();
      if (!existOnly) {
        options.setDetails(new BlobListDetails().setRetrieveMetadata(true));
      }
      if (ParametersChecker.isNotEmpty(prefix)) {
        options.setPrefix(prefix);
      }
      final var stream = client.listBlobs(options, null).stream();
      if (from != null || to != null) {
        return stream.filter(blobItem -> {
          var lastModified = blobItem.getProperties().getLastModified().toInstant();
          if (from != null && from.isAfter(lastModified)) {
            return false;
          }
          if (to != null) {
            return !to.isBefore(lastModified);
          }
          return true;
        });
      }
      return stream;
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  private String removeSha256(final Map<String, String> map) {
    return map.remove(SHA_256);
  }

  private Instant removeExpiry(final Map<String, String> map) {
    final var expiry = map.remove(EXPIRY);
    Instant expiryInstant = null;
    if (ParametersChecker.isNotEmpty(expiry)) {
      expiryInstant = Instant.parse(expiry);
    }
    return expiryInstant;
  }

  private Instant getLastModified(final OffsetDateTime lastModified) {
    return lastModified != null ? lastModified.toInstant() : Instant.now();
  }

  StorageObject fromBlobItem(final String bucket, final BlobItem object) throws DriverException {
    try {
      final var map = new HashMap<>(getMetadata(object));
      final var sha256 = removeSha256(map);
      final var expiryInstant = removeExpiry(map);
      var lastModified = getLastModified(object.getProperties().getLastModified());
      return new StorageObject(bucket, object.getName(), sha256, object.getProperties().getContentLength(),
          lastModified, expiryInstant, map);
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    }
  }

  StorageObject fromBlobProperties(final String bucket, final String name, final BlobProperties properties)
      throws DriverException {
    try {
      final var map = new HashMap<>(getMetadata(properties));
      final var sha256 = removeSha256(map);
      final var expiryInstant = removeExpiry(map);
      var lastModified = getLastModified(properties.getLastModified());
      return new StorageObject(bucket, name, sha256, properties.getBlobSize(), lastModified, expiryInstant, map);
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    }
  }

  boolean existObjectInBucket(final String bucket, final String object) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, object);
      final var client = blobServiceClient.getBlobContainerClient(bucket);
      return client.getBlobClient(object).exists();
    } catch (final BlobStorageException e) {
      if (e.getStatusCode() == 404) {
        return false;
      }
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageType existDirectoryOrObjectInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, directoryOrObject);
      if (existObjectInBucket(bucket, directoryOrObject)) {
        return StorageType.OBJECT;
      }
      var count = getObjectsStreamFilteredInBucket(bucket, directoryOrObject, null, null, true).count();
      if (count > 0) {
        return StorageType.DIRECTORY;
      }
      return StorageType.NONE;
    } catch (final DriverNotFoundException e) {
      return StorageType.NONE;
    } catch (final BlobStorageException e) {
      if (e.getStatusCode() == 404) {
        return StorageType.NONE;
      }
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  long objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, object, inputStream);
      final var blobClient = blobServiceClient.getBlobContainerClient(object.bucket()).getBlobClient(object.name());
      if (object.size() > 0 && object.size() < DriverAzureProperties.getMaxPartSize()) {
        blobClient.upload(inputStream, object.size());
        writeMetadata(object, blobClient);
        return object.size();
      } else {
        long len = writeInputStreamToObject(object, inputStream, blobClient);
        writeMetadata(object, blobClient);
        return len;
      }
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  private void writeMetadata(final StorageObject object, final BlobClient blobClient) {
    final var map = new HashMap<>(object.metadata());
    if (ParametersChecker.isNotEmpty(object.hash())) {
      map.put(SHA_256, object.hash());
    }
    if (ParametersChecker.isNotEmpty(object.expiresDate())) {
      map.put(EXPIRY, object.expiresDate().toString());
    }
    blobClient.setMetadata(map);
  }

  private long writeInputStreamToObject(final StorageObject object, final InputStream inputStream,
                                        final BlobClient blobClient) throws DriverException {
    final var blobBlockClient = blobClient.getBlockBlobClient();
    final var partSize =
        Math.min(object.size() > 0 ? object.size() : DriverAzureProperties.getMaxPartSizeForUnknownLength(),
            DriverAzureProperties.getMaxPartSizeForUnknownLength());
    ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
        // 2 Concurrency requests as max,you can set more than it to accelerate uploading
        .setMaxConcurrency(DriverAzureProperties.getMaxConcurrency()).setBlockSizeLong(partSize)
        .setMaxSingleUploadSizeLong(DriverAzureProperties.getMaxPartSize());
    BlobHttpHeaders headers = new BlobHttpHeaders().setContentLanguage("en-US").setContentType("binary");
    var options =
        new BlockBlobOutputStreamOptions().setParallelTransferOptions(parallelTransferOptions).setHeaders(headers);
    try (final var blobOS = blobBlockClient.getBlobOutputStream(options)) {
      final var bytes = new byte[StandardProperties.getBufSize()];
      int read;
      long len = 0;
      while ((read = inputStream.read(bytes, 0, bytes.length)) >= 0) {
        if (read > 0) {
          blobOS.write(bytes, 0, read);
          len += read;
        }
      }
      blobOS.flush();
      return len;
    } catch (final IOException e) {
      throw new DriverException(e);
    }
  }

  StorageObject finalizeObject(final String bucket, final String object, final String sha256, final long realLen)
      throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, object);
      final var client = blobServiceClient.getBlobContainerClient(bucket).getBlobClient(object);
      final var properties = client.getProperties();
      if (ParametersChecker.isNotEmpty(sha256)) {
        final var map = new HashMap<>(properties.getMetadata());
        map.put(SHA_256, sha256);
        client.setMetadata(map);
        var expiry = removeExpiry(map);
        removeSha256(map);
        var lastModified = getLastModified(properties.getLastModified());
        return new StorageObject(bucket, object, sha256, realLen, lastModified, expiry, map);
      }
      return fromBlobProperties(bucket, object, properties);
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  InputStream getObjectBodyInBucket(final String bucket, final String object) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, object);
      var options = new BlobInputStreamOptions().setBlockSize(DriverAzureProperties.getMaxPartSizeForUnknownLength());
      final var client = blobServiceClient.getBlobContainerClient(bucket);
      return client.getBlobClient(object).openInputStream(options);
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  private Map<String, String> getMetadata(final BlobItem blobItem) {
    if (blobItem.getMetadata() == null) {
      return new HashMap<>();
    }
    return new HashMap<>(blobItem.getMetadata());
  }

  private Map<String, String> getMetadata(final BlobProperties properties) {
    if (properties.getMetadata() == null) {
      return new HashMap<>();
    }
    return new HashMap<>(properties.getMetadata());
  }

  StorageObject getObjectInBucket(final String bucket, final String object) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, object);
      final var client = blobServiceClient.getBlobContainerClient(bucket).getBlobClient(object);
      final var properties = client.getProperties();
      return fromBlobProperties(bucket, object, properties);
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageObject objectCopyToAnother(final StorageObject source, final StorageObject target)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter("Source and Target cannot be null", source, target);
      final BlobClient sourceBlob =
          blobServiceClient.getBlobContainerClient(source.bucket()).getBlobClient(source.name());
      final BlobClient targetBlob =
          blobServiceClient.getBlobContainerClient(target.bucket()).getBlobClient(target.name());
      StorageObject targetUpdated =
          new StorageObject(target.bucket(), target.name(), source.hash(), source.size(), Instant.now(),
              target.expiresDate(), target.metadata());
      // Setup Sas Token to allow copy between Container
      OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
      BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
      BlobServiceSasSignatureValues values =
          new BlobServiceSasSignatureValues(expiryTime, permission).setStartTime(OffsetDateTime.now());
      String sasToken = sourceBlob.generateSas(values);
      targetBlob.copyFromUrl(sourceBlob.getBlobUrl() + "?" + sasToken);
      writeMetadata(targetUpdated, targetBlob);
      return fromBlobProperties(target.bucket(), target.name(), targetBlob.getProperties());
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  void deleteObjectInBucket(final String bucket, final String object) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, object);
      final var client = blobServiceClient.getBlobContainerClient(bucket);
      client.getBlobClient(object).delete();
    } catch (final BlobStorageException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getStatusCode(), e);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
  }

  private static class BlobItemIterator implements Iterator<BlobItem> {
    private final Iterator<BlobItem> iterator;
    private final Instant start;
    private final Instant end;
    private BlobItem blobItem;

    BlobItemIterator(final Iterator<BlobItem> iterator, final Instant start, final Instant end) {
      this.iterator = iterator;
      this.start = start;
      this.end = end;
      blobItem = null;
    }

    private BlobItem nextInternal() {
      while (iterator.hasNext()) {
        final var item = iterator.next();
        final var lastModified = item.getProperties().getLastModified().toInstant();
        if ((start != null && start.isAfter(lastModified)) || (end != null && end.isBefore(lastModified))) {
          continue;
        }
        return item;
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      if (blobItem == null) {
        blobItem = nextInternal();
      }
      return blobItem != null;
    }

    @Override
    public BlobItem next() {
      if (blobItem != null) {
        var temp = blobItem;
        blobItem = null;
        return temp;
      }
      throw new NoSuchElementException();
    }
  }
}
