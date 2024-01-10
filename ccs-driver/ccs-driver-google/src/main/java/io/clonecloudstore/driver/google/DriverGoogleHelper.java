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

package io.clonecloudstore.driver.google;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.google.api.gax.paging.Page;
import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import io.clonecloudstore.common.quarkus.stream.ChunkInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;

import static io.clonecloudstore.driver.google.DriverGoogle.BUCKET_DOES_NOT_EXIST;
import static io.clonecloudstore.driver.google.DriverGoogle.OBJECT_DOES_NOT_EXIST;
import static io.clonecloudstore.driver.google.DriverGoogleProperties.EXPIRY;
import static io.clonecloudstore.driver.google.DriverGoogleProperties.MAX_ITEMS;
import static io.clonecloudstore.driver.google.DriverGoogleProperties.SHA_256;

@ApplicationScoped
@Unremovable
public class DriverGoogleHelper {
  private static final Logger LOGGER = Logger.getLogger(DriverGoogleHelper.class);
  private final Storage storage;

  public DriverGoogleHelper(final Storage storage) {
    this.storage = storage;
  }

  public Page<Bucket> getBuckets() throws DriverException {
    try {
      return storage.list(Storage.BucketListOption.pageSize(MAX_ITEMS));
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public StorageBucket fromBucketInfo(final BucketInfo bucket) {
    return new StorageBucket(bucket.getName(), bucket.getCreateTimeOffsetDateTime().toInstant());
  }

  public StorageBucket createBucket(final StorageBucket bucket) throws DriverException {
    try {
      var result = storage.create(BucketInfo.newBuilder(bucket.bucket()).build());
      return fromBucketInfo(result.asBucketInfo());
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public void deleteBucket(final String bucket) throws DriverException {
    try {
      if (countObjectsInBucket(bucket) > 0) {
        throw new DriverNotAcceptableException("Bucket not empty");
      }
      storage.get(bucket).delete();
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public boolean existBucket(final String bucket) throws DriverException {
    try {
      var result = storage.get(bucket, Storage.BucketGetOption.fields());
      return result != null && result.exists();
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public long countObjectsInBucket(final String bucket) throws DriverException {
    return countObjectsInBucket(bucket, null);
  }

  private long countObjectsInBucket(final String bucket, final String prefix) throws DriverException {
    try {
      final Page<Blob> page;
      var container = storage.get(bucket);
      if (container == null) {
        throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
      }
      if (ParametersChecker.isEmpty(prefix)) {
        page = container.list(Storage.BlobListOption.pageSize(MAX_ITEMS), Storage.BlobListOption.fields());
      } else {
        page = container.list(Storage.BlobListOption.pageSize(MAX_ITEMS), Storage.BlobListOption.fields(),
            Storage.BlobListOption.prefix(prefix));
      }
      var iterable = page.iterateAll();
      final AtomicLong count = new AtomicLong();
      iterable.forEach(blob -> count.getAndIncrement());
      return count.get();
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public Iterator<Blob> getObjectsIteratorFilteredInBucket(final String bucket, final String prefix, final Instant from,
                                                           final Instant to) throws DriverException {
    try {
      final Page<Blob> page = getBlobPage(bucket, prefix);
      final var iterator = page.iterateAll().iterator();
      if (from != null || to != null) {
        return new BlobIterator(iterator, from, to);
      }
      return iterator;
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public Stream<Blob> getObjectsStreamFilteredInBucket(final String bucket, final String prefix, final Instant from,
                                                       final Instant to) throws DriverException {
    try {
      final Page<Blob> page;
      var container = storage.get(bucket);
      if (container == null) {
        throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
      }
      if (ParametersChecker.isNotEmpty(prefix)) {
        page = container.list(Storage.BlobListOption.pageSize(MAX_ITEMS), Storage.BlobListOption.prefix(prefix));
      } else {
        page = container.list(Storage.BlobListOption.pageSize(MAX_ITEMS));
      }
      final var stream = page.streamAll();
      if (from != null || to != null) {
        return stream.filter(blobItem -> {
          var lastModified = blobItem.asBlobInfo().getUpdateTimeOffsetDateTime().toInstant();
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
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  private Page<Blob> getBlobPage(final String bucket, final String prefix) throws DriverNotFoundException {
    final Page<Blob> page;
    var container = storage.get(bucket);
    if (container == null) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
    }
    if (ParametersChecker.isNotEmpty(prefix)) {
      page = container.list(Storage.BlobListOption.pageSize(MAX_ITEMS), Storage.BlobListOption.prefix(prefix));
    } else {
      page = container.list(Storage.BlobListOption.pageSize(MAX_ITEMS));
    }
    return page;
  }

  public StorageObject fromBlob(final Blob object) throws DriverException {
    try {
      final var map = getMetadata(object);
      final var sha256 = map.remove(SHA_256);
      final var expiry = map.remove(EXPIRY);
      Instant expryInstant = null;
      if (ParametersChecker.isNotEmpty(expiry)) {
        expryInstant = Instant.parse(expiry);
      }
      var info = object.asBlobInfo();
      var lastModified = info.getUpdateTimeOffsetDateTime();
      var instantLastModified = lastModified != null ? lastModified.toInstant() : Instant.now();
      return new StorageObject(object.getBucket(), object.getName(), sha256, info.getSize(), instantLastModified,
          expryInstant, map);
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public boolean existObjectInBucket(final String bucket, final String object) throws DriverException {
    try {
      BlobId blobId = BlobId.of(bucket, object);
      var blob = storage.get(blobId, Storage.BlobGetOption.fields());
      return blob != null && blob.exists();
    } catch (final BaseServiceException e) {
      if (e.getCode() == 404) {
        return false;
      }
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public StorageType existDirectoryOrObjectInBucket(final String bucket, final String directoryOrObject)
      throws DriverException {
    try {
      if (existObjectInBucket(bucket, directoryOrObject)) {
        return StorageType.OBJECT;
      }
      var count = countObjectsInBucket(bucket, directoryOrObject);
      if (count > 0) {
        return StorageType.DIRECTORY;
      }
      return StorageType.NONE;
    } catch (final DriverNotFoundException e) {
      return StorageType.NONE;
    } catch (final BaseServiceException e) {
      if (e.getCode() == 404) {
        return StorageType.NONE;
      }
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public long objectPrepareCreateInBucket(final StorageObject object, final InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      BlobId blobId = BlobId.of(object.bucket(), object.name());
      if (existObjectInBucket(object.bucket(), object.name())) {
        throw new DriverAlreadyExistException(object.bucket() + ":" + object);
      }
      if (DriverGoogleProperties.isGoogleDisableGzip()) {
        return writeInputStreamDirect(object, inputStream, blobId);
      } else {
        return writeInputStreamWriteChannel(object, inputStream, blobId);
      }
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  private Storage.BlobWriteOption[] getBlobWriteOption() {
    if (DriverGoogleProperties.isGoogleDisableGzip()) {
      return new Storage.BlobWriteOption[]{Storage.BlobWriteOption.disableGzipContent()};
    }
    return new Storage.BlobWriteOption[0];
  }

  long writeInputStreamDirect(final StorageObject object, final InputStream inputStream, final BlobId blobId)
      throws DriverException {
    final var map = new HashMap<>(object.metadata());
    if (ParametersChecker.isNotEmpty(object.hash())) {
      map.put(SHA_256, object.hash());
    }
    if (ParametersChecker.isNotEmpty(object.expiresDate())) {
      map.put(EXPIRY, object.expiresDate().toString());
    }
    var bucket = storage.get(object.bucket());
    if (bucket == null) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + object.bucket());
    }
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId).setMetadata(map).setContentType(MediaType.APPLICATION_OCTET_STREAM).build();
    var blob = storage.create(blobInfo, inputStream, getBlobWriteOption());
    return blob.asBlobInfo().getSize();
  }

  /**
   * Slow version (up to 2 times than Direct) if gzip off, faster (up to 2 times) if gzip on
   */
  long writeInputStreamWriteChannel(final StorageObject object, final InputStream inputStream, final BlobId blobId)
      throws DriverException {
    final var map = new HashMap<>(object.metadata());
    if (ParametersChecker.isNotEmpty(object.hash())) {
      map.put(SHA_256, object.hash());
    }
    if (ParametersChecker.isNotEmpty(object.expiresDate())) {
      map.put(EXPIRY, object.expiresDate().toString());
    }
    BlobInfo blobInfo =
        BlobInfo.newBuilder(blobId).setMetadata(map).setContentType(MediaType.APPLICATION_OCTET_STREAM).build();
    try (final var writeChannel = storage.writer(blobInfo, getBlobWriteOption())) {
      writeChannel.setChunkSize(DriverGoogleProperties.getMaxBufSize());
      int read = 0;
      long size = 0;
      byte[] bytes = new byte[StandardProperties.getBufSize()];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      while ((read = inputStream.read(bytes, 0, bytes.length)) >= 0) {
        if (read > 0) {
          size += read;
          buffer.clear();
          buffer.limit(read);
          var write = writeChannel.write(buffer);
          read -= write;
          while (read > 0) {
            write = writeChannel.write(buffer);
            read -= write;
          }
        }
      }
      SystemTools.silentlyCloseNoException(inputStream);
      return size;
    } catch (final IOException e) {
      throw new DriverException(e);
    }
  }

  private Storage.BlobTargetOption[] getBlobTargetOption() {
    if (DriverGoogleProperties.isGoogleDisableGzip()) {
      return new Storage.BlobTargetOption[]{Storage.BlobTargetOption.disableGzipContent()};
    }
    return new Storage.BlobTargetOption[0];
  }

  /**
   * Slow version (2 times than Direct and slower than WriteChannel, whatever gzip)
   */
  long writeInputStreamWCompose(final StorageObject object, final InputStream inputStream, final BlobId blobId)
      throws DriverException {
    List<BlobId> blobIds = new ArrayList<>();
    final var map = new HashMap<>(object.metadata());
    if (ParametersChecker.isNotEmpty(object.hash())) {
      map.put(SHA_256, object.hash());
    }
    if (ParametersChecker.isNotEmpty(object.expiresDate())) {
      map.put(EXPIRY, object.expiresDate().toString());
    }
    // Per chunk
    int rank = 1;
    long globalSize = object.size() > 0 ? object.size() : DriverGoogleProperties.getMaxBufSize();
    var partSizeFinal = (int) Math.min(DriverGoogleProperties.getMaxBufSize(), globalSize);
    LOGGER.infof("PartSize %d (%d) %d", partSizeFinal, globalSize, DriverGoogleProperties.getMaxBufSize());
    try (final var chunkInputStream = new ChunkInputStream(inputStream, object.size(), partSizeFinal)) {
      while (chunkInputStream.nextChunk()) {
        final var chunkSize = chunkInputStream.getBufferSize();
        BlobInfo blobInfoPart = BlobInfo.newBuilder(object.bucket(), object.name() + "_" + rank)
            .setContentType(MediaType.APPLICATION_OCTET_STREAM).build();
        blobIds.add(blobInfoPart.getBlobId());
        storage.create(blobInfoPart, chunkInputStream.getBuffer(), 0, chunkSize, getBlobTargetOption());
        rank++;
      }
      rank--;
      LOGGER.infof("Rank %d", rank);
      // Compose per 32
      double pow = 1.0 / (Math.log(32) / Math.log(rank));
      if (pow > 1.0) {
        LOGGER.infof("Will have to compute %d in %f", rank, pow);
        throw new DriverException("Not yet supported");
      }
      // 32 parts maximum
      BlobInfo blobInfo =
          BlobInfo.newBuilder(blobId).setMetadata(map).setContentType(MediaType.APPLICATION_OCTET_STREAM).build();
      final var compose = Storage.ComposeRequest.newBuilder().setTarget(blobInfo);
      if (DriverGoogleProperties.isGoogleDisableGzip()) {
        compose.setTargetOptions(Storage.BlobTargetOption.disableGzipContent());
      }
      for (int i = 1; i <= rank; i++) {
        compose.addSource(object.name() + "_" + i);
      }
      storage.compose(compose.build());
      return chunkInputStream.getCurrentTotalRead();
    } catch (final IOException e) {
      throw new DriverException(e);
    } finally {
      storage.delete(blobIds);
    }
  }

  public StorageObject finalizeObject(final String bucket, final String object, final String sha256)
      throws DriverException {
    try {
      BlobId blobId = BlobId.of(bucket, object);
      var blob = storage.get(blobId);
      if (blob == null || !blob.exists()) {
        throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + object);
      }
      final var blobInfo = blob.asBlobInfo();
      final var metadata = blobInfo.getMetadata();
      if (ParametersChecker.isNotEmpty(sha256)) {
        if (metadata == null) {
          final var map = new HashMap<String, String>();
          map.put(SHA_256, sha256);
          blob = blob.toBuilder().setMetadata(map).build().update();
        } else if (ParametersChecker.isEmpty(metadata.get(SHA_256))) {
          final var map = new HashMap<>(metadata);
          map.put(SHA_256, sha256);
          blob = blob.toBuilder().setMetadata(map).build().update();
        }
      }
      return fromBlob(blob);
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public InputStream getObjectBodyInBucket(final String bucket, final String object) throws DriverException {
    try {
      BlobId blobId = BlobId.of(bucket, object);
      var blob = storage.get(blobId, Storage.BlobGetOption.fields());
      if (blob == null || !blob.exists()) {
        throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + object);
      }
      final var readChannel = blob.reader(Blob.BlobSourceOption.shouldReturnRawInputStream(true));
      readChannel.setChunkSize(StandardProperties.getBufSize());
      final var inputStream = new PipedInputStream(StandardProperties.DEFAULT_PIPED_BUFFER_SIZE); // NOSONAR intentional
      final var outputStream = new PipedOutputStream(inputStream); // NOSONAR intentional
      final var finalInputStream =
          new InputStreamClosing(inputStream, outputStream, readChannel); // NOSONAR intentional
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          var read = 0;
          final var buf = new byte[StandardProperties.getBufSize()];
          final var buffer = ByteBuffer.wrap(buf);
          while ((read = readChannel.read(buffer)) >= 0) {
            if (read > 0) {
              outputStream.write(buf, 0, read);
            }
            buffer.clear();
          }
          outputStream.flush();
          outputStream.close();
        } catch (final IOException e) {
          LOGGER.warn(e, e);
          finalInputStream.setException(e);
          Thread.yield();
        }
      });
      return finalInputStream;
    } catch (final IOException e) {
      throw new DriverException(e);
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  private Map<String, String> getMetadata(final Blob blobItem) {
    if (blobItem.getMetadata() == null) {
      return new HashMap<>();
    }
    return new HashMap<>(blobItem.getMetadata());
  }

  public StorageObject getObjectInBucket(final String bucket, final String object) throws DriverException {
    try {
      BlobId blobId = BlobId.of(bucket, object);
      var blob = storage.get(blobId);
      if (blob == null || !blob.exists()) {
        throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + object);
      }
      return fromBlob(blob);
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  public void deleteObjectInBucket(final String bucket, final String object) throws DriverException {
    try {
      BlobId blobId = BlobId.of(bucket, object);
      var blob = storage.get(blobId, Storage.BlobGetOption.fields());
      if (blob == null || !blob.exists()) {
        throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + object);
      }
      blob.delete();
    } catch (final BaseServiceException e) {
      throw DriverException.getDriverExceptionFromStatus(e.getCode(), e);
    }
  }

  private static class BlobIterator implements Iterator<Blob> {
    private final Iterator<Blob> iterator;
    private final Instant start;
    private final Instant end;
    private Blob blob;

    public BlobIterator(final Iterator<Blob> iterator, final Instant start, final Instant end) {
      this.iterator = iterator;
      this.start = start;
      this.end = end;
      blob = null;
    }

    private Blob nextInternal() {
      while (iterator.hasNext()) {
        final var item = iterator.next();
        final var lastModified = item.asBlobInfo().getUpdateTimeOffsetDateTime().toInstant();
        if ((start != null && start.isAfter(lastModified)) || (end != null && end.isBefore(lastModified))) {
          continue;
        }
        return item;
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      if (blob == null) {
        blob = nextInternal();
      }
      return blob != null;
    }

    @Override
    public Blob next() {
      if (blob != null) {
        var temp = blob;
        blob = null;
        return temp;
      }
      throw new NoSuchElementException();
    }
  }

  private static class InputStreamClosing extends InputStream {
    private final PipedInputStream pipedInputStream;
    private final PipedOutputStream pipedOutputStream;
    private final ReadChannel readChannel;
    private IOException exception = null;

    private InputStreamClosing(PipedInputStream pipedInputStream, PipedOutputStream pipedOutputStream,
                               ReadChannel readChannel) {
      this.pipedInputStream = pipedInputStream;
      this.pipedOutputStream = pipedOutputStream;
      this.readChannel = readChannel;
    }

    private void setException(final IOException e) {
      exception = e;
    }

    private void checkException() throws IOException {
      if (exception != null) {
        throw exception;
      }
    }

    @Override
    public int read() throws IOException {
      checkException();
      return pipedInputStream.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
      checkException();
      return pipedInputStream.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      checkException();
      return pipedInputStream.read(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
      checkException();
      return pipedInputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
      checkException();
      return pipedInputStream.available();
    }

    @Override
    public void close() {
      SystemTools.silentlyCloseNoException(pipedInputStream);
      SystemTools.silentlyCloseNoException(pipedOutputStream);
      readChannel.close();
    }

    @Override
    public void mark(final int readlimit) {
      pipedInputStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
      checkException();
      pipedInputStream.reset();
    }

    @Override
    public boolean markSupported() {
      return pipedInputStream.markSupported();
    }

    @Override
    public long transferTo(final OutputStream out) throws IOException {
      checkException();
      return SystemTools.transferTo(pipedInputStream, out);
    }
  }
}
