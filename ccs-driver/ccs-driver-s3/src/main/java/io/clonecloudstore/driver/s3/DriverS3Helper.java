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

package io.clonecloudstore.driver.s3;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.exception.DriverRuntimeException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.io.ReleasableInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import static io.clonecloudstore.driver.s3.DriverS3Properties.CLIENT_ID;
import static io.clonecloudstore.driver.s3.DriverS3Properties.MAX_ITEMS;
import static io.clonecloudstore.driver.s3.DriverS3Properties.SHA_256;
import static io.clonecloudstore.driver.s3.DriverS3Properties.getDriverS3Host;
import static io.clonecloudstore.driver.s3.DriverS3Properties.getDriverS3Key;
import static io.clonecloudstore.driver.s3.DriverS3Properties.getDriverS3KeyId;
import static io.clonecloudstore.driver.s3.DriverS3Properties.getDriverS3Region;

/**
 * Internal S3 real actions Helper
 */
@ApplicationScoped
@Unremovable
public class DriverS3Helper {
  static final String BUCKET_DOES_NOT_EXIST = "Bucket does not exist: ";
  static final String OBJECT_DOES_NOT_EXIST = "Object does not exist: ";
  static final String BUCKET_ALREADY_EXISTS = "Bucket already exists: ";
  private static final Logger LOGGER = Logger.getLogger(DriverS3Helper.class);
  private static final String BUCKET_CANNOT_BE_NULL = "Bucket cannot be null";
  private static final String BUCKET_OR_OBJECT_CANNOT_BE_NULL = "Bucket or Object cannot be null";
  public static final String FOR = " for ";
  public static final String OBJECT_CANNOT_BE_CREATED_CODE = "Object cannot be created, code: ";


  DriverS3Helper() {
    // Empty
  }

  S3Client getClient() throws DriverRuntimeException {
    try {
      LOGGER.debugf("Charge configuration S3: %s %s", getDriverS3Host(), getDriverS3Region());
      return S3Client.builder().endpointOverride(new URI(getDriverS3Host())).credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create(getDriverS3KeyId(), getDriverS3Key())))
          .region(Region.of(getDriverS3Region())).build();
    } catch (final URISyntaxException | RuntimeException e) {
      throw new DriverRuntimeException("Wrong URI or client build", e);
    }
  }

  StorageBucket createBucket(final S3Client s3Client, final StorageBucket bucket)
      throws DriverAlreadyExistException, DriverNotAcceptableException, DriverException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      // Check if Bucket already exists
      if (!existBucket(s3Client, bucket.bucket())) {
        final var bucketRequest = CreateBucketRequest.builder().bucket(bucket.bucket()).build();
        internalCreateBucket(s3Client, bucket, bucketRequest);
        final var bucketRequestWait = HeadBucketRequest.builder().bucket(bucket.bucket()).build();
        // Wait until the bucket is created
        final var s3Waiter = s3Client.waiter();
        final var waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
        final var responseWait = waiterResponse.matched().response();
        if (responseWait.isEmpty() || !responseWait.get().sdkHttpResponse().isSuccessful()) {
          // KO
          throw new DriverNotAcceptableException("Cannot create Bucket: " + bucket);
        }
        final var response = getBucket(s3Client, bucket.bucket());
        if (response == null) {
          throw new DriverException("Cannot find created Bucket: " + bucket);
        }
        return response;
      } else {
        throw new DriverAlreadyExistException(BUCKET_ALREADY_EXISTS + bucket);
      }
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageBucket importBucket(final S3Client s3Client, final StorageBucket bucket)
      throws DriverAlreadyExistException, DriverNotAcceptableException, DriverException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      // Check if Bucket already exists
      if (existBucket(s3Client, bucket.bucket())) {
        createTag(s3Client, bucket);
        final var response = getBucket(s3Client, bucket.bucket());
        if (response == null) {
          throw new DriverException("Cannot find imported Bucket: " + bucket);
        }
        return response;
      } else {
        throw new DriverNotFoundException("Bucket Not Found");
      }
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private static void internalCreateBucket(final S3Client s3Client, final StorageBucket bucket,
                                           final CreateBucketRequest bucketRequest)
      throws DriverNotAcceptableException, DriverAlreadyExistException {
    try {
      final var response = s3Client.createBucket(bucketRequest);
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new DriverNotAcceptableException(
            "Bucket cannot be created, code: " + response.sdkHttpResponse().statusCode() + FOR + bucket);
      }
      createTag(s3Client, bucket);
    } catch (final BucketAlreadyExistsException ignored) {
      throw new DriverAlreadyExistException(BUCKET_ALREADY_EXISTS + bucket);
    }
  }

  private static void createTag(final S3Client s3Client, final StorageBucket bucket)
      throws DriverNotAcceptableException {
    final var tag = Tag.builder().key(CLIENT_ID).value(bucket.clientId()).build();
    final var tagging = Tagging.builder().tagSet(tag).build();
    final var putTag = PutBucketTaggingRequest.builder().bucket(bucket.bucket()).tagging(tagging).build();
    final var responseTag = s3Client.putBucketTagging(putTag);
    if (!responseTag.sdkHttpResponse().isSuccessful()) {
      throw new DriverNotAcceptableException(
          "Bucket cannot be tagged, code: " + responseTag.sdkHttpResponse().statusCode() + FOR + bucket);
    }
  }

  private StorageBucket internalGetBucket(final S3Client s3Client, final String bucket) throws DriverException {
    final var response = getBuckets(s3Client);
    final var buckets = response.buckets();
    for (final var bucket1 : buckets) {
      if (bucket1.name().equals(bucket)) {
        final var clientId = getClientIdTag(s3Client, bucket);
        return fromBucket(bucket1, clientId);
      }
    }
    return null;
  }

  StorageBucket getBucket(final S3Client s3Client, final String bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var storageBucket = internalGetBucket(s3Client, bucket);
      if (storageBucket == null) {
        throw new DriverNotFoundException("Bucket Not Found");
      }
      return storageBucket;
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  boolean existBucket(final S3Client s3Client, final String bucket) throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
      final var bucketRequestExist = HeadBucketRequest.builder().bucket(bucket).build();
      return s3Client.headBucket(bucketRequestExist).sdkHttpResponse().isSuccessful();
    } catch (final NoSuchBucketException ignored) {
      return false;
    } catch (final RuntimeException e) {
      if (e instanceof S3Exception se && (se.statusCode() == 400)) {
        return false;
      }
      throw new DriverException(e);
    }
  }

  ListBucketsResponse getBuckets(final S3Client s3Client) throws DriverException {
    final var listBucketsRequest = ListBucketsRequest.builder().build();
    try {
      return s3Client.listBuckets(listBucketsRequest);
    } catch (final NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  String getClientIdTag(final S3Client s3Client, final String bucket) throws DriverException {
    final var getTag = GetBucketTaggingRequest.builder().bucket(bucket).build();
    try {
      final var response = s3Client.getBucketTagging(getTag);
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new DriverNotFoundException("Bucket tag not found");
      }
      final var optional =
          response.tagSet().stream().filter(tag -> CLIENT_ID.equals(tag.key())).map(Tag::value).findFirst();
      if (optional.isPresent()) {
        return optional.get();
      }
      throw new DriverNotFoundException("Bucket tag not found");
    } catch (final NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageBucket fromBucket(final Bucket bucket, final String clientId) {
    return new StorageBucket(bucket.name(), clientId, bucket.creationDate());
  }

  void deleteBucket(final S3Client s3Client, final String bucket)
      throws DriverException, DriverNotFoundException, DriverNotAcceptableException { // NOSONAR Exception details
    if (!existBucket(s3Client, bucket)) {
      // Not found
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
    }
    var status = Response.Status.NOT_ACCEPTABLE.getStatusCode();
    try {
      final var response = s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
      status = response.sdkHttpResponse().statusCode();
      if (!response.sdkHttpResponse().isSuccessful()) {
        // KO
        throw new DriverNotAcceptableException(
            "Cannot delete Bucket, code: " + response.sdkHttpResponse().statusCode() + " for: " + bucket);
      }
    } catch (final NoSuchBucketException e) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket, e);
    } catch (final SdkException e) {
      throw new DriverNotAcceptableException("Cannot delete Bucket, code: " + status + " for: " + bucket);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  long countObjectsInBucket(final S3Client s3Client, final String bucket)
      throws DriverException, DriverNotFoundException { // NOSONAR Exception details
    final var iterator = getObjectsIteratorFilteredInBucket(s3Client, bucket, null, null, null);
    return SystemTools.consumeAll(iterator);
  }

  /**
   * Gets S3 objects that reside in a specific bucket and whose keys conform to the
   * specified prefix using v2 of the AWS Java SDK.
   * <br><br>
   * The objects returned will have a last-modified date between {@code start} and
   * {@code end}.
   * <br><br>
   * Any objects that have been modified outside of the specified date-time range will
   * not be returned.
   *
   * @param s3Client The v2 AWS S3 client used to make the request to S3.
   * @param bucket   The bucket where the S3 objects are located.
   * @param prefix   The common prefix that the keys of the S3 objects must conform to.
   * @param start    The objects returned will have been modified after this instant.
   * @param end      The objects returned will have been modified before this instant.
   * @return A {@link Stream} of {@link S3Object} objects.
   */
  Stream<S3Object> getObjectsStreamFilteredInBucket(final S3Client s3Client, final String bucket, final String prefix,
                                                    final Instant start, final Instant end)
      throws DriverException, DriverNotFoundException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
    final ListObjectsV2Request request = getListObjectsV2Request(s3Client, bucket, prefix);
    try {
      if (start != null || end != null) {
        return s3Client.listObjectsV2Paginator(request).contents().stream().filter(s3Object -> {
          final var lastModified = s3Object.lastModified();
          if (start != null && start.isAfter(lastModified)) {
            return false;
          }
          if (end != null) {
            return !end.isBefore(lastModified);
          }
          return true;
        });
      }
      return s3Client.listObjectsV2Paginator(request).contents().stream();
    } catch (final NoSuchBucketException e) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket, e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private ListObjectsV2Request getListObjectsV2Request(final S3Client s3Client, final String bucket,
                                                       final String prefix) throws DriverException {
    if (!existBucket(s3Client, bucket)) {
      // Not found
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
    }
    final ListObjectsV2Request request;
    // If start or end is set, LIMIT must not be set
    if (ParametersChecker.isNotEmpty(prefix)) {
      request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(MAX_ITEMS).build();
    } else {
      request = ListObjectsV2Request.builder().bucket(bucket).maxKeys(MAX_ITEMS).build();
    }
    return request;
  }

  /**
   * Gets S3 objects that reside in a specific bucket and whose keys conform to the
   * specified prefix using v2 of the AWS Java SDK.
   * <br><br>
   * The objects returned will have a last-modified date between {@code start} and
   * {@code end}.
   * <br><br>
   * Any objects that have been modified outside the specified date-time range will
   * not be returned.
   *
   * @param s3Client The v2 AWS S3 client used to make the request to S3.
   * @param bucket   The bucket where the S3 objects are located.
   * @param prefix   The common prefix that the keys of the S3 objects must conform to.
   * @param start    The objects returned will have been modified after this instant.
   * @param end      The objects returned will have been modified before this instant.
   * @return A {@link Stream} of {@link S3Object} objects.
   */
  Iterator<S3Object> getObjectsIteratorFilteredInBucket(final S3Client s3Client, final String bucket,
                                                        final String prefix, final Instant start, final Instant end)
      throws DriverException, DriverNotFoundException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter(BUCKET_CANNOT_BE_NULL, bucket);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
    final ListObjectsV2Request request = getListObjectsV2Request(s3Client, bucket, prefix);
    try {
      if (start != null || end != null) {
        final var iterator = s3Client.listObjectsV2Paginator(request).contents().iterator();
        return new S3ObjectIterator(iterator, start, end);
      }
      return s3Client.listObjectsV2Paginator(request).contents().iterator();
    } catch (final NoSuchBucketException e) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket, e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageObject fromS3Object(final S3Client s3Client, final String bucket, final S3Object object)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    return getObjectInBucket(s3Client, bucket, object.key());
  }

  StorageObject getObjectInBucket(final S3Client s3Client, final String bucket, final String s3name)
      throws DriverException, DriverNotFoundException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, s3name);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
    final var objectRequestExist =
        HeadObjectRequest.builder().bucket(bucket).key(s3name).checksumMode(ChecksumMode.ENABLED).build();
    var status = 500;
    try {
      final var response = s3Client.headObject(objectRequestExist);
      if (response.sdkHttpResponse().isSuccessful()) {
        return fromS3Head(s3Client, bucket, s3name, response);
      }
      status = response.sdkHttpResponse().statusCode();
    } catch (final NoSuchBucketException e) {
      throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket, e);
    } catch (final NoSuchKeyException e) {
      throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + s3name, e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
    // Not found
    throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + ", code: " + status + FOR + bucket + ":" + s3name);
  }

  StorageObject fromS3Head(final S3Client s3Client, final String bucket, final String name,
                           final HeadObjectResponse response) throws DriverException {
    LOGGER.debugf("DEBUG %s/s %s %d %s %s", bucket, name, response.checksumSHA256(), response.contentLength(),
        response.lastModified(), response.metadata());
    final String sha;
    try {
      final var map = new HashMap<>(response.metadata());
      if (map.containsKey(SHA_256)) {
        sha = map.remove(SHA_256);
      } else {
        final var getObjectTaggingResponse =
            s3Client.getObjectTagging(GetObjectTaggingRequest.builder().bucket(bucket).key(name).build());
        if (!getObjectTaggingResponse.sdkHttpResponse().isSuccessful()) {
          sha = null;
        } else {
          final var tag = getObjectTaggingResponse.tagSet().stream().filter(t -> t.key().equals(SHA_256)).findFirst();
          sha = tag.map(Tag::value).orElse(null);
        }
      }
      return new StorageObject(bucket, name, sha, response.contentLength(), response.lastModified(), response.expires(),
          map);
    } catch (final NoSuchBucketException | NoSuchKeyException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  InputStream getObjectBodyInBucket(final S3Client s3Client, final String bucket, final String s3name,
                                    final boolean checkExistence)
      throws DriverNotFoundException, DriverException { // NOSONAR Exception details
    if (checkExistence && !existDirectoryOrObjectInBucket(s3Client, bucket, s3name).equals(StorageType.OBJECT)) {
      // Not found
      if (!existBucket(s3Client, bucket)) {
        throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
      }
      throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + s3name);
    }
    try {
      return s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(s3name).build());
    } catch (final NoSuchKeyException | NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  StorageType existDirectoryOrObjectInBucket(final S3Client s3Client, final String bucket,
                                             final String directoryOrObject) throws DriverException {
    try {
      // First try for an Object then for Directory
      if (existObjectInBucket(s3Client, bucket, directoryOrObject)) {
        return StorageType.OBJECT;
      }
      final var iterator = getObjectsIteratorFilteredInBucket(s3Client, bucket, directoryOrObject, null, null);
      var storageType = iterator.hasNext() ? StorageType.DIRECTORY : StorageType.NONE;
      SystemTools.consumeAll(iterator);
      return storageType;
    } catch (final DriverNotFoundException | NoSuchBucketException | NoSuchKeyException e) {
      return StorageType.NONE;
    } catch (final RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new DriverException(e.getMessage(), e);
    }
  }

  boolean existObjectInBucket(final S3Client s3Client, final String bucket, final String s3name)
      throws DriverException {
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, bucket, s3name);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
    final var objectRequestExist = HeadObjectRequest.builder().bucket(bucket).key(s3name).build();
    try {
      return s3Client.headObject(objectRequestExist).sdkHttpResponse().isSuccessful();
    } catch (final RuntimeException ignored) {
      return false;
    }
  }

  StorageObject objectCopyToAnother(final S3Client s3Client, final StorageObject source, final StorageObject target)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException { // NOSONAR Exception details
    try {
      ParametersChecker.checkParameter("Source and Target cannot be null", source, target);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      throw new DriverException(e);
    }
    final var builder = CopyObjectRequest.builder().sourceBucket(source.bucket()).sourceKey(source.name())
        .destinationBucket(target.bucket()).destinationKey(target.name());
    if (target.expiresDate() != null) {
      builder.expires(target.expiresDate());
    }
    final Map<String, String> map = HashMap.newHashMap(1);
    if (target.metadata() != null) {
      map.putAll(target.metadata());
    }
    if (ParametersChecker.isNotEmpty(source.hash())) {
      map.put(SHA_256, source.hash());
    }
    LOGGER.debugf("Metadata %s", map);
    if (!map.isEmpty()) {
      builder.metadata(map).metadataDirective(MetadataDirective.REPLACE);
    } else {
      builder.metadataDirective(MetadataDirective.COPY);
    }
    try {
      final var response = s3Client.copyObject(builder.build());
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new DriverNotAcceptableException(
            OBJECT_CANNOT_BE_CREATED_CODE + response.sdkHttpResponse().statusCode() + FOR + target.bucket() + ":" +
                target.name());
      }
      return waitUntilObjectExist(s3Client, target.bucket(), target.name(), null);
    } catch (final NoSuchKeyException | NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  void deleteObjectInBucket(final S3Client s3Client, final String bucket, final String s3name)
      throws DriverNotFoundException, DriverNotAcceptableException, DriverException { // NOSONAR Exception details
    if (!existDirectoryOrObjectInBucket(s3Client, bucket, s3name).equals(StorageType.OBJECT)) {
      // Not found
      if (!existBucket(s3Client, bucket)) {
        throw new DriverNotFoundException(BUCKET_DOES_NOT_EXIST + bucket);
      }
      throw new DriverNotFoundException(OBJECT_DOES_NOT_EXIST + bucket + ":" + s3name);
    }
    try {
      if (!s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(s3name).build()).sdkHttpResponse()
          .isSuccessful()) {
        // KO
        throw new DriverNotAcceptableException("Cannot delete Object in Bucket: " + bucket + ":" + s3name);
      }
    } catch (final NoSuchKeyException | NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private StorageObject checkExistenceWithRetry(final S3Client s3Client, final String bucket, final String s3name)
      throws DriverException {
    StorageObject storageObject = null;
    DriverNotFoundException notFoundException = null;
    for (int i = 0; i < 3; i++) {
      SystemTools.wait1ms();
      try {
        storageObject = getObjectInBucket(s3Client, bucket, s3name);
        break;
      } catch (final DriverNotFoundException e) {
        SystemTools.wait1ms();
        notFoundException = e;
      }
    }
    if (storageObject == null) {
      if (notFoundException != null) {
        throw notFoundException;
      } else {
        throw new DriverNotFoundException("Object not found");
      }
    }
    return storageObject;
  }

  StorageObject waitUntilObjectExist(final S3Client s3Client, final String bucket, final String s3name,
                                     final String sha256ForTag)
      throws DriverNotAcceptableException, DriverException { // NOSONAR Exception details
    try {
      StorageObject storageObject = checkExistenceWithRetry(s3Client, bucket, s3name);
      if (ParametersChecker.isNotEmpty(sha256ForTag)) {
        // Set the new Sha256 as Tag since object already created
        // Maybe use the copy to rewrite metadata instead of Tag
        setShaAsTagForObject(s3Client, bucket, s3name, sha256ForTag);
        return new StorageObject(storageObject.bucket(), storageObject.name(), sha256ForTag, storageObject.size(),
            storageObject.creationDate(), storageObject.expiresDate(), storageObject.metadata());
      }
      return storageObject;
    } catch (final NoSuchKeyException | NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private static void setShaAsTagForObject(final S3Client s3Client, final String bucket, final String s3name,
                                           final String sha256ForTag) {
    try {
      final var putObjectTaggingResponse = s3Client.putObjectTagging(
          PutObjectTaggingRequest.builder().bucket(bucket).key(s3name)
              .tagging(t -> t.tagSet(b -> b.key(SHA_256).value(sha256ForTag))).build());
      if (!putObjectTaggingResponse.sdkHttpResponse().isSuccessful()) {
        LOGGER.warn("Cannot set SHA256 tag");
      }
    } catch (final RuntimeException e) {
      LOGGER.warnf("Cannot tag object: %s (%s)", s3name, e);
    }
  }

  void createObjectInBucket(final S3Client s3Client, final StorageObject object, final InputStream inputStream)
      throws DriverException, DriverNotAcceptableException { // NOSONAR Exception details
    final RequestBody requestBody;
    try {
      ParametersChecker.checkParameter(BUCKET_OR_OBJECT_CANNOT_BE_NULL, object);
      if (object.size() > 0) {
        requestBody = RequestBody.fromInputStream(inputStream, object.size());
      } else {
        LOGGER.warn("Unknown length shall not be OK with upload");
        final ContentStreamProvider provider = () -> ReleasableInputStream.wrap(inputStream).disableClose();
        requestBody = RequestBody.fromContentProvider(provider, MediaType.APPLICATION_OCTET_STREAM);
      }
      final var builder = PutObjectRequest.builder().bucket(object.bucket()).key(object.name());
      if (object.expiresDate() != null) {
        builder.expires(object.expiresDate());
      }
      final Map<String, String> map = HashMap.newHashMap(1);
      if (object.metadata() != null) {
        map.putAll(object.metadata());
      }
      if (ParametersChecker.isNotEmpty(object.hash())) {
        map.put(SHA_256, object.hash());
      }
      LOGGER.debugf("Metadata %s", map);
      if (!map.isEmpty()) {
        builder.metadata(map);
      }
      final var response = s3Client.putObject(builder.build(), requestBody);
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new DriverNotAcceptableException(
            OBJECT_CANNOT_BE_CREATED_CODE + response.sdkHttpResponse().statusCode() + FOR + object.bucket() + ":" +
                object.name());
      }
      LOGGER.debugf("MAI %s", inputStream);
    } catch (final NoSuchKeyException | NoSuchBucketException e) {
      throw new DriverNotFoundException(e);
    } catch (final RuntimeException e) {
      throw new DriverException(e);
    }
  }

  private static class S3ObjectIterator implements Iterator<S3Object> {
    private final Iterator<S3Object> iterator;
    private final Instant start;
    private final Instant end;
    private S3Object s3Object;

    public S3ObjectIterator(final Iterator<S3Object> iterator, final Instant start, final Instant end) {
      this.iterator = iterator;
      this.start = start;
      this.end = end;
      s3Object = null;
    }

    private S3Object nextInternal() {
      while (iterator.hasNext()) {
        final var item = iterator.next();
        final var lastModified = item.lastModified();
        if ((start != null && start.isAfter(lastModified)) || (end != null && end.isBefore(lastModified))) {
          continue;
        }
        return item;
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      if (s3Object == null) {
        s3Object = nextInternal();
      }
      return s3Object != null;
    }

    @Override
    public S3Object next() {
      if (s3Object != null) {
        var temp = s3Object;
        s3Object = null;
        return temp;
      }
      throw new NoSuchElementException();
    }
  }
}
