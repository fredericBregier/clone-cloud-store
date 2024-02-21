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

import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.exception.DriverRuntimeException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.driver.s3.example.client.ApiClientFactory;
import io.clonecloudstore.test.resource.NoResourceProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(NoResourceProfile.class)
class DriverS3NoS3ConfiguredTest {
  @Inject
  DriverS3Helper driverS3Helper;
  static boolean bucketAlready = false;
  static int headBucket = 404;
  static int headObject = 400;

  @Test
  void noS3WrongConfigurationCheck() {
    DriverS3Properties.setDynamicS3Parameters("http*localhost:8999:z", "AccessKey", "SecretKey", "Region");
    assertThrows(DriverRuntimeException.class, () -> driverS3Helper.getClient());
  }

  @Test
  void noS3ConfiguredCheck() {
    DriverS3Properties.setDynamicS3Parameters("http://localhost:8999", "AccessKey", "SecretKey", "Region");
    assertEquals("http://localhost:8999", DriverS3Properties.getDriverS3Host());
    assertEquals("AccessKey", DriverS3Properties.getDriverS3KeyId());
    assertEquals("SecretKey", DriverS3Properties.getDriverS3Key());
    assertEquals("Region", DriverS3Properties.getDriverS3Region());
    Assertions.assertEquals(DriverS3Properties.DEFAULT_SIZE_NOT_PART, DriverS3Properties.getMaxPartSize());
    assertEquals(10000000, DriverS3Properties.getMaxPartSizeForUnknownLength());
    DriverS3Properties.setDynamicPartSize(DriverS3Properties.DEFAULT_MAX_SIZE_NOT_PART);
    Assertions.assertEquals(DriverS3Properties.DEFAULT_MAX_SIZE_NOT_PART, DriverS3Properties.getMaxPartSize());
    DriverS3Properties.setDynamicPartSizeForUnknownLength(10000000);
    assertEquals(10000000, DriverS3Properties.getMaxPartSizeForUnknownLength());

    final var factory = new ApiClientFactory();
    try (final var driverApi = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(null, null, 0, null));
    }
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    final String sha = null;
    final long length = 0;

    final var storageBucket = new StorageBucket(bucket, "client", null);
    final var storageObject = new StorageObject(bucket, object1, sha, length, null);

    try (final var driverApi = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      assertThrows(DriverException.class, () -> driverApi.bucketsStream());
      assertThrows(DriverException.class, () -> driverApi.bucketsCount());
      assertThrows(DriverException.class, () -> driverApi.bucketExists(bucket));
      assertThrows(DriverException.class, () -> driverApi.bucketCreate(storageBucket));
      assertThrows(DriverException.class, () -> driverApi.directoryOrObjectExistsInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.objectsStreamInBucket(bucket));
      assertThrows(DriverException.class, () -> driverApi.objectsStreamInBucket(bucket, prefix, null, null));
      assertThrows(DriverException.class, () -> driverApi.objectsCountInBucket(bucket));
      assertThrows(DriverException.class, () -> driverApi.objectsCountInBucket(bucket, prefix, null, null));
      assertThrows(DriverException.class,
          () -> driverApi.objectPrepareCreateInBucket(storageObject, new FakeInputStream(length)));
      assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(bucket, object1, length, null));
      assertThrows(DriverException.class, () -> driverApi.objectGetInputStreamInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.objectGetMetadataInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.objectDeleteInBucket(bucket, object1));
      assertThrows(DriverException.class, () -> driverApi.bucketDelete(bucket));

      assertThrows(DriverException.class, () -> new MultipartUploadHelper(null, storageObject));
      try (final var client = driverS3Helper.getClient()) {
        assertThrows(DriverException.class, () -> new MultipartUploadHelper(client, storageObject));
      }

      bucketAlready = false;
      assertThrows(DriverNotAcceptableException.class,
          () -> driverS3Helper.createBucket(new S3ClientFake(), storageBucket));
      bucketAlready = true;
      assertThrows(DriverAlreadyExistException.class,
          () -> driverS3Helper.createBucket(new S3ClientFake(), storageBucket));
      assertThrows(DriverNotFoundException.class, () -> driverS3Helper.getBuckets(new S3ClientFake()));

      headBucket = 200;
      bucketAlready = false;
      assertThrows(DriverNotAcceptableException.class, () -> driverS3Helper.deleteBucket(new S3ClientFake(), "bucket"));
      bucketAlready = true;
      assertThrows(DriverNotFoundException.class, () -> driverS3Helper.deleteBucket(new S3ClientFake(), "bucket"));

      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.getObjectsStreamFilteredInBucket(new S3ClientFake(), "bucket", null, null, null));

      bucketAlready = false;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.getObjectInBucket(new S3ClientFake(), "bucket", "name"));
      bucketAlready = true;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.getObjectInBucket(new S3ClientFake(), "bucket", "name"));

      bucketAlready = false;
      headObject = 200;
      assertThrows(DriverNotAcceptableException.class,
          () -> driverS3Helper.deleteObjectInBucket(new S3ClientFake(), "bucket", "name"));
      bucketAlready = true;
      headBucket = 400;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.deleteObjectInBucket(new S3ClientFake(), "bucket", "name"));
      headBucket = 200;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.deleteObjectInBucket(new S3ClientFake(), "bucket", "name"));

      bucketAlready = false;
      StorageObject storageObject1 = new StorageObject("bucket", "name", "aaa", 10, null);
      assertThrows(DriverNotAcceptableException.class,
          () -> driverS3Helper.createObjectInBucket(new S3ClientFake(), storageObject1, new FakeInputStream(10)));
      bucketAlready = true;
      headBucket = 400;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.createObjectInBucket(new S3ClientFake(), storageObject1, new FakeInputStream(10)));
      headBucket = 200;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.createObjectInBucket(new S3ClientFake(), storageObject1, new FakeInputStream(10)));

      bucketAlready = false;
      var responseBuilder = HeadObjectResponse.builder();
      responseBuilder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
      var response = responseBuilder.build();
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.fromS3Head(new S3ClientFake(), "bucket", "name", response));
      bucketAlready = false;
      assertThrows(DriverNotFoundException.class,
          () -> driverS3Helper.fromS3Head(new S3ClientFake(), "bucket", "name", response));
    }
    factory.close();
  }

  private static class S3ClientFake implements S3Client {

    @Override
    public String serviceName() {
      return "s3";
    }

    @Override
    public void close() {
      // Empty
    }

    @Override
    public HeadBucketResponse headBucket(final HeadBucketRequest headBucketRequest)
        throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
      final var response = HeadBucketResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(headBucket).build());
      return response.build();
    }

    @Override
    public HeadObjectResponse headObject(final HeadObjectRequest headObjectRequest)
        throws NoSuchKeyException, AwsServiceException, SdkClientException, S3Exception {
      if (headObject == 200) {
        final var response = HeadObjectResponse.builder();
        response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
        return response.build();
      }
      if (bucketAlready) {
        throw NoSuchBucketException.builder().build();
      }
      final var response = HeadObjectResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(409).build());
      return response.build();
    }

    @Override
    public CreateBucketResponse createBucket(final CreateBucketRequest createBucketRequest)
        throws BucketAlreadyExistsException, BucketAlreadyOwnedByYouException, AwsServiceException, SdkClientException,
        S3Exception {
      if (bucketAlready) {
        throw BucketAlreadyExistsException.builder().build();
      }
      final var response = CreateBucketResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(409).build());
      return response.build();
    }

    @Override
    public DeleteBucketResponse deleteBucket(final DeleteBucketRequest deleteBucketRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
      if (bucketAlready) {
        throw NoSuchBucketException.builder().build();
      }
      final var response = DeleteBucketResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(409).build());
      return response.build();
    }

    @Override
    public DeleteObjectResponse deleteObject(final DeleteObjectRequest deleteObjectRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
      if (bucketAlready) {
        if (headBucket == 400) {
          throw NoSuchBucketException.builder().build();
        }
        throw NoSuchKeyException.builder().build();
      }
      final var response = DeleteObjectResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(409).build());
      return response.build();
    }

    @Override
    public GetObjectTaggingResponse getObjectTagging(final GetObjectTaggingRequest getObjectTaggingRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
      if (bucketAlready) {
        throw NoSuchBucketException.builder().build();
      }
      throw NoSuchKeyException.builder().build();
    }

    @Override
    public ListBucketsResponse listBuckets(final ListBucketsRequest listBucketsRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
      throw NoSuchBucketException.builder().build();
    }

    @Override
    public ListObjectsV2Iterable listObjectsV2Paginator(final ListObjectsV2Request listObjectsV2Request)
        throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
      throw NoSuchBucketException.builder().build();
    }

    @Override
    public PutObjectResponse putObject(final PutObjectRequest putObjectRequest, final RequestBody requestBody)
        throws AwsServiceException, SdkClientException, S3Exception {
      if (bucketAlready) {
        if (headBucket == 400) {
          throw NoSuchBucketException.builder().build();
        }
        throw NoSuchKeyException.builder().build();
      }
      final var response = PutObjectResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(409).build());
      return response.build();
    }
  }
}
