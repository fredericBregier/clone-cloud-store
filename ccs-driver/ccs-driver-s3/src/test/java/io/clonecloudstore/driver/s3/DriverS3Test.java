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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;

import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.exception.DriverRuntimeException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectSpy;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class DriverS3Test {
  private static final int chunk = 5 * 1024 * 1024;
  private static final int lenBig = 20 * 1024 * 1024;
  @InjectSpy
  DriverS3Helper driverS3Helper;
  @Inject
  DriverS3ApiFactory factory;
  static int status = 200;
  static int uploadStatus = 0;

  @Test
  void checkSpecialErrorCases() throws DriverException, NoSuchAlgorithmException, IOException {
    // Given
    Mockito.doReturn(new S3ClientFake()).when(driverS3Helper).getClient();
    Mockito.doReturn(Stream.of(S3Object.builder().build())).when(driverS3Helper)
        .getObjectsStreamFilteredInBucket(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.doThrow(DriverNotFoundException.class).when(driverS3Helper)
        .fromS3Object(Mockito.any(), Mockito.any(), Mockito.any());

    // Then error in the middle of object listing after object found
    try (final DriverApi driver = factory.getInstance()) {
      var exc = assertThrows(DriverRuntimeException.class, () -> driver.objectsStreamInBucket("bucket").findFirst());
      assertInstanceOf(DriverNotFoundException.class, exc.getCause());
    }

    // Given
    Mockito.doReturn(true).when(driverS3Helper).existBucket(Mockito.any(), Mockito.any());
    Mockito.doReturn(false).when(driverS3Helper).existObjectInBucket(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.doReturn(new S3ClientFake()).when(driverS3Helper).getClient();
    final var old = DriverS3Properties.getMaxPartSize();
    final var oldUnknown = DriverS3Properties.getMaxPartSizeForUnknownLength();
    try {
      DriverS3Properties.setDynamicPartSize(chunk);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(chunk);
      final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(lenBig), DigestAlgo.SHA256);
      FakeInputStream.consumeAll(digestInputStream);
      final var sha = digestInputStream.getDigestBase32();

      // Then Object upload in error during last step upload (multipart)
      try (final DriverApi driver = factory.getInstance()) {
        final var storageObject = new StorageObject("bucket", "object1", sha, lenBig, null);
        status = 200;
        assertThrows(DriverException.class,
            () -> driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(lenBig)));
        assertThrows(DriverException.class,
            () -> driver.objectFinalizeCreateInBucket("bucket", "object1", lenBig, sha));
      }
      // Then Object upload in error during last step upload (multipart)
      try (final DriverApi driver = factory.getInstance()) {
        final var storageObject = new StorageObject("bucket", "object1", sha, lenBig, null);
        status = 200;
        uploadStatus = 404;
        assertThrows(DriverException.class,
            () -> driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(lenBig)));
        assertThrows(DriverException.class,
            () -> driver.objectFinalizeCreateInBucket("bucket", "object1", lenBig, sha));
      }
      // Then Object upload in error during last step upload (monopart)
      Mockito.doThrow(DriverException.class).when(driverS3Helper)
          .createObjectInBucket(Mockito.any(), Mockito.any(), Mockito.any());
      try (final DriverApi driver = factory.getInstance()) {
        final var storageObject = new StorageObject("bucket", "object1", sha, 100, null);
        status = 200;
        assertThrows(DriverException.class,
            () -> driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(100)));
        assertThrows(DriverException.class, () -> driver.objectFinalizeCreateInBucket("bucket", "object1", 100, sha));
      }
    } finally {
      DriverS3Properties.setDynamicPartSize(old);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }

    // Given
    Mockito.doReturn(true).when(driverS3Helper).existBucket(Mockito.any(), Mockito.any());
    Mockito.doReturn(false).when(driverS3Helper).existObjectInBucket(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.doReturn(new S3ClientFake()).when(driverS3Helper).getClient();
    try {
      DriverS3Properties.setDynamicPartSize(chunk);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(chunk);
      final var digestInputStream = new MultipleActionsInputStream(new FakeInputStream(lenBig), DigestAlgo.SHA256);
      FakeInputStream.consumeAll(digestInputStream);
      final var sha = digestInputStream.getDigestBase32();

      // Then Object upload in error during multipart startup
      try (final DriverApi driver = factory.getInstance()) {
        final var storageObject = new StorageObject("bucket", "object1", sha, lenBig, null);
        status = 404;
        assertThrows(DriverException.class,
            () -> driver.objectPrepareCreateInBucket(storageObject, new FakeInputStream(lenBig)));
        assertThrows(DriverException.class,
            () -> driver.objectFinalizeCreateInBucket("bucket", "object1", lenBig, sha));
      }
    } finally {
      DriverS3Properties.setDynamicPartSize(old);
      DriverS3Properties.setDynamicPartSizeForUnknownLength(oldUnknown);
    }

    // Given
    Mockito.doThrow(NoSuchBucketException.class).when(driverS3Helper)
        .getObjectBodyInBucket(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    Mockito.doReturn(new S3ClientFake()).when(driverS3Helper).getClient();

    // Then Bucket not found
    try (final DriverApi driver = factory.getInstance()) {
      assertThrows(DriverNotFoundException.class, () -> driver.objectGetInputStreamInBucket("bucket", "object1"));
    }

    // Given
    Mockito.doThrow(NoSuchKeyException.class).when(driverS3Helper)
        .getObjectBodyInBucket(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    Mockito.doReturn(new S3ClientFake()).when(driverS3Helper).getClient();

    // Then Object not found
    try (final DriverApi driver = factory.getInstance()) {
      assertThrows(DriverNotFoundException.class, () -> driver.objectGetInputStreamInBucket("bucket", "object1"));
    }

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
    public CreateMultipartUploadResponse createMultipartUpload(
        final CreateMultipartUploadRequest createMultipartUploadRequest)
        throws AwsServiceException, SdkClientException, S3Exception {
      final var response = CreateMultipartUploadResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(status).build());
      response.uploadId("id");
      return response.build();
    }

    @Override
    public UploadPartResponse uploadPart(final UploadPartRequest uploadPartRequest, final RequestBody requestBody)
        throws AwsServiceException, SdkClientException, S3Exception {
      if (uploadStatus == 0) {
        throw SdkClientException.create("failed");
      }
      final var response = UploadPartResponse.builder();
      response.sdkHttpResponse(SdkHttpResponse.builder().statusCode(uploadStatus).build());
      return response.build();
    }
  }
}
