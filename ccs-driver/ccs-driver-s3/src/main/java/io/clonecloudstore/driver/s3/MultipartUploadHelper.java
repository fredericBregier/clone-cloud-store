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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageObject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import software.amazon.awssdk.core.io.ReleasableInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static io.clonecloudstore.driver.s3.DriverS3Properties.SHA_256;

/**
 * Multi part handler for S3
 */
class MultipartUploadHelper {
  private static final Logger LOGGER = Logger.getLogger(MultipartUploadHelper.class);

  private final S3Client s3Client;
  private final String bucket;
  private final String destinationKey;
  private final String uploadId;
  private final List<CompletedPart> parts = new ArrayList<>();

  MultipartUploadHelper(final S3Client s3Client, final StorageObject object) throws DriverException {
    this.s3Client = s3Client;
    this.bucket = object.bucket();
    this.destinationKey = object.name();
    try {
      final var builder = CreateMultipartUploadRequest.builder().bucket(object.bucket()).key(destinationKey);
      // Checksum not compatible with multipart (checksum of checksums) so cannot be compared
      final Map<String, String> map = HashMap.newHashMap(1);
      if (ParametersChecker.isNotEmpty(object.hash())) {
        map.put(SHA_256, object.hash());
      }
      if (object.metadata() != null) {
        map.putAll(object.metadata());
      }
      if (!map.isEmpty()) {
        LOGGER.debug("Will add Metadata SHA256");
        builder.metadata(map);
      }
      if (object.expiresDate() != null) {
        builder.expires(object.expiresDate());
      }
      LOGGER.debugf("Metadata %s", map);
      final var multipartUpload = s3Client.createMultipartUpload(builder.build());
      if (!multipartUpload.sdkHttpResponse().isSuccessful()) {
        throw new DriverException(
            "Start of S3 chunk is invalid, code: " + multipartUpload.sdkHttpResponse().statusCode());
      }
      uploadId = multipartUpload.uploadId();
    } catch (final RuntimeException e) {
      throw new DriverException("Start of S3 chunk is invalid", e);
    }
  }

  void partUpload(final InputStream inputStream, final long len) throws DriverException {
    try {
      final Integer partNumber = parts.size() + 1;
      final RequestBody requestBody;
      if (len >= 0) {
        requestBody = RequestBody.fromInputStream(inputStream, len);
      } else {
        LOGGER.warn("Unknown length shall not be OK with multipart");
        final ContentStreamProvider provider = () -> ReleasableInputStream.wrap(inputStream).disableClose();
        requestBody = RequestBody.fromContentProvider(provider, MediaType.APPLICATION_OCTET_STREAM);
      }
      final var uploadPartResponse = s3Client.uploadPart(
          UploadPartRequest.builder().bucket(bucket).key(destinationKey).uploadId(uploadId).partNumber(partNumber)
              .build(), requestBody);
      if (!uploadPartResponse.sdkHttpResponse().isSuccessful()) {
        throw new DriverException(
            "Sending one chunk is invalid, code: " + uploadPartResponse.sdkHttpResponse().statusCode());
      }
      parts.add(CompletedPart.builder().partNumber(partNumber).eTag(uploadPartResponse.eTag()).build());
    } catch (final RuntimeException e) {
      throw new DriverException("Sending one chunk is invalid: " + (parts.size() + 1), e);
    }
  }

  void cancel() throws DriverException {
    try {
      final var response = s3Client.abortMultipartUpload(
          AbortMultipartUploadRequest.builder().uploadId(uploadId).bucket(bucket).key(destinationKey).build());
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new DriverException("Cancelling upload is invalid, code: " + response.sdkHttpResponse().statusCode());
      }
    } catch (final RuntimeException e) {
      throw new DriverException("Cancelling upload is invalid", e);
    }
  }

  void complete() throws DriverException {
    try {
      final var request =
          ListPartsRequest.builder().bucket(bucket).key(destinationKey).uploadId(uploadId).maxParts(parts.size() + 1)
              .build();
      var responsePart = s3Client.listParts(request);
      while (responsePart.parts().size() != parts.size()) {
        Thread.yield();
        LOGGER.debugf("Redo count since %d != %d", responsePart.parts().size(), parts.size());
        responsePart = s3Client.listParts(request);
      }
      final var partList = responsePart.parts();
      checkResponseParts(partList);
      LOGGER.debug("Part are all uploaded");
      Thread.yield();
      final var completeMultipartUploadRequest =
          CompleteMultipartUploadRequest.builder().uploadId(uploadId).bucket(bucket).key(destinationKey)
              .multipartUpload(c -> c.parts(parts)).build();
      final var response = s3Client.completeMultipartUpload(completeMultipartUploadRequest);
      if (!response.sdkHttpResponse().isSuccessful()) {
        throw new DriverException("Completing upload is invalid, code: " + response.sdkHttpResponse().statusCode());
      }
      LOGGER.debugf("Multipart Upload complete with %d parts", parts.size());
      parts.clear();
    } catch (final RuntimeException e) {
      throw new DriverException("Completing upload is invalid", e);
    }
  }

  private void checkResponseParts(final List<Part> partList) throws DriverException {
    for (final var part1 : parts) {
      var found = false;
      for (final var part : partList) {
        if (part1.partNumber().equals(part.partNumber())) {
          if (part1.eTag().equals(part.eTag())) {
            found = true;
            break;
          } else {
            LOGGER.info("Recv but Wrong eTag: " + part.partNumber() + ' ' + part.eTag() + " vs " + part1.eTag() + ' ' +
                part.size() + ' ' + part.lastModified());
          }
        }
      }
      if (!found) {
        LOGGER.error("Completed Part not found: " + part1.partNumber() + ' ' + part1.eTag());
        throw new DriverException("Completed Part not found: " + part1.partNumber() + ' ' + part1.eTag());
      }
    }
  }
}
