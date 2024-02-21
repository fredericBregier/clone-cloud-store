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

package io.clonecloudstore.driver.azure;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.driver.azure.example.client.ApiClientFactory;
import io.clonecloudstore.test.resource.NoResourceProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(NoResourceProfile.class)
class DriverAzureNotConfiguredTest {
  @Inject
  DriverAzureHelper driverAzureHelper;
  static boolean bucketAlready = false;
  static int headBucket = 404;
  static int headObject = 400;

  @Test
  void noS3ConfiguredCheck() {
    assertEquals(DriverAzureProperties.DEFAULT_SIZE_NOT_PART, DriverAzureProperties.getMaxPartSize());
    assertEquals(10000000, DriverAzureProperties.getMaxPartSizeForUnknownLength());
    DriverAzureProperties.setDynamicPartSize(DriverAzureProperties.DEFAULT_MAX_SIZE_NOT_PART);
    assertEquals(DriverAzureProperties.DEFAULT_MAX_SIZE_NOT_PART, DriverAzureProperties.getMaxPartSize());
    DriverAzureProperties.setDynamicPartSizeForUnknownLength(10000000);
    assertEquals(10000000, DriverAzureProperties.getMaxPartSizeForUnknownLength());

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

      bucketAlready = false;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.createBucket(storageBucket));
      bucketAlready = true;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.createBucket(storageBucket));
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.getBuckets());

      headBucket = 200;
      bucketAlready = false;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.deleteBucket("bucket"));
      bucketAlready = true;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.deleteBucket("bucket"));

      assertThrows(IllegalArgumentException.class,
          () -> driverAzureHelper.getObjectsStreamFilteredInBucket("bucket", null, null, null));

      bucketAlready = false;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.getObjectInBucket("bucket", "name"));
      bucketAlready = true;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.getObjectInBucket("bucket", "name"));

      bucketAlready = false;
      headObject = 200;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.deleteObjectInBucket("bucket", "name"));
      bucketAlready = true;
      headBucket = 400;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.deleteObjectInBucket("bucket", "name"));
      headBucket = 200;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.deleteObjectInBucket("bucket", "name"));

      bucketAlready = false;
      StorageObject storageObject1 = new StorageObject("bucket", "name", "aaa", 10, null);
      assertThrows(IllegalArgumentException.class,
          () -> driverAzureHelper.objectPrepareCreateInBucket(storageObject1, new FakeInputStream(10)));
      bucketAlready = true;
      headBucket = 400;
      assertThrows(IllegalArgumentException.class,
          () -> driverAzureHelper.objectPrepareCreateInBucket(storageObject1, new FakeInputStream(10)));
      headBucket = 200;
      assertThrows(IllegalArgumentException.class,
          () -> driverAzureHelper.objectPrepareCreateInBucket(storageObject1, new FakeInputStream(10)));

      bucketAlready = false;
      BlobItem blobItem = new BlobItem().setName("name").setProperties(
          new BlobItemProperties().setContentLength(1000L).setCreationTime(Instant.now().atOffset(ZoneOffset.UTC))
              .setExpiryTime(Instant.now().atOffset(ZoneOffset.UTC))).setMetadata(new HashMap<>());
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.fromBlobItem("bucket", blobItem));
      bucketAlready = false;
      assertThrows(IllegalArgumentException.class, () -> driverAzureHelper.fromBlobItem("bucket", blobItem));
    }
    factory.close();
  }
}
