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

package io.clonecloudstore.driver.google;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.driver.google.example.client.ApiClientFactory;
import io.clonecloudstore.test.resource.NoResourceProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkiverse.googlecloudservices.common.GcpBootstrapConfiguration;
import io.quarkiverse.googlecloudservices.common.GcpConfigHolder;
import io.quarkiverse.googlecloudservices.storage.runtime.StorageConfiguration;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
@TestProfile(NoResourceProfile.class)
class DriverGoogleNotConfiguredTest {
  static boolean bucketAlready = false;
  static int headBucket = 404;
  static int headObject = 400;
  private static boolean setup = false;
  DriverGoogleHelper driverGoogleHelper;
  DriverApi driverApi;
  @Inject
  GoogleCredentials googleCredentials;
  @Inject
  GcpConfigHolder gcpConfigHolder;
  @Inject
  StorageConfiguration storageConfiguration;


  @BeforeEach
  void beforeAll() {
    if (setup) {
      return;
    }
    TransportOptions transportOptions =
        HttpTransportOptions.newBuilder().setConnectTimeout(100).setReadTimeout(500).build();
    GcpBootstrapConfiguration gcpConfiguration = gcpConfigHolder.getBootstrapConfig();
    StorageOptions.Builder builder = StorageOptions.newBuilder().setCredentials(googleCredentials)
        .setProjectId(gcpConfiguration.projectId().orElse(null));
    storageConfiguration.hostOverride.ifPresent(builder::setHost);
    RetrySettings retrySettings = RetrySettings.newBuilder().setInitialRetryDelay(Duration.ofMillis(100))
        .setInitialRpcTimeout(Duration.ofMillis(100)).setLogicalTimeout(Duration.ofMillis(100)).setMaxAttempts(1)
        .setMaxRpcTimeout(Duration.ofMillis(200)).setMaxRetryDelay(Duration.ofMillis(200))
        .setTotalTimeout(Duration.ofMillis(500)).build();
    Storage storage =
        builder.setTransportOptions(transportOptions).setRetrySettings(retrySettings).build().getService();
    driverGoogleHelper = new DriverGoogleHelper(storage);
    driverApi = new DriverGoogle(driverGoogleHelper);
  }

  @Test
  void noS3ConfiguredCheck() {
    assertEquals(DriverGoogleProperties.DEFAULT_SIZE_NOT_PART, DriverGoogleProperties.getMaxPartSize());
    assertEquals(134217728, DriverGoogleProperties.getMaxBufSize());
    DriverGoogleProperties.setDynamicPartSize(DriverGoogleProperties.DEFAULT_MAX_SIZE_NOT_PART);
    assertEquals(DriverGoogleProperties.DEFAULT_MAX_SIZE_NOT_PART, DriverGoogleProperties.getMaxPartSize());
    DriverGoogleProperties.setDynamicBufSize(10000000);
    assertEquals(10000000, DriverGoogleProperties.getMaxBufSize());

    final var factory = new ApiClientFactory();
    assertThrows(DriverException.class, () -> driverApi.objectFinalizeCreateInBucket(null, null, 0, null));
    final var bucket = "test1";
    final var object1 = "dir/object1";
    final var prefix = "dir/";
    final String sha = null;
    final long length = 0;

    final var storageBucket = new StorageBucket(bucket, null);
    final var storageObject = new StorageObject(bucket, object1, sha, length, null);

    assertThrows(DriverException.class, () -> driverApi.bucketsStream());
    assertThrows(DriverException.class, () -> driverApi.bucketsIterator());
    assertThrows(DriverException.class, () -> driverApi.bucketsCount());
    assertThrows(DriverException.class, () -> driverApi.bucketExists(bucket));
    assertThrows(DriverException.class, () -> driverApi.bucketCreate(storageBucket));
    assertThrows(DriverException.class, () -> driverApi.directoryOrObjectExistsInBucket(bucket, object1));
    assertThrows(DriverException.class, () -> driverApi.objectsStreamInBucket(bucket));
    assertThrows(DriverException.class, () -> driverApi.objectsStreamInBucket(bucket, prefix, null, null));
    assertThrows(DriverException.class, () -> driverApi.objectsIteratorInBucket(bucket));
    assertThrows(DriverException.class, () -> driverApi.objectsIteratorInBucket(bucket, prefix, null, null));
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
    assertThrows(DriverException.class, () -> driverGoogleHelper.createBucket(storageBucket));
    bucketAlready = true;
    assertThrows(DriverException.class, () -> driverGoogleHelper.createBucket(storageBucket));
    assertThrows(DriverException.class, () -> driverGoogleHelper.getBuckets());

    headBucket = 200;
    bucketAlready = false;
    assertThrows(DriverException.class, () -> driverGoogleHelper.deleteBucket("bucket"));
    bucketAlready = true;
    assertThrows(DriverException.class, () -> driverGoogleHelper.deleteBucket("bucket"));

    assertThrows(DriverException.class,
        () -> driverGoogleHelper.getObjectsStreamFilteredInBucket("bucket", null, null, null));

    bucketAlready = false;
    assertThrows(DriverException.class, () -> driverGoogleHelper.getObjectInBucket("bucket", "name"));
    bucketAlready = true;
    assertThrows(DriverException.class, () -> driverGoogleHelper.getObjectInBucket("bucket", "name"));

    bucketAlready = false;
    headObject = 200;
    assertThrows(DriverException.class, () -> driverGoogleHelper.deleteObjectInBucket("bucket", "name"));
    bucketAlready = true;
    headBucket = 400;
    assertThrows(DriverException.class, () -> driverGoogleHelper.deleteObjectInBucket("bucket", "name"));
    headBucket = 200;
    assertThrows(DriverException.class, () -> driverGoogleHelper.deleteObjectInBucket("bucket", "name"));

    bucketAlready = false;
    StorageObject storageObject1 = new StorageObject("bucket", "name", "aaa", 10, null);
    assertThrows(DriverException.class,
        () -> driverGoogleHelper.objectPrepareCreateInBucket(storageObject1, new FakeInputStream(10)));
    bucketAlready = true;
    headBucket = 400;
    assertThrows(DriverException.class,
        () -> driverGoogleHelper.objectPrepareCreateInBucket(storageObject1, new FakeInputStream(10)));
    headBucket = 200;
    assertThrows(DriverException.class,
        () -> driverGoogleHelper.objectPrepareCreateInBucket(storageObject1, new FakeInputStream(10)));
    factory.close();
  }

}
