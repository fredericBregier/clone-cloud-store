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

package io.clonecloudstore.test.resource.google;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import io.clonecloudstore.test.resource.ResourcesConstants;
import io.clonecloudstore.test.resource.s3.EmptyClass;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(GoogleProfile.class)
class GoogleTest {
  @Inject
  Storage storage;

  @Test
  void checkConfiguration() {
    assertNotNull(GoogleResource.getConnectionString());
    assertEquals(GoogleResource.getConnectionString(),
        ConfigProvider.getConfig().getValue(ResourcesConstants.QUARKUS_GOOGLE_HOST, String.class));
  }

  @Test
  void azureCheck() {
    new EmptyClass();
    final var bucket = "namebucket";
    final var blob = "nameblob";
    Bucket bucket1 = storage.get(bucket);
    if (bucket1 == null) {
      bucket1 = storage.create(BucketInfo.newBuilder(bucket).build());
    }
    assertTrue(bucket1.exists());
    Blob notFound = bucket1.get(blob);
    assertNull(notFound);
    bucket1.create(blob, new FakeInputStream(100));
    assertEquals(1, bucket1.list().streamAll().count());
    Blob blob1 = bucket1.get(blob);
    assertTrue(blob1.exists());
    var out = new VoidOutputStream();
    blob1.downloadTo(out);
    assertEquals(100, out.getSize());
    blob1.delete();
    assertEquals(0, bucket1.list().streamAll().count());
    assertTrue(bucket1.delete());
  }
}
