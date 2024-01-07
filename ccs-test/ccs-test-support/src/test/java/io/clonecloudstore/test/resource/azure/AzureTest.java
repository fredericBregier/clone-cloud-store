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

package io.clonecloudstore.test.resource.azure;

import com.azure.storage.blob.BlobServiceClient;
import io.clonecloudstore.test.resource.ResourcesConstants;
import io.clonecloudstore.test.resource.s3.EmptyClass;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@TestProfile(AzureProfile.class)
class AzureTest {
  @Inject
  BlobServiceClient blobServiceClient;

  @Test
  void checkConfiguration() {
    assertNotNull(AzureResource.getConnectionString());
    assertEquals(AzureResource.getConnectionString(),
        ConfigProvider.getConfig().getValue(ResourcesConstants.QUARKUS_AZURE_CONNECTION_STRING, String.class));
  }

  @Test
  void azureCheck() {
    new EmptyClass();
    final var bucket = "namebucket";
    final var blob = "nameblob";
    var containerClient = blobServiceClient.createBlobContainerIfNotExists(bucket);
    var blobClient = containerClient.getBlobClient(blob);
    assertDoesNotThrow(() -> blobClient.upload(new FakeInputStream(100)));
    var output = new VoidOutputStream();
    assertDoesNotThrow(() -> blobClient.downloadStream(output));
    assertEquals(100, output.getSize());
    assertDoesNotThrow(() -> blobClient.delete());
    assertDoesNotThrow(() -> containerClient.delete());
  }
}
