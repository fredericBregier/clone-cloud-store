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

package io.clonecloudstore.replicator.server.remote;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import io.clonecloudstore.accessor.client.AccessorBucketApiFactory;
import io.clonecloudstore.accessor.client.AccessorObjectApiFactory;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.replicator.server.remote.client.RemoteReplicatorApiClientFactory;
import io.clonecloudstore.replicator.server.remote.client.api.RemoteReplicatorApiService;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.replicator.server.test.conf.Constants.BUCKET_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.BUCKET_NAME;
import static io.clonecloudstore.replicator.server.test.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OBJECT_PATH;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OP_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.URI_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(KafkaProfile.class)
class RemoteReplicatorApiCacheTest {
  private static final Logger LOGGER = Logger.getLogger(RemoteReplicatorApiCacheTest.class);
  @Inject
  RemoteReplicatorApiClientFactory remoteReplicatorApiClientFactory;
  @Inject
  AccessorBucketApiFactory accessorBucketApiFactory;
  @Inject
  AccessorObjectApiFactory accessorObjectApiFactory;
  @Inject
  RemoteReplicatorApiService remoteReplicatorApiService;
  @Inject
  DriverApiFactory driverFactory;

  @BeforeEach
  void beforeEach() {
    FakeDriverFactory.cleanUp();
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
  }

  @Test
  void testRemoteCheckAndAccessCache() throws InterruptedException {
    // First error return
    remoteReplicatorApiService.invalidateCacheBucket();
    remoteReplicatorApiService.invalidateCacheObject();
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      FakeCommonBucketResourceHelper.errorCode = 400;
      FakeCommonObjectResourceHelper.errorCode = 400;
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkBucketCache(BUCKET_ID, true, CLIENT_ID, OP_ID)).getStatus());
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectoryCache(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID)).getStatus());
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, OP_ID, 0)).getStatus());
    } finally {
      FakeCommonBucketResourceHelper.errorCode = 0;
      FakeCommonObjectResourceHelper.errorCode = 0;
    }
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      FakeCommonBucketResourceHelper.errorCode = 400;
      FakeCommonObjectResourceHelper.errorCode = 400;
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkBucketCache(BUCKET_ID, true, CLIENT_ID, OP_ID)).getStatus());
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectoryCache(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID)).getStatus());
      assertEquals(400, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, OP_ID, 0)).getStatus());
    } finally {
      FakeCommonBucketResourceHelper.errorCode = 0;
      FakeCommonObjectResourceHelper.errorCode = 0;
    }
    remoteReplicatorApiService.invalidateCacheBucket();
    remoteReplicatorApiService.invalidateCacheObject();
    // Second absent items
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.NONE, client.checkBucketCache(BUCKET_ID, true, CLIENT_ID, OP_ID));
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectoryCache(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID));
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, OP_ID, 0)).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.NONE, client.checkBucketCache(BUCKET_ID, true, CLIENT_ID, OP_ID));
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectoryCache(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID));
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, OP_ID, 0)).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    remoteReplicatorApiService.invalidateCacheBucket();
    remoteReplicatorApiService.invalidateCacheObject();
    // Now check with existing items
    String digest = null;
    try (final var accessorClient = accessorBucketApiFactory.newClient()) {
      accessorClient.createBucket(BUCKET_NAME, CLIENT_ID);
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var accessorClient = accessorObjectApiFactory.newClient();
         final var inputStream0 = new FakeInputStream(120L, (byte) 'A');
         final var digestInputStream = new MultipleActionsInputStream(inputStream0);
         final var inputStream = new FakeInputStream(120L, (byte) 'A')) {
      digestInputStream.computeDigest(DigestAlgo.SHA256);
      FakeInputStream.consumeAll(digestInputStream);
      digest = digestInputStream.getDigestBase32();
      final var accessorObject =
          new AccessorObject().setName(OBJECT_PATH).setSite(AccessorProperties.getAccessorSite()).setHash(digest)
              .setSize(120).setBucket(BUCKET_NAME);
      accessorClient.createObject(accessorObject, CLIENT_ID, inputStream);
      final var result = accessorClient.getObject(BUCKET_NAME, OBJECT_PATH, CLIENT_ID);
      assertEquals(digest, result.dtoOut().getHash());
      final var len = FakeInputStream.consumeAll(result.inputStream());
      assertEquals(120, len);
    } catch (final IOException | NoSuchAlgorithmException | CcsWithStatusException e) {
      fail(e);
    }
    try (final var driverClient = driverFactory.getInstance()) {
      assertTrue(driverClient.bucketExists(BUCKET_ID));
      assertEquals(StorageType.OBJECT, driverClient.directoryOrObjectExistsInBucket(BUCKET_ID, OBJECT_PATH));
      final var inputStream = driverClient.objectGetInputStreamInBucket(BUCKET_ID, OBJECT_PATH);
      final var len = FakeInputStream.consumeAll(inputStream);
      assertEquals(120, len);
    } catch (final DriverException | IOException e) {
      fail(e);
    }
    remoteReplicatorApiService.clearCacheBucket(URI.create(URI_SERVER), BUCKET_ID);
    remoteReplicatorApiService.clearCacheObject(URI.create(URI_SERVER), BUCKET_ID, OBJECT_PATH);
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.BUCKET, client.checkBucketCache(BUCKET_ID, true, CLIENT_ID, OP_ID));
      assertEquals(StorageType.OBJECT,
          client.checkObjectOrDirectoryCache(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      final var result = client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, OP_ID, 0);
      assertEquals(digest, result.dtoOut().getHash());
      final var len = FakeInputStream.consumeAll(result.inputStream());
      assertEquals(120, len);
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH + "NotExist", true, CLIENT_ID, OP_ID));
    } catch (final CcsWithStatusException | IOException e) {
      fail(e);
    }
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.BUCKET, client.checkBucketCache(BUCKET_ID, true, CLIENT_ID, OP_ID));
      assertEquals(StorageType.OBJECT,
          client.checkObjectOrDirectoryCache(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID));
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      final var result = client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, OP_ID, 0);
      assertEquals(digest, result.dtoOut().getHash());
      final var len = FakeInputStream.consumeAll(result.inputStream());
      assertEquals(120, len);
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH + "NotExist", true, CLIENT_ID, OP_ID));
    } catch (final CcsWithStatusException | IOException e) {
      fail(e);
    }
  }
}
