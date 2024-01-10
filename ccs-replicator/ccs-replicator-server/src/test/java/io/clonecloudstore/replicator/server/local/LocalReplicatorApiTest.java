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

package io.clonecloudstore.replicator.server.local;

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
import io.clonecloudstore.replicator.client.LocalReplicatorApiClientFactory;
import io.clonecloudstore.replicator.server.local.application.LocalReplicatorService;
import io.clonecloudstore.replicator.server.remote.client.api.RemoteReplicatorApiService;
import io.clonecloudstore.replicator.server.test.fake.topology.FakeTopologyResource;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.driver.fake.FakeDriverFactory;
import io.clonecloudstore.test.resource.kafka.KafkaProfile;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.topology.client.TopologyApiClientFactory;
import io.clonecloudstore.topology.model.Topology;
import io.clonecloudstore.topology.model.TopologyStatus;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.replicator.server.test.conf.Constants.BUCKET_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.BUCKET_NAME;
import static io.clonecloudstore.replicator.server.test.conf.Constants.CLIENT_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OBJECT_PATH;
import static io.clonecloudstore.replicator.server.test.conf.Constants.OP_ID;
import static io.clonecloudstore.replicator.server.test.conf.Constants.TOPOLOGY_NAME;
import static io.clonecloudstore.replicator.server.test.conf.Constants.URI_SERVER;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(KafkaProfile.class)
class LocalReplicatorApiTest {
  private static final Logger LOGGER = Logger.getLogger(LocalReplicatorApiTest.class);
  @Inject
  LocalReplicatorApiClientFactory localReplicatorApiClientFactory;
  @Inject
  AccessorBucketApiFactory accessorBucketApiFactory;
  @Inject
  AccessorObjectApiFactory accessorObjectApiFactory;
  @Inject
  LocalReplicatorService localReplicatorService;
  @Inject
  RemoteReplicatorApiService remoteReplicatorApiService;
  @Inject
  TopologyApiClientFactory topologyApiClientFactory;
  @Inject
  DriverApiFactory driverFactory;

  @BeforeAll
  static void beforeAll() {
    final var topology = new Topology(TOPOLOGY_NAME, TOPOLOGY_NAME, URI_SERVER, TopologyStatus.UP);
    FakeTopologyResource.topology = topology;
    FakeDriverFactory.cleanUp();
    FakeCommonBucketResourceHelper.errorCode = 0;
    FakeCommonObjectResourceHelper.errorCode = 0;
  }

  @Test
  void testRemoteCheckAndAccess() throws InterruptedException {
    // First error return
    LOGGER.infof("Error as 400 to 500");
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      FakeCommonBucketResourceHelper.errorCode = 400;
      FakeCommonObjectResourceHelper.errorCode = 400;
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID)).getStatus());
      assertEquals(500,
          assertThrows(CcsWithStatusException.class, () -> client.getBucket(BUCKET_ID, CLIENT_ID, OP_ID)).getStatus());
      assertEquals(500, assertThrows(CcsWithStatusException.class,
          () -> client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID)).getStatus());
      assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, "", OP_ID)).getStatus();
    } finally {
      FakeCommonBucketResourceHelper.errorCode = 0;
      FakeCommonObjectResourceHelper.errorCode = 0;
    }
    // Second absent items
    LOGGER.infof("Error as 404");
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.NONE, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(BUCKET_ID, CLIENT_ID, OP_ID).response());
      assertEquals(StorageType.NONE, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, "", OP_ID)).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.NONE, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(BUCKET_ID, CLIENT_ID, OP_ID).response());
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID).response());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, "", OP_ID)).getStatus());
      assertFalse(localReplicatorService.getTopologies(null).isEmpty());
      assertFalse(localReplicatorService.getTopologies(AccessorProperties.getAccessorSite()).isEmpty());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
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
    remoteReplicatorApiService.invalidateCacheBucket();
    remoteReplicatorApiService.invalidateCacheObject();
    LOGGER.infof("OK with check");
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.BUCKET, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertEquals(BUCKET_ID, client.getBucket(BUCKET_ID, CLIENT_ID, OP_ID).response().getId());
      assertEquals(StorageType.OBJECT,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID).response());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.BUCKET, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertEquals(BUCKET_ID, client.getBucket(BUCKET_ID, CLIENT_ID, OP_ID).response().getId());
      assertEquals(StorageType.OBJECT,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID).response());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    LOGGER.infof("OK with read");
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      final var result = client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, "", OP_ID);
      assertEquals(digest, result.dtoOut().getHash());
      final var len = FakeInputStream.consumeAll(result.inputStream());
      assertEquals(120, len);
      LOGGER.infof("KO with unknown Check");
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH + "NotExist", true, CLIENT_ID, OP_ID).response());
    } catch (final CcsWithStatusException | IOException e) {
      fail(e);
    }
    topologyApiClientFactory.clearCache();
    remoteReplicatorApiService.invalidateCacheBucket();
    remoteReplicatorApiService.invalidateCacheObject();
    LOGGER.infof("KO with no topology available");
    FakeTopologyResource.topology = null;
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.NONE, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertThrows(CcsWithStatusException.class, () -> client.getBucket(BUCKET_ID, CLIENT_ID, OP_ID).response());
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID).response());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, "", OP_ID)).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
    try (final var client = localReplicatorApiClientFactory.newClient(URI.create(URI_SERVER))) {
      assertEquals(StorageType.NONE, client.checkBucket(BUCKET_ID, true, CLIENT_ID, OP_ID).response());
      assertEquals(StorageType.NONE,
          client.checkObjectOrDirectory(BUCKET_ID, OBJECT_PATH, true, CLIENT_ID, OP_ID).response());
      assertEquals(404, assertThrows(CcsWithStatusException.class,
          () -> client.readRemoteObject(BUCKET_ID, OBJECT_PATH, CLIENT_ID, "", OP_ID)).getStatus());
    } catch (final CcsWithStatusException e) {
      fail(e);
    }
  }

  @Test
  void getOpenAPI() {
    final var openAPI = given().get("/q/openapi").then().statusCode(200).extract().response().asString();
    Log.infof("OpenAPI: \n%s", openAPI);
  }
}
