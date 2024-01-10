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

package io.clonecloudstore.accessor.apache.client;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.clonecloudstore.accessor.apache.client.fakeserver.FakeObjectService;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class AccessorObjectResourceTest {
  AccessorApiFactory factory;
  private static final String clientId = UUID.randomUUID().toString();

  @BeforeEach
  void beforeEach() {
    factory = new AccessorApiFactory("http://127.0.0.1:8081", clientId);
  }

  @AfterEach
  void afterEach() {
    factory.close();
  }

  @Test
  void invalidApi() {
    FakeObjectService.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkObjectOrDirectory("bucket", "objectname"));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getObjectInfo("bucket", "objectName");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getObject("bucket", "objectName", false);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", null);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", new AccessorFilter().setNamePrefix("prefix"));
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket("bucket").setName("objectName").setSize(100);
      client.createObject(accessorObject, new FakeInputStream(100), false);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode && e.getStatus() != 400) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket("bucket").setName("objectName");
      client.createObject(accessorObject, new FakeInputStream(100), true);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode && e.getStatus() != 400) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.deleteObject("bucket", "objectName");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var filter = new AccessorFilter().setNamePrefix("object");
      client.listObjects("bucket", filter);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    FakeObjectService.errorCode = 406;
    try (final var client = factory.newClient()) {
      client.deleteObject("bucket", "objectName");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    FakeObjectService.errorCode = 410;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkObjectOrDirectory("bucket", "objectname"));
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getObjectInfo("bucket", "objectName");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getObject("bucket", "objectName", false);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", null);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      var map = new HashMap<String, String>();
      map.put("key1", "value1");
      client.listObjects("bucket",
          new AccessorFilter().setNamePrefix("prefix").setExpiresBefore(Instant.MAX).setExpiresAfter(Instant.MIN)
              .setCreationAfter(Instant.MIN).setCreationBefore(Instant.MAX).setSizeGreaterThan(1).setSizeLessThan(10000)
              .setStatuses(new AccessorStatus[]{AccessorStatus.READY}).setMetadataFilter(map));
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket("bucket").setName("objectName").setSize(100);
      client.createObject(accessorObject, new FakeInputStream(100), false);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode && e.getStatus() != 400) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket("bucket").setName("objectName");
      client.createObject(accessorObject, new FakeInputStream(100), true);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode && e.getStatus() != 400) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.deleteObject("bucket", "objectName");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      final var filter = new AccessorFilter().setNamePrefix("object");
      client.listObjects("bucket", filter);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
    FakeObjectService.errorCode = 409;
    try (final var client = factory.newClient()) {
      client.deleteObject("bucket", "objectName");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != FakeObjectService.errorCode) {
        Log.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void validApi() {
    FakeObjectService.errorCode = 0;
    FakeObjectService.length = 100;
    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.OBJECT, client.checkObjectOrDirectory("bucket", "objectname"));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getObjectInfo("bucket", "objectName");
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var result = client.getObject("bucket", "objectName", false);
      assertEquals(FakeObjectService.length, result.inputStream().transferTo(new VoidOutputStream()));
    } catch (final CcsWithStatusException | IOException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var result = client.getObject("bucket", "objectName", true);
      assertEquals(FakeObjectService.length, result.inputStream().transferTo(new VoidOutputStream()));
    } catch (final CcsWithStatusException | IOException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var accessorObject =
          new AccessorObject().setBucket("bucket").setName("objectName").setSize(FakeObjectService.length)
              .setMetadata(map).setExpires(Instant.MAX).setHash("hash");
      client.createObject(accessorObject, new FakeInputStream(FakeObjectService.length), false);
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var accessorObject = new AccessorObject().setBucket("bucket").setName("objectName");
      client.createObject(accessorObject, new FakeInputStream(FakeObjectService.length), true);
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      var iterable = client.listObjects("bucket", null);
      int cpt = 0;
      while (iterable.hasNext()) {
        iterable.next();
        cpt++;
      }
      assertEquals(1, cpt);
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var result = client.listObjects("bucket", new AccessorFilter().setNamePrefix("object"));
      assertEquals(1, SystemTools.consumeAll(result));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.deleteObject("bucket", "objectName");
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var filter = new AccessorFilter().setNamePrefix("object");
      final var result = client.listObjects("bucket", filter);
      assertEquals(1, SystemTools.consumeAll(result));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
  }

  @Test
  void validApiBiFile() {
    FakeObjectService.errorCode = 0;
    FakeObjectService.length = 256 * 1024 * 1024;
    try (final var client = factory.newClient()) {
      var start = System.nanoTime();
      final var result = client.getObject("bucket", "objectName");
      assertEquals(FakeObjectService.length, FakeInputStream.consumeAll(result.inputStream()));
      var stop = System.nanoTime();
      Log.infof("Speed DOWN NoZstd In %f",
          FakeObjectService.length / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
    } catch (final CcsWithStatusException | IOException e) {
      Log.warn(e, e);
      fail(e);
    }
    InputStream inputStream = null;
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var result = client.getObject("bucket", "objectName");
      inputStream = result.inputStream();
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try {
      assertEquals(FakeObjectService.length, FakeInputStream.consumeAll(inputStream));
    } catch (IOException e) {
      Log.warn(e, e);
      fail(e);
    }
    var stop = System.nanoTime();
    Log.infof("Speed DOWN NoZstd Out %f", FakeObjectService.length / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var result = client.getObject("bucket", "objectName", true);
      inputStream = result.inputStream();
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try {
      assertEquals(FakeObjectService.length, FakeInputStream.consumeAll(inputStream));
    } catch (IOException e) {
      Log.warn(e, e);
      fail(e);
    }
    stop = System.nanoTime();
    Log.infof("Speed DOWN Zstd Out %f", FakeObjectService.length / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
    try (final var client = factory.newClient()) {
      final var accessorObject =
          new AccessorObject().setBucket("bucket").setName("objectName").setSize(FakeObjectService.length);
      start = System.nanoTime();
      client.createObject(accessorObject, new FakeInputStream(FakeObjectService.length));
      stop = System.nanoTime();
      Log.infof("Speed UPL NoZstd %f", FakeObjectService.length / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      start = System.nanoTime();
      final var accessorObject = new AccessorObject().setBucket("bucket").setName("objectName");
      client.createObject(accessorObject, new FakeInputStream(FakeObjectService.length), true);
      stop = System.nanoTime();
      Log.infof("Speed UPL Zstd %f", FakeObjectService.length / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
  }
}
