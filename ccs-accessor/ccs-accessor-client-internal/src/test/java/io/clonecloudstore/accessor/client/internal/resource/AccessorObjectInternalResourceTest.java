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

package io.clonecloudstore.accessor.client.internal.resource;

import java.io.IOException;

import io.clonecloudstore.accessor.client.internal.AccessorObjectInternalApiFactory;
import io.clonecloudstore.accessor.client.internal.resource.fakeserver.FakeObjectInternalService;
import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class AccessorObjectInternalResourceTest {
  @Inject
  AccessorObjectInternalApiFactory factory;

  @Test
  void invalidApiReplicator() {
    FakeObjectInternalService.errorCode = 404;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkObjectOrDirectory("bucket", "objectname", "clientid", false));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.NONE, client.checkObjectOrDirectory("bucket", "objectname", "clientid", true));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", "clientid", null);
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", "clientid", new AccessorFilter().setNamePrefix("prefix"));
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getObjectInfo("bucket", "objectName", "clientid");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
    try (final var client = factory.newClient()) {
      client.getObject("bucket", "objectName", "clientid");
      fail("Should failed");
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() != 404) {
        Log.warn(e, e);
        fail(e);
      }
    }
  }

  @Test
  void validApiReplicator() {
    FakeObjectInternalService.errorCode = 0;
    FakeObjectInternalService.length = 100;
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.OBJECT, client.checkObjectOrDirectory("bucket", "objectname", "clientid", false));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      assertEquals(StorageType.OBJECT, client.checkObjectOrDirectory("bucket", "objectname", "clientid", true));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.getObjectInfo("bucket", "objectName", "clientid");
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var result = client.getObject("bucket", "objectName", "clientid");
      assertEquals(FakeObjectInternalService.length, result.inputStream().transferTo(new VoidOutputStream()));
    } catch (final CcsWithStatusException | IOException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", "clientid", null);
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      client.listObjects("bucket", "clientid", new AccessorFilter().setNamePrefix("object"));
    } catch (final CcsWithStatusException e) {
      Log.warn(e, e);
      fail(e);
    }
  }
}
