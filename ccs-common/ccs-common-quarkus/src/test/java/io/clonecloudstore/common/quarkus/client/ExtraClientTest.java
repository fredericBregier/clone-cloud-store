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

package io.clonecloudstore.common.quarkus.client;

import java.net.URI;

import io.clonecloudstore.common.quarkus.client.example.ApiClientFactory;
import io.clonecloudstore.common.quarkus.client.example.SimpleApiClientFactory;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class ExtraClientTest {
  private static final Logger LOG = Logger.getLogger(ApiClientTest.class);

  @Inject
  ApiClientFactory factory;
  @Inject
  SimpleApiClientFactory simpleFactory;

  @Test
  void testClientAbstract() {
    final var uri = factory.getUri();
    try {
      try (final var client = factory.newClient(URI.create("http://127.0.0.1:8081/base"))) {
        assertEquals("http://127.0.0.1:8081/base", factory.getUri().toString().trim());
        assertThrows(Exception.class, () -> client.postInputStream("name", new FakeInputStream(10), 10));
        assertThrows(Exception.class, () -> client.getInputStreamBusinessOut("name", 0));
      }
      try (final var client = factory.newClient(URI.create("http://127.0.0.1:8081/"))) {
        assertThrows(Exception.class,
            () -> client.postInputStream("INTERRUPT", new FakeInputStream(1000 * StandardProperties.getBufSize()), 0));
        assertThrows(Exception.class,
            () -> client.getInputStreamBusinessOut("INTERRUPT", 1000 * StandardProperties.getBufSize()).inputStream()
                .transferTo(new VoidOutputStream()));
      }
      factory.prepare(false, "localhost", 8081, "/test");
      assertEquals(URI.create("http://localhost:8081/test"), factory.getUri());
    } finally {
      factory.prepare(uri);
    }
  }

  @Test
  void testSimpleClientAbstract() {
    final var uri = simpleFactory.getUri();
    try {
      try (final var client = simpleFactory.newClient(URI.create("http://127.0.0.1:8081/base"))) {
        assertEquals(URI.create("http://127.0.0.1:8081/base"), client.getUri());
      }
      try (final var client = simpleFactory.newClient(URI.create("http://127.0.0.1:8081/"))) {
        assertEquals(URI.create("http://127.0.0.1:8081/"), client.getUri());
      }
      simpleFactory.prepare(false, "127.0.0.1", 8081, "path");
      assertEquals(URI.create("http://127.0.0.1:8081/path"), simpleFactory.getUri());
      simpleFactory.prepare(URI.create("base"));
      assertEquals(URI.create("http://127.0.0.1:8081/base"), simpleFactory.getUri());
      simpleFactory.prepare(false, "localhost", 8081, "");
      assertEquals(URI.create("http://localhost:8081/"), simpleFactory.getUri());
    } finally {
      simpleFactory.prepare(uri);
    }
  }
}
