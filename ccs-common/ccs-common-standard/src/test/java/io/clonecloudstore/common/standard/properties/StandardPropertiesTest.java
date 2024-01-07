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

package io.clonecloudstore.common.standard.properties;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.impl.EventBusImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class StandardPropertiesTest {
  @Test
  void validProperties() throws JsonProcessingException {
    final var bufSize = StandardProperties.getBufSize();
    StandardProperties.setBufSize(bufSize + 1);
    assertEquals(bufSize + 1, StandardProperties.getBufSize());
    StandardProperties.setBufSize(bufSize);
    assertEquals(bufSize, StandardProperties.getBufSize());

    final var maxWaitMs = StandardProperties.getMaxWaitMs();
    StandardProperties.setMaxWaitMs(maxWaitMs + 1);
    assertEquals(maxWaitMs + 1, StandardProperties.getMaxWaitMs());
    StandardProperties.setMaxWaitMs(maxWaitMs);
    assertEquals(maxWaitMs, StandardProperties.getMaxWaitMs());

    final var vertx = StandardProperties.getVertx();
    assertNotNull(vertx);
    assertThrows(IllegalStateException.class, () -> ((EventBusImpl) vertx.eventBus()).start(null));
    final var vertx1 = Vertx.vertx();
    StandardProperties.setCdiVertx(vertx1);
    assertThrows(IllegalStateException.class, () -> ((EventBusImpl) vertx.eventBus()).start(null));

    final var guid = GuidLike.getGuid();
    StandardProperties.getObjectMapper().writeValueAsString(guid);
    Dto dto = new Dto("itemvalue", Instant.now().truncatedTo(ChronoUnit.MILLIS));
    String sdto = StandardProperties.getObjectMapper().writeValueAsString(dto);
    assertEquals("{\"item\":\"" + dto.item() + "\",\"instant\":\"" + dto.instant() + "\"}", sdto);
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> StandardProperties.setCdiObjectMapper(null));
    StandardProperties.setCdiObjectMapper(StandardProperties.getObjectMapper());
  }

  private record Dto(String item, Instant instant) {
    // Empty
  }
}
