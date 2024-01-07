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

package io.clonecloudstore.common.quarkus.properties;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JsonUtilNoQuarkusTest {
  @Test
  void testJsonUtil() throws JsonProcessingException {
    assertNotNull(JsonUtil.getInstance());
    assertNotNull(JsonUtil.getInstanceNoNull());
    Dto dto = new Dto("itemvalue", Instant.now().truncatedTo(ChronoUnit.MILLIS));
    String sdto = StandardProperties.getObjectMapper().writeValueAsString(dto);
    assertEquals("{\"item\":\"" + dto.item() + "\",\"instant\":\"" + dto.instant() + "\"}", sdto);
    assertNotEquals(JsonUtil.getInstance(), JsonUtil.getInstanceNoNull());
  }

  private record Dto(String item, Instant instant) {
    // Empty
  }
}
