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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.spi.CDI;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@QuarkusTest
class JsonUtilTest {
  @Test
  void testJsonUtil() throws JsonProcessingException {
    Dto dto = new Dto("itemvalue", Instant.now().truncatedTo(ChronoUnit.MILLIS));
    String sdto = StandardProperties.getObjectMapper().writeValueAsString(dto);
    assertEquals("{\"item\":\"" + dto.item() + "\",\"instant\":\"" + dto.instant() + "\"}", sdto);
    assertEquals(CDI.current().select(ObjectMapper.class).get(), JsonUtil.getInstance());
    assertNotEquals(CDI.current().select(ObjectMapper.class).get(), JsonUtil.getInstanceNoNull());
  }

  private record Dto(String item, Instant instant) {
    // Empty
  }
}
