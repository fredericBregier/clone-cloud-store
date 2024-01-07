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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.quarkus.arc.Arc;
import io.quarkus.arc.Unremovable;
import io.quarkus.jackson.ObjectMapperCustomizer;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * Utility class for Json to get the Jackson ObjectMapper, either from CDI of Quarkus, or from
 * scratch if not in CDI (but should, in that case a Warning will be raised)
 */
@ApplicationScoped
@Startup
@Unremovable
public final class JsonUtil {
  private static final Logger LOGGER = Logger.getLogger(JsonUtil.class);
  @Unremovable
  private static final JsonUtil jsonHandler = new JsonUtil();
  @Unremovable
  private ObjectMapper mapper;
  @Unremovable
  private ObjectMapper mapperNoNull;

  JsonUtil() {
    startup(null);
  }

  @Inject
  JsonUtil(final ObjectMapper objectMapper) {
    this.mapper = objectMapper;
    setupCcsProperties();
  }

  void startup(@Observes final StartupEvent event) { // NOSONAR intentional
    ObjectMapper objectMapper = null;
    if (Arc.container() != null) {
      objectMapper = Arc.container().instance(ObjectMapper.class).get();
    }
    if (objectMapper == null) {
      LOGGER.warn("Cannot initialize ObjectMapper using Default Arc instance");
      objectMapper = StandardProperties.getObjectMapper();
    }
    mapper = objectMapper;
    setupCcsProperties();
  }

  private void setupCcsProperties() {
    mapperNoNull = mapper.copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    StandardProperties.setCdiObjectMapper(mapper);
  }

  /**
   * @return an ObjectMapper, either the one in the Quarkus CDI or a from scratch if none exists yet
   */
  @Unremovable
  public static ObjectMapper getInstance() {
    return jsonHandler.mapper;
  }

  /**
   * @return an ObjectMapper with No Null field serialization
   */
  @Unremovable
  public static ObjectMapper getInstanceNoNull() {
    return jsonHandler.mapperNoNull;
  }

  @Singleton
  public static class RegisterEmptyModuleCustomizer implements ObjectMapperCustomizer {

    @Override
    public void customize(final ObjectMapper objectMapper) {
      // nothing
    }
  }
}
