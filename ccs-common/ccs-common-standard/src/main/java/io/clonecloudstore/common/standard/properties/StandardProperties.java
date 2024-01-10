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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.SystemPropertyUtil;
import io.vertx.core.Vertx;

public class StandardProperties {
  /**
   * Compatible with 64KB, 96 KB and 128 KB buffer sizes (Default 384 KB)
   */
  public static final int DEFAULT_PIPED_BUFFER_SIZE = 384 * 1024;
  /**
   * Internal Machine Id used by JvmProcessMacIds for MacAddress (null or empty meaning using default value)
   */
  public static final String CCS_MACHINE_ID = "ccs.machineId";
  /**
   * Property to define Buffer Size
   */
  public static final String CCS_BUFFER_SIZE = "ccs.bufferSize";
  /**
   * Property to define Max waiting time in milliseconds before Time Out within packets (in particular unknown
   * size)
   */
  public static final String CCS_MAX_WAIT_MS = "ccs.maxWaitMs";
  private static final long DEFAULT_MAX_WAIT_MS = 1000;
  /**
   * Optimal is between 64KB, 96KB and 128KB.
   * Note: Quarkus seems to limit to 64KB but setting the same value gives smaller chunk size
   */
  private static final int DEFAULT_BUFFER_SIZE = 131072;
  private static int bufSize = SystemPropertyUtil.get(CCS_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
  private static long maxWaitMs = SystemPropertyUtil.get(CCS_MAX_WAIT_MS, DEFAULT_MAX_WAIT_MS);
  /**
   * Default global Vertx
   */
  private static Vertx vertx = null;
  /**
   * Default global ObjectMapper
   */
  private static ObjectMapper objectMapper = null;

  /**
   * @return the MachineId if specified as ccs.machineId using 6 bytes in Hexadecimal format
   */
  public static String getCcsMachineId() {
    return SystemPropertyUtil.get(CCS_MACHINE_ID);
  }

  /**
   * @return the maximum Buffer Size (default 128 KB)
   */
  public static int getBufSize() {
    return bufSize;
  }

  public static void setBufSize(final int bufSize) {
    SystemPropertyUtil.set(CCS_BUFFER_SIZE, bufSize);
    StandardProperties.bufSize = bufSize;
  }

  /**
   * @return Max Wait in milliseconds before TimeOut occurs within transmission (end of unknown size) (Default 1000 ms)
   */
  public static long getMaxWaitMs() {
    return maxWaitMs;
  }

  public static void setMaxWaitMs(final long maxWaitMs) {
    SystemPropertyUtil.set(CCS_MAX_WAIT_MS, maxWaitMs);
    StandardProperties.maxWaitMs = maxWaitMs;
  }

  /**
   * @return the global Vertx core
   */
  public static Vertx getVertx() {
    if (vertx == null) {
      vertx = Vertx.vertx();
    }
    return vertx;
  }

  /**
   * To set up from CDI as Quarkus
   */
  public static void setCdiVertx(final Vertx vertx) {
    if (vertx != null && StandardProperties.vertx != null) {
      StandardProperties.vertx.close();
    }
    StandardProperties.vertx = vertx;
  }

  /**
   * @return the global ObjectMapper
   */
  public static ObjectMapper getObjectMapper() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }
    return objectMapper;
  }

  /**
   * To set up from CDI as Quarkus
   */
  public static void setCdiObjectMapper(final ObjectMapper objectMapper) {
    if (objectMapper == null) {
      throw new CcsInvalidArgumentRuntimeException("ObjectMapper cannot be null");
    }
    StandardProperties.objectMapper = objectMapper;
  }

  protected StandardProperties() {
    // Empty
  }

  public static String confugrationToString() {
    return String.format("\"%s\":\"%s\", \"%s\":%d, \"%s\":%d", CCS_MACHINE_ID, getCcsMachineId(), CCS_BUFFER_SIZE,
        getBufSize(), CCS_MAX_WAIT_MS, getMaxWaitMs());
  }
}
