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

import java.time.Duration;

import io.clonecloudstore.common.standard.properties.Module;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;
import org.jboss.logmanager.MDC;

/**
 * Configurable values
 */
@ApplicationScoped
@Unremovable
public class QuarkusProperties extends StandardProperties {
  private static final Logger LOGGER = Logger.getLogger(QuarkusProperties.class);
  /**
   * Property to define Buffer Size for a Driver Chunk
   */
  public static final String CCS_DRIVER_MAX_CHUNK_SIZE = "ccs.driverMaxChunkSize";
  /**
   * Property to define if Server will compute SHA 256 on the fly
   */
  public static final String CCS_SERVER_COMPUTE_SHA_256 = "ccs.server.computeSha256";
  /**
   * Property to define Max transferring time in milliseconds before Time Out (must take into account large file and
   * bandwidth)
   */
  public static final String CCS_CLIENT_RESPONSE_TIMEOUT = "ccs.client.response.timeout";
  static final int DEFAULT_DRIVER_MAX_CHUNK_SIZE = 512 * 1024 * 1024;
  private static final int DEFAULT_RESPONSE_TIMEOUT_MS = 300000;
  public static final String MODULE_MDC = "module";
  private static long clientResponseTimeOut =
      SystemPropertyUtil.get(CCS_CLIENT_RESPONSE_TIMEOUT, DEFAULT_RESPONSE_TIMEOUT_MS);
  private static Duration durationResponseTimeout = Duration.ofMillis(clientResponseTimeOut);
  private static int driverMaxChunkSize =
      QuarkusSystemPropertyUtil.getIntegerConfig(CCS_DRIVER_MAX_CHUNK_SIZE, DEFAULT_DRIVER_MAX_CHUNK_SIZE);
  private static Module module = Module.UNKNOWN;
  private static boolean computeSha256 = QuarkusSystemPropertyUtil.getBooleanConfig(CCS_SERVER_COMPUTE_SHA_256, false);
  private static boolean hasDatabase = true;

  static {
    // Get the MACHINE_ID if specified and set it too in System Properties
    var optional = ConfigProvider.getConfig().getOptionalValue(StandardProperties.CCS_MACHINE_ID, String.class);
    optional.ifPresent(s -> SystemPropertyUtil.set(StandardProperties.CCS_MACHINE_ID, s));
    // Get the CCS_BUFFER_SIZE if specified and set it too in System Properties
    optional = ConfigProvider.getConfig().getOptionalValue(CCS_BUFFER_SIZE, String.class);
    optional.ifPresent(s -> setBufSize(Integer.parseInt(s)));
    // Get the CCS_MAX_WAIT_MS if specified and set it too in System Properties
    optional = ConfigProvider.getConfig().getOptionalValue(CCS_MAX_WAIT_MS, String.class);
    optional.ifPresent(s -> setMaxWaitMs(Integer.parseInt(s)));
    // Get the CCS_MAX_TRANSFER_MS if specified and compute dependent values
    optional = ConfigProvider.getConfig().getOptionalValue(CCS_CLIENT_RESPONSE_TIMEOUT, String.class);
    optional.ifPresent(s -> setClientResponseTimeOut(Integer.parseInt(s)));
  }

  /**
   * @return The duration before TimeOut occurs except InputStream
   */
  public static Duration getDurationResponseTimeout() {
    return durationResponseTimeout;
  }

  public static long clientResponseTimeOut() {
    return QuarkusProperties.clientResponseTimeOut;
  }

  public static void setClientResponseTimeOut(final long clientResponseTimeOut) {
    SystemPropertyUtil.set(CCS_CLIENT_RESPONSE_TIMEOUT, clientResponseTimeOut);
    QuarkusProperties.clientResponseTimeOut = clientResponseTimeOut;
    durationResponseTimeout = Duration.ofMillis(clientResponseTimeOut);
  }

  @Startup
  void initVertx() {
    setCdiVertx(CDI.current().select(Vertx.class).get().getDelegate());
    MDC.put(MODULE_MDC, module.name());
  }

  /**
   * @return the maximum Chunk Size (Default 512 MB)
   */
  public static int getDriverMaxChunkSize() {
    return driverMaxChunkSize;
  }

  public static void setDriverMaxChunkSize(final int driverMaxChunkSize) {
    QuarkusProperties.driverMaxChunkSize = driverMaxChunkSize;
  }

  public static Module getCcsModule() {
    return module;
  }

  public static void setCcsModule(final Module module) {
    if (module.ordinal() < QuarkusProperties.module.ordinal()) {
      QuarkusProperties.module = module;
      refreshModuleMdc();
      LOGGER.debugf("Set Module = %s", module);
    }
  }

  public static boolean hasDatabase() {
    return hasDatabase;
  }

  public static void setHasDatabase(final boolean hasDb) {
    hasDatabase = hasDb;
  }

  public static void refreshModuleMdc() {
    MDC.put(MODULE_MDC, module.name());
  }

  protected QuarkusProperties() {
    // Nothing
  }

  /**
   * @return True if the server shall compute Sha256 on the fly
   */
  public static boolean serverComputeSha256() {
    return computeSha256;
  }

  /***
   * To change the behavior dynamically
   * @param computeSha256 True if the client shall compute Sha256 on the fly
   */
  public static void setServerComputeSha256(final boolean computeSha256) {
    QuarkusProperties.computeSha256 = computeSha256;
  }

  public static String confugrationToString() {
    return String.format("%s, \"%s\":%d, \"%s\":%d, \"%s\":%d, \"%s\":\"%s\"",
        StandardProperties.confugrationToString(), CCS_DRIVER_MAX_CHUNK_SIZE, getDriverMaxChunkSize(),
        CCS_SERVER_COMPUTE_SHA_256, getDriverMaxChunkSize(), CCS_CLIENT_RESPONSE_TIMEOUT, clientResponseTimeOut(),
        MODULE_MDC, getCcsModule());
  }
}
