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

package io.clonecloudstore.driver.google;

import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Configurable values for Google
 */
@ApplicationScoped
@Unremovable
public class DriverGoogleProperties {
  public static final String SHA_256 = "sha256";
  public static final String CLIENT_ID = "clientid";
  public static final String EXPIRY = "expiry";
  public static final int MAX_ITEMS = 1000;
  public static final int DEFAULT_MIN_PART_SIZE = 5 * 1024 * 1024;
  public static final long DEFAULT_SIZE_NOT_PART = 256 * 1024 * 1024L;
  public static final long DEFAULT_MAX_SIZE_NOT_PART = 1024 * 1024 * 1024L;
  public static final int DEFAULT_PART_SIZE_INT = 128 * 1024 * 1024;
  public static final int DEFAULT_MAX_PART_SIZE_INT = 2000 * 1024 * 1024;
  /**
   * Default is to use Gzip content, but may be disabled (default: true so disabled)
   */
  public static final String CCS_DRIVER_GOOGLE_DISABLE_GZIP = "ccs.driver.google.disableGzip";
  /**
   * MultiPart size (minimum 5 MB, maximum 5 GB, default 256 MB)
   */
  public static final String CCS_DRIVER_GOOGLE_MAX_PART_SIZE = "ccs.driver.google.maxPartSize";
  /**
   * Buffer size (minimum 5 MB, maximum 512 MB, not related with Max Part Size of ~2 GB): will be used to decide
   * which method to use (no memory impact)
   */
  public static final String CCS_DRIVER_GOOGLE_MAX_BUF_SIZE = "ccs.driver.google.maxBufSize";
  private static boolean googleDisableGzip =
      QuarkusSystemPropertyUtil.getBooleanConfig(CCS_DRIVER_GOOGLE_DISABLE_GZIP, true);
  private static long googleMaxPartSize = Math.min(
      Math.max(QuarkusSystemPropertyUtil.getLongConfig(CCS_DRIVER_GOOGLE_MAX_PART_SIZE, DEFAULT_SIZE_NOT_PART),
          DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_SIZE_NOT_PART);
  private static int googleMaxBufSize = Math.min(
      Math.max(QuarkusSystemPropertyUtil.getIntegerConfig(CCS_DRIVER_GOOGLE_MAX_BUF_SIZE, DEFAULT_PART_SIZE_INT),
          DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_PART_SIZE_INT);

  private final DriverGoogleRegister driverGoogleRegister; // NOSONAR intentional

  public DriverGoogleProperties(final DriverGoogleRegister driverGoogleRegister) {
    this.driverGoogleRegister = driverGoogleRegister;
  }

  /**
   * Default is to use Gzip content, but may be disabled (default: true so disabled)
   */
  public static boolean isGoogleDisableGzip() {
    return googleDisableGzip;
  }

  /**
   * MultiPart size (minimum 5 MB, maximum 5 GB, default 256 MB)
   */
  public static long getMaxPartSize() {
    return googleMaxPartSize;
  }

  /**
   * Buffer size (minimum 5 MB, maximum 512 MB, not related with Max Part Size of ~2 GB): will be used to buffer
   * InputStream if length is unknown, so take care of the Memory consumption associated (128 MB, default)
   */
  public static int getMaxBufSize() {
    return googleMaxBufSize;
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicPartSize(final long size) {
    googleMaxPartSize = Math.min(Math.max(size, DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_SIZE_NOT_PART);
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicBufSize(final int size) {
    googleMaxBufSize = Math.min(Math.max(size, DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_PART_SIZE_INT);
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicDisableGzip(final boolean disable) {
    googleDisableGzip = disable;
  }

}
