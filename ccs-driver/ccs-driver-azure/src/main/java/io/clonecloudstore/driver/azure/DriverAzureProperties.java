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

package io.clonecloudstore.driver.azure;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Configurable values for Azure
 */
@ApplicationScoped
@Unremovable
public class DriverAzureProperties {
  public static final String SHA_256 = "sha256";
  public static final String CLIENT_ID = "clientid";
  public static final String EXPIRY = "expiry";
  public static final int DEFAULT_CONCURRENCY = 2;
  public static final int DEFAULT_MAX_CONCURRENCY = 8;
  public static final int DEFAULT_MIN_PART_SIZE = 5 * 1024 * 1024;
  public static final long DEFAULT_SIZE_NOT_PART = 256 * 1024 * 1024L;
  public static final long DEFAULT_MAX_SIZE_NOT_PART = 4 * 1024 * 1024 * 1024L;
  public static final int DEFAULT_MAX_PART_SIZE_INT = 2000 * 1024 * 1024;
  /**
   * MultiPart concurrency (minimum 1, maximum 8, default 2)
   */
  public static final String CCS_DRIVER_AZURE_MAX_CONCURRENCY = "ccs.driver.azure.maxConcurrency";
  /**
   * MultiPart size (minimum 5 MB, maximum 4 GB, default 256 MB)
   */
  public static final String CCS_DRIVER_AZURE_MAX_PART_SIZE = "ccs.driver.azure.maxPartSize";
  /**
   * MultiPart size (minimum 5 MB, maximum ~2 GB): will be used decide with method to use (no memory impact)
   */
  public static final String CCS_DRIVER_AZURE_MAX_PART_SIZE_FOR_UNKNOWN_LENGTH =
      "ccs.driver.azure.maxPartSizeForUnknownLength";
  private static final int AZURE_MAX_CONCURRENCY = Math.min(
      Math.max(QuarkusSystemPropertyUtil.getIntegerConfig(CCS_DRIVER_AZURE_MAX_CONCURRENCY, DEFAULT_CONCURRENCY), 1),
      DEFAULT_MAX_CONCURRENCY);
  private static long azureMaxPartSize = Math.min(
      Math.max(QuarkusSystemPropertyUtil.getLongConfig(CCS_DRIVER_AZURE_MAX_PART_SIZE, DEFAULT_SIZE_NOT_PART),
          DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_SIZE_NOT_PART);
  private static int azureMaxPartSizeForUnknownLength = Math.min(Math.max(
      QuarkusSystemPropertyUtil.getIntegerConfig(CCS_DRIVER_AZURE_MAX_PART_SIZE_FOR_UNKNOWN_LENGTH,
          QuarkusProperties.getDriverMaxChunkSize()), DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_PART_SIZE_INT);

  static {
    QuarkusProperties.setDriverMaxChunkSize(azureMaxPartSizeForUnknownLength);
  }

  private final DriverAzureRegister driverAzureRegister; // NOSONAR intentional

  public DriverAzureProperties(final DriverAzureRegister driverAzureRegister) {
    this.driverAzureRegister = driverAzureRegister;
  }

  public static int getMaxConcurrency() {
    return AZURE_MAX_CONCURRENCY;
  }

  public static long getMaxPartSize() {
    return azureMaxPartSize;
  }

  public static int getMaxPartSizeForUnknownLength() {
    return azureMaxPartSizeForUnknownLength;
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicPartSize(final long size) {
    azureMaxPartSize = Math.min(Math.max(size, DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_SIZE_NOT_PART);
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicPartSizeForUnknownLength(final int size) {
    azureMaxPartSizeForUnknownLength = Math.min(Math.max(size, DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_PART_SIZE_INT);
    QuarkusProperties.setDriverMaxChunkSize(azureMaxPartSizeForUnknownLength);
  }
}
