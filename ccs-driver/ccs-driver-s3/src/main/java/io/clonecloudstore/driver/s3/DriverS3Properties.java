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

package io.clonecloudstore.driver.s3;

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Configurable values for S3
 */
@ApplicationScoped
@Unremovable
public class DriverS3Properties {
  private static final Logger LOGGER = Logger.getLogger(DriverS3Properties.class);
  public static final int MAX_ITEMS = 1000;
  public static final String SHA_256 = "sha256";
  public static final int DEFAULT_MIN_PART_SIZE = 5 * 1024 * 1024;
  public static final long DEFAULT_SIZE_NOT_PART = 256 * 1024 * 1024L;
  public static final long DEFAULT_MAX_SIZE_NOT_PART = 5 * 1024 * 1024 * 1024L;
  public static final int DEFAULT_MAX_PART_SIZE_INT = 2000 * 1024 * 1024;
  public static final String CCS_DRIVER_S3_HOST = "ccs.driver.s3.host";
  public static final String CCS_DRIVER_S3_KEY_ID = "ccs.driver.s3.keyId";
  public static final String CCS_DRIVER_S3_KEY = "ccs.driver.s3.key";
  public static final String CCS_DRIVER_S3_REGION = "ccs.driver.s3.region";
  /**
   * MultiPart size (minimum 5 MB, maximum 5 GB, default 256 MB)
   */
  public static final String CCS_DRIVER_S_3_MAX_PART_SIZE = "ccs.driver.s3.maxPartSize";
  /**
   * MultiPart size (minimum 5 MB, maximum ~2 GB): will be used to buffer InputStream if length is unknown, so take
   * care of the Memory consumption associated (512 MB, default, will limit the total InputStream length to 5 TB
   * since 10K parts)
   */
  public static final String CCS_DRIVER_S3_MAX_PART_SIZE_FOR_UNKNOWN_LENGTH =
      "ccs.driver.s3.maxPartSizeForUnknownLength";
  private static String s3Host = QuarkusSystemPropertyUtil.getStringConfig(CCS_DRIVER_S3_HOST, "");
  private static String s3KeyId = QuarkusSystemPropertyUtil.getStringConfig(CCS_DRIVER_S3_KEY_ID, "");
  private static String s3Key = QuarkusSystemPropertyUtil.getStringConfig(CCS_DRIVER_S3_KEY, "");
  private static String s3Region = QuarkusSystemPropertyUtil.getStringConfig(CCS_DRIVER_S3_REGION, "");
  private static long s3MaxPartSize = Math.min(
      Math.max(QuarkusSystemPropertyUtil.getLongConfig(CCS_DRIVER_S_3_MAX_PART_SIZE, DEFAULT_SIZE_NOT_PART),
          DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_SIZE_NOT_PART);
  private static int s3MaxPartSizeForUnknownLength = Math.min(Math.max(
      QuarkusSystemPropertyUtil.getIntegerConfig(CCS_DRIVER_S3_MAX_PART_SIZE_FOR_UNKNOWN_LENGTH,
          QuarkusProperties.getDriverMaxChunkSize()), DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_PART_SIZE_INT);

  static {
    QuarkusProperties.setDriverMaxChunkSize(s3MaxPartSizeForUnknownLength);
  }

  private final DriverS3Register driverS3Register; // NOSONAR intentional

  public DriverS3Properties(final DriverS3Register driverS3Register) {
    this.driverS3Register = driverS3Register;
  }

  /**
   * @return S3 Host
   */
  public static String getDriverS3Host() {
    return s3Host;
  }

  /**
   * @return S3 KeyId
   */
  public static String getDriverS3KeyId() {
    return s3KeyId;
  }

  /**
   * @return S3 Key
   */
  public static String getDriverS3Key() {
    return s3Key;
  }

  /**
   * @return S3 Region
   */
  public static String getDriverS3Region() {
    return s3Region;
  }

  public static long getMaxPartSize() {
    return s3MaxPartSize;
  }

  public static int getMaxPartSizeForUnknownLength() {
    return s3MaxPartSizeForUnknownLength;
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicS3Parameters(final String host, final String keyId, final String key,
                                            final String region) {
    s3Host = host;
    s3KeyId = keyId;
    s3Key = key;
    s3Region = region;
    LOGGER.debugf("Change configuration S3: %s %s %s", s3Host, s3KeyId, s3Region);
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicPartSize(final long size) {
    s3MaxPartSize = Math.min(Math.max(size, DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_SIZE_NOT_PART);
  }

  /**
   * Used to change dynamically the setup
   */
  public static void setDynamicPartSizeForUnknownLength(final int size) {
    s3MaxPartSizeForUnknownLength = Math.min(Math.max(size, DEFAULT_MIN_PART_SIZE), DEFAULT_MAX_PART_SIZE_INT);
    QuarkusProperties.setDriverMaxChunkSize(s3MaxPartSizeForUnknownLength);
  }
}
