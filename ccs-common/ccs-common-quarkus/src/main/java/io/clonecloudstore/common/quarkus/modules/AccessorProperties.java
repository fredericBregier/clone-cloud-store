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

package io.clonecloudstore.common.quarkus.modules;

import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Accessor Configurable Values
 */
@ApplicationScoped
@Unremovable
public class AccessorProperties extends ServiceProperties {
  public static final String CCS_ACCESSOR_REMOTE_READ = "ccs.accessor.remote.read";
  public static final String CCS_ACCESSOR_REMOTE_FIX_ON_ABSENT = "ccs.accessor.remote.fixOnAbsent";
  public static final String CCS_INTERNAL_COMPRESSION = "ccs.internal.compression";
  public static final String CCS_ACCESSOR_STORE_PATH = "ccs.accessor.store.path";
  public static final String CCS_ACCESSOR_STORE_ACTIVE = "ccs.accessor.store.active";
  public static final String CCS_ACCESSOR_STORE_MIN_SPACE_GB = "ccs.accessor.store.min_space_gb";
  public static final String CCS_ACCESSOR_STORE_PURGE_RETENTION_SECONDS = "ccs.accessor.store.purge.retention_seconds";
  public static final String CCS_ACCESSOR_STORE_SCHEDULE_DELAY = "ccs.accessor.store.schedule.delay";
  private static boolean remoteRead = QuarkusSystemPropertyUtil.getBooleanConfig(CCS_ACCESSOR_REMOTE_READ, false);
  private static boolean fixOnAbsent =
      QuarkusSystemPropertyUtil.getBooleanConfig(CCS_ACCESSOR_REMOTE_FIX_ON_ABSENT, false);
  private static boolean internalCompression =
      QuarkusSystemPropertyUtil.getBooleanConfig(CCS_INTERNAL_COMPRESSION, false);
  private static boolean storeActive = QuarkusSystemPropertyUtil.getBooleanConfig(CCS_ACCESSOR_STORE_ACTIVE, false);
  private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
  private static final String STORE_PATH =
      QuarkusSystemPropertyUtil.getStringConfig(CCS_ACCESSOR_STORE_PATH, System.getProperty(JAVA_IO_TMPDIR) + "/CCS");
  private static final int STORE_MIN_SPACE_GB =
      QuarkusSystemPropertyUtil.getIntegerConfig(CCS_ACCESSOR_STORE_MIN_SPACE_GB, 5);
  private static long storePurgeRetentionSeconds =
      QuarkusSystemPropertyUtil.getLongConfig(CCS_ACCESSOR_STORE_PURGE_RETENTION_SECONDS, 3600);
  private static final String STORE_SCHEDULE_DELAY =
      QuarkusSystemPropertyUtil.getStringConfig(CCS_ACCESSOR_STORE_SCHEDULE_DELAY, "10s");

  protected AccessorProperties() {
    // Nothing
  }

  /**
   * @return True if the remote read/check is active
   */
  public static boolean isRemoteRead() {
    return remoteRead;
  }

  /**
   * package-protected method
   *
   * @param isRemoteRead new remote read status
   */
  public static void setRemoteRead(final boolean isRemoteRead) {
    remoteRead = isRemoteRead;
  }

  /**
   * @return True if the remote content is used to fix missing local data
   */
  public static boolean isFixOnAbsent() {
    return fixOnAbsent;
  }

  /**
   * package-protected method
   *
   * @param isFixOnAbsent new remote content used if local is missing data
   */
  public static void setFixOnAbsent(final boolean isFixOnAbsent) {
    fixOnAbsent = isFixOnAbsent;
  }

  /**
   * @return True if the internal compression is active
   */
  public static boolean isInternalCompression() {
    return internalCompression;
  }

  /**
   * package-protected method
   *
   * @param isInternalCompression new internal compression state
   */
  public static void setInternalCompression(final boolean isInternalCompression) {
    internalCompression = isInternalCompression;
  }

  /**
   * Warning: enable this will use local storage and could lead to out of space.
   * The intention is to allow to be more flexible regarding Driver temporary issues.
   *
   * @return True if the local storage is to be uses (default false)
   */
  public static boolean isStoreActive() {
    return storeActive;
  }

  public static void setStoreActive(final boolean active) {
    storeActive = active;
  }

  /**
   * @return the path to use for local store (default being java.io.tmpdir extended with "/CCS" such as "/tmp/CCS")
   */
  public static String getStorePath() {
    return STORE_PATH;
  }

  /**
   * @return the minimum space that must be available on local storage before trying to save the content
   */
  public static int getStoreMinSpaceGb() {
    return STORE_MIN_SPACE_GB;
  }

  /**
   * @return the delay in seconds before an upload is considered out of time and to be purged
   */
  public static long getStorePurgeRetentionSeconds() {
    return storePurgeRetentionSeconds;
  }

  public static void setStorePurgeRetentionSeconds(final long retention) {
    storePurgeRetentionSeconds = retention;
  }

  /**
   * @return the delay in duration format between each schedule
   */
  public static String getStoreScheduleDelay() {
    return STORE_SCHEDULE_DELAY;
  }

  public static String confugrationToString() {
    return String.format(
        "%s, \"%s\":%b, \"%s\":%b, \"%s\":%b, \"%s\":%b, \"%s\":\"%s\", \"%s\":%d, \"%s\":%d, \"%s\":\"%s\"",
        ServiceProperties.confugrationToString(), CCS_ACCESSOR_REMOTE_READ, isRemoteRead(),
        CCS_ACCESSOR_REMOTE_FIX_ON_ABSENT, isFixOnAbsent(), CCS_INTERNAL_COMPRESSION, isInternalCompression(),
        CCS_ACCESSOR_STORE_ACTIVE, isStoreActive(), CCS_ACCESSOR_STORE_PATH, getStorePath(),
        CCS_ACCESSOR_STORE_MIN_SPACE_GB, getStoreMinSpaceGb(), CCS_ACCESSOR_STORE_PURGE_RETENTION_SECONDS,
        getStorePurgeRetentionSeconds(), CCS_ACCESSOR_STORE_SCHEDULE_DELAY, getStoreScheduleDelay());
  }
}
