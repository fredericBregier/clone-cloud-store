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
  public static final String CCS_ACCESSOR_INTERNAL_COMPRESSION = "ccs.accessor.internal.compression";
  private static boolean remoteRead = QuarkusSystemPropertyUtil.getBooleanConfig(CCS_ACCESSOR_REMOTE_READ, false);
  private static boolean fixOnAbsent =
      QuarkusSystemPropertyUtil.getBooleanConfig(CCS_ACCESSOR_REMOTE_FIX_ON_ABSENT, false);
  private static boolean internalCompression =
      QuarkusSystemPropertyUtil.getBooleanConfig(CCS_ACCESSOR_INTERNAL_COMPRESSION, false);

  protected AccessorProperties() {
    super();
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
  // FIXME
  public static boolean isInternalCompression() {
    return internalCompression;
  }

  /**
   * package-protected method
   *
   * @param isInternalCompression new internal compression state
   */
  static void setInternalCompression(final boolean isInternalCompression) {
    internalCompression = isInternalCompression;
  }

  public static String confugrationToString() {
    return String.format("%s, \"%s\":%b, \"%s\":%b, \"%s\":%b", ServiceProperties.confugrationToString(),
        CCS_ACCESSOR_REMOTE_READ, isRemoteRead(), CCS_ACCESSOR_REMOTE_FIX_ON_ABSENT, isFixOnAbsent(),
        CCS_ACCESSOR_INTERNAL_COMPRESSION, isInternalCompression());
  }
}
