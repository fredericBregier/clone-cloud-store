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

import io.clonecloudstore.common.standard.system.SystemPropertyUtil;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * A collection of utility methods to retrieve and parse the values of the Java
 * system properties and from ConfigProvider.
 */
public final class QuarkusSystemPropertyUtil extends SystemPropertyUtil {
  // Since logger could be not available yet, one must not declare there a Logger

  private QuarkusSystemPropertyUtil() {
    super();
  }

  /**
   * @return the associated value or null if the key is not in the Configuration
   */
  public static String getStringConfig(final String key) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, String.class);
    return optional.orElse(null);
  }

  /**
   * @return the associated value or null if the key is not in the Configuration
   */
  public static Long getLongConfig(final String key) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Long.class);
    return optional.orElse(null);
  }

  /**
   * @return the associated value or null if the key is not in the Configuration
   */
  public static Integer getIntegerConfig(final String key) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Integer.class);
    return optional.orElse(null);
  }

  /**
   * @return the associated value or null if the key is not in the Configuration
   */
  public static Boolean getBooleanConfig(final String key) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Boolean.class);
    return optional.orElse(null);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static String getStringConfig(final String key, final String defaultValue) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, String.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static int getIntegerConfig(final String key, final int defaultValue) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Integer.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static long getLongConfig(final String key, final long defaultValue) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Long.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static boolean getBooleanConfig(final String key, final boolean defaultValue) {
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Boolean.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return if originalValue is null, the associated value or default one if the key is not in
   * the Configuration, or if not null the originalValue itself
   */
  public static String getStringConfig(final String key, final String originalValue, final String defaultValue) {
    if (originalValue != null) {
      return originalValue;
    }
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, String.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static int getIntegerConfig(final String key, final Integer originalValue, final int defaultValue) {
    if (originalValue != null) {
      return originalValue;
    }
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Integer.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static long getLongConfig(final String key, final Long originalValue, final long defaultValue) {
    if (originalValue != null) {
      return originalValue;
    }
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Long.class);
    return optional.orElse(defaultValue);
  }

  /**
   * @return the associated value or default one if the key is not in the Configuration
   */
  public static boolean getBooleanConfig(final String key, final Boolean originalValue, final boolean defaultValue) {
    if (originalValue != null) {
      return originalValue;
    }
    final var optional = ConfigProvider.getConfig().getOptionalValue(key, Boolean.class);
    return optional.orElse(defaultValue);
  }
}
