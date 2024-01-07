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

import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Cloud Cloud Store Common Configurable Values
 */
@ApplicationScoped
@Unremovable
public abstract class ServiceProperties extends QuarkusProperties {
  public static final String CCS_ACCESSOR_SITE = "ccs.accessor.site";
  private static final String DEFAULT_ACCESSOR_SITE = "unconfigured";
  private static String accessorSite =
      QuarkusSystemPropertyUtil.getStringConfig(CCS_ACCESSOR_SITE, DEFAULT_ACCESSOR_SITE);

  protected ServiceProperties() {
    super();
    // Nothing
  }

  /**
   * @return the current Accessor Site
   */
  public static String getAccessorSite() {
    return accessorSite;
  }

  /**
   * package-protected method
   *
   * @param site new accessor site to set
   */
  static void setAccessorSite(final String site) {
    accessorSite = site;
  }

  public static String confugrationToString() {
    return String.format("%s, \"%s\":\"%s\"", QuarkusProperties.confugrationToString(), CCS_ACCESSOR_SITE,
        getAccessorSite());
  }
}
