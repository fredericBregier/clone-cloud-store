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

package io.clonecloudstore.driver.api;

import org.jboss.logging.Logger;

/**
 * Register on which Driver API Factory will register once initialized
 */
public class DriverApiRegistry {
  private static final Logger LOGGER = Logger.getLogger(DriverApiRegistry.class);
  private static DriverApiFactory driverApiFactory;

  /**
   * Used by DriverApi implementation to setup the registry (only one value kept)
   */
  public static void setDriverApiFactory(final DriverApiFactory driverApiFactorySetup) {
    if (driverApiFactorySetup == null) {
      LOGGER.error("Driver Registry initialized with null Driver!");
      throw new IllegalArgumentException("DriverApiFactory cannot be null for registration");
    }
    if (driverApiFactory != null && driverApiFactory.getClass() != driverApiFactorySetup.getClass()) {
      LOGGER.errorf("Driver Registry initialized with 2 Drivers! Initial is %s while second is %s. Check dependencies",
          driverApiFactory.getClass().getName(), driverApiFactorySetup.getClass().getName());
      throw new IllegalArgumentException("DriverApiFactory registered twice");
    }
    LOGGER.infof("Setup DriverApiFactory to %s", driverApiFactorySetup.getClass().getName());
    driverApiFactory = driverApiFactorySetup;
  }

  /**
   * @return the current DriverApiFactory
   */
  public static DriverApiFactory getDriverApiFactory() {
    if (driverApiFactory == null) {
      LOGGER.warn("Driver Api Registry is not setup: check dependency");
    }
    return driverApiFactory;
  }

  private DriverApiRegistry() {
    // Empty
  }
}
