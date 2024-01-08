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

package io.clonecloudstore.test.driver.fake;

import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/**
 * Register FakeDriverFactory into DriverApiRegistry
 */
@ApplicationScoped
@Unremovable
@Startup
public class FakeDriverRegister {
  private final FakeDriverFactory factory;

  public FakeDriverRegister(final FakeDriverFactory factory) {
    this.factory = factory;
  }

  void startup(@Observes final StartupEvent event) { // NOSONAR intentional
    DriverApiRegistry.setDriverApiFactory(factory);
  }
}
