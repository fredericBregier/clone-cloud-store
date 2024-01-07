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

import io.clonecloudstore.driver.api.DriverApi;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Factory for Fake Driver
 */
@ApplicationScoped
@Unremovable
public class FakeDriverFactory implements DriverApiFactory {
  private static final FakeDriver instance = new FakeDriver();

  @Override
  public DriverApi getInstance() {
    return instance;
  }

  /**
   * CleanUp function for testing purpose
   */
  public static void cleanUp() {
    instance.cleanUp();
  }
}
