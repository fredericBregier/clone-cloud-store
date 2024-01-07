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

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class DriverApiRegistryTest {
  static class DriverApiFactoryFake implements DriverApiFactory {

    @Override
    public DriverApi getInstance() {
      return null;
    }
  }

  @Test
  void checkRegister() {
    assertNull(DriverApiRegistry.getDriverApiFactory());
    // Cannot register NULL
    assertThrows(IllegalArgumentException.class, () -> DriverApiRegistry.setDriverApiFactory(null));
    DriverApiRegistry.setDriverApiFactory(() -> null);
    assertNotNull(DriverApiRegistry.getDriverApiFactory());
    // Cannot register NULL
    assertThrows(IllegalArgumentException.class, () -> DriverApiRegistry.setDriverApiFactory(null));
    assertNotNull(DriverApiRegistry.getDriverApiFactory());
    // Cannot register twice
    assertThrows(IllegalArgumentException.class,
        () -> DriverApiRegistry.setDriverApiFactory(new DriverApiFactoryFake()));
    assertNotNull(DriverApiRegistry.getDriverApiFactory());
  }
}
