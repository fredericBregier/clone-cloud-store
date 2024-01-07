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

import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.exception.DriverRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class ExceptionTest {

  @Test
  void checkBean() {
    final var exception = new DriverException("message");
    var exception2 = new DriverException(exception.getMessage(), exception);
    assertEquals(exception.getMessage(), exception2.getMessage());
    assertEquals(exception, exception2.getCause());
    exception2 = new DriverException(exception);
    assertEquals(exception, exception2.getCause());

    final var alreadyExistException = new DriverAlreadyExistException("message");
    var alreadyExistException1 =
        new DriverAlreadyExistException(alreadyExistException.getMessage(), alreadyExistException);
    assertEquals(alreadyExistException.getMessage(), alreadyExistException1.getMessage());
    assertEquals(alreadyExistException, alreadyExistException1.getCause());
    alreadyExistException1 = new DriverAlreadyExistException(alreadyExistException);
    assertEquals(alreadyExistException, alreadyExistException1.getCause());

    final var driverNotFoundException = new DriverNotFoundException("message");
    var driverNotFoundException1 =
        new DriverNotFoundException(driverNotFoundException.getMessage(), driverNotFoundException);
    assertEquals(driverNotFoundException.getMessage(), driverNotFoundException1.getMessage());
    assertEquals(driverNotFoundException, driverNotFoundException1.getCause());
    driverNotFoundException1 = new DriverNotFoundException(driverNotFoundException);
    assertEquals(driverNotFoundException, driverNotFoundException1.getCause());

    final var driverNotAcceptableException = new DriverNotAcceptableException("message");
    var driverNotAcceptableException1 =
        new DriverNotAcceptableException(driverNotAcceptableException.getMessage(), driverNotAcceptableException);
    assertEquals(driverNotAcceptableException.getMessage(), driverNotAcceptableException1.getMessage());
    assertEquals(driverNotAcceptableException, driverNotAcceptableException1.getCause());
    driverNotAcceptableException1 = new DriverNotAcceptableException(driverNotAcceptableException);
    assertEquals(driverNotAcceptableException, driverNotAcceptableException1.getCause());

    final var driverClientRuntimeException = new DriverRuntimeException("message", null);
    var driverClientRuntimeException1 =
        new DriverRuntimeException(driverClientRuntimeException.getMessage(), driverClientRuntimeException);
    assertEquals(driverClientRuntimeException.getMessage(), driverClientRuntimeException1.getMessage());
    assertEquals(driverClientRuntimeException, driverClientRuntimeException1.getCause());
    driverClientRuntimeException1 = new DriverRuntimeException(driverClientRuntimeException);
    assertEquals(driverClientRuntimeException, driverClientRuntimeException1.getCause());
  }
}
