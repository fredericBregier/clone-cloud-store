/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.driver.api.exception.DriverException;
import org.jboss.logging.Logger;

public class CleanupTestUtil {
  private static final Logger LOGGER = Logger.getLogger(CleanupTestUtil.class);

  public static void cleanUp() {
    final Path root = new File(AccessorProperties.getStorePath()).toPath();
    try (final Stream<Path> stream = Files.walk(root)) {
      stream.forEach(path -> {
        if (!path.equals(root)) {
          try {
            Files.delete(path);
          } catch (final IOException ignore) {
            // Ignore
          }
        }
      });
    } catch (final IOException e) {
      LOGGER.infof("Error during cleanup: %s", e);
    }
    try {
      final var driver = DriverApiRegistry.getDriverApiFactory().getInstance();
      for (final var bucket : driver.bucketsStream().toList()) {
        for (final var object : driver.objectsStreamInBucket(bucket).toList()) {
          driver.objectDeleteInBucket(object);
        }
        driver.bucketDelete(bucket);
      }
    } catch (final DriverException e) {
      LOGGER.infof("Error during cleanup: %s", e);
    }
  }
}
