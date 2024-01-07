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

package io.clonecloudstore.test.metrics;

import io.micrometer.core.instrument.Counter;
import org.jboss.logging.Logger;

public class MetricsCheck {
  private static final Logger LOGGER = Logger.getLogger(MetricsCheck.class);

  /**
   * Check the counter to reach the desired value up to maxWait ms.
   *
   * @return the new value (possibly greater) or -1 if out of time
   */
  public static double waitForValueTest(final Counter counter, final double valueSearch, final int maxWait)
      throws InterruptedException {
    final int maxWaitReal = maxWait >= 100 ? maxWait / 100 : 1;
    for (int i = 0; i < 100; i++) {
      final double result = counter.count();
      if (result >= valueSearch) {
        return result;
      }
      Thread.sleep(maxWaitReal);
    }
    LOGGER.errorf("Cannot find for Counter %s the value %f while having %f", counter.getId(), valueSearch,
        counter.count());
    return -1;
  }

  private MetricsCheck() {
    // Empty
  }
}
