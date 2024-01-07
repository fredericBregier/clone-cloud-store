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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class MetricsCheckTest {

  @Test
  void checkMetricHelper() throws InterruptedException {
    MeterRegistry registry = Metrics.globalRegistry;
    final var counter =
        Counter.builder("count.me").baseUnit("beans").description("a description").tags("region", "test")
            .register(registry);
    assertEquals(0.0, counter.count());
    assertEquals(0.0, MetricsCheck.waitForValueTest(counter, 0.0, 100));
    counter.increment(1.0);
    assertEquals(1.0, MetricsCheck.waitForValueTest(counter, 1.0, 200));
    assertEquals(-1.0, MetricsCheck.waitForValueTest(counter, 2.0, 10));
  }
}
