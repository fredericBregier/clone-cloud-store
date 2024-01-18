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

package io.clonecloudstore.common.quarkus.metrics;

import io.clonecloudstore.test.metrics.MetricsCheck;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class BulkMetricsTest {
  @Inject
  BulkMetrics bulkMetrics;

  @Test
  void testMetrics() throws InterruptedException {
    String name = "ccs.test";
    var counter1 = bulkMetrics.getCounterInternal(name, "tag1", "value1");
    bulkMetrics.incrementCounter(1, name, "tag1", "value1");
    MetricsCheck.waitForValueTest(counter1, 1, 100);
    assertEquals(1, bulkMetrics.getCounterInternal(name, "tag1", "value1").count());
    Log.infof("C1: %s", counter1.getId());
    assertEquals(name, counter1.getId().getName());
    var counter2 = bulkMetrics.getCounter(BulkMetricsTest.class, "tag1", "value1");
    bulkMetrics.incrementCounter(1, BulkMetricsTest.class, "tag1", "value1");
    MetricsCheck.waitForValueTest(counter2, 1, 100);
    assertEquals(1, bulkMetrics.getCounter(BulkMetricsTest.class, "tag1", "value1").count());
    Log.infof("C2: %s", counter2.getId());
    assertEquals(bulkMetrics.getName(BulkMetricsTest.class), counter2.getId().getName());
  }

  @Test
  void microBenchmark() {
    String name = "ccs.test.bench";
    var start = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      bulkMetrics.incrementCounter(1, name, "tag" + i, "value1");
      assertEquals(1, bulkMetrics.getCounter(name, "tag" + i, "value1").count());
    }
    var stop = System.nanoTime();
    var start2 = System.nanoTime();
    String name2 = "ccs.test.bench2";
    for (int i = 0; i < 1000; i++) {
      var counter = bulkMetrics.getCounterInternal(name2, "tag" + i, "value1");
      counter.increment(1);
      assertEquals(1, bulkMetrics.getCounterInternal(name2, "tag" + i, "value1").count());
    }
    var stop2 = System.nanoTime();
    Log.infof("Time1: %d Time2: %d (Ratio: %f)", (stop - start), (stop2 - start2),
        (stop2 - start2) / (float) (stop - start));
  }
}
