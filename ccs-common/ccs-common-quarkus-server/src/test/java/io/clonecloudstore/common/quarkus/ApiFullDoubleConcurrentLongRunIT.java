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

package io.clonecloudstore.common.quarkus;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
@Disabled("Only for checking")
class ApiFullDoubleConcurrentLongRunIT extends ApiFullServerDoubleAbstract {
  private static final Logger LOG = Logger.getLogger(ApiFullDoubleConcurrentLongRunIT.class);
  private static final int NB_THREADS = 3;
  private static final int NB_ITER_PER_THREADS = 3;
  private static final int NB_TEST = 3;
  private static final int NB_STREAM_PER_TEST = 3;
  final Random random = new Random();

  @Test
  void checkStabilityManualTest() throws InterruptedException {
    final var start2 = System.nanoTime();
    final var res2 = checkForStabilityConcurrentManual();
    final var stop2 = System.nanoTime();
    double value2 = ((float) ApiQuarkusService.LEN) * (res2 * NB_STREAM_PER_TEST) /
        (1024 * 1024.0 * ((stop2 - start2) / 1000000000.0));
    LOG.infof("Global Bandwidth with %d threads on %d transfers: %f", NB_THREADS, res2 * NB_STREAM_PER_TEST, value2);

    assertEquals(NB_THREADS * NB_ITER_PER_THREADS * NB_TEST, res2);
  }

  long checkForStabilityConcurrentManual() throws InterruptedException {
    LOG.infof("START CONCURRENT");
    ExecutorService executorService = Executors.newFixedThreadPool(NB_THREADS);
    AtomicLong cpt = new AtomicLong();
    for (int i = 0; i < NB_THREADS; i++) {
      executorService.execute(() -> {
        for (int j = 0; j < NB_ITER_PER_THREADS; j++) {
          Thread.yield();
          executeTests(cpt);
        }
      });
    }
    Thread.sleep(2000);
    executorService.shutdown();
    executorService.awaitTermination(1000, TimeUnit.SECONDS);
    LOG.infof("END CONCURRENT");
    return cpt.get();
  }

  private void executeTests(final AtomicLong cpt) {
    check34GetInputStreamQuarkusDoubleTest();
    cpt.incrementAndGet();
    check35GetInputStreamQuarkusDoubleNoSizeTest();
    cpt.incrementAndGet();
    check51GetInputStreamQuarkusDoubleNoSizeCompressedIntraTest();
    cpt.incrementAndGet();
  }
}
