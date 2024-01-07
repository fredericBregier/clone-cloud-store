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
class ApiFullDoubleLongRunIT extends ApiFullServerDoubleAbstract {
  private static final Logger LOG = Logger.getLogger(ApiFullDoubleLongRunIT.class);
  private static final int NB_ITER_SEQUENTIAL = 5;
  private static final int NB_TEST = 8;
  private static final int NB_STREAM_PER_TEST = 3;

  @Test
  void checkStabilityManualTest() throws InterruptedException {
    final var start = System.nanoTime();
    final var res1 = checkForStabilityManual();
    final var stop = System.nanoTime();
    double value1 = ((float) ApiQuarkusService.LEN) * (res1 * NB_STREAM_PER_TEST) /
        (1024 * 1024.0 * ((stop - start) / 1000000000.0));
    LOG.infof("Global Bandwidth on %d transfers: %f", res1 * NB_STREAM_PER_TEST, value1);

    assertEquals(NB_ITER_SEQUENTIAL * NB_TEST, res1);
  }

  long checkForStabilityManual() {
    LOG.infof("START UNITARY");
    AtomicLong cpt = new AtomicLong();
    for (int i = 0; i < NB_ITER_SEQUENTIAL; i++) {
      executeTests(cpt);
    }
    LOG.infof("END UNITARY");
    return cpt.get();
  }

  private void executeTests(final AtomicLong cpt) {
    check30PostInputStreamQuarkusDoubleTest();
    cpt.incrementAndGet();
    check31PostInputStreamQuarkusDoubleNoSizeTest();
    cpt.incrementAndGet();
    check33PostInputStreamQuarkusShaDoubleNoSizeTest();
    cpt.incrementAndGet();
    check34GetInputStreamQuarkusDoubleTest();
    cpt.incrementAndGet();
    check35GetInputStreamQuarkusDoubleNoSizeTest();
    cpt.incrementAndGet();
    check41PostInputStreamQuarkusDoubleCompressedIntraTest();
    cpt.incrementAndGet();
    check43PostInputStreamQuarkusShaDoubleCompressedIntraTest();
    cpt.incrementAndGet();
    check51GetInputStreamQuarkusDoubleNoSizeCompressedIntraTest();
    cpt.incrementAndGet();
  }
}
