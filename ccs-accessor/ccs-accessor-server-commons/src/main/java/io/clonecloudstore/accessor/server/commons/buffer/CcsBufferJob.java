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

package io.clonecloudstore.accessor.server.commons.buffer;

import io.quarkus.arc.Unremovable;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;

@ApplicationScoped
@Unremovable
public class CcsBufferJob {
  private final CcsBufferService bufferService;

  public CcsBufferJob() {
    this.bufferService = CDI.current().select(CcsBufferService.class).get();
  }

  @Scheduled(every = "${ccs.accessor.store.schedule.delay:10s}", skipExecutionIf = CcsBufferSkipPredicate.class,
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void ccsBufferJob() {
    bufferService.asyncJobRetryImport();
    bufferService.asyncJobCleanup();
  }
}
