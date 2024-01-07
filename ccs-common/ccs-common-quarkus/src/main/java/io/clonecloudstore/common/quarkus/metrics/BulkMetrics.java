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

package io.clonecloudstore.common.quarkus.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Metrics can be heavy cost in list operation, this class helps to limit this cost
 */
@ApplicationScoped
@Unremovable
public class BulkMetrics {
  public static final String KEY_BUCKET = "bucket";
  public static final String KEY_OBJECT = "object";

  public Counter getCounter(final Class<?> name, final String... tagPairs) {
    return getCounter(name.getName().toLowerCase(), tagPairs);
  }

  public synchronized Counter getCounter(final String name, final String... tagPairs) {
    final var counter = Metrics.globalRegistry.find(name).tags(tagPairs).counter();
    if (counter != null) {
      return counter;
    }
    return Counter.builder(name).baseUnit(BaseUnits.EVENTS).tags(tagPairs).register(Metrics.globalRegistry);
  }
}
