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

import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Metrics can be heavy cost in list operation, this class helps to limit this cost
 */
@ApplicationScoped
@Unremovable
public class BulkMetrics {
  public static final String PREFIX_NAME = "ccs.";
  public static final String KEY_BUCKET = "bucket";
  public static final String KEY_OBJECT = "object";
  public static final String KEY_ORDER = "order";
  public static final String KEY_SITE = "site";
  public static final String TAG_ERROR = "error";
  public static final String TAG_ERROR_WRITE = "error_write";
  public static final String TAG_ERROR_READ = "error_read";
  public static final String TAG_ERROR_DELETE = "error_delete";
  public static final String TAG_CREATE = "create";
  public static final String TAG_DELETE = "delete";
  public static final String TAG_REPLICATE = "replicate";
  public static final String TAG_COUNT = "count";
  public static final String TAG_STREAM = "stream";
  public static final String TAG_EXISTS = "exists";
  public static final String TAG_READ_MD = "read_md";
  public static final String TAG_READ = "read";
  public static final String TAG_COPY = "copy";
  public static final String TAG_PURGE = "purge";
  public static final String TAG_ARCHIVE = "archive";
  public static final String TAG_FROM_DB = "from.db";
  public static final String TAG_FROM_DRIVER = "from.driver";
  public static final String TAG_UPDATE_FROM_DRIVER = "update_from_driver";
  public static final String TAG_TO_SITES_LISTING = "to.sites_listing";
  public static final String TAG_TO_REMOTE_SITE = "to.remote_site";
  public static final String TAG_FROM_REMOTE_SITE = "from.remote_site";
  public static final String TAG_FROM_REMOTE_SITES_LISTING = "from.remote_sites_listing";
  public static final String TAG_TO_ACTIONS = "to.actions";
  public static final String TAG_REGISTER = "register";
  public static final String TAG_UNREGISTER = "unregister";
  private static final Map<String, Counter> internalCountersMap = new HashMap<>();

  String getName(final Class<?> name) {
    return PREFIX_NAME + name.getSimpleName().toLowerCase();
  }

  public void incrementCounter(final long value, final Class<?> name, final String... tagPairs) {
    incrementCounter(value, getName(name), tagPairs);
  }

  public synchronized Counter getCounter(final String name, final String... tagPairs) {
    final var internalName = name + "#" + String.join("|", tagPairs);
    return internalCountersMap.computeIfAbsent(internalName, key -> getCounterInternal(name, tagPairs));
  }

  public void incrementCounter(final long value, final String name, final String... tagPairs) {
    if (value <= 0) {
      return;
    }
    final var counter = getCounter(name, tagPairs);
    counter.increment(value);
  }

  public Counter getCounter(final Class<?> name, final String... tagPairs) {
    return getCounter(getName(name), tagPairs);
  }

  synchronized Counter getCounterInternal(final String name, final String... tagPairs) {
    try {
      return Metrics.globalRegistry.get(name).tags(tagPairs).counter();
    } catch (final MeterNotFoundException e) {
      return Counter.builder(name).baseUnit(BaseUnits.OPERATIONS).tags(tagPairs).register(Metrics.globalRegistry);
    }
  }
}
