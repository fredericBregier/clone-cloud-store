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

package io.clonecloudstore.common.database.postgre;

import java.util.concurrent.atomic.AtomicInteger;

import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;

/**
 * Postgre implementation of the BulkHelper
 */
public class PostgreBulkHelper {
  private int maxBatch =
      QuarkusSystemPropertyUtil.getIntegerConfig("quarkus.hibernate-orm.jdbc.statement-batch-size", 50);
  private final AtomicInteger bulkCount = new AtomicInteger(0);

  /**
   * Constructor
   */
  public PostgreBulkHelper() {
    // Empty
  }

  protected void changeBulkSize(final int bulkSize) {
    if (bulkSize > 0) {
      maxBatch = bulkSize;
    }
  }

  protected int getBulkSize() {
    return maxBatch;
  }

  /**
   * @return True if bulk operation reaches the limit
   */
  public boolean addToBulk() {
    final var cpt = bulkCount.addAndGet(1);
    final var toFlush = cpt >= getMaxBatch();
    if (toFlush) {
      resetBulk();
    }
    return toFlush;
  }

  /**
   * @return the current max batch value
   */
  protected int getMaxBatch() {
    return maxBatch;
  }

  public void resetBulk() {
    bulkCount.set(0);
  }
}
