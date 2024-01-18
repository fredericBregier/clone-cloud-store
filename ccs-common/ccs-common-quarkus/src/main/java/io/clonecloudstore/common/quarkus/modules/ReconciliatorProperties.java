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

package io.clonecloudstore.common.quarkus.modules;

import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Reconciliator Configurable Values
 */
@ApplicationScoped
@Unremovable
public class ReconciliatorProperties extends AccessorProperties {
  public static final String CCS_RECONCILIATOR_THREADS = "ccs.reconciliator.threads";
  public static final String CCS_RECONCILIATOR_PURGE_LOG = "ccs.reconciliator.purge.log";
  private static int reconciliatorThreads = Math.max(
      QuarkusSystemPropertyUtil.getIntegerConfig(CCS_RECONCILIATOR_THREADS,
          Runtime.getRuntime().availableProcessors() / 2), 2);
  private static boolean reconciliatorPurgeLog =
      QuarkusSystemPropertyUtil.getBooleanConfig(CCS_RECONCILIATOR_PURGE_LOG, true);

  /**
   * @return the number of threads to use in reconciliation steps (between 2 and number of cores)
   */
  public static int getReconciliatorThreads() {
    return reconciliatorThreads;
  }

  /**
   * @return True if the log are active during purge
   */
  public static boolean isReconciliatorPurgeLog() {
    return reconciliatorPurgeLog;
  }

  public static void setCcsReconciliatorPurgeLog(final boolean purgeLog) {
    reconciliatorPurgeLog = purgeLog;
  }

  public static String confugrationToString() {
    return String.format("%s, \"%s\":%d, \"%s\":%b", AccessorProperties.confugrationToString(),
        CCS_RECONCILIATOR_THREADS, getReconciliatorThreads(), CCS_RECONCILIATOR_PURGE_LOG, isReconciliatorPurgeLog());
  }

  protected ReconciliatorProperties() {
    // Nothing
  }
}
