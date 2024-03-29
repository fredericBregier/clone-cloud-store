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

package io.clonecloudstore.reconciliator.database.mongodb;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.reconciliator.database.model.DaoSitesListingRepository;
import org.jboss.logging.Logger;

import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.BUCKET;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.DB;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.DRIVER;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.EVENT;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NAME;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.NSTATUS;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.REQUESTID;
import static io.clonecloudstore.reconciliator.database.model.DaoNativeListingRepository.SITE;

public class MgDaoReconciliationUtils {
  private static final Logger LOGGER = Logger.getLogger(MgDaoReconciliationUtils.class);
  public static final List<String> DEFAULT_PK = List.of(REQUESTID, BUCKET, NAME);
  public static final String MG_MATCH = "$match";
  public static final String MG_ADD_FIELDS = "$addFields";
  public static final String MG_PROJECT = "$project";
  public static final String MG_MERGE = "$merge";
  public static final String MG_WHEN_MATCHED = "whenMatched";
  public static final String MG_WHEN_NOT_MATCHED = "whenNotMatched";
  public static final String MG_KEEP_EXISTING = "keepExisting";
  public static final String MG_INSERT = "insert";
  public static final String MG_INTO = "into";
  public static final String MG_ON = "on";
  public static final String MG_REPLACE = "replace";
  public static final String MG_CONCAT_ARRAYS = "$concatArrays";
  public static final String MG_INDEX_OF_ARRAY = "$indexOfArray";
  public static final String MG_UNSET = "$unset";
  public static final String MG_COND = "$cond";
  public static final String MG_IN = "$in";
  public static final String MG_NIN = "$nin";
  public static final String MG_AND = "$and";
  public static final String MG_OR = "$or";
  public static final String MG_EXISTS = "$exists";
  public static final String MG_ARRAY_ELEM_AT = "$arrayElemAt";
  public static final String MG_ELEM_MATCH = "$elemMatch";
  public static final String MG_IF_NULL = "$ifNull";
  public static final String MG_FILTER = "$filter";
  public static final String MG_INPUT = "input";
  public static final String COND = "cond";
  public static final String MG_EQ = "$eq";
  public static final String IN = "in";
  public static final String MG_MAX = "$max";
  public static final String MG_LET = "$let";
  public static final String MG_IF = "if";
  public static final String MG_GTE = "$gte";
  public static final String MG_LTE = "$lte";
  public static final String MG_THEN = "then";
  public static final String MG_ELSE = "else";
  public static final String MG_VARS = "vars";
  public static final String MG_NE = "$ne";
  public static final String MG_NOT = "$not";
  public static final String MG_SIZE = "$size";
  public static final String MG_ALL = "$all";
  public static final String MG_SET_UNION = "$setUnion";
  public static final String MG_SET = "$set";
  public static final String MG_TO_LONG = "$toLong";
  public static final String MG_MULTIPLY = "$multiply";
  public static final String MG_RAND = "$rand";
  public static final String MG_CONCAT = "$concat";
  public static final String MG_TO_STRING = "$toString";
  public static final String MG_NEW = "$$new.";
  public static final String MG_THIS = "$$this.";
  public static final String MG_DISCARD = "discard";
  public static final String MG_MERGE_MATCHED = "merge";
  public static final String DRIVER_EVENT = DRIVER + "." + EVENT;
  public static final String DRIVER_SITE = DRIVER + "." + SITE;
  public static final String DB_EVENT = DB + "." + EVENT;
  public static final String DB_SITE = DB + "." + SITE;
  public static final String DB_NSTATUS = DB + "." + NSTATUS;
  public static final String LOCAL_SITE = DaoSitesListingRepository.LOCAL + "." + SITE;
  public static final String LOCAL_NSTATUS = DaoSitesListingRepository.LOCAL + "." + NSTATUS;
  public static final String MG_PUSH = "$push";
  public static final String MG_PULL = "$pull";
  public static final String MG_EACH = "$each";
  public static final String MG_SORT = "$sort";
  public static final String CCS_FIRST = "first";
  public static final String MG_FROM = "from";
  public static final String LOCAL_O_EVENT = "$" + DaoSitesListingRepository.LOCAL + ".0." + EVENT;
  public static final String GG_THIS = "$$this";

  private MgDaoReconciliationUtils() {
    // Empty
  }

  static void runInThread(final AtomicReference<CcsDbException> exceptionAtomicReference, final Runnable... runnables) {
    final CountDownLatch countDownLatch = new CountDownLatch(runnables.length);
    for (var runnable : runnables) {
      SystemTools.STANDARD_EXECUTOR_SERVICE.submit(() -> {
        try {
          runnable.run();
        } finally {
          countDownLatch.countDown();
        }
      });
    }
    try {
      countDownLatch.await();
    } catch (final InterruptedException e) { // NOSONAR intentional
      LOGGER.warn(e);
      exceptionAtomicReference.compareAndSet(null, new CcsDbException(e));
    }
  }
}
