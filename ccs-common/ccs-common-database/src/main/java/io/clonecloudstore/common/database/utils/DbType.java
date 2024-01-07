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

package io.clonecloudstore.common.database.utils;

import io.clonecloudstore.common.quarkus.properties.QuarkusSystemPropertyUtil;
import org.jboss.logging.Logger;

/**
 * Detect type of DB: Mongo ro Postgres
 */
public class DbType {
  public static final String CCS_DB_TYPE = "ccs.db.type";
  public static final String MONGO = "mongo";
  public static final String POSTGRE = "postgre";
  private static final Logger LOGGER = Logger.getLogger(DbType.class);
  private static final DbType INSTANCE = new DbType();
  private final boolean isMongoDbType;

  private DbType() {
    this.isMongoDbType = checkMongoDbType();
  }

  /**
   * Use to filter on Mongo code
   *
   * @return True if Mongo is enabled
   */
  public boolean isMongoDbType() {
    return isMongoDbType;
  }

  public static DbType getInstance() {
    return INSTANCE;
  }

  /**
   * Use to filter on Mongo code
   *
   * @return True if Mongo is enabled
   */
  private static boolean checkMongoDbType() {
    final var value = QuarkusSystemPropertyUtil.getStringConfig(CCS_DB_TYPE, "");
    LOGGER.debugf("Found DbType: %s", value);
    return !POSTGRE.equals(value);
  }
}
