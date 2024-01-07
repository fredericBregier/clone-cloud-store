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

package io.clonecloudstore.common.quarkus.client.example;

public class ApiConstants {
  public static final String X_LEN = "X-ccs-LEN";
  public static final String X_NAME = "X-ccs-NAME";
  public static final String X_CREATION_DATE = "X-ccs-CREATION-DATE";
  public static final String API_ROOT = "/test";
  public static final String API_COLLECTIONS = "/collections";
  public static final long MB10 = 10 * 1024 * 1024L;
  public static final long BIG_LONG = 1024L * 1024 * 1024 * 1024 * 1024L;
}
