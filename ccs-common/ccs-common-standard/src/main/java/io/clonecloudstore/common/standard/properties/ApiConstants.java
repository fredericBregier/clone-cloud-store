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

package io.clonecloudstore.common.standard.properties;

public class ApiConstants {

  public static final String X_OP_ID = "x-clonecloudstore-op-id";
  public static final String X_ERROR = "x-clonecloudstore-error";
  public static final String X_MODULE = "x-clonecloudstore-module";
  /**
   * ZSTD compression
   */
  public static final String COMPRESSION_ZSTD = "zstd";
  public static final String TRANSFER_ENCODING = "Transfer-Encoding";
  public static final String CHUNKED = "chunked";
  public static final String CONNECTION = "Connection";
  public static final String CLOSE = "close";

  private ApiConstants() {
  }
}
