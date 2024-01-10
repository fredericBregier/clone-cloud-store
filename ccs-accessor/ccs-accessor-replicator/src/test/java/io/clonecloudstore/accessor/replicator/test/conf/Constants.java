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

package io.clonecloudstore.accessor.replicator.test.conf;

import java.util.UUID;

import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;

public class Constants {
  public static final String BUCKET_NAME = "test-bucket";
  public static final String TOPOLOGY_NAME = "test-topology";
  public static final String SITE = "test-site";
  public static final String CLIENT_ID = UUID.randomUUID().toString();
  public static final String OP_ID = UUID.randomUUID().toString();
  public static final String OBJECT_PATH = "/test-file.txt";
  public static final String OBJECT_HASH = "hash";
  public static final String OBJECT_REPLICATED_PATH = "/test-file-replicated.txt";
  public static final String URI_SERVER = "http://localhost:8081";
  public static final String BUCKET_ID =
      FakeCommonBucketResourceHelper.getBucketTechnicalName(CLIENT_ID, BUCKET_NAME, true);

  public static final boolean FULL_CHECK = true;

  public static final int REMOTE_READ_STREAM_LEN = 123;
  public static final String EXCEPTION_MESSAGE = "test exception message";
  public static final String EXCEPTION_MESSAGE_THROWN = "yes another test exception message";

}
