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

package io.clonecloudstore.administration.resource.conf;

import java.util.UUID;

public class Constants {
  public static final String SITE = "test-site";
  public static final String SITE_NOT_FOUND = "test-site-not-found";
  public static final String CLIENT_ID = UUID.randomUUID().toString();
  public static final String TOPOLOGY_NAME = "test-topology";
  public static final String URI_SERVER = "http://localhost:8081";
  public static final String OWNERSHIP_BUCKET = "test-bucket";

}
