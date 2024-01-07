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

package io.clonecloudstore.replicator.server.remote.client.api;

import java.lang.reflect.Method;
import java.net.URI;

import io.quarkus.arc.Unremovable;
import io.quarkus.cache.CacheKeyGenerator;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
@Unremovable
public class RemoteReplicatorApiKeyGenerator implements CacheKeyGenerator {
  private static final Logger LOGGER = Logger.getLogger(RemoteReplicatorApiKeyGenerator.class);

  @Override
  public Object generate(final Method method, final Object... methodParams) {
    var pos = 0;
    boolean isBucket;
    switch (methodParams.length) {
      case 3 -> isBucket = false;
      case 6 -> {
        pos = 1;
        isBucket = true;
      }
      case 7 -> {
        pos = 1;
        isBucket = false;
      }
      default -> isBucket = true;
    }
    if (isBucket) {
      LOGGER.debugf("BKEY: %s%d/%s", ((URI) methodParams[pos]).getHost(), ((URI) methodParams[pos]).getPort(),
          methodParams[pos + 1]);
      return ((URI) methodParams[pos]).getHost() + ((URI) methodParams[pos]).getPort() + "/" + methodParams[pos + 1];
    }
    LOGGER.debugf("OKEY: %s%d/%s/%s", ((URI) methodParams[pos]).getHost(), ((URI) methodParams[pos]).getPort(),
        methodParams[pos + 1], methodParams[pos + 2]);
    return ((URI) methodParams[pos]).getHost() + ((URI) methodParams[pos]).getPort() + "/" + methodParams[pos + 1] +
        "/" + methodParams[pos + 2];
  }
}
