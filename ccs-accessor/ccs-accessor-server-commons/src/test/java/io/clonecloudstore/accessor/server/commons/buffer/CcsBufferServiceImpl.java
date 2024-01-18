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

package io.clonecloudstore.accessor.server.commons.buffer;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CcsBufferServiceImpl extends CcsBufferService {
  public static AccessorObject accessorObjectToReturn = null;

  @Override
  protected AccessorObject getAccessorObjectFromDb(final String bucket, final String object) {
    return accessorObjectToReturn;
  }

  @Override
  protected void updateStatusAccessorObject(final AccessorObject object, final AccessorStatus status) {
    Log.infof("Update %s with %s", object, status);
    object.setStatus(status);
  }
}
