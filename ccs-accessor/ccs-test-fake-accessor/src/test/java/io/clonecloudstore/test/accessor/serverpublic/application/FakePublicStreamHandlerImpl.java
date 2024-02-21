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

package io.clonecloudstore.test.accessor.serverpublic.application;

import java.io.InputStream;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.test.accessor.common.FakeStreamHandlerAbstract;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class FakePublicStreamHandlerImpl extends FakeStreamHandlerAbstract {

  @Override
  protected boolean isPublic() {
    return true;
  }

  @Override
  protected void remoteCreation(final AccessorObject objectOut, final String clientId) {
    // Here does nothing
    super.remoteCreation(objectOut, clientId);
  }

  @Override
  protected boolean checkRemotePullable(final AccessorObject object, final MultiMap headers, final String clientId) {
    // Here does return false
    return super.checkRemotePullable(object, headers, clientId);
  }

  @Override
  protected InputStream getRemotePullInputStream(final AccessorObject object, final String clientId) {
    // Here does throw not found
    return super.getRemotePullInputStream(object, clientId);
  }
}
