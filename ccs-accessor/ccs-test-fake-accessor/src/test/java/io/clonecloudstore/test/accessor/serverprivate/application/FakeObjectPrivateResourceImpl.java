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

package io.clonecloudstore.test.accessor.serverprivate.application;

import io.clonecloudstore.test.accessor.server.resource.internal.FakeObjectPrivateResourceAbstract;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.INTERNAL_ROOT;

@Path(INTERNAL_ROOT)
public class FakeObjectPrivateResourceImpl extends FakeObjectPrivateResourceAbstract<FakePrivateStreamHandlerImpl> {
  protected FakeObjectPrivateResourceImpl(final HttpHeaders httpHeaders) {
    super(httpHeaders);
  }
}
