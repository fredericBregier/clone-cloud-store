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

package io.clonecloudstore.replicator.test.fake;

import io.clonecloudstore.test.accessor.server.resource.internal.FakeObjectPrivateResourceAbstract;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_BUCKETS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.LOCAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.REPLICATOR_ROOT;

@Path(REPLICATOR_ROOT + LOCAL + COLL_BUCKETS)
public class FakeObjectResourceImpl extends FakeObjectPrivateResourceAbstract<FakePrivateStreamHandlerImpl> {
  protected FakeObjectResourceImpl(final HttpHeaders httpHeaders) {
    super(httpHeaders);
  }
}

