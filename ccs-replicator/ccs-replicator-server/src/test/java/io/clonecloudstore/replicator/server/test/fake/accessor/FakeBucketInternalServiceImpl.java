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

package io.clonecloudstore.replicator.server.test.fake.accessor;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.test.accessor.server.resource.internal.FakeBucketPrivateServiceAbstract;
import jakarta.ws.rs.Path;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.INTERNAL_ROOT;

@Path(INTERNAL_ROOT)
public class FakeBucketInternalServiceImpl extends FakeBucketPrivateServiceAbstract {
  private static final Logger logger = Logger.getLogger(FakeBucketInternalServiceImpl.class);

  @Override
  protected void remoteCreateBucket(final AccessorBucket accessorBucket, final String clientId) {
    logger.debug(accessorBucket);
    // Empty
  }

  @Override
  protected void remoteDeleteBucket(final AccessorBucket accessorBucket, final String clientId) {
    logger.debug(accessorBucket);
    // Empty
  }
}
