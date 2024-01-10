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

package io.clonecloudstore.accessor.server.simple.resource;

import java.util.Collection;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.server.commons.AbstractAccessorPublicBucketResource;
import io.clonecloudstore.accessor.server.simple.application.AccessorBucketService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

/**
 * Bucket API Resource
 */
@Path(AccessorConstants.Api.API_ROOT)
public class AccessorPublicBucketResource extends AbstractAccessorPublicBucketResource {

  public AccessorPublicBucketResource(final AccessorBucketService service) {
    super(service);
  }

  @Override
  @Blocking
  public Uni<Collection<AccessorBucket>> getBuckets(final String clientId, final String opId) {
    return super.getBuckets(clientId, opId);
  }

  @Override
  @Blocking
  public Uni<Response> checkBucket(final String bucketName, final String clientId, final String opId) {
    return super.checkBucket(bucketName, clientId, opId);
  }

  @Override
  @Blocking
  public Uni<AccessorBucket> getBucket(final String bucketName, final String clientId, final String opId) {
    return super.getBucket(bucketName, clientId, opId);
  }

  @Override
  @Blocking
  public Uni<AccessorBucket> createBucket(final String bucketName, final String clientId, final String opId) {
    return super.createBucket(bucketName, clientId, opId);
  }

  @Override
  @Blocking
  public Uni<Response> deleteBucket(final String bucketName, final String clientId, final String opId) {
    return super.deleteBucket(bucketName, clientId, opId);
  }
}
