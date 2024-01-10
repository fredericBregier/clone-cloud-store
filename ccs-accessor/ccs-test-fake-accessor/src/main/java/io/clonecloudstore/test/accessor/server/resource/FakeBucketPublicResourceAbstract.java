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

package io.clonecloudstore.test.accessor.server.resource;

import java.util.Collection;

import io.clonecloudstore.accessor.client.api.AccessorBucketApi;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.test.accessor.common.FakeBucketPublicAbstract;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.core.Response;

@Dependent
public abstract class FakeBucketPublicResourceAbstract extends FakeBucketPublicAbstract implements AccessorBucketApi {
  @Override
  protected boolean isPublic() {
    return true;
  }

  @Override
  public Uni<Collection<AccessorBucket>> getBuckets(final String clientId, final String opId) {
    return getBuckets0(clientId);
  }

  @Override
  public Uni<Response> checkBucket(final String bucketName, final String clientId, final String opId) {
    return checkBucket0(bucketName, false, clientId, isPublic());
  }

  @Override
  public Uni<AccessorBucket> getBucket(final String bucketName, final String clientId, final String opId) {
    return getBucket0(bucketName, clientId, isPublic());
  }

  @Override
  public Uni<AccessorBucket> createBucket(final String bucketName, final String clientId, final String opId) {
    return createBucket0(bucketName, clientId, isPublic());
  }

  @Override
  public Uni<Response> deleteBucket(final String bucketName, final String clientId, final String opId) {
    return deleteBucket0(bucketName, clientId, isPublic());
  }

  @Override
  public void close() {
    // Empty
  }
}
