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

package io.clonecloudstore.test.accessor.common;

import java.util.Collection;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.core.Response;

@Dependent
public abstract class FakeBucketPublicAbstract {
  protected boolean isPublic() {
    return true;
  }

  protected Uni<Collection<AccessorBucket>> getBuckets0(final String clientId) {
    return FakeCommonBucketResourceHelper.getBuckets0Helper(clientId);
  }

  protected Uni<Response> checkBucket0(final String bucketName, final boolean fullCheck, final String clientId) {
    return FakeCommonBucketResourceHelper.checkBucket0Helper(bucketName);
  }

  protected Uni<AccessorBucket> getBucket0(final String bucketName, final String clientId) {
    return FakeCommonBucketResourceHelper.getBucket0Helper(bucketName, clientId);
  }

  protected Uni<AccessorBucket> createBucket0(final String bucketName, final String clientId) {
    return FakeCommonBucketResourceHelper.createBucket0Helper(bucketName, clientId);
  }

  protected Uni<Response> deleteBucket0(final String bucketName, final String clientId) {
    return FakeCommonBucketResourceHelper.deleteBucket0Helper(bucketName, clientId);
  }
}