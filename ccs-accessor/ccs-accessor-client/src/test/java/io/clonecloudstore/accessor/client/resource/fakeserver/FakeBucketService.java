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

package io.clonecloudstore.accessor.client.resource.fakeserver;

import java.time.Instant;
import java.util.Collection;

import io.clonecloudstore.accessor.client.api.AccessorBucketApi;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsServerExceptionMapper;
import io.clonecloudstore.common.standard.system.SingletonUtils;
import io.clonecloudstore.driver.api.StorageType;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

@Path(AccessorConstants.Api.API_ROOT)
public class FakeBucketService implements AccessorBucketApi {
  public static int errorCode = 0;

  @Override
  public Uni<Collection<AccessorBucket>> getBuckets(final String clientId, final String opId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode);
      }
      em.complete(SingletonUtils.singletonList());
    });
  }

  @Override
  public Uni<Response> checkBucket(final String bucketName, final String clientId, final String opId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode != 404) {
          throw CcsServerExceptionMapper.getCcsException(errorCode);
        }
        em.complete((Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
            .build()));
        return;
      }
      em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, StorageType.BUCKET).build());
    });
  }

  @Override
  public Uni<AccessorBucket> getBucket(final String bucketName, final String clientId, final String opId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode);
      }
      final var accessorBucket =
          new AccessorBucket().setSite("site").setStatus(AccessorStatus.READY).setCreation(Instant.now())
              .setId(bucketName).setClientId(clientId);
      em.complete(accessorBucket);
    });
  }

  @Override
  public Uni<AccessorBucket> createBucket(final String bucketName, final String clientId, final String opId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode);
      }
      final var accessorBucket =
          new AccessorBucket().setSite("site").setStatus(AccessorStatus.READY).setCreation(Instant.now())
              .setId(bucketName).setClientId(clientId);
      em.complete(accessorBucket);
    });
  }

  @Override
  public Uni<Response> deleteBucket(final String bucketName, final String clientId, final String opId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode);
      }
      em.complete(Response.noContent().build());
    });
  }

  @Override
  public void close() {
    // Empty
  }
}
