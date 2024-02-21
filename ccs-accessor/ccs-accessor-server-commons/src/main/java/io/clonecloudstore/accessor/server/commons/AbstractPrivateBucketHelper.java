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

package io.clonecloudstore.accessor.server.commons;

import java.util.Collection;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

public abstract class AbstractPrivateBucketHelper {
  //TODO : Security check and get Client ID
  private static final Logger LOGGER = Logger.getLogger(AbstractPrivateBucketHelper.class);
  protected final AccessorBucketServiceInterface service;

  protected AbstractPrivateBucketHelper(final AccessorBucketServiceInterface service) {
    this.service = service;
  }

  public Uni<Collection<AccessorBucket>> getBuckets(final String clientId, final String opId) {
    LOGGER.debug("List all buckets");
    return Uni.createFrom().emitter(em -> {
      try {
        final var buckets = service.getBuckets(clientId);
        em.complete(buckets);
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  public Uni<Response> checkBucket(final String bucketName, final boolean fullCheck, final String clientId,
                                   final String opId) {
    LOGGER.debugf("Get Bucket %s ", bucketName);
    return Uni.createFrom().emitter(em -> {
      try {
        final var santizedName = ParametersChecker.getSanitizedBucketName(bucketName);
        if (service.checkBucket(santizedName, fullCheck, clientId, opId, false)) {
          em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, StorageType.BUCKET).build());
        } else {
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  public Uni<AccessorBucket> getBucket(final String bucketName, final String clientId, final String opId) {
    LOGGER.debugf("Get Bucket %s - %s ", clientId, bucketName);
    return Uni.createFrom().emitter(em -> {
      try {
        final var santizedName = ParametersChecker.getSanitizedBucketName(bucketName);
        final var bucket = service.getBucket(santizedName, clientId, opId, false);
        em.complete(bucket);
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  public void close() {
    // Empty
  }
}
