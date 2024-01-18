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

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.quarkus.server.service.ServerStreamHandlerResponseException;
import io.clonecloudstore.common.quarkus.server.service.StreamHandlerAbstract;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

public abstract class AbstractPrivateObjectHelper<H extends StreamHandlerAbstract<AccessorObject, AccessorObject>>
    extends StreamServiceAbstract<AccessorObject, AccessorObject, H> {
  private static final Logger LOGGER = Logger.getLogger(AbstractPrivateObjectHelper.class);
  private static final String BUCKETNAME_OBJECT = "BucketName: %s Directory or Object ID: %s";

  private final AccessorObjectServiceInterface service;

  protected AbstractPrivateObjectHelper(final AccessorObjectServiceInterface service) {
    this.service = service;
  }

  public Uni<Response> listObjects(final String bucketName, final String acceptHeader,
                                   final String acceptEncodingHeader, final String clientId, final String opId,
                                   final String xNamePrefix, final String xStatuses, final String xCreationBefore,
                                   final String xCreationAfter, final String xExpiresBefore, final String xExpiresAfter,
                                   final long xSizeLt, final long xSizeGt, final String xMetadataEq,
                                   final HttpServerRequest request, final Closer closer) {
    final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
    LOGGER.debugf(BUCKETNAME_OBJECT, decodedBucket, request.method().name());
    final var object = new AccessorObject().setBucket(decodedBucket);
    return readObjectList(request, closer, object, false);
  }

  public Uni<Response> checkObjectOrDirectory(final String bucketName, final String pathDirectoryOrObject,
                                              final boolean fullCheck, final String clientId, final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
      final var finalObjectName = ParametersChecker.getSanitizedObjectName(pathDirectoryOrObject);
      LOGGER.debugf(BUCKETNAME_OBJECT, decodedBucket, finalObjectName);
      try {
        final var storageType = service.objectOrDirectoryExists(decodedBucket, finalObjectName, fullCheck, clientId);
        if (storageType.equals(StorageType.NONE)) {
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, storageType).build());
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  public Uni<AccessorObject> getObjectInfo(final String bucketName, final String objectName, final String clientId,
                                           final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
      final var finalObjectName = ParametersChecker.getSanitizedObjectName(objectName);
      LOGGER.debugf(BUCKETNAME_OBJECT, decodedBucket, finalObjectName);
      try {
        final var object = service.getObjectInfo(decodedBucket, finalObjectName, clientId);
        em.complete(object);
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  /**
   * Returns both the content Object and the associated DTO through Headers
   */
  public Uni<Response> getObject(final String bucketName, final String objectName, final String acceptHeader,
                                 final String acceptEncodingHeader, final String clientId, final String opId,
                                 final HttpServerRequest request, final Closer closer) {
    final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
    final var finalObjectName = ParametersChecker.getSanitizedObjectName(objectName);
    LOGGER.debugf(BUCKETNAME_OBJECT, decodedBucket, finalObjectName);
    final var object = new AccessorObject().setBucket(decodedBucket).setName(finalObjectName);
    return readObject(request, closer, object, false);
  }

  /**
   * Listing of Objects
   *
   * @param alreadyCompressed If True, and if the InputStream is to be compressed, will be kept as is; else will
   *                          compress the InputStream if it has to be
   */
  protected Uni<Response> readObjectList(final HttpServerRequest request, final Closer closer,
                                         final AccessorObject businessIn, final boolean alreadyCompressed) {
    LOGGER.debugf("GET start");
    getNativeStream().setup(request, closer, false, businessIn, 0, null, alreadyCompressed, false);
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(getNativeStream().pullList());
      } catch (ServerStreamHandlerResponseException e) {
        em.complete(e.getResponse());
      } catch (final Exception e) {
        em.complete(createErrorResponse(e));
      }
    });
  }
}
