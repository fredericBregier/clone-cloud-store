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
import io.clonecloudstore.common.quarkus.server.service.NativeServerResponseException;
import io.clonecloudstore.common.quarkus.server.service.NativeStreamHandlerAbstract;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.FULL_CHECK;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.server.commons.AbstractPublicBucketHelper.getTechnicalBucketName;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

public abstract class AbstractPrivateObjectHelper<H extends NativeStreamHandlerAbstract<AccessorObject, AccessorObject>>
    extends StreamServiceAbstract<AccessorObject, AccessorObject, H> {
  private static final Logger LOGGER = Logger.getLogger(AbstractPrivateObjectHelper.class);
  private static final String BUCKETNAME_OBJECT = "BucketName: %s Directory or Object ID: %s";

  private final AccessorObjectServiceInterface service;

  protected AbstractPrivateObjectHelper(final AccessorObjectServiceInterface service) {
    this.service = service;
  }

  public Uni<Response> listObjects(@PathParam("bucketName") final String bucketName,
                                   @HeaderParam(ACCEPT) final String acceptHeader,
                                   @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                   @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                       in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                       = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                   @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX) final String xNamePrefix,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_STATUSES) final String xStatuses,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE) final String xCreationBefore,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER) final String xCreationAfter,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE) final String xExpiresBefore,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER) final String xExpiresAfter,
                                   @DefaultValue("0") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT) final long xSizeLt,
                                   @DefaultValue("0") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT) final long xSizeGt,
                                   @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ) final String xMetadataEq,
                                   final HttpServerRequest request, @Context final Closer closer) {
    final var finalBucketName = getTechnicalBucketName(clientId, bucketName, false);
    LOGGER.debugf(BUCKETNAME_OBJECT, finalBucketName, request.method().name());
    final var object = new AccessorObject().setBucket(finalBucketName);
    // TODO choose compression model
    return readObjectList(request, closer, object, false);
  }

  public Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") String bucketName,
                                              @PathParam("pathDirectoryOrObject") String pathDirectoryOrObject,
                                              @Parameter(name = FULL_CHECK, description = "If True implies Storage " +
                                                  "checking", in = ParameterIn.QUERY, schema = @Schema(type =
                                                  SchemaType.BOOLEAN), required = false) @DefaultValue("false") @QueryParam(FULL_CHECK) boolean fullCheck,
                                              @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description =
                                                  "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) String clientId,
                                              @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                                  ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                                  required = false) @HeaderParam(X_OP_ID) final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var finalBucketName = getTechnicalBucketName(clientId, bucketName, false);
      final var finalObjectName = ParametersChecker.getSanitizedName(pathDirectoryOrObject);
      LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, finalObjectName);
      try {
        final var storageType = service.objectOrDirectoryExists(finalBucketName, finalObjectName, fullCheck);
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

  public Uni<AccessorObject> getObjectInfo(@PathParam("bucketName") final String bucketName,
                                           @PathParam("objectName") final String objectName,
                                           @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client" +
                                               " ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                               SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                           @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                               ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                               required = false) @HeaderParam(X_OP_ID) final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var finalBucketName = getTechnicalBucketName(clientId, bucketName, false);
      final var finalObjectName = ParametersChecker.getSanitizedName(objectName);
      LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, finalObjectName);
      try {
        final var object = service.getObjectInfo(finalBucketName, finalObjectName);
        em.complete(object);
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  /**
   * Returns both the content Object and the associated DTO through Headers
   */
  public Uni<Response> getObject(@PathParam("bucketName") final String bucketName,
                                 @PathParam("objectName") final String objectName,
                                 @HeaderParam(ACCEPT) final String acceptHeader,
                                 @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                 @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                     ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                 @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                     schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId,
                                 final HttpServerRequest request, @Context final Closer closer) {
    final var finalBucketName = getTechnicalBucketName(clientId, bucketName, false);
    final var finalObjectName = ParametersChecker.getSanitizedName(objectName);
    LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, finalObjectName);
    final var object = new AccessorObject().setBucket(finalBucketName).setName(finalObjectName);
    // TODO choose compression model
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
    getNativeStream().setup(request, closer, false, businessIn, 0, null, alreadyCompressed);
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(getNativeStream().pullList());
      } catch (NativeServerResponseException e) {
        em.complete(e.getResponse());
      } catch (final Exception e) {
        em.complete(createErrorResponse(e));
      }
    });
  }
}
