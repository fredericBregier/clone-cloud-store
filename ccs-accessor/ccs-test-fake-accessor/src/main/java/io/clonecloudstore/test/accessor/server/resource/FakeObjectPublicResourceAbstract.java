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

import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.test.accessor.common.FakeAccessorObjectPublicAbstract;
import io.clonecloudstore.test.accessor.common.FakeNativeStreamHandlerAbstract;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@Dependent
public abstract class FakeObjectPublicResourceAbstract<H extends FakeNativeStreamHandlerAbstract>
    extends FakeAccessorObjectPublicAbstract<H> {
  @Override
  public Uni<Response> listObjects(@PathParam("bucketName") final String bucketName,
                                   @HeaderParam(ACCEPT) final String acceptHeader,
                                   @DefaultValue("") @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                   @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                       in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                       = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                   @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX) final String xNamePrefix,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_STATUSES) final String xStatuses,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE) final String xCreationBefore,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER) final String xCreationAfter,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE) final String xExpiresBefore,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER) final String xExpiresAfter,
                                   @DefaultValue("0") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT) final long xSizeLt,
                                   @DefaultValue("0") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT) final long xSizeGt,
                                   @DefaultValue("") @HeaderParam(AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ) final String xMetadataEq,
                                   final HttpServerRequest request, @Context final Closer closer) {
    return listObjects0(bucketName, acceptHeader, acceptEncodingHeader, clientId, opId, xNamePrefix, xStatuses,
        xCreationBefore, xCreationAfter, xExpiresBefore, xExpiresAfter, xSizeLt, xSizeGt, xMetadataEq, isPublic(),
        request, closer);
  }

  @Override
  public Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") final String bucketName,
                                              @PathParam("pathDirectoryOrObject") final String pathDirectoryOrObject,
                                              @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description =
                                                  "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                              @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                                  ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                                  required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId) {
    final var decodedName = ParametersChecker.getSanitizedName(pathDirectoryOrObject);
    return checkObjectOrDirectory0(bucketName, decodedName, false, clientId, isPublic());
  }

  @Override
  public Uni<AccessorObject> getObjectInfo(@PathParam("bucketName") final String bucketName,
                                           @PathParam("objectName") final String objectName,
                                           @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client" +
                                               " ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                               SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                           @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                               ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                               required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    return getObjectInfo0(bucketName, decodedName, clientId, isPublic());
  }

  @Override
  public Uni<Response> deleteObject(@PathParam("bucketName") final String bucketName,
                                    @PathParam("objectName") final String objectName,
                                    @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                        in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                        required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                    @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                        schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue(
                                            "") @HeaderParam(X_OP_ID) final String opId) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    return deleteObject0(bucketName, decodedName, clientId, isPublic());
  }

  @Override
  public Uni<Response> createObject(final HttpServerRequest request, @Context final Closer closer,
                                    @PathParam("bucketName") final String bucketName,
                                    @PathParam("objectName") final String objectName,
                                    @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) @HeaderParam(CONTENT_TYPE) final String contentTypeHeader,
                                    @HeaderParam(CONTENT_ENCODING) final String contentEncodingHeader,
                                    @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                    @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                        schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_ID) final String xObjectId,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_SITE) final String xObjectSite,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_BUCKET) final String xObjectBucket,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_NAME) final String xObjectName,
                                    @DefaultValue("0") @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_SIZE) final long xObjectSize,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_HASH) final String xObjectHash,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_METADATA) final String xObjectMetadata,
                                    @HeaderParam(AccessorConstants.HeaderObject.X_OBJECT_EXPIRES) final String xObjectExpires,
                                    final InputStream inputStream) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    return createObject0(request, closer, bucketName, decodedName, contentTypeHeader, contentEncodingHeader, clientId,
        opId, xObjectId, xObjectSite, xObjectBucket, xObjectName, xObjectSize, xObjectHash, xObjectMetadata,
        xObjectExpires, isPublic(), inputStream);
  }

  @Override
  public Uni<Response> getObject(@PathParam("bucketName") final String bucketName,
                                 @PathParam("objectName") final String objectName,
                                 @HeaderParam(ACCEPT) final String acceptHeader,
                                 @DefaultValue("") @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                 @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                     ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                 @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                     schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
                                 final HttpServerRequest request, @Context final Closer closer) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    return getObject0(bucketName, decodedName, acceptHeader, acceptEncodingHeader, clientId, opId, isPublic(), request,
        closer);
  }
}
