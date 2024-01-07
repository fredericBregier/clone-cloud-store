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

package io.clonecloudstore.test.accessor.server.resource.internal;

import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.test.accessor.common.FakeNativeStreamHandlerAbstract;
import io.clonecloudstore.test.accessor.common.FakeObjectServiceAbstract;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.headers.Header;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.FULL_CHECK;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Dependent
public abstract class FakeObjectPrivateServiceAbstract<H extends FakeNativeStreamHandlerAbstract>
    extends FakeObjectServiceAbstract<H> {
  @Override
  protected boolean isPublic() {
    return false;
  }

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}")
  @PUT
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Operation(summary = "List objects from filter", description = "List objects from filter as a Stream of Json lines")
  @Parameters({
      @Parameter(name = ACCEPT, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX, description = "Filter based on name " +
          "prefix", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_STATUSES, description = "Filter based on list of " +
          "status", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE, description = "Filter based on " +
          "creation before", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER, description = "Operation Filter " +
          "based on creation after", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
          false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE, description = "Operation Filter " +
          "based on expires before", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
          false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER, description = "Operation Filter " +
          "based on expires after", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
          false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT, description = "Operation Filter based on" +
          " size less than", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.INTEGER), required = false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT, description = "Operation Filter based on" +
          " size greater than", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.INTEGER), required = false),
      @Parameter(name = AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ, description = "Filter based on " +
          "metadatata containing", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
          false)})
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType =
      MediaType.APPLICATION_OCTET_STREAM))
  @APIResponse(responseCode = "200", description = "OK")
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Blocking
  public Uni<InputStream> listObjects(@PathParam("bucketName") final String bucketName,
                                      @HeaderParam(ACCEPT) final String acceptHeader,
                                      @DefaultValue("") @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                      @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                          in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                          required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                      @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                          ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
                                          false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
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

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{pathDirectoryOrObject:.+}")
  @HEAD
  @Operation(summary = "Check if object or directory exist", description = "Check if object or directory exist")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = AccessorConstants.Api.X_TYPE, description = "Type as StorageType", schema = @Schema(type =
          SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "500", description = "Internal Error")
  public Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") String bucketName,
                                              @PathParam("pathDirectoryOrObject") String pathDirectoryOrObject,
                                              @Parameter(name = FULL_CHECK, description = "If True implies Storage " +
                                                  "checking", in = ParameterIn.QUERY, schema = @Schema(type =
                                                  SchemaType.BOOLEAN), required = false) @DefaultValue("false") @QueryParam(FULL_CHECK) final boolean fullCheck,
                                              @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description =
                                                  "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                              @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                                  ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                                  required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId) {
    final var decodedName = ParametersChecker.getSanitizedName(pathDirectoryOrObject);
    return checkObjectOrDirectory0(bucketName, decodedName, fullCheck, clientId, isPublic());
  }

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{objectName:.+}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get object", description = "Get object binary with type application/octet-stream and get " +
      "object metadata with type application/json")
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorObject.class)))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  public Uni<AccessorObject> getObjectInfo(@PathParam("bucketName") String bucketName,
                                           @PathParam("objectName") String objectName,
                                           @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client" +
                                               " ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                               SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                           @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                               ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                               required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    return getObjectInfo0(bucketName, decodedName, clientId, isPublic());
  }

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{objectName:.+}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Operation(summary = "Get object", description = "Get object binary with type application/octet-stream and get " +
      "object metadata with type application/json")
  @Parameters({
      @Parameter(name = ACCEPT, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = false)})
  @APIResponse(responseCode = "200", description = "OK", headers = {
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_ID, description = "Id", schema = @Schema(type =
          SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_SITE, description = "Site", schema = @Schema(type =
          SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_BUCKET, description = "Bucket Name", schema =
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_NAME, description = "Object Name", schema =
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_CREATION, description = "Creation Date", schema =
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_SIZE, description = "Object Size", schema =
      @Schema(type = SchemaType.INTEGER)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_HASH, description = "Object Hash SHA-256", schema =
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_METADATA, description = "Object Metadata", schema =
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_STATUS, description = "Object Status", schema =
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_EXPIRES, description = "Expiration Date", schema =
      @Schema(type = SchemaType.STRING))}, content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Blocking
  public Uni<Response> getObject(@PathParam("bucketName") final String bucketName,
                                 @PathParam("objectName") final String objectName,
                                 @HeaderParam(ACCEPT) final String acceptHeader,
                                 @DefaultValue("") @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                 @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client" +
                                     " ID", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                     required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                 @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                     schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
                                 final HttpServerRequest request, @Context final Closer closer) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    return getObject0(bucketName, decodedName, acceptHeader, acceptEncodingHeader, clientId, opId, isPublic(), request,
        closer);
  }
}
