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

package io.clonecloudstore.accessor.client.api;

import java.io.Closeable;
import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.quarkus.rest.client.reactive.ComputedParamContext;
import io.quarkus.rest.client.reactive.NotBody;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
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
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.NoCache;

import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

/**
 * API REST for Accessor Object
 */
@Path(AccessorConstants.Api.API_ROOT)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@RegisterProvider(ResponseObjectClientFilter.class)
@NoCache
public interface AccessorObjectApi extends Closeable {
  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{pathDirectoryOrObject:.+}")
  @HEAD
  @Operation(summary = "Check if object or directory exist", description = "Check if object or directory exist")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = AccessorConstants.Api.X_TYPE, description = "Type as StorageType", schema = @Schema(type =
          SchemaType.STRING, enumeration = {
          "NONE", "BUCKET", "DIRECTORY", "OBJECT"}))})
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") String bucketName,
                                       @PathParam("pathDirectoryOrObject") String pathDirectoryOrObject,
                                       @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID"
                                           , in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                           required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                       @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                           ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
                                           false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

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
  Uni<AccessorObject> getObjectInfo(@PathParam("bucketName") String bucketName,
                                    @PathParam("objectName") String objectName,
                                    @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                        in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                        required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                    @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                        schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue(
                                            "") @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{objectName:.+}")
  @DELETE
  @Operation(summary = "Delete object", description = "Delete object")
  @APIResponse(responseCode = "204", description = "OK")
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "406", description = "Bucket is not empty")
  @APIResponse(responseCode = "409", description = "Conflict since Object status not compatible with Operation")
  @APIResponse(responseCode = "410", description = "Object already deleted")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<Response> deleteObject(@PathParam("bucketName") String bucketName, @PathParam("objectName") String objectName,
                             @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                 ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                             @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                 schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

  default String computeCompressionModel(ComputedParamContext context) {
    int argPos = 0;
    if ((boolean) context.methodParameters().get(argPos).value()) {
      return COMPRESSION_ZSTD;
    }
    return null;
  }

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}")
  @PUT
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Operation(summary = "List objects from filter", description = "List objects from filter as a Stream of Json lines")
  @RequestBody(required = false, content = {}, description = "No content", ref = "Void")
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType =
      MediaType.APPLICATION_OCTET_STREAM, schema = @Schema(type = SchemaType.STRING, format = "binary")))
  @APIResponse(responseCode = "200", description = "OK")
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Parameters({
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
  @ClientHeaderParam(name = ACCEPT, value = MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT_ENCODING, value = "{computeCompressionModel}", required = false)
  Uni<InputStream> listObjects(@NotBody final boolean acceptCompression,
                               @PathParam("bucketName") final String bucketName,
                               @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                   ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                               @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                   schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{objectName:.+}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Operation(summary = "Get object", description = "Get object binary with type application/octet-stream and get " +
      "object metadata with type application/json")
  @RequestBody(required = false, content = {}, description = "No content", ref = "Void")
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
      @Schema(type = SchemaType.STRING, enumeration = {
          "UNKNOWN", "UPLOAD", "READY", "ERR_UPL", "DELETING", "DELETED", "ERR_DEL"})),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_EXPIRES, description = "Expiration Date", schema =
      @Schema(type = SchemaType.STRING))}, content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM, schema
      = @Schema(type = SchemaType.STRING, format = "binary")))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Parameters({
      @Parameter(name = ACCEPT, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false)})
  @ClientHeaderParam(name = ACCEPT, value = MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT_ENCODING, value = "{computeCompressionModel}", required = false)
  Uni<InputStream> getObject(@NotBody final boolean acceptCompression, @PathParam("bucketName") final String bucketName,
                             @PathParam("objectName") final String objectName,
                             @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                 ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                             @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                 schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{objectName:.+}")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create object", description = "Create object")
  @RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM), description =
      "InputStream as content")
  @APIResponse(responseCode = "201", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorObject.class)))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "406", description = "Object already in creation")
  @APIResponse(responseCode = "409", description = "Conflict since Object already exist or invalid")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Parameters({
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_ID, description = "Object Id", in =
          ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_SITE, description = "Object " +
          "Site", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_BUCKET, description = "Bucket Name", in =
          ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_NAME, description = "Object Name", in =
          ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_SIZE, description = "Object Size", in =
          ParameterIn.HEADER, schema = @Schema(type = SchemaType.INTEGER), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_HASH, description = "Object Hash", in =
          ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_METADATA, description = "Object Metadata as Json from" +
          " Map<String,String>", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.HeaderObject.X_OBJECT_EXPIRES, description = "Expiration Date", in =
          ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = false)})
  @ClientHeaderParam(name = CONTENT_TYPE, value = MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = "Transfer-Encoding", value = "chunked")
  @ClientHeaderParam(name = CONTENT_ENCODING, value = "{computeCompressionModel}", required = false)
  Uni<Response> createObject(@NotBody final boolean provideCompressed, @PathParam("bucketName") final String bucketName,
                             @PathParam("objectName") final String objectName,
                             @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                 ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                             @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                 schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId,
                             final InputStream inputStream);
}
