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
import java.util.Collection;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.client.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.RequestHeaderFactory;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
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
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.NoCache;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.TAG_PUBLIC;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

/**
 * API REST for Accessor Bucket
 */
@Path(AccessorConstants.Api.API_ROOT)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@RegisterProvider(ResponseBucketClientFilter.class)
@NoCache
public interface AccessorBucketApi extends Closeable {
  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_BUCKET)
  @Path("/")
  @Operation(summary = "List all buckets in repository", description = "List all buckets in repository")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorBucket.class)))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<Collection<AccessorBucket>> getBuckets(
      @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
      @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_BUCKET)
  @Path("/{bucketName}")
  @HEAD
  @Operation(summary = "Check if bucket exist", description = "Check if bucket exist and return BUCKET/NONE in header")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = AccessorConstants.Api.X_TYPE, description = "Type as StorageType", schema = @Schema(type =
          SchemaType.STRING, enumeration = {
          "NONE", "BUCKET", "DIRECTORY", "OBJECT"}))})
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<Response> checkBucket(@PathParam("bucketName") String bucketName,
                            @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                            @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema
                                = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_BUCKET)
  @Path("/{bucketName}")
  @GET
  @Operation(summary = "Get bucket metadata", description = "Get bucket metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorBucket.class)))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "410", description = "Bucket deleted")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<AccessorBucket> getBucket(@PathParam("bucketName") String bucketName,
                                @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                    ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                    schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_BUCKET)
  @Path("/{bucketName}")
  @POST
  @Operation(summary = "Create bucket", description = "Create bucket in storage")
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "201", description = "Bucket created", content = @Content(mediaType =
      MediaType.APPLICATION_JSON, schema = @Schema(implementation = AccessorBucket.class)))
  @APIResponse(responseCode = "400", description = "Bad request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "409", description = "Bucket already exist")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<AccessorBucket> createBucket(@PathParam("bucketName") String bucketName,
                                   @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                       in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                       = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                   @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_BUCKET)
  @Path("/{bucketName}")
  @DELETE
  @Operation(summary = "Delete bucket", description = "Delete bucket in storage")
  @APIResponse(responseCode = "204", description = "Bucket deleted")
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "406", description = "Bucket found but not empty")
  @APIResponse(responseCode = "410", description = "Bucket deleted")
  @APIResponse(responseCode = "500", description = "Internal Error")
  Uni<Response> deleteBucket(@PathParam("bucketName") String bucketName,
                             @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                 ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                             @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                 schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId);

}
