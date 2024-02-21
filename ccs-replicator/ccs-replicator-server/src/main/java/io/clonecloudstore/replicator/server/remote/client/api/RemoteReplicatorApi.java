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

package io.clonecloudstore.replicator.server.remote.client.api;

import java.io.Closeable;
import java.io.InputStream;
import java.util.List;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.clonecloudstore.replicator.client.api.ResponseObjectClientFilter;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.quarkus.rest.client.reactive.ComputedParamContext;
import io.quarkus.rest.client.reactive.NotBody;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
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
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.NoCache;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.FULL_CHECK;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.REMOTE;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TYPE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_BUCKET;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_CREATION;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_EXPIRES;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_HASH;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_METADATA;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_NAME;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_SITE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_SIZE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_STATUS;
import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_ERROR;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_MODULE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Path(AccessorConstants.Api.REPLICATOR_ROOT + REMOTE)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@RegisterProvider(ResponseObjectClientFilter.class)
@NoCache
public interface RemoteReplicatorApi extends Closeable {
  @GET
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @Operation(summary = "Get bucket metadata", description = "Get bucket metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorBucket.class)), headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Bad Request", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Bucket not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "410", description = "Bucket deleted", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal Error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  Uni<AccessorBucket> getBucket(@PathParam("bucketName") String bucketName,
                                @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
                                    schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) String clientId,
                                @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                    schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId);

  @HEAD
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Bucket not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal Error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Operation(summary = "Check if bucket exists on a remote replicator", description = "Loops through the topology and" +
      " search for a remote replicator owning the bucket")
  Uni<Response> checkBucket(@PathParam("bucketName") String bucketName,
                            @Parameter(name = FULL_CHECK, description = "If True implies Storage checking", in =
                                ParameterIn.QUERY, schema = @Schema(type = SchemaType.BOOLEAN), required = false) @DefaultValue("false") @QueryParam(FULL_CHECK) boolean fullCheck,
                            @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
                                schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) String clientId,
                            @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema
                                = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId);

  @HEAD
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}/{pathDirectoryOrObject:.+}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Object not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal Error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Operation(summary = "Check if object exists on a remote replicator", description = "Loops through the topology and" +
      " search for a remote replicator owning the object")
  Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") String bucketName,
                                       @PathParam("pathDirectoryOrObject") String pathDirectoryOrObject,
                                       @Parameter(name = FULL_CHECK, description = "If True implies Storage checking"
                                           , in = ParameterIn.QUERY, schema = @Schema(type = SchemaType.BOOLEAN),
                                           required = false) @DefaultValue("false") @QueryParam(FULL_CHECK) boolean fullCheck,
                                       @Parameter(name = X_CLIENT_ID, description = "Client ID", in =
                                           ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
                                           true) @HeaderParam(X_CLIENT_ID) String clientId,
                                       @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                           ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
                                           false) @HeaderParam(X_OP_ID) final String opId);

  @GET
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}/{objectName:.+}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Parameters({
      @Parameter(name = ACCEPT, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = true),
      @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = false)})
  @APIResponse(responseCode = "200", description = "OK", headers = {
      @Header(name = X_OBJECT_ID, description = "Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_SITE, description = "Site", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_BUCKET, description = "Bucket Name", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_NAME, description = "Object Name", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_CREATION, description = "Creation Date", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_SIZE, description = "Object Size", schema = @Schema(type = SchemaType.INTEGER)),
      @Header(name = X_OBJECT_HASH, description = "Object Hash SHA-256", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_METADATA, description = "Object Metadata", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_STATUS, description = "Object Status", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OBJECT_EXPIRES, description = "Expiration Date", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))}, content =
  @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM, schema = @Schema(type = SchemaType.STRING, format =
      "binary")))
  @APIResponse(responseCode = "401", description = "Unauthorized", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Object not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal Error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Operation(summary = "Read Object from a remote replicator", description = "Loops through topology and search for a" +
      " remote replicator able to service the request. Open up a stream with remote replicator which reads from its " +
      "local accessor")
  @ClientHeaderParam(name = ACCEPT, value = MediaType.APPLICATION_OCTET_STREAM)
  @ClientHeaderParam(name = ACCEPT_ENCODING, value = "{computeCompressionModel}", required = false)
  Uni<InputStream> remoteReadObject(@NotBody final boolean acceptCompression,
                                    @PathParam("bucketName") final String bucketName,
                                    @PathParam("objectName") final String objectName,
                                    @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER
                                        , schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) final String xClientId,
                                    @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                        schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String xOpId);

  default String computeCompressionModel(ComputedParamContext context) {
    int argPos = 0;
    if ((boolean) context.methodParameters().get(argPos).value()) {
      return COMPRESSION_ZSTD;
    }
    return null;
  }

  /**
   * This method returns a Uni response, meaning the caller must call await when he wants the request to be activated.
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(AccessorConstants.Api.COLL_ORDERS)
  @Operation(summary = "Create order", description = "Create replication order remotely")
  @Consumes(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "201", description = "Order created", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Bad request", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "409", description = "Bucket already exist", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal Error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  Uni<Response> createOrder(ReplicatorOrder replicatorOrder);

  /**
   * This method returns a Uni response, meaning the caller must call await when he wants the request to be activated.
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(AccessorConstants.Api.COLL_ORDERS + AccessorConstants.Api.COLL_ORDERS_MULTIPLE)
  @Operation(summary = "Create orders", description = "Create replication orders remotely")
  @Consumes(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "201", description = "Order created", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Bad request", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "409", description = "Bucket already exist", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal Error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  Uni<Response> createOrders(List<ReplicatorOrder> replicatorOrders);
  // FIXME later on will have Reconciliation here
}
