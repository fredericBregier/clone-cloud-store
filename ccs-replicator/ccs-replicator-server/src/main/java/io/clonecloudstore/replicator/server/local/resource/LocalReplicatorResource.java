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

package io.clonecloudstore.replicator.server.local.resource;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.model.ReplicatorResponse;
import io.clonecloudstore.replicator.server.local.application.LocalReplicatorService;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
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
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.FULL_CHECK;
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
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.LOCAL;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Path(ReplicatorConstants.Api.BASE + LOCAL)
public class LocalReplicatorResource
    extends StreamServiceAbstract<ReplicatorOrder, AccessorObject, LocalReplicatorNativeStreamHandler> {
  private static final Logger LOGGER = Logger.getLogger(LocalReplicatorResource.class);
  private static final String NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR = "No bucket found on any remote replicator";
  private final LocalReplicatorService localReplicatorService;

  public LocalReplicatorResource(final LocalReplicatorService localReplicatorService) {
    this.localReplicatorService = localReplicatorService;
  }

  @GET
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}/{objectName:.+}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Parameters({
      @Parameter(name = ACCEPT, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = true),
      @Parameter(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Target ID", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
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
      @Header(name = X_OBJECT_EXPIRES, description = "Expiration Date", schema = @Schema(type = SchemaType.STRING))},
      content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM))
  @APIResponse(responseCode = "200", description = "OK")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Operation(summary = "Read Object from a remote replicator", description = "Loops through topology and search for a" +
      " remote replicator able to service the request. Open up a stream with remote replicator which reads from its " +
      "local accessor")
  @Blocking
  public Uni<Response> remoteReadObject(@PathParam("bucketName") final String bucketName,
                                        @PathParam("objectName") final String objectName,
                                        @Parameter(name = ACCEPT, description = "Must contain " +
                                            "application/octet-stream", in = ParameterIn.HEADER, schema =
                                        @Schema(type = SchemaType.STRING), required = true) @HeaderParam(ACCEPT) final String acceptHeader,
                                        @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for " +
                                            "compression", in = ParameterIn.HEADER, schema = @Schema(type =
                                            SchemaType.STRING), required = false) @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                        @Parameter(name = X_CLIENT_ID, description = "Client ID", in =
                                            ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                            = true) @HeaderParam(X_CLIENT_ID) final String xClientId,
                                        @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                            ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                            = false) @HeaderParam(X_OP_ID) final String xOpId,
                                        @Parameter(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Target " +
                                            "ID", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING)
                                            , required = false) @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) final String xTargetId,
                                        final HttpServerRequest request, @Context final Closer closer) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    LOGGER.debugf("Remote read object [%s] from bucket [%s]", decodedName, bucketName);
    final var replicatorObject =
        new ReplicatorOrder(xOpId, ServiceProperties.getAccessorSite(), xTargetId, xClientId, bucketName, decodedName,
            0, null, ReplicatorConstants.Action.UNKNOWN);
    // TODO Handle compression parameter, currently set to false
    // TODO choose compression model
    return readObject(request, closer, replicatorObject, false);
  }

  @HEAD
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}/{pathDirectoryOrObject:.+}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING,
          enumeration = {
          "NONE", "BUCKET", "DIRECTORY", "OBJECT"})),
      @Header(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Id of Remote Topology", schema =
      @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Operation(summary = "Check if object exists on a remote replicator", description = "Loops through the topology and" +
      " search for a remote replicator owning the object")
  @Blocking
  public Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") final String bucketName,
                                              @PathParam("pathDirectoryOrObject") final String pathDirectoryOrObject,
                                              @Parameter(name = FULL_CHECK, description = "If True implies Storage " +
                                                  "checking", in = ParameterIn.QUERY, schema = @Schema(type =
                                                  SchemaType.BOOLEAN), required = false) @DefaultValue("false") @QueryParam(AccessorConstants.Api.FULL_CHECK) final boolean fullCheck,
                                              @Parameter(name = X_CLIENT_ID, description = "Client ID", in =
                                                  ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                                  required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String xClientId,
                                              @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                                  ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                                  required = false) @HeaderParam(X_OP_ID) final String xOpId,
                                              @Parameter(name = ReplicatorConstants.Api.X_TARGET_ID, description =
                                                  "Target ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = false) @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) final String xTargetId) {
    return Uni.createFrom().emitter(em -> {
      final var decodedName = ParametersChecker.getSanitizedName(pathDirectoryOrObject);
      LOGGER.debugf("Check object exists : [bucket:%s][objectPath:%s][clientId:%s]", bucketName, decodedName,
          xClientId);

      try {
        final var topologyOptional =
            localReplicatorService.findValidTopologyObject(bucketName, decodedName, fullCheck, xClientId, xTargetId,
                xOpId);
        if (topologyOptional.isEmpty()) {
          LOGGER.debugf("No object found on any remote replicator");
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          final var topology = topologyOptional.get();
          em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, StorageType.OBJECT)
              .header(ReplicatorConstants.Api.X_TARGET_ID, topology.id()).build());
        }
      } catch (final CcsWithStatusException e) {
        LOGGER.errorf("Could not check object on any remote replicator: %s", e.getMessage());
        if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .header(AccessorConstants.Api.X_TYPE, StorageType.NONE).build());
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  @HEAD
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING,
          enumeration = {
          "NONE", "BUCKET", "DIRECTORY", "OBJECT"})),
      @Header(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Id of Remote Topology", schema =
      @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Operation(summary = "Check if bucket exists on a remote replicator", description = "Loops through the topology and" +
      " search for a remote replicator owning the bucket")
  @Blocking
  public Uni<Response> checkBucket(@PathParam("bucketName") final String bucketName,
                                   @Parameter(name = FULL_CHECK, description = "If True implies Storage checking",
                                       in = ParameterIn.QUERY, schema = @Schema(type = SchemaType.BOOLEAN), required
                                       = false) @DefaultValue("false") @QueryParam(AccessorConstants.Api.FULL_CHECK) final boolean fullCheck,
                                   @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String xClientId,
                                   @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String xOpId,
                                   @Parameter(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Target ID",
                                       in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                       = false) @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) final String xTargetId) {
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Check bucket exists : [bucket:%s][clientId:%s]", bucketName, xClientId);
      try {
        final var topologyOptional =
            localReplicatorService.findValidTopologyBucket(bucketName, fullCheck, xClientId, xTargetId, xOpId);
        if (topologyOptional.isEmpty()) {
          LOGGER.debugf(NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR);
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          final var topology = topologyOptional.get();
          em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, StorageType.BUCKET)
              .header(ReplicatorConstants.Api.X_TARGET_ID, topology.id()).build());
        }
      } catch (final CcsWithStatusException e) {
        LOGGER.errorf("Could not check bucket on any remote replicator: %s", e.getMessage());
        if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .header(AccessorConstants.Api.X_TYPE, StorageType.NONE).build());
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  @GET
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @Operation(summary = "Get bucket metadata", description = "Get bucket metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorBucket.class)))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "410", description = "Bucket deleted")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Blocking
  public Uni<ReplicatorResponse<AccessorBucket>> getBucket(@PathParam("bucketName") String bucketName,
                                                           @Parameter(name = X_CLIENT_ID, description = "Client ID",
                                                               in = ParameterIn.HEADER, schema = @Schema(type =
                                                               SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) String xClientId,
                                                           @Parameter(name = X_OP_ID, description = "Operation ID",
                                                               in = ParameterIn.HEADER, schema = @Schema(type =
                                                               SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String xOpId,
                                                           @Parameter(name = ReplicatorConstants.Api.X_TARGET_ID,
                                                               description = "Target ID", in = ParameterIn.HEADER,
                                                               schema = @Schema(type = SchemaType.STRING), required =
                                                               false) @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) String xTargetId) {
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Get bucket : [bucket:%s][clientId:%s]", bucketName, xClientId);
      try {
        final var topologyOptional =
            localReplicatorService.findValidTopologyBucket(bucketName, false, xClientId, xTargetId, xOpId);
        if (topologyOptional.isEmpty()) {
          LOGGER.debugf(NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR);
          em.fail(new CcsNotExistException(NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR));
        } else {
          final var topology = topologyOptional.get();
          final var response = localReplicatorService.getBucket(bucketName, xClientId, topology, xOpId);
          em.complete(new ReplicatorResponse<>(response, topology.id()));
        }
      } catch (final CcsWithStatusException e) {
        LOGGER.errorf("Could not check bucket on any remote replicator: %s", e.getMessage());
        if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(e.getStatus()));
        } else {
          em.fail(new CcsOperationException(e));
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }
}
