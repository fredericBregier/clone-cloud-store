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

import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
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
import jakarta.ws.rs.POST;
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
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_CENTRAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_LOCAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_RECONCILIATIONS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_REQUESTS;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.FULL_CHECK;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.LOCAL;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.SUB_COLL_LISTING;
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
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_ERROR;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_MODULE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Path(AccessorConstants.Api.REPLICATOR_ROOT + LOCAL)
public class LocalReplicatorResource
    extends StreamServiceAbstract<ReplicatorOrder, AccessorObject, LocalReplicatorStreamHandler> {
  private static final Logger LOGGER = Logger.getLogger(LocalReplicatorResource.class);
  private static final String NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR = "No bucket found on any remote replicator";
  private final LocalReplicatorService localReplicatorService;

  public LocalReplicatorResource(final LocalReplicatorService localReplicatorService) {
    this.localReplicatorService = localReplicatorService;
  }

  @GET
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}/{objectName:.+}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Parameters({
      @Parameter(name = ACCEPT, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = ACCEPT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = true),
      @Parameter(name = AccessorConstants.Api.X_TARGET_ID, description = "Target ID", in = ParameterIn.HEADER,
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
                                        @Parameter(name = AccessorConstants.Api.X_TARGET_ID, description = "Target " +
                                            "ID", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING)
                                            , required = false) @HeaderParam(AccessorConstants.Api.X_TARGET_ID) final String xTargetId,
                                        final HttpServerRequest request, @Context final Closer closer) {
    final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
    final var decodedName = ParametersChecker.getSanitizedObjectName(objectName);
    LOGGER.debugf("Remote read object [%s] from bucket [%s]", decodedName, decodedBucket);
    final var replicatorObject =
        new ReplicatorOrder(xOpId, ServiceProperties.getAccessorSite(), xTargetId, xClientId, decodedBucket,
            decodedName, 0, null, ReplicatorConstants.Action.UNKNOWN);
    return readObject(request, closer, replicatorObject, false);
  }

  @HEAD
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}/{pathDirectoryOrObject:.+}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING,
          enumeration = {
          "NONE", "BUCKET", "DIRECTORY", "OBJECT"})),
      @Header(name = AccessorConstants.Api.X_TARGET_ID, description = "Id of Remote Topology", schema = @Schema(type
          = SchemaType.STRING)),
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
                                              @Parameter(name = AccessorConstants.Api.X_TARGET_ID, description =
                                                  "Target ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = false) @HeaderParam(AccessorConstants.Api.X_TARGET_ID) final String xTargetId) {
    return Uni.createFrom().emitter(em -> {
      final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
      final var decodedName = ParametersChecker.getSanitizedObjectName(pathDirectoryOrObject);
      LOGGER.debugf("Check object exists : [bucket:%s][objectPath:%s][clientId:%s]", decodedBucket, decodedName,
          xClientId);

      try {
        final var topologyOptional =
            localReplicatorService.findValidTopologyObject(decodedBucket, decodedName, fullCheck, xClientId, xTargetId,
                xOpId);
        if (topologyOptional.isEmpty()) {
          LOGGER.debugf("No object found on any remote replicator");
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          final var topology = topologyOptional.get();
          em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, StorageType.OBJECT)
              .header(AccessorConstants.Api.X_TARGET_ID, topology.id()).build());
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
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING,
          enumeration = {
          "NONE", "BUCKET", "DIRECTORY", "OBJECT"})),
      @Header(name = AccessorConstants.Api.X_TARGET_ID, description = "Id of Remote Topology", schema = @Schema(type
          = SchemaType.STRING)),
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
  @Blocking
  public Uni<Response> checkBucket(@PathParam("bucketName") final String bucketName,
                                   @Parameter(name = FULL_CHECK, description = "If True implies Storage checking",
                                       in = ParameterIn.QUERY, schema = @Schema(type = SchemaType.BOOLEAN), required
                                       = false) @DefaultValue("false") @QueryParam(AccessorConstants.Api.FULL_CHECK) final boolean fullCheck,
                                   @Parameter(name = X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String xClientId,
                                   @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                       schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String xOpId,
                                   @Parameter(name = AccessorConstants.Api.X_TARGET_ID, description = "Target ID",
                                       in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                       = false) @HeaderParam(AccessorConstants.Api.X_TARGET_ID) final String xTargetId) {
    final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Check bucket exists : [bucket:%s][clientId:%s]", decodedBucket, xClientId);
      try {
        final var topologyOptional =
            localReplicatorService.findValidTopologyBucket(decodedBucket, fullCheck, xClientId, xTargetId, xOpId);
        if (topologyOptional.isEmpty()) {
          LOGGER.debugf(NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR);
          em.complete(Response.status(Response.Status.NOT_FOUND).header(AccessorConstants.Api.X_TYPE, StorageType.NONE)
              .build());
        } else {
          final var topology = topologyOptional.get();
          em.complete(Response.ok().header(AccessorConstants.Api.X_TYPE, StorageType.BUCKET)
              .header(AccessorConstants.Api.X_TARGET_ID, topology.id()).build());
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
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + LOCAL)
  @Path(AccessorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @Operation(summary = "Get bucket metadata", description = "Get bucket metadata through topology")
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
  @Blocking
  public Uni<ReplicatorResponse<AccessorBucket>> getBucket(@PathParam("bucketName") String bucketName,
                                                           @Parameter(name = X_CLIENT_ID, description = "Client ID",
                                                               in = ParameterIn.HEADER, schema = @Schema(type =
                                                               SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) String xClientId,
                                                           @Parameter(name = X_OP_ID, description = "Operation ID",
                                                               in = ParameterIn.HEADER, schema = @Schema(type =
                                                               SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String xOpId,
                                                           @Parameter(name = AccessorConstants.Api.X_TARGET_ID,
                                                               description = "Target ID", in = ParameterIn.HEADER,
                                                               schema = @Schema(type = SchemaType.STRING), required =
                                                               false) @HeaderParam(AccessorConstants.Api.X_TARGET_ID) String xTargetId) {
    final var decodedBucket = ParametersChecker.getSanitizedBucketName(bucketName);
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Get bucket : [bucket:%s][clientId:%s]", decodedBucket, xClientId);
      try {
        final var topologyOptional =
            localReplicatorService.findValidTopologyBucket(decodedBucket, false, xClientId, xTargetId, xOpId);
        if (topologyOptional.isEmpty()) {
          LOGGER.debugf(NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR);
          em.fail(new CcsNotExistException(NO_BUCKET_FOUND_ON_ANY_REMOTE_REPLICATOR));
        } else {
          final var topology = topologyOptional.get();
          final var response = localReplicatorService.getBucket(decodedBucket, xClientId, topology, xOpId);
          em.complete(new ReplicatorResponse<>(response, topology.id()));
        }
      } catch (final CcsWithStatusException e) {
        LOGGER.errorf("Could not check bucket on any remote replicator: %s", e.getMessage());
        if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
          em.fail(CcsServerExceptionMapper.getCcsException(e.getStatus()));
        } else {
          em.fail(new CcsOperationException(e));
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  /**
   * Local creation of Reconciliation Request from existing one in Central.
   * Will run all local steps (1 to 5). <p>
   * If request is with purge, will clean nativeListing
   * <p>
   * Probably returns with Accepted status
   */
  @POST
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + COLL_LOCAL + COLL_REQUESTS)
  @Path(COLL_RECONCILIATIONS + COLL_LOCAL + COLL_REQUESTS)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Response> createRequestLocal(final ReconciliationRequest request) {
    return Uni.createFrom().emitter(em -> {
      // FIXME
    });
  }

  /**
   * Once Local sites listing is ready, inform through Replicator the Central of the local process end.
   */
  @PUT
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + COLL_CENTRAL + SUB_COLL_LISTING)
  @Path(COLL_RECONCILIATIONS + COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  public Uni<Response> endRequestLocal(@PathParam("idRequest") final String idRequest,
                                       @PathParam("remoteId") final String remoteId) {
    return Uni.createFrom().emitter(em -> {
      // FIXME
    });
  }

  /**
   * Once Local sites listing is ready, and it has informed through Replicator the Central,
   * then Central will request the listing from remote Local. Once all listing are done, Central will compute Actions.
   * <p>
   * If request is with purge, will clean sitesListing
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + COLL_LOCAL + SUB_COLL_LISTING)
  @Path(COLL_RECONCILIATIONS + COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Uni<InputStream> getSitesListing(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      // FIXME
    });
  }

  /**
   * Once Central action listing is ready, inform through Replicator the Local remote of the process end.
   */
  @PUT
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + COLL_LOCAL + SUB_COLL_LISTING)
  @Path(COLL_RECONCILIATIONS + COLL_LOCAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING)
  public Uni<Response> endRequestCentral(@PathParam("idRequest") final String idRequest) {
    return Uni.createFrom().emitter(em -> {
      // FIXME
    });
  }

  /**
   * Once all listing are done, Central will compute Actions and then inform through Replicator remote sites.
   * Each one will ask for their own local actions. Those will be saved locally as final actions.
   */
  @GET
  @Tag(name = AccessorConstants.Api.TAG_REPLICATOR + COLL_CENTRAL + SUB_COLL_LISTING)
  @Path(COLL_RECONCILIATIONS + COLL_CENTRAL + COLL_REQUESTS + "/{idRequest}" + SUB_COLL_LISTING + "/{remoteId}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Uni<InputStream> getActionsListing(@PathParam("idRequest") final String idRequest,
                                            @PathParam("remoteId") final String remoteId) {
    return Uni.createFrom().emitter(em -> {
      // FIXME
    });
  }
}
