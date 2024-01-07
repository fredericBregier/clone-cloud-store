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

package io.clonecloudstore.replicator.server.remote.resource;

import java.util.List;

import io.clonecloudstore.accessor.client.internal.AccessorBucketInternalApiFactory;
import io.clonecloudstore.accessor.client.internal.AccessorObjectInternalApiFactory;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
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
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
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
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.REMOTE;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Path(ReplicatorConstants.Api.BASE + ReplicatorConstants.Api.REMOTE)
public class RemoteReplicatorResource
    extends StreamServiceAbstract<ReplicatorOrder, AccessorObject, RemoteReplicatorNativeStreamHandler> {
  private static final Logger LOGGER = Logger.getLogger(RemoteReplicatorResource.class);
  private final AccessorBucketInternalApiFactory accessorBucketInternalApiFactory;
  private final AccessorObjectInternalApiFactory accessorObjectInternalApiFactory;
  private final RemoteReplicatorOrderEmitter replicatorOrderEmitter;

  public RemoteReplicatorResource(final AccessorBucketInternalApiFactory accessorBucketInternalApiFactory,
                                  final AccessorObjectInternalApiFactory accessorObjectInternalApiFactory,
                                  final RemoteReplicatorOrderEmitter replicatorOrderEmitter) {
    this.accessorBucketInternalApiFactory = accessorBucketInternalApiFactory;
    this.accessorObjectInternalApiFactory = accessorObjectInternalApiFactory;
    this.replicatorOrderEmitter = replicatorOrderEmitter;
  }

  @GET
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}/{objectName:.+}")
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
      @Header(name = X_OBJECT_EXPIRES, description = "Expiration Date", schema = @Schema(type = SchemaType.STRING))},
      content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM))
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Operation(summary = "Read Object from a remote replicator", description = "Loops through topology and search for a" +
      " remote replicator able to service the request. Open up a stream with remote replicator which reads from its " +
      "local accessor")
  @Blocking
  public Uni<Response> remoteReadObject(@PathParam("bucketName") final String bucketName,
                                        @PathParam("objectName") final String objectName,
                                        @HeaderParam(ACCEPT) final String acceptHeader,
                                        @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                        @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client " +
                                            "ID", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING)
                                            , required = true) @HeaderParam(X_CLIENT_ID) final String xClientId,
                                        @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                            ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                            = false) @HeaderParam(X_OP_ID) final String xOpId,
                                        final HttpServerRequest request, @Context final Closer closer) {
    final var decodedName = ParametersChecker.getSanitizedName(objectName);
    LOGGER.debugf("Remote read object [%s] from bucket [%s]", decodedName, bucketName);
    final var replicatorObject =
        new ReplicatorOrder(xOpId, ServiceProperties.getAccessorSite(), ServiceProperties.getAccessorSite(), xClientId,
            bucketName, decodedName, 0, null, ReplicatorConstants.Action.UNKNOWN);
    // TODO Handle compression parameter, currently set to false
    // TODO choose compression model
    return readObject(request, closer, replicatorObject, false);
  }

  @HEAD
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}/{pathDirectoryOrObject:.+}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING))})
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
                                              @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description =
                                                  "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String xClientId,
                                              @HeaderParam(X_OP_ID) final String xOpId) {
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Check object exists : [bucket:%s][objectPath:%s][clientId:%s]", bucketName, pathDirectoryOrObject,
          xClientId);
      try (final var client = accessorObjectInternalApiFactory.newClient()) {
        client.setOpId(xOpId);
        final var storageType = client.checkObjectOrDirectory(bucketName, pathDirectoryOrObject, xClientId, fullCheck);
        em.complete(Response.status(StorageType.NONE.equals(storageType) ? Status.NOT_FOUND : Status.OK)
            .header(AccessorConstants.Api.X_TYPE, storageType).build());
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
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(ReplicatorConstants.Api.COLL_BUCKETS + "/{bucketName}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING))})
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
                                   @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                       in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                       = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String xClientId,
                                   @HeaderParam(X_OP_ID) final String xOpId) {
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Check bucket exists : [bucket:%s][clientId:%s]", bucketName, xClientId);
      try (final var client = accessorBucketInternalApiFactory.newClient()) {
        client.setOpId(xOpId);
        final var storageType = client.checkBucket(bucketName, xClientId, fullCheck);
        em.complete(Response.status(StorageType.NONE.equals(storageType) ? Status.NOT_FOUND : Status.OK)
            .header(AccessorConstants.Api.X_TYPE, storageType).build());
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
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + REMOTE)
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
  public Uni<AccessorBucket> getBucket(@PathParam("bucketName") final String bucketName,
                                       @Parameter(name = X_CLIENT_ID, description = "Client ID", in =
                                           ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
                                           true) @HeaderParam(X_CLIENT_ID) String xClientId,
                                       @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                           ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required =
                                           false) @HeaderParam(X_OP_ID) final String xOpId) {
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Get bucket : [bucket:%s][clientId:%s]", bucketName, xClientId);
      try (final var client = accessorBucketInternalApiFactory.newClient()) {
        client.setOpId(xOpId);
        final var bucket = client.getBucket(bucketName, xClientId);
        em.complete(bucket);
      } catch (final CcsWithStatusException e) {
        LOGGER.errorf("Could not check bucket on any remote replicator: %s", e.getMessage());
        if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
          em.fail(e);
        } else {
          em.fail(new CcsOperationException(e));
        }
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @POST
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(ReplicatorConstants.Api.COLL_ORDERS)
  @Operation(summary = "Create order", description = "Create replication order remotely")
  @Consumes(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "201", description = "Order created")
  @APIResponse(responseCode = "400", description = "Bad request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "409", description = "Bucket already exist")
  @APIResponse(responseCode = "500", description = "Internal Error")
  public Uni<Response> createOrder(final ReplicatorOrder replicatorOrder) {
    return Uni.createFrom().emitter(em -> {
      LOGGER.debugf("Order to create: %s", replicatorOrder);
      try {
        replicatorOrderEmitter.generate(replicatorOrder);
        em.complete(Response.status(Status.CREATED).build());
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  @POST
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR + REMOTE)
  @Path(ReplicatorConstants.Api.COLL_ORDERS + ReplicatorConstants.Api.COLL_ORDERS_MULTIPLE)
  @Operation(summary = "Create orders", description = "Create replication orders remotely")
  @Consumes(MediaType.APPLICATION_JSON)
  @APIResponse(responseCode = "201", description = "Order created")
  @APIResponse(responseCode = "400", description = "Bad request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "409", description = "Bucket already exist")
  @APIResponse(responseCode = "500", description = "Internal Error")
  public Uni<Response> createOrders(final List<ReplicatorOrder> replicatorOrders) {
    return Uni.createFrom().emitter(em -> {
      try {
        for (final var replicatorOrder : replicatorOrders) {
          LOGGER.debugf("Order to create: %s", replicatorOrder);
          replicatorOrderEmitter.generate(replicatorOrder);
        }
        em.complete(Response.status(Status.CREATED).build());
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }
}
