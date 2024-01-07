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

package io.clonecloudstore.accessor.server.resource;

import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.application.AccessorObjectService;
import io.clonecloudstore.accessor.server.application.ObjectNativeStreamHandler;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorBucketRepository;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.server.service.NativeServerResponseException;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
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
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.TAG_PUBLIC;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

/**
 * Object API Resource
 */
@Path(AccessorConstants.Api.API_ROOT)
public class AccessorObjectResource
    extends StreamServiceAbstract<AccessorObject, AccessorObject, ObjectNativeStreamHandler> {
  private static final Logger LOGGER = Logger.getLogger(AccessorObjectResource.class);
  private static final String BUCKETNAME_OBJECT = "BucketName: %s Directory or Object ID: %s";

  private final AccessorObjectService service;

  public AccessorObjectResource(final AccessorObjectService service) {
    this.service = service;
  }

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_OBJECT)
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
  @Blocking
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
    final var finalBucketName = DaoAccessorBucketRepository.getRealBucketName(clientId, bucketName, true);
    LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, request.method().name());
    final var object = new AccessorObject().setBucket(finalBucketName);
    // TODO choose compression model
    return readObjectList(request, closer, object, false);
  }

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_OBJECT)
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
  @Blocking
  public Uni<Response> checkObjectOrDirectory(@PathParam("bucketName") final String bucketName,
                                              @PathParam("pathDirectoryOrObject") final String pathDirectoryOrObject,
                                              @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description =
                                                  "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                              @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                                  ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                                  required = false) @HeaderParam(X_OP_ID) final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var finalBucketName = DaoAccessorBucketRepository.getRealBucketName(clientId, bucketName, true);
      final var finalObjectName = ParametersChecker.getSanitizedName(pathDirectoryOrObject);
      LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, finalObjectName);
      try {
        final var storageType =
            service.objectOrDirectoryExists(finalBucketName, finalObjectName, false, clientId, opId, true);
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

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_OBJECT)
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
  @Blocking
  public Uni<AccessorObject> getObjectInfo(@PathParam("bucketName") final String bucketName,
                                           @PathParam("objectName") final String objectName,
                                           @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client" +
                                               " ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                               SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                           @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                               ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                               required = false) @HeaderParam(X_OP_ID) final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var finalBucketName = DaoAccessorBucketRepository.getRealBucketName(clientId, bucketName, true);
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
  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_OBJECT)
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
      @Schema(type = SchemaType.STRING)),
      @Header(name = AccessorConstants.HeaderObject.X_OBJECT_EXPIRES, description = "Expiration Date", schema =
      @Schema(type = SchemaType.STRING))}, content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM, schema
      = @Schema(type = SchemaType.STRING, format = "binary")))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Blocking
  public Uni<Response> getObject(@PathParam("bucketName") final String bucketName,
                                 @PathParam("objectName") final String objectName,
                                 @HeaderParam(ACCEPT) final String acceptHeader,
                                 @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                 @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in =
                                     ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) final String clientId,
                                 @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                     schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId,
                                 final HttpServerRequest request, @Context final Closer closer) {
    final var finalBucketName = DaoAccessorBucketRepository.getRealBucketName(clientId, bucketName, true);
    final var finalObjectName = ParametersChecker.getSanitizedName(objectName);
    LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, finalObjectName);
    final var object = new AccessorObject().setBucket(finalBucketName).setName(finalObjectName);
    // TODO choose compression model
    return readObject(request, closer, object, false);
  }

  /**
   * Create the Object and returns the associated DTO
   */
  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_OBJECT)
  @Path("{bucketName}/{objectName:.+}")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Create object", description = "Create object")
  @Parameters({
      @Parameter(name = CONTENT_TYPE, description = "Must contain application/octet-stream", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = CONTENT_ENCODING, description = "May contain ZSTD for compression", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = false),
      @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID", in = ParameterIn.HEADER,
          schema = @Schema(type = SchemaType.STRING), required = true),
      @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER, schema = @Schema(type =
          SchemaType.STRING), required = false),
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
  @RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM, schema =
  @Schema(type = SchemaType.STRING, format = "binary")), description = "InputStream as content")
  @APIResponse(responseCode = "201", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON,
      schema = @Schema(implementation = AccessorObject.class)))
  @APIResponse(responseCode = "400", description = "Bad Request")
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "406", description = "Object already in creation")
  @APIResponse(responseCode = "409", description = "Conflict since Object already exist or invalid")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Blocking
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
    final var finalBucketName = DaoAccessorBucketRepository.getRealBucketName(clientId, bucketName, true);
    final var finalObjectName = ParametersChecker.getSanitizedName(objectName);
    LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, objectName);
    final var object = new AccessorObject().setBucket(finalBucketName).setName(finalObjectName).setSize(xObjectSize)
        .setHash(xObjectHash);
    if (xObjectSize <= 0) {
      final var length = request.headers().get(HttpHeaderNames.CONTENT_LENGTH);
      if (ParametersChecker.isNotEmpty(length)) {
        object.setSize(Long.parseLong(length));
      }
    }
    // TODO choose compression model
    return createObject(request, closer, object, object.getSize(), object.getHash(), false, inputStream);
  }

  @Tag(name = TAG_PUBLIC + AccessorConstants.Api.TAG_OBJECT)
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
  @Blocking
  public Uni<Response> deleteObject(@PathParam("bucketName") final String bucketName,
                                    @PathParam("objectName") final String objectName,
                                    @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                        in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                        required = true) @HeaderParam(AccessorConstants.Api.X_CLIENT_ID) String clientId,
                                    @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER,
                                        schema = @Schema(type = SchemaType.STRING), required = false) @HeaderParam(X_OP_ID) final String opId) {
    return Uni.createFrom().emitter(em -> {
      final var finalBucketName = DaoAccessorBucketRepository.getRealBucketName(clientId, bucketName, true);
      final var finalObjectName = ParametersChecker.getSanitizedName(objectName);
      LOGGER.infof(BUCKETNAME_OBJECT, finalBucketName, finalObjectName);
      try {
        service.deleteObject(finalBucketName, finalObjectName, clientId, true);
        em.complete(Response.noContent().build());
      } catch (final CcsDeletedException e) {
        em.complete(Response.noContent().build());
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
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
