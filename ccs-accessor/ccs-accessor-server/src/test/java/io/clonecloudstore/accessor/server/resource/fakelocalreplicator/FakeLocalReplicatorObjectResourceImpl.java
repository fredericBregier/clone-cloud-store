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

package io.clonecloudstore.accessor.server.resource.fakelocalreplicator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.inputstream.ZstdCompressInputStream;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeCommonObjectResourceHelper;
import io.clonecloudstore.test.accessor.common.FakeObjectPrivateAbstract;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
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
import org.jboss.resteasy.reactive.RestPath;

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
import static io.clonecloudstore.common.standard.properties.ApiConstants.CHUNKED;
import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;
import static io.clonecloudstore.common.standard.properties.ApiConstants.TRANSFER_ENCODING;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.BASE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.COLL_BUCKETS;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.LOCAL;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;

@Path(BASE + LOCAL + COLL_BUCKETS)
public class FakeLocalReplicatorObjectResourceImpl
    extends FakeObjectPrivateAbstract<FakeNativeLocalReplicatorStreamHandlerImpl> {

  public FakeLocalReplicatorObjectResourceImpl(final HttpHeaders httpHeaders) {
    super(httpHeaders);
  }

  @HEAD
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR)
  @Path("/{bucketName}/{pathDirectoryOrObject:.+}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Id of Remote Topology", schema =
      @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Object not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Operation(summary = "Check if object exists on a remote replicator", description = "Loops through the topology and" +
      " search for a remote replicator owning the object")
  @Blocking
  public Uni<Response> checkObjectOrDirectory(@RestPath String bucketName, @RestPath String pathDirectoryOrObject,
                                              @Parameter(name = FULL_CHECK, description = "If True implies Storage " +
                                                  "checking", in = ParameterIn.QUERY, schema = @Schema(type =
                                                  SchemaType.BOOLEAN), required = false) @DefaultValue("false") @QueryParam(FULL_CHECK) boolean fullCheck,
                                              @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description =
                                                  "Client ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                                  SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) String clientId,
                                              @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) String targetId,
                                              @HeaderParam(X_OP_ID) final String opId) {
    return checkObjectOrDirectory0(bucketName, pathDirectoryOrObject, fullCheck, clientId, isPublic());
  }

  @Override
  protected Uni<Response> checkObjectOrDirectory0(final String bucketName, final String pathDirectoryOrObject,
                                                  final boolean fullCheck, final String clientId,
                                                  final boolean isPublic) {
    return Uni.createFrom().emitter(em -> {
      if (FakeCommonObjectResourceHelper.errorCode > 0) {
        if (FakeCommonObjectResourceHelper.errorCode >= 400 && FakeCommonObjectResourceHelper.errorCode != 404) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(FakeCommonObjectResourceHelper.errorCode));
        } else if (FakeCommonObjectResourceHelper.errorCode == 404) {
          em.complete((Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build()));
        } else {
          em.complete((Response.status(Response.Status.NO_CONTENT).header(X_TYPE, StorageType.OBJECT)
              .header(ReplicatorConstants.Api.X_TARGET_ID, AccessorProperties.getAccessorSite()).build()));
        }
      } else {
        checkObjectOrDirectory(em, bucketName, pathDirectoryOrObject, fullCheck, clientId, isPublic);
      }
    });
  }

  @GET
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR)
  @Path("/{bucketName}/{objectName:.+}")
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
  public Uni<Response> remoteReadObject(@RestPath final String bucketName, @RestPath final String objectName,
                                        @HeaderParam(X_CLIENT_ID) final String xClientId,
                                        @HeaderParam(ACCEPT) final String acceptHeader,
                                        @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                        @HeaderParam(X_OP_ID) final String xOpId,
                                        @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) final String xTargetId,
                                        final HttpServerRequest request, @Context final Closer closer) {
    final var decoded = ParametersChecker.getSanitizedName(objectName);
    return getObject0(bucketName, decoded, acceptHeader, acceptEncodingHeader, xClientId, xOpId, isPublic(), request,
        closer);
  }

  @Override
  protected Uni<Response> getObject0(final String bucketName, final String objectName, final String acceptHeader,
                                     final String acceptEncodingHeader, final String clientId, final String opId,
                                     final boolean isPublic, final HttpServerRequest request, final Closer closer) {
    if (FakeCommonObjectResourceHelper.errorCode > 0) {
      return Uni.createFrom().emitter(em -> {
        if (FakeCommonObjectResourceHelper.errorCode >= 400 && FakeCommonObjectResourceHelper.errorCode != 404) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(FakeCommonObjectResourceHelper.errorCode));
        } else if (FakeCommonObjectResourceHelper.errorCode == 404) {
          em.complete((Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build()));
        } else {
          final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
          final var accessorObject = new AccessorObject().setName(ParametersChecker.getSanitizedName(objectName))
              .setSite(FakeCommonBucketResourceHelper.site).setBucket(techName)
              .setSize(FakeCommonObjectResourceHelper.length);
          InputStream inputStream = new FakeInputStream(FakeCommonObjectResourceHelper.length, (byte) 'A');
          final Map<String, String> map = new HashMap<>();
          AccessorHeaderDtoConverter.objectToMap(accessorObject, map);
          final var response = Response.ok();
          map.put(TRANSFER_ENCODING, CHUNKED);
          if (acceptEncodingHeader.equalsIgnoreCase(COMPRESSION_ZSTD)) {
            map.put(HttpHeaders.CONTENT_ENCODING, COMPRESSION_ZSTD);
            try {
              inputStream = new ZstdCompressInputStream(inputStream);
            } catch (final IOException e) {
              throw new CcsOperationException(e);
            }
          }
          closer.add(inputStream);
          for (final var entry : map.entrySet()) {
            response.header(entry.getKey(), entry.getValue());
          }
          response.entity(inputStream);
          em.complete((response.build()));
        }
      });
    } else {
      return super.getObject0(bucketName, objectName, acceptHeader, acceptEncodingHeader, clientId, opId, isPublic,
          request, closer);
    }
  }
}

