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

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorResponse;
import io.clonecloudstore.test.accessor.common.FakeBucketPrivateAbstract;
import io.clonecloudstore.test.accessor.common.FakeCommonBucketResourceHelper;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
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
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.FULL_CHECK;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TYPE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.BASE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.COLL_BUCKETS;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.LOCAL;

@Path(BASE + LOCAL + COLL_BUCKETS)
public class FakeLocalReplicatorBucketServiceImpl extends FakeBucketPrivateAbstract {
  @GET
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR)
  @Path("/{bucketName}")
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
  public Uni<ReplicatorResponse<AccessorBucket>> getBucket(@RestPath String bucketName,
                                                           @HeaderParam(X_CLIENT_ID) String clientId,
                                                           @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) String targetId,
                                                           @HeaderParam(X_OP_ID) final String opId) {
    final var uni = getBucket0(bucketName, clientId, isPublic());
    return Uni.createFrom().emitter(em -> {
      try {
        final var result = uni.await().indefinitely();
        em.complete(new ReplicatorResponse<>(result, AccessorProperties.getAccessorSite()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @HEAD
  @Tag(name = ReplicatorConstants.Api.TAG_REPLICATOR)
  @Path("/{bucketName}")
  @APIResponse(responseCode = "204", description = "OK", headers = {
      @Header(name = X_TYPE, description = "Type as StorageType", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = ReplicatorConstants.Api.X_TARGET_ID, description = "Id of Remote Topology", schema =
      @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "401", description = "Unauthorized")
  @APIResponse(responseCode = "404", description = "Bucket not found")
  @APIResponse(responseCode = "500", description = "Internal Error")
  @Operation(summary = "Check if bucket exists on a remote replicator", description = "Loops through the topology and" +
      " search for a remote replicator owning the bucket")
  @Blocking
  public Uni<Response> checkBucket(@RestPath String bucketName,
                                   @Parameter(name = FULL_CHECK, description = "If True implies Storage checking",
                                       in = ParameterIn.QUERY, schema = @Schema(type = SchemaType.BOOLEAN), required
                                       = false) @DefaultValue("false") @QueryParam(FULL_CHECK) boolean fullCheck,
                                   @HeaderParam(X_CLIENT_ID) String clientId,
                                   @HeaderParam(ReplicatorConstants.Api.X_TARGET_ID) String targetId,
                                   @HeaderParam(X_OP_ID) final String opId) {
    return checkBucket0(bucketName, fullCheck, clientId, isPublic());
  }

  @Override
  protected Uni<Response> checkBucket0(final String bucketName, final boolean fullCheck, final String clientId,
                                       final boolean isPublic) {
    return Uni.createFrom().emitter(em -> {
      if (FakeCommonBucketResourceHelper.errorCode > 0) {
        if (FakeCommonBucketResourceHelper.errorCode >= 400 && FakeCommonBucketResourceHelper.errorCode != 404) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(FakeCommonBucketResourceHelper.errorCode));
        } else if (FakeCommonBucketResourceHelper.errorCode != 204) {
          em.complete((Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build()));
        } else {
          em.complete((Response.status(Response.Status.NO_CONTENT).header(X_TYPE, StorageType.BUCKET)
              .header(ReplicatorConstants.Api.X_TARGET_ID, AccessorProperties.getAccessorSite()).build()));
        }
      } else {
        FakeCommonBucketResourceHelper.checkBucketHelper(em, bucketName, fullCheck, clientId, isPublic);
      }
    });
  }
}
