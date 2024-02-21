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

package io.clonecloudstore.administration.client.api;

import java.io.Closeable;
import java.util.Collection;

import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.headers.Header;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.ADMINISTRATION_ROOT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_TOPOLOGIES;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.TAG_ADMINISTRATION;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.TAG_TOPOLOGY;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_ERROR;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_MODULE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path(ADMINISTRATION_ROOT + COLL_TOPOLOGIES)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@RegisterProvider(ResponseClientFilter.class)
public interface TopologyApi extends Closeable {

  @GET
  @Tag(name = TAG_ADMINISTRATION + TAG_TOPOLOGY)
  @Produces(APPLICATION_JSON)
  @APIResponse(responseCode = "200", description = "Successfully retrieved list from topology", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Operation(summary = "Get list of remote sites from topology", description = "Get list of remote sites from topology")
  Uni<Collection<Topology>> listAll(@QueryParam("status") @DefaultValue("UNKNOWN") final TopologyStatus status);

  @POST
  @Tag(name = TAG_ADMINISTRATION + TAG_TOPOLOGY)
  @APIResponse(responseCode = "201", description = "Successfully added remote site", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Remote site not valid", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Produces(APPLICATION_JSON)
  @Consumes(APPLICATION_JSON)
  @Operation(summary = "Add a remote site to topology", description = "Add a remote site to topology")
  Uni<Topology> add(Topology topology);

  @PUT
  @Tag(name = TAG_ADMINISTRATION + TAG_TOPOLOGY)
  @APIResponse(responseCode = "202", description = "Successfully updated remote site status", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Remote site not valid", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Remote site not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Produces(APPLICATION_JSON)
  @Consumes(APPLICATION_JSON)
  @Operation(summary = "Update a remote site into topology", description = "Update a remote site into topology")
  Uni<Topology> update(Topology topology);

  @GET
  @Tag(name = TAG_ADMINISTRATION + TAG_TOPOLOGY)
  @Path("/{site}")
  @APIResponse(responseCode = "200", description = "Successfully retrieved Remote site", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Remote site id not valid", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Remote site not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Produces(APPLICATION_JSON)
  @Operation(summary = "Get a remote site from topology", description = "Get a remote site from topology " +
      "based on its site")
  Uni<Topology> findBySite(@RestPath String site);

  @Path("/{site}")
  @Tag(name = TAG_ADMINISTRATION + TAG_TOPOLOGY)
  @DELETE
  @APIResponse(responseCode = "204", description = "Successfully deleted Remote site", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "400", description = "Remote site not valid", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "404", description = "Remote site not found", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {
      @Header(name = X_OP_ID, description = "Operation ID", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_MODULE, description = "Module Id", schema = @Schema(type = SchemaType.STRING)),
      @Header(name = X_ERROR, description = "Error Message", schema = @Schema(type = SchemaType.STRING))})
  @Produces(APPLICATION_JSON)
  @Operation(summary = "Delete a remote site from topology", description = "Delete a remote site from topology")
  Uni<Response> delete(@RestPath String site);
}
