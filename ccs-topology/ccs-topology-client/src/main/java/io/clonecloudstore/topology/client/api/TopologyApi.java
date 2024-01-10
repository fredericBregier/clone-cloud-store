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

package io.clonecloudstore.topology.client.api;

import java.io.Closeable;
import java.util.Collection;

import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.clonecloudstore.topology.model.Topology;
import io.clonecloudstore.topology.model.TopologyStatus;
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
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.TAG_ADMINISTRATION;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.BASE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.TOPOLOGIES;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path(BASE + TOPOLOGIES)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@RegisterProvider(ResponseClientFilter.class)
public interface TopologyApi extends Closeable {

  @GET
  @Tag(name = TAG_ADMINISTRATION)
  @Produces(APPLICATION_JSON)
  @APIResponse(responseCode = "200", description = "Successfully retrieved list of topology", headers = {})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {})
  @Operation(summary = "Get list of replicators", description = "Get list of replicators in the  topology")
  Uni<Collection<Topology>> listAll(@QueryParam("status") @DefaultValue("UNKNOWN") final TopologyStatus status);

  @POST
  @Tag(name = TAG_ADMINISTRATION)
  @APIResponse(responseCode = "201", description = "Successfully added topology", headers = {})
  @APIResponse(responseCode = "400", description = "Topology not valid", headers = {})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {})
  @Produces(APPLICATION_JSON)
  @Consumes(APPLICATION_JSON)
  @Operation(summary = "Add a replicator to topology", description = "Add a replicator to topology")
  Uni<Topology> add(Topology topology);

  @PUT
  @Tag(name = TAG_ADMINISTRATION)
  @APIResponse(responseCode = "202", description = "Successfully added topology", headers = {})
  @APIResponse(responseCode = "400", description = "Topology not valid", headers = {})
  @APIResponse(responseCode = "404", description = "Topology not found", headers = {})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {})
  @Produces(APPLICATION_JSON)
  @Consumes(APPLICATION_JSON)
  @Operation(summary = "Update a replicator in the topology", description = "Update a replicator in the topology")
  Uni<Topology> update(Topology topology);

  @GET
  @Tag(name = TAG_ADMINISTRATION)
  @Path("/{site}")
  @APIResponse(responseCode = "200", description = "Successfully retrieved topology", headers = {})
  @APIResponse(responseCode = "400", description = "Topology id not valid", headers = {})
  @APIResponse(responseCode = "404", description = "Topology not found", headers = {})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {})
  @Produces(APPLICATION_JSON)
  @Operation(summary = "Get a replicator from topology", description = "Get a replicator full details from topology " +
      "based on its site")
  Uni<Topology> findBySite(@RestPath String site);

  @Path("/{site}")
  @Tag(name = TAG_ADMINISTRATION)
  @DELETE
  @APIResponse(responseCode = "204", description = "Successfully deleted topology", headers = {})
  @APIResponse(responseCode = "400", description = "Topology id not valid", headers = {})
  @APIResponse(responseCode = "404", description = "Topology not found", headers = {})
  @APIResponse(responseCode = "500", description = "Internal server error", headers = {})
  @Produces(APPLICATION_JSON)
  @Operation(summary = "Delete a replicator from topology", description = "Delete a replicator from topology")
  Uni<Response> delete(@RestPath String site);
}
