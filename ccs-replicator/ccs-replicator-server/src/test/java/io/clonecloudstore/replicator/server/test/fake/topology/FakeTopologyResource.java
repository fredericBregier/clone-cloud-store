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

package io.clonecloudstore.replicator.server.test.fake.topology;

import java.util.Collection;
import java.util.List;

import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.topology.client.api.TopologyApi;
import io.clonecloudstore.topology.model.Topology;
import io.clonecloudstore.topology.model.TopologyStatus;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;


@Path(ReplicatorConstants.Api.BASE + ReplicatorConstants.Api.TOPOLOGIES)
public class FakeTopologyResource implements TopologyApi {
  private static final Logger LOGGER = Logger.getLogger(FakeTopologyResource.class);
  public static Topology topology = null;

  @Override
  public Uni<Collection<Topology>> listAll(@QueryParam("status") @DefaultValue("UNKNOWN") final TopologyStatus status) {
    return Uni.createFrom().emitter(em -> {
      if (topology == null) {
        em.fail(new CcsNotExistException("not found"));
      }
      em.complete(List.of(topology, topology));
    });
  }

  @Override
  public Uni<Topology> findBySite(@RestPath final String site) {
    return Uni.createFrom().emitter(em -> {
      if (topology == null) {
        em.fail(new CcsNotExistException("not found"));
      }
      em.complete(topology);
    });
  }

  @Override
  public Uni<Topology> add(final Topology topology) {
    return Uni.createFrom().emitter(em -> {
      FakeTopologyResource.topology = topology;
      em.complete(topology);
    });
  }

  @Override
  public Uni<Topology> update(final Topology topology) {
    return Uni.createFrom().emitter(em -> {
      FakeTopologyResource.topology = topology;
      em.complete(topology);
    });
  }

  @Override
  public Uni<Response> delete(@RestPath final String site) {
    return Uni.createFrom().emitter(em -> {
      topology = null;
      em.complete(Response.noContent().build());
    });
  }

  @Override
  public void close() {

  }
}
