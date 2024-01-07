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

package io.clonecloudstore.topology.client.fake;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.topology.client.api.TopologyApi;
import io.clonecloudstore.topology.model.Topology;
import io.clonecloudstore.topology.model.TopologyStatus;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.BASE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.TOPOLOGIES;


@Path(BASE + TOPOLOGIES)
public class FakeTopologyResource implements TopologyApi {
  private final Map<String, Topology> repository = new HashMap<>();
  public static int errorCode = 0;

  @Override
  public Uni<Collection<Topology>> listAll(@QueryParam("status") @DefaultValue("UNKNOWN") final TopologyStatus status) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerGenericExceptionMapper.getCcsException(errorCode, null, null);
      }
      if (TopologyStatus.UNKNOWN.equals(status)) {
        em.complete(repository.values());
      } else {
        final var list = repository.values();
        if (list.isEmpty()) {
          em.complete(list);
        } else {
          em.complete(list.stream().filter(topology -> topology.status().equals(status)).toList());
        }
      }
    });
  }

  @Override
  public Uni<Topology> findBySite(@RestPath final String site) {
    return Uni.createFrom().emitter(em -> {
      final var topology = repository.get(site);
      if (topology != null) {
        em.complete(topology);
      } else {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      }
    });
  }

  @Override
  public Uni<Topology> add(final Topology topology) {
    return Uni.createFrom().emitter(em -> {
      if (repository.containsKey(topology.id())) {
        em.fail(new CcsAlreadyExistException(Response.Status.CONFLICT.getReasonPhrase()));
      } else {
        repository.put(topology.id(), topology);
        em.complete(topology);
      }
    });
  }

  @Override
  public Uni<Topology> update(final Topology topology) {
    return Uni.createFrom().emitter(em -> {
      if (!repository.containsKey(topology.id())) {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      } else {
        repository.put(topology.id(), topology);
        em.complete(topology);
      }
    });
  }

  @Override
  public Uni<Response> delete(@RestPath final String site) {
    return Uni.createFrom().emitter(em -> {
      if (!repository.containsKey(site)) {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      } else {
        repository.remove(site);
        em.complete(Response.noContent().build());
      }
    });
  }

  @Override
  public void close() {

  }
}
